"""LLM-based data cleaning for the Silver Layer."""

import json
from typing import Any

from pydantic import BaseModel, Field

from crm_medallion.config.framework_config import LLMConfig
from crm_medallion.silver.models import (
    CleanedRecord,
    FieldCorrection,
    LLMCleaningResult,
)
from crm_medallion.utils.errors import LLMError
from crm_medallion.utils.logging import get_logger
from crm_medallion.utils.retry import execute_with_retry

logger = get_logger(__name__)


class CorrectionItem(BaseModel):
    """A single field correction."""

    field: str = Field(default="", description="Name of the corrected field")
    original: str = Field(default="", description="Original value")
    corrected: str = Field(default="", description="Corrected value")
    reasoning: str = Field(default="", description="Why this correction was made")


class LLMCorrectionResponse(BaseModel):
    """Structured response from LLM for data correction."""

    corrected_fields: dict[str, Any] = Field(
        default_factory=dict,
        description="Dictionary mapping field names to their corrected values",
    )
    corrections: list[CorrectionItem] = Field(
        default_factory=list,
        description="List of corrections made",
    )
    confidence: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="Confidence score between 0 and 1",
    )
    reasoning: str = Field(
        default="",
        description="Overall explanation of the corrections made",
    )


CLEANING_PROMPT_TEMPLATE = """You are a data cleaning assistant for Spanish CRM invoice data.

You will receive a record that failed validation, along with the specific validation errors.
Your task is to correct the data so it passes validation.

## Field Descriptions:
- num_factura: Invoice number (format: FAC-YYYY-NNNN)
- fecha: Date (ISO format: YYYY-MM-DD)
- proveedor: Supplier/provider name (proper capitalization)
- nif_cif: Spanish tax ID (format: letter + 8 digits or 8 digits + letter)
- tipo: Transaction type (must be exactly "Ingreso" or "Gasto")
- categoria: Category (proper capitalization)
- importe_base: Base amount (numeric, >= 0)
- iva: VAT amount (numeric, >= 0)
- importe_total: Total amount (numeric, >= 0)
- estado_factura: Invoice status (Pagada, Pendiente, Vencida, Parcialmente pagada)
- importe_pendiente: Pending amount (numeric, >= 0, optional)

## Special Handling:
1. Amounts written as text: Convert "mil doscientos" to 1200.00
2. Dates in wrong format: Convert to YYYY-MM-DD
3. tipo must be exactly "Ingreso" or "Gasto" (case sensitive)
4. estado_factura must be exactly one of: Pagada, Pendiente, Vencida, Parcialmente pagada

## Record Data:
{record_data}

## Validation Errors:
{validation_errors}

Respond with a JSON object with this exact structure:
{{
  "corrected_fields": {{"field_name": "corrected_value", ...}},
  "corrections": [
    {{"field": "field_name", "original": "old_value", "corrected": "new_value", "reasoning": "why"}}
  ],
  "confidence": 0.9,
  "reasoning": "Overall explanation"
}}

Only include fields that need correction in corrected_fields. Be conservative.
Respond ONLY with valid JSON, no other text."""


class LLMCleaner:
    """Uses LLM (via LangChain) to clean data that fails basic validation."""

    def __init__(self, config: LLMConfig):
        """
        Initialize with LLM configuration.

        Args:
            config: LLM configuration with API key, model, etc.
        """
        self.config = config
        self._llm = None
        self._initialized = False

    def _ensure_initialized(self) -> None:
        """Lazy initialization of LLM to avoid import errors when not using LLM features."""
        if self._initialized:
            return

        try:
            from langchain_openai import ChatOpenAI

            self._llm = ChatOpenAI(
                model=self.config.model_name,
                temperature=self.config.temperature,
                api_key=self.config.api_key,
            )
            self._initialized = True
        except ImportError as e:
            raise LLMError(
                "LangChain OpenAI package not installed. "
                "Install with: pip install 'crm-medallion[llm]'",
                context={"error": str(e)},
            ) from None

    def _format_record_data(self, record: CleanedRecord) -> str:
        """Format record data for the prompt."""
        lines = []
        for field, value in record.data.items():
            lines.append(f"- {field}: {repr(value)}")
        return "\n".join(lines)

    def _format_validation_errors(self, errors: list[str]) -> str:
        """Format validation errors for the prompt."""
        return "\n".join(f"- {error}" for error in errors)

    def _parse_response(self, content: str) -> LLMCorrectionResponse:
        """Parse LLM response, extracting JSON from the content."""
        content = content.strip()

        if content.startswith("```json"):
            content = content[7:]
        if content.startswith("```"):
            content = content[3:]
        if content.endswith("```"):
            content = content[:-3]
        content = content.strip()

        start = content.find("{")
        end = content.rfind("}") + 1

        if start < 0 or end <= start:
            raise LLMError(
                "No JSON object found in LLM response",
                context={"response": content[:200]},
            )

        json_str = content[start:end]

        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            raise LLMError(
                f"Invalid JSON in LLM response: {e}",
                context={"json_str": json_str[:200]},
            )

        corrections_raw = data.get("corrections", [])
        corrections = []
        for c in corrections_raw:
            if isinstance(c, dict):
                corrections.append(CorrectionItem(
                    field=str(c.get("field", "")),
                    original=str(c.get("original", "")),
                    corrected=str(c.get("corrected", "")),
                    reasoning=str(c.get("reasoning", "")),
                ))

        return LLMCorrectionResponse(
            corrected_fields=data.get("corrected_fields", {}),
            corrections=corrections,
            confidence=float(data.get("confidence", 0.5)),
            reasoning=str(data.get("reasoning", "")),
        )

    def _call_llm(self, prompt: str) -> LLMCorrectionResponse:
        """Call the LLM with retry logic."""
        self._ensure_initialized()

        def _invoke() -> LLMCorrectionResponse:
            response = self._llm.invoke(prompt)
            content = response.content if hasattr(response, "content") else str(response)
            return self._parse_response(content)

        return execute_with_retry(
            _invoke,
            max_retries=self.config.max_retries,
            initial_delay=self.config.initial_retry_delay,
            backoff_multiplier=self.config.backoff_multiplier,
        )

    def clean(
        self,
        record: CleanedRecord,
        validation_errors: list[str],
    ) -> LLMCleaningResult:
        """
        Attempt to clean record using LLM.

        Args:
            record: The record that failed validation
            validation_errors: List of validation error messages

        Returns:
            LLMCleaningResult with corrected data and confidence score
        """
        logger.debug(f"Attempting LLM cleaning for row {record.row_number}")

        prompt = CLEANING_PROMPT_TEMPLATE.format(
            record_data=self._format_record_data(record),
            validation_errors=self._format_validation_errors(validation_errors),
        )

        try:
            response = self._call_llm(prompt)

            corrected_data = record.data.copy()
            corrected_data.update(response.corrected_fields)

            corrections = [
                FieldCorrection(
                    field=c.field,
                    original_value=c.original,
                    corrected_value=c.corrected,
                    reasoning=c.reasoning,
                )
                for c in response.corrections
            ]

            logger.info(
                f"Row {record.row_number}: LLM made {len(corrections)} corrections "
                f"with confidence {response.confidence:.2f}"
            )

            for correction in corrections:
                logger.debug(
                    f"  {correction.field}: {correction.original_value!r} -> "
                    f"{correction.corrected_value!r} ({correction.reasoning})"
                )

            return LLMCleaningResult(
                original_record=record,
                corrected_data=corrected_data,
                confidence_score=response.confidence,
                corrections=corrections,
                llm_reasoning=response.reasoning,
                success=True,
            )

        except LLMError as e:
            logger.error(f"LLM cleaning failed for row {record.row_number}: {e}")
            return LLMCleaningResult(
                original_record=record,
                corrected_data=record.data,
                confidence_score=0.0,
                corrections=[],
                llm_reasoning="",
                success=False,
                error_message=str(e),
            )

    def batch_clean(
        self,
        records: list[tuple[CleanedRecord, list[str]]],
    ) -> list[LLMCleaningResult]:
        """
        Clean multiple records.

        Args:
            records: List of (record, validation_errors) tuples

        Returns:
            List of LLMCleaningResult objects
        """
        results = []
        for record, errors in records:
            result = self.clean(record, errors)
            results.append(result)
        return results

    def should_flag_for_manual_review(self, result: LLMCleaningResult) -> bool:
        """Check if a result should be flagged for manual review."""
        if not result.success:
            return True
        return result.confidence_score < self.config.confidence_threshold
