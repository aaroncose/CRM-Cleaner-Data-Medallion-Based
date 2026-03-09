"""Silver Layer orchestrator."""

import csv
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Type

from pydantic import BaseModel

from crm_medallion.bronze.models import BronzeDataset
from crm_medallion.config.framework_config import LLMConfig, SilverConfig
from crm_medallion.silver.cleaner import DataCleaner
from crm_medallion.silver.models import (
    CleanedRecord,
    LLMCleaningResult,
    ProcessingStatus,
    SilverDataset,
    ValidatedRecord,
)
from crm_medallion.silver.parser import RecordParser
from crm_medallion.silver.rules import CleaningRule, get_default_cleaning_rules
from crm_medallion.silver.validator import SchemaValidator
from crm_medallion.utils.errors import FrameworkError
from crm_medallion.utils.hooks import HookExecutor, HookPhase, HookRegistry, HookResult
from crm_medallion.utils.logging import get_logger

logger = get_logger(__name__)


class SilverLayer:
    """Orchestrates validation and cleaning pipeline."""

    def __init__(
        self,
        schema_model: Type[BaseModel],
        config: SilverConfig | None = None,
        cleaning_rules: list[CleaningRule] | None = None,
        llm_config: LLMConfig | None = None,
        hook_registry: HookRegistry | None = None,
    ):
        """
        Initialize Silver layer with configuration.

        Args:
            schema_model: Pydantic model for validation
            config: Silver layer configuration
            cleaning_rules: List of cleaning rules (uses defaults if None)
            llm_config: Optional LLM configuration for enhanced cleaning
            hook_registry: Optional hook registry for extensibility
        """
        self.schema_model = schema_model
        self.config = config or SilverConfig()
        self.cleaning_rules = cleaning_rules if cleaning_rules is not None else get_default_cleaning_rules()
        self.llm_config = llm_config

        self._hook_registry = hook_registry
        self._hook_executor = HookExecutor(hook_registry) if hook_registry else None

        self.parser = RecordParser(chunk_size=self.config.batch_size)
        self.cleaner = DataCleaner(rules=self.cleaning_rules)
        self.validator = SchemaValidator(schema_model=schema_model)

        self._llm_cleaner = None
        if llm_config:
            from crm_medallion.silver.llm_cleaner import LLMCleaner
            self._llm_cleaner = LLMCleaner(config=llm_config)

        self._ensure_output_directory()

    def _ensure_output_directory(self) -> None:
        """Create output directory if it doesn't exist."""
        self.config.output_path.mkdir(parents=True, exist_ok=True)

    def _execute_hook(
        self,
        phase: HookPhase,
        data: BronzeDataset | SilverDataset,
        metadata: dict | None = None,
    ) -> tuple[HookResult, BronzeDataset | SilverDataset]:
        """Execute hooks if registry is configured."""
        if self._hook_executor is None:
            return HookResult.CONTINUE, data

        return self._hook_executor.execute_hooks(
            layer="silver",
            phase=phase,
            data=data,
            metadata=metadata or {},
        )

    def process(self, bronze_dataset: BronzeDataset) -> SilverDataset:
        """
        Process Bronze data through cleaning and validation.

        Pipeline:
        1. Parse records from Bronze
        2. Apply cleaning rules
        3. Validate against schema
        4. Optionally use LLM for failed records
        5. Write valid records to Clean CSV

        Args:
            bronze_dataset: The Bronze dataset to process

        Returns:
            SilverDataset with clean data and processing statistics
        """
        start_time = time.time()
        dataset_id = str(uuid.uuid4())

        pre_result, bronze_dataset = self._execute_hook(
            HookPhase.PRE,
            bronze_dataset,
            {"operation": "process"},
        )

        if pre_result == HookResult.SKIP:
            logger.info("Pre-hook requested skip, returning empty dataset")
            return SilverDataset(
                id=dataset_id,
                bronze_dataset_id=bronze_dataset.id,
                clean_csv_path=self.config.output_path / "skipped.csv",
                processing_timestamp=datetime.now(),
                total_records=0,
                valid_records=0,
                invalid_records=0,
                llm_corrected_records=0,
                manual_review_records=0,
                processing_time_seconds=0.0,
                validation_errors_log=[],
            )

        if pre_result == HookResult.ABORT:
            raise FrameworkError(
                "Silver layer processing aborted by hook",
                context={"phase": "pre"},
            )

        logger.info(f"Processing Bronze dataset: {bronze_dataset.id}")
        if self._llm_cleaner:
            logger.info("LLM enhancement enabled")

        valid_records: list[ValidatedRecord] = []
        invalid_records: list[ValidatedRecord] = []
        manual_review_records: list[ValidatedRecord] = []
        llm_corrected_count = 0
        validation_errors_log: list[dict] = []

        total_records = 0

        for raw_record in self.parser.parse(bronze_dataset):
            total_records += 1

            cleaned_record = self.cleaner.clean(raw_record)
            result = self.validator.validate(cleaned_record)

            if result.success:
                validated_record = self.validator.to_validated_record(cleaned_record, result)
                valid_records.append(validated_record)
                continue

            error_messages = [f"{e.field}: {e.message}" for e in result.errors]

            if self._llm_cleaner:
                llm_result = self._attempt_llm_cleaning(cleaned_record, error_messages)

                if llm_result and llm_result.success:
                    llm_cleaned = CleanedRecord(
                        row_number=cleaned_record.row_number,
                        data=llm_result.corrected_data,
                        cleaning_log=cleaned_record.cleaning_log + [
                            f"LLM: {c.field}: {c.original_value!r} -> {c.corrected_value!r}"
                            for c in llm_result.corrections
                        ],
                        source_dataset_id=cleaned_record.source_dataset_id,
                    )

                    revalidation_result = self.validator.validate(llm_cleaned)

                    if revalidation_result.success:
                        if self._llm_cleaner.should_flag_for_manual_review(llm_result):
                            validated_record = ValidatedRecord(
                                row_number=cleaned_record.row_number,
                                data=revalidation_result.validated_data,
                                status=ProcessingStatus.MANUAL_REVIEW,
                                validation_errors=[],
                                source_dataset_id=cleaned_record.source_dataset_id,
                            )
                            manual_review_records.append(validated_record)
                            self._log_llm_correction(llm_result, "flagged for manual review")
                        else:
                            validated_record = ValidatedRecord(
                                row_number=cleaned_record.row_number,
                                data=revalidation_result.validated_data,
                                status=ProcessingStatus.LLM_CORRECTED,
                                validation_errors=[],
                                source_dataset_id=cleaned_record.source_dataset_id,
                            )
                            valid_records.append(validated_record)
                            llm_corrected_count += 1
                            self._log_llm_correction(llm_result, "accepted")
                        continue

            validated_record = self.validator.to_validated_record(cleaned_record, result)
            invalid_records.append(validated_record)

            validation_errors_log.append({
                "row_number": raw_record.row_number,
                "errors": [
                    {"field": e.field, "message": e.message, "value": e.value}
                    for e in result.errors
                ],
                "cleaning_log": cleaned_record.cleaning_log,
            })

            for error in result.errors:
                logger.warning(
                    f"Row {raw_record.row_number}: Validation error in '{error.field}': {error.message}"
                )

        output_filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{dataset_id[:8]}_clean.csv"
        output_path = self.config.output_path / output_filename

        self._write_clean_csv(valid_records, output_path)

        processing_time = time.time() - start_time

        logger.info(
            f"Silver layer processing complete: "
            f"{len(valid_records)}/{total_records} valid records "
            f"(LLM corrected: {llm_corrected_count}, manual review: {len(manual_review_records)}) "
            f"({processing_time:.2f}s)"
        )

        silver_dataset = SilverDataset(
            id=dataset_id,
            bronze_dataset_id=bronze_dataset.id,
            clean_csv_path=output_path,
            processing_timestamp=datetime.now(),
            total_records=total_records,
            valid_records=len(valid_records),
            invalid_records=len(invalid_records),
            llm_corrected_records=llm_corrected_count,
            manual_review_records=len(manual_review_records),
            processing_time_seconds=processing_time,
            validation_errors_log=validation_errors_log,
        )

        post_result, silver_dataset = self._execute_hook(
            HookPhase.POST,
            silver_dataset,
            {"operation": "process"},
        )

        if post_result == HookResult.ABORT:
            raise FrameworkError(
                "Silver layer processing aborted by post-hook",
                context={"phase": "post"},
            )

        return silver_dataset

    def _attempt_llm_cleaning(
        self,
        record: CleanedRecord,
        validation_errors: list[str],
    ) -> LLMCleaningResult | None:
        """Attempt to clean a record using LLM."""
        try:
            return self._llm_cleaner.clean(record, validation_errors)
        except Exception as e:
            logger.error(f"LLM cleaning failed for row {record.row_number}: {e}")
            return None

    def _log_llm_correction(self, result: LLMCleaningResult, status: str) -> None:
        """Log LLM correction details."""
        logger.info(
            f"Row {result.original_record.row_number}: LLM correction {status} "
            f"(confidence: {result.confidence_score:.2f}, "
            f"corrections: {len(result.corrections)})"
        )

    def _write_clean_csv(
        self,
        records: list[ValidatedRecord],
        output_path: Path,
    ) -> None:
        """Write validated records to a clean CSV file."""
        if not records:
            output_path.touch()
            return

        fieldnames = list(self.schema_model.model_fields.keys())

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for record in records:
                row = {}
                for field in fieldnames:
                    value = record.data.get(field)
                    if isinstance(value, datetime):
                        row[field] = value.strftime("%Y-%m-%d")
                    elif hasattr(value, "value"):
                        row[field] = value.value
                    else:
                        row[field] = value
                writer.writerow(row)

    def register_cleaning_rule(self, rule: CleaningRule) -> None:
        """Register a custom cleaning rule."""
        self.cleaner.register_rule(rule)
