"""Schema validator for the Silver Layer."""

from typing import Any, Type

from pydantic import BaseModel, ValidationError as PydanticValidationError

from crm_medallion.silver.models import (
    CleanedRecord,
    ValidatedRecord,
    ValidationError,
    ProcessingStatus,
)
from crm_medallion.utils.logging import get_logger

logger = get_logger(__name__)


class ValidationResult:
    """Result of validation operation."""

    def __init__(
        self,
        success: bool,
        errors: list[ValidationError] | None = None,
        warnings: list[str] | None = None,
        validated_data: dict[str, Any] | None = None,
    ):
        self.success = success
        self.errors = errors or []
        self.warnings = warnings or []
        self.validated_data = validated_data or {}

    def __bool__(self) -> bool:
        return self.success


class SchemaValidator:
    """Validates records against Pydantic schema."""

    def __init__(self, schema_model: Type[BaseModel]):
        """
        Initialize with Pydantic model.

        Args:
            schema_model: The Pydantic model to validate against
        """
        self.schema_model = schema_model

    def validate(self, record: CleanedRecord) -> ValidationResult:
        """
        Validate record against schema.

        Args:
            record: The cleaned record to validate

        Returns:
            ValidationResult with status and errors if any
        """
        try:
            validated = self.schema_model.model_validate(record.data)
            return ValidationResult(
                success=True,
                validated_data=validated.model_dump(),
            )
        except PydanticValidationError as e:
            errors = []
            for error in e.errors():
                field_path = ".".join(str(loc) for loc in error["loc"])
                errors.append(
                    ValidationError(
                        field=field_path,
                        message=error["msg"],
                        value=record.data.get(field_path),
                    )
                )

            return ValidationResult(
                success=False,
                errors=errors,
            )

    def validate_batch(self, records: list[CleanedRecord]) -> list[ValidationResult]:
        """
        Validate multiple records efficiently.

        Args:
            records: List of cleaned records to validate

        Returns:
            List of ValidationResult objects
        """
        return [self.validate(record) for record in records]

    def to_validated_record(
        self,
        record: CleanedRecord,
        result: ValidationResult,
    ) -> ValidatedRecord:
        """
        Convert a CleanedRecord and ValidationResult to a ValidatedRecord.

        Args:
            record: The cleaned record
            result: The validation result

        Returns:
            ValidatedRecord with appropriate status
        """
        if result.success:
            return ValidatedRecord(
                row_number=record.row_number,
                data=result.validated_data,
                status=ProcessingStatus.VALID,
                validation_errors=[],
                source_dataset_id=record.source_dataset_id,
            )
        else:
            return ValidatedRecord(
                row_number=record.row_number,
                data=record.data,
                status=ProcessingStatus.INVALID,
                validation_errors=result.errors,
                source_dataset_id=record.source_dataset_id,
            )
