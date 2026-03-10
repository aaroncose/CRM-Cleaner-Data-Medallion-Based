"""CSV ingestion for the Bronze Layer."""

import csv
import re
import shutil
import uuid
from datetime import datetime
from pathlib import Path

import chardet

from crm_medallion.bronze.models import BronzeDataset, BronzeValidationResult
from crm_medallion.config.framework_config import BronzeConfig
from crm_medallion.config.schema import FieldDefinition, FieldType, SchemaDefinition
from crm_medallion.utils.errors import ConfigurationError, FrameworkError
from crm_medallion.utils.hooks import HookExecutor, HookPhase, HookRegistry, HookResult
from crm_medallion.utils.logging import get_logger

logger = get_logger(__name__)


class CSVIngester:
    """Ingests raw CSV files into Bronze layer without transformation."""

    def __init__(
        self,
        config: BronzeConfig,
        hook_registry: HookRegistry | None = None,
    ):
        self.config = config
        self._hook_registry = hook_registry
        self._hook_executor = HookExecutor(hook_registry) if hook_registry else None
        self._ensure_storage_directory()

    def _ensure_storage_directory(self) -> None:
        """Create storage directory if it doesn't exist."""
        self.config.storage_path.mkdir(parents=True, exist_ok=True)

    def detect_encoding(self, file_path: Path) -> str:
        """
        Detect file encoding using chardet library.

        Args:
            file_path: Path to the file

        Returns:
            Detected encoding string (e.g., 'utf-8', 'latin-1')
        """
        with open(file_path, "rb") as f:
            raw_data = f.read()

        result = chardet.detect(raw_data)
        encoding = result.get("encoding", "utf-8")

        if encoding is None:
            encoding = "utf-8"

        encoding = encoding.lower()
        encoding_map = {
            "iso-8859-1": "latin-1",
            "ascii": "utf-8",
        }
        encoding = encoding_map.get(encoding, encoding)

        logger.debug(
            f"Detected encoding for {file_path.name}: {encoding} "
            f"(confidence: {result.get('confidence', 0):.2%})"
        )

        return encoding

    def validate_csv_structure(self, file_path: Path) -> BronzeValidationResult:
        """
        Check for basic CSV structure issues.

        Args:
            file_path: Path to the CSV file

        Returns:
            BronzeValidationResult with validation details
        """
        encoding = self.detect_encoding(file_path) if self.config.encoding_detection else "utf-8"

        warnings: list[str] = []
        errors: list[str] = []
        column_names: list[str] = []
        row_count = 0
        expected_column_count = 0

        try:
            with open(file_path, "r", encoding=encoding, newline="") as f:
                reader = csv.reader(f)

                try:
                    header = next(reader)
                    column_names = [col.strip() for col in header]
                    expected_column_count = len(column_names)

                    if not column_names or all(not col for col in column_names):
                        errors.append("CSV header is empty or contains only empty values")

                    if len(column_names) != len(set(column_names)):
                        duplicates = [
                            col for col in column_names
                            if column_names.count(col) > 1
                        ]
                        warnings.append(f"Duplicate column names found: {set(duplicates)}")

                except StopIteration:
                    errors.append("CSV file is empty (no header row)")
                    return BronzeValidationResult(
                        is_valid=False,
                        row_count=0,
                        column_count=0,
                        column_names=[],
                        warnings=warnings,
                        errors=errors,
                    )

                for row_num, row in enumerate(reader, start=2):
                    row_count += 1

                    if len(row) != expected_column_count:
                        warnings.append(
                            f"Row {row_num}: expected {expected_column_count} columns, "
                            f"found {len(row)}"
                        )

        except UnicodeDecodeError as e:
            errors.append(f"Encoding error: {e}")
        except csv.Error as e:
            errors.append(f"CSV parsing error: {e}")

        is_valid = len(errors) == 0

        return BronzeValidationResult(
            is_valid=is_valid,
            row_count=row_count,
            column_count=expected_column_count,
            column_names=column_names,
            warnings=warnings,
            errors=errors,
        )

    def _execute_hook(
        self,
        phase: HookPhase,
        data: Path | BronzeDataset,
        metadata: dict | None = None,
    ) -> tuple[HookResult, Path | BronzeDataset]:
        """Execute hooks if registry is configured."""
        if self._hook_executor is None:
            return HookResult.CONTINUE, data

        return self._hook_executor.execute_hooks(
            layer="bronze",
            phase=phase,
            data=data,
            metadata=metadata or {},
        )

    def ingest(self, file_path: Path) -> BronzeDataset:
        """
        Read CSV file and store in Bronze layer.

        Args:
            file_path: Path to the dirty CSV file

        Returns:
            BronzeDataset with metadata and raw data reference

        Raises:
            FileNotFoundError: If CSV file doesn't exist
            FrameworkError: If file cannot be read or has critical issues
        """
        if isinstance(file_path, str):
            file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(
                f"CSV file not found: {file_path}"
            )

        if not file_path.is_file():
            raise FrameworkError(
                f"Path is not a file: {file_path}",
                context={"path": str(file_path)},
            )

        pre_result, file_path = self._execute_hook(
            HookPhase.PRE,
            file_path,
            {"operation": "ingest"},
        )

        if pre_result == HookResult.SKIP:
            logger.info("Pre-hook requested skip, returning empty dataset")
            return BronzeDataset(
                id=str(uuid.uuid4()),
                source_file=file_path,
                ingestion_timestamp=datetime.now(),
                encoding="utf-8",
                row_count=0,
                column_names=[],
                storage_path=self.config.storage_path / "skipped",
                metadata={"skipped": True},
            )

        if pre_result == HookResult.ABORT:
            raise FrameworkError(
                "Bronze layer processing aborted by hook",
                context={"phase": "pre"},
            )

        logger.info(f"Ingesting CSV file: {file_path.name}")

        encoding = self.detect_encoding(file_path) if self.config.encoding_detection else "utf-8"

        validation_result = self.validate_csv_structure(file_path)

        if not validation_result.is_valid:
            error_msg = "; ".join(validation_result.errors)
            raise FrameworkError(
                f"CSV validation failed: {error_msg}",
                context={"path": str(file_path), "errors": validation_result.errors},
            )

        for warning in validation_result.warnings:
            logger.warning(f"CSV structure warning: {warning}")

        dataset_id = str(uuid.uuid4())
        timestamp = datetime.now()

        storage_filename = f"{timestamp.strftime('%Y%m%d_%H%M%S')}_{dataset_id[:8]}_{file_path.name}"
        storage_path = self.config.storage_path / storage_filename

        shutil.copy2(file_path, storage_path)

        logger.info(
            f"Ingested {validation_result.row_count} rows from {file_path.name} "
            f"into Bronze layer"
        )

        metadata = {
            "original_filename": file_path.name,
            "file_size_bytes": file_path.stat().st_size,
            "validation_warnings": validation_result.warnings,
        }

        dataset = BronzeDataset(
            id=dataset_id,
            source_file=file_path,
            ingestion_timestamp=timestamp,
            encoding=encoding,
            row_count=validation_result.row_count,
            column_names=validation_result.column_names,
            storage_path=storage_path,
            metadata=metadata,
        )

        post_result, dataset = self._execute_hook(
            HookPhase.POST,
            dataset,
            {"operation": "ingest"},
        )

        if post_result == HookResult.ABORT:
            raise FrameworkError(
                "Bronze layer processing aborted by post-hook",
                context={"phase": "post"},
            )

        return dataset

    def detect_schema(
        self,
        file_path: Path,
        sample_rows: int = 20,
        schema_name: str | None = None,
    ) -> SchemaDefinition:
        """
        Auto-detect schema from CSV headers and sample data.

        Args:
            file_path: Path to the CSV file
            sample_rows: Number of rows to sample for type inference (default: 20)
            schema_name: Optional name for the schema

        Returns:
            SchemaDefinition with inferred field types

        Raises:
            FrameworkError: If CSV has no header or cannot be read
        """
        if isinstance(file_path, str):
            file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        encoding = self.detect_encoding(file_path) if self.config.encoding_detection else "utf-8"

        logger.info(f"Detecting schema from: {file_path.name}")

        with open(file_path, "r", encoding=encoding, newline="") as f:
            reader = csv.reader(f)

            try:
                header = next(reader)
            except StopIteration:
                raise FrameworkError(
                    "CSV file is empty or has no header",
                    context={"path": str(file_path)},
                )

            column_names = [col.strip() for col in header]
            if not column_names or all(not col for col in column_names):
                raise FrameworkError(
                    "CSV header is empty or contains only empty values",
                    context={"path": str(file_path)},
                )

            # Collect sample values for each column
            column_values: dict[str, list[str]] = {col: [] for col in column_names}

            for i, row in enumerate(reader):
                if i >= sample_rows:
                    break
                for j, value in enumerate(row):
                    if j < len(column_names):
                        column_values[column_names[j]].append(value.strip())

        # Infer types for each column
        fields = []
        for col_name in column_names:
            values = column_values[col_name]
            field_type = self._infer_field_type(values)

            fields.append(FieldDefinition(
                name=col_name,
                field_type=field_type,
                required=True,
                description=f"Auto-detected as {field_type.value}",
            ))

        schema = SchemaDefinition(
            name=schema_name or file_path.stem.title().replace("_", "") + "Schema",
            fields=fields,
            description=f"Auto-generated schema from {file_path.name}",
        )

        logger.info(f"Detected schema with {len(fields)} fields")
        return schema

    def _infer_field_type(self, values: list[str]) -> FieldType:
        """
        Infer the field type from sample values.

        Args:
            values: List of sample values for the column

        Returns:
            Inferred FieldType
        """
        non_empty_values = [v for v in values if v]

        if not non_empty_values:
            return FieldType.STRING

        # Check for integer
        int_pattern = re.compile(r"^-?\d+$")
        if all(int_pattern.match(v) for v in non_empty_values):
            return FieldType.INTEGER

        # Check for float (handles European format with comma)
        float_pattern = re.compile(r"^-?\d+[.,]?\d*$|^-?\d*[.,]\d+$")
        currency_pattern = re.compile(r"^-?[\d.,]+\s*(€|EUR|USD|\$)?$", re.IGNORECASE)
        if all(float_pattern.match(v) or currency_pattern.match(v) for v in non_empty_values):
            return FieldType.FLOAT

        # Check for date/datetime patterns
        date_patterns = [
            r"^\d{4}-\d{2}-\d{2}$",  # ISO format: 2024-01-15
            r"^\d{2}/\d{2}/\d{4}$",  # DD/MM/YYYY
            r"^\d{2}-\d{2}-\d{4}$",  # DD-MM-YYYY
            r"^\d{1,2}\s+de\s+\w+\s+de\s+\d{4}$",  # Spanish: 15 de enero de 2024
            r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}",  # ISO datetime
        ]
        for pattern in date_patterns:
            if all(re.match(pattern, v, re.IGNORECASE) for v in non_empty_values):
                return FieldType.DATE

        # Check for boolean
        bool_values = {"true", "false", "yes", "no", "si", "sí", "1", "0", "verdadero", "falso"}
        if all(v.lower() in bool_values for v in non_empty_values):
            return FieldType.BOOLEAN

        # Default to string
        return FieldType.STRING
