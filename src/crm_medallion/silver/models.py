"""Data models for the Silver Layer."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class ProcessingStatus(str, Enum):
    """Status of record processing."""

    PENDING = "pending"
    VALID = "valid"
    INVALID = "invalid"
    LLM_CORRECTED = "llm_corrected"
    MANUAL_REVIEW = "manual_review"


class TipoFactura(str, Enum):
    """Invoice type enumeration."""

    INGRESO = "Ingreso"
    GASTO = "Gasto"


class RawRecord(BaseModel):
    """Unparsed record from Bronze layer."""

    row_number: int
    data: dict[str, str]
    source_dataset_id: str


class CleanedRecord(BaseModel):
    """Record after cleaning rules applied."""

    row_number: int
    data: dict[str, Any]
    cleaning_log: list[str] = Field(default_factory=list)
    source_dataset_id: str


class ValidationError(BaseModel):
    """Detailed validation error."""

    field: str
    message: str
    value: Any = None


class ValidatedRecord(BaseModel):
    """Record after schema validation."""

    row_number: int
    data: dict[str, Any]
    status: ProcessingStatus
    validation_errors: list[ValidationError] = Field(default_factory=list)
    source_dataset_id: str


class FacturaVenta(BaseModel):
    """Sales invoice schema for CRM data."""

    num_factura: str = Field(..., min_length=1, description="Invoice number")
    fecha: datetime = Field(..., description="Invoice date")
    proveedor: str = Field(..., min_length=1, description="Supplier/provider name")
    nif_cif: str | None = Field(None, description="Tax ID number (NIF/CIF)")
    tipo: TipoFactura = Field(..., description="Transaction type (Ingreso/Gasto)")
    categoria: str = Field(..., min_length=1, description="Category")
    importe_base: float = Field(..., ge=0, description="Base amount before tax")
    iva: float = Field(..., ge=0, description="VAT/tax amount")
    importe_total: float = Field(..., ge=0, description="Total amount including tax")
    estado_factura: str = Field(..., min_length=1, description="Invoice status")
    importe_pendiente: float | None = Field(None, ge=0, description="Pending amount")


class FieldCorrection(BaseModel):
    """A single field correction made by LLM."""

    field: str
    original_value: Any
    corrected_value: Any
    reasoning: str


class LLMCleaningResult(BaseModel):
    """Result of LLM cleaning attempt."""

    original_record: CleanedRecord
    corrected_data: dict[str, Any]
    confidence_score: float = Field(ge=0.0, le=1.0)
    corrections: list[FieldCorrection] = Field(default_factory=list)
    llm_reasoning: str = ""
    success: bool = True
    error_message: str | None = None


@dataclass
class SilverDataset:
    """Output of Silver layer processing."""

    id: str
    bronze_dataset_id: str
    clean_csv_path: Path
    processing_timestamp: datetime
    total_records: int
    valid_records: int
    invalid_records: int
    llm_corrected_records: int = 0
    manual_review_records: int = 0
    processing_time_seconds: float = 0.0
    validation_errors_log: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        if isinstance(self.clean_csv_path, str):
            self.clean_csv_path = Path(self.clean_csv_path)
