"""Silver Layer: Validated and cleaned data."""

from crm_medallion.silver.models import (
    ProcessingStatus,
    TipoFactura,
    RawRecord,
    CleanedRecord,
    ValidatedRecord,
    ValidationError,
    FacturaVenta,
    SilverDataset,
    FieldCorrection,
    LLMCleaningResult,
)
from crm_medallion.silver.parser import RecordParser
from crm_medallion.silver.cleaner import DataCleaner
from crm_medallion.silver.validator import SchemaValidator, ValidationResult
from crm_medallion.silver.layer import SilverLayer
from crm_medallion.silver.llm_cleaner import LLMCleaner
from crm_medallion.silver.deduplicator import EntityDeduplicator, DeduplicationResult, EntityGroup
from crm_medallion.silver.rules import (
    CleaningRule,
    WhitespaceStripper,
    CurrencyNormalizer,
    DateNormalizer,
    CaseNormalizer,
    InvoiceNumberNormalizer,
    NifCifNormalizer,
    TipoNormalizer,
    EstadoFacturaNormalizer,
    ConsistencyChecker,
    get_default_cleaning_rules,
)

__all__ = [
    "ProcessingStatus",
    "TipoFactura",
    "RawRecord",
    "CleanedRecord",
    "ValidatedRecord",
    "ValidationError",
    "FacturaVenta",
    "SilverDataset",
    "FieldCorrection",
    "LLMCleaningResult",
    "RecordParser",
    "DataCleaner",
    "SchemaValidator",
    "ValidationResult",
    "SilverLayer",
    "LLMCleaner",
    "EntityDeduplicator",
    "DeduplicationResult",
    "EntityGroup",
    "CleaningRule",
    "WhitespaceStripper",
    "CurrencyNormalizer",
    "DateNormalizer",
    "CaseNormalizer",
    "InvoiceNumberNormalizer",
    "NifCifNormalizer",
    "TipoNormalizer",
    "EstadoFacturaNormalizer",
    "ConsistencyChecker",
    "get_default_cleaning_rules",
]
