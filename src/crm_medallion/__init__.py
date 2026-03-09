"""
CRM Data Medallion Framework.

Multi-layered data processing pipeline for CRM data cleaning using the Medallion architecture.
"""

from crm_medallion.config.framework_config import FrameworkConfig
from crm_medallion.config.schema import SchemaDefinition
from crm_medallion.framework import Framework, PipelineResult
from crm_medallion.utils.errors import (
    FrameworkError,
    ConfigurationError,
    DataValidationError,
    LLMError,
)

__version__ = "0.1.0"

__all__ = [
    "Framework",
    "PipelineResult",
    "FrameworkConfig",
    "SchemaDefinition",
    "FrameworkError",
    "ConfigurationError",
    "DataValidationError",
    "LLMError",
]
