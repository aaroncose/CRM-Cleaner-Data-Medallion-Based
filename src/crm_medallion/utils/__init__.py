"""Utility modules for logging, error handling, and helpers."""

from crm_medallion.utils.errors import (
    FrameworkError,
    ConfigurationError,
    DataValidationError,
    LLMError,
)
from crm_medallion.utils.logging import get_logger, redact_sensitive_data
from crm_medallion.utils.retry import retry_with_backoff, execute_with_retry
from crm_medallion.utils.hooks import (
    Hook,
    HookPhase,
    HookResult,
    HookContext,
    HookResponse,
    HookRegistry,
    HookExecutor,
    FunctionHook,
    get_global_registry,
    register_hook,
    create_hook,
)
from crm_medallion.utils.security import (
    sanitize_path,
    sanitize_query,
    validate_api_key,
    mask_sensitive_value,
    resolve_env_vars,
)

__all__ = [
    "FrameworkError",
    "ConfigurationError",
    "DataValidationError",
    "LLMError",
    "get_logger",
    "redact_sensitive_data",
    "retry_with_backoff",
    "execute_with_retry",
    "Hook",
    "HookPhase",
    "HookResult",
    "HookContext",
    "HookResponse",
    "HookRegistry",
    "HookExecutor",
    "FunctionHook",
    "get_global_registry",
    "register_hook",
    "create_hook",
    "sanitize_path",
    "sanitize_query",
    "validate_api_key",
    "mask_sensitive_value",
    "resolve_env_vars",
]
