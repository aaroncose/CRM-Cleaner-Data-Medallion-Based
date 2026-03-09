"""Security utilities for input validation and sanitization."""

import os
import re
from pathlib import Path

from crm_medallion.utils.errors import ConfigurationError, FrameworkError


def sanitize_path(path: Path | str, base_dir: Path | None = None) -> Path:
    """Sanitize file path to prevent directory traversal attacks.

    Args:
        path: Path to sanitize.
        base_dir: Optional base directory to restrict access to.

    Returns:
        Resolved absolute path.

    Raises:
        FrameworkError: If path attempts directory traversal outside base_dir.
    """
    if isinstance(path, str):
        path = Path(path)

    resolved = path.resolve()

    if base_dir is not None:
        base_resolved = base_dir.resolve()
        try:
            resolved.relative_to(base_resolved)
        except ValueError:
            raise FrameworkError(
                "Path traversal not allowed",
                context={"path": str(path), "base_dir": str(base_dir)},
            )

    return resolved


def validate_api_key(api_key: str, provider: str = "openai") -> bool:
    """Validate API key format without exposing the key.

    Args:
        api_key: The API key to validate.
        provider: API provider name for format validation.

    Returns:
        True if format is valid.

    Raises:
        ConfigurationError: If API key format is invalid.
    """
    if not api_key or not api_key.strip():
        raise ConfigurationError(
            f"API key for {provider} is empty",
            context={"provider": provider},
        )

    if api_key.startswith("${") or api_key.startswith("$"):
        raise ConfigurationError(
            f"API key contains unresolved environment variable",
            context={"provider": provider},
        )

    if provider == "openai":
        if not api_key.startswith("sk-"):
            raise ConfigurationError(
                "OpenAI API key should start with 'sk-'",
                context={"provider": provider},
            )
        if len(api_key) < 20:
            raise ConfigurationError(
                "OpenAI API key appears too short",
                context={"provider": provider},
            )

    return True


def sanitize_query(query: str, max_length: int = 2000) -> str:
    """Sanitize user query to prevent prompt injection.

    Args:
        query: User input query.
        max_length: Maximum allowed query length.

    Returns:
        Sanitized query string.

    Raises:
        FrameworkError: If query exceeds max length.
    """
    if len(query) > max_length:
        raise FrameworkError(
            f"Query exceeds maximum length of {max_length} characters",
            context={"length": len(query), "max_length": max_length},
        )

    injection_patterns = [
        r"ignore\s+(previous|above|all)\s+instructions?",
        r"disregard\s+(previous|above|all)",
        r"forget\s+(previous|above|all)",
        r"new\s+instructions?:",
        r"system\s*:",
        r"\[INST\]",
        r"<\|im_start\|>",
        r"<\|system\|>",
    ]

    query_lower = query.lower()
    for pattern in injection_patterns:
        if re.search(pattern, query_lower, re.IGNORECASE):
            return re.sub(pattern, "[FILTERED]", query, flags=re.IGNORECASE)

    return query.strip()


def mask_sensitive_value(value: str, visible_chars: int = 4) -> str:
    """Mask sensitive value for logging.

    Args:
        value: Sensitive value to mask.
        visible_chars: Number of characters to show at start.

    Returns:
        Masked string like "sk-a1***".
    """
    if not value or len(value) <= visible_chars:
        return "***"

    return value[:visible_chars] + "***"


def resolve_env_vars(value: str) -> str:
    """Resolve environment variables in a string.

    Args:
        value: String potentially containing ${VAR} or $VAR patterns.

    Returns:
        String with environment variables resolved.
    """
    pattern = r'\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)'

    def replacer(match):
        var_name = match.group(1) or match.group(2)
        return os.environ.get(var_name, match.group(0))

    return re.sub(pattern, replacer, value)
