"""Custom exception classes for the framework."""


class FrameworkError(Exception):
    """Base exception for all framework errors."""

    def __init__(self, message: str, context: dict | None = None):
        super().__init__(message)
        self.message = message
        self.context = context or {}

    def __str__(self) -> str:
        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            return f"{self.message} [{context_str}]"
        return self.message


class ConfigurationError(FrameworkError):
    """Raised when configuration is invalid."""

    pass


class DataValidationError(FrameworkError):
    """Raised when data validation fails critically."""

    def __init__(
        self,
        message: str,
        field_name: str | None = None,
        row_number: int | None = None,
        context: dict | None = None,
    ):
        ctx = context or {}
        if field_name:
            ctx["field"] = field_name
        if row_number is not None:
            ctx["row"] = row_number
        super().__init__(message, ctx)
        self.field_name = field_name
        self.row_number = row_number


class LLMError(FrameworkError):
    """Raised when LLM processing fails."""

    def __init__(
        self,
        message: str,
        retry_count: int = 0,
        context: dict | None = None,
    ):
        ctx = context or {}
        if retry_count > 0:
            ctx["retries"] = retry_count
        super().__init__(message, ctx)
        self.retry_count = retry_count
