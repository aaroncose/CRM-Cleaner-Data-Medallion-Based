"""Tests for custom exception classes."""

import pytest

from crm_medallion.utils.errors import (
    FrameworkError,
    ConfigurationError,
    DataValidationError,
    LLMError,
)


class TestFrameworkError:
    def test_basic_message(self):
        error = FrameworkError("Test error")
        assert str(error) == "Test error"
        assert error.message == "Test error"
        assert error.context == {}

    def test_with_context(self):
        error = FrameworkError("Test error", context={"key": "value"})
        assert "key=value" in str(error)
        assert error.context == {"key": "value"}


class TestConfigurationError:
    def test_inherits_from_framework_error(self):
        error = ConfigurationError("Config error")
        assert isinstance(error, FrameworkError)

    def test_with_context(self):
        error = ConfigurationError(
            "Invalid config",
            context={"field": "api_key"},
        )
        assert "field=api_key" in str(error)


class TestDataValidationError:
    def test_with_field_name(self):
        error = DataValidationError(
            "Validation failed",
            field_name="email",
        )
        assert error.field_name == "email"
        assert "field=email" in str(error)

    def test_with_row_number(self):
        error = DataValidationError(
            "Validation failed",
            field_name="email",
            row_number=42,
        )
        assert error.row_number == 42
        assert "row=42" in str(error)

    def test_without_optional_fields(self):
        error = DataValidationError("Validation failed")
        assert error.field_name is None
        assert error.row_number is None


class TestLLMError:
    def test_with_retry_count(self):
        error = LLMError("API failed", retry_count=3)
        assert error.retry_count == 3
        assert "retries=3" in str(error)

    def test_without_retry_count(self):
        error = LLMError("API failed")
        assert error.retry_count == 0
        assert "retries" not in str(error)
