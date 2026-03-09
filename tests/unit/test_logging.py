"""Tests for logging configuration and sensitive data redaction."""

import logging

import pytest

from crm_medallion.utils.logging import (
    redact_sensitive_data,
    get_logger,
    RedactingFilter,
)


class TestRedactSensitiveData:
    def test_redacts_email(self):
        message = "User email is user@example.com"
        result = redact_sensitive_data(message)
        assert "[EMAIL]" in result
        assert "user@example.com" not in result

    def test_redacts_phone_with_dashes(self):
        message = "Phone: 123-456-7890"
        result = redact_sensitive_data(message)
        assert "[PHONE]" in result
        assert "123-456-7890" not in result

    def test_redacts_phone_with_dots(self):
        message = "Phone: 123.456.7890"
        result = redact_sensitive_data(message)
        assert "[PHONE]" in result

    def test_redacts_phone_with_spaces(self):
        message = "Phone: 123 456 7890"
        result = redact_sensitive_data(message)
        assert "[PHONE]" in result

    def test_redacts_nif(self):
        message = "NIF: 12345678A"
        result = redact_sensitive_data(message)
        assert "[NIF]" in result
        assert "12345678A" not in result

    def test_redacts_cif(self):
        message = "CIF: A12345678"
        result = redact_sensitive_data(message)
        assert "[CIF]" in result
        assert "A12345678" not in result

    def test_redacts_openai_api_key(self):
        message = "API key: sk-abcdefghijklmnopqrstuvwxyz123456"
        result = redact_sensitive_data(message)
        assert "[API_KEY]" in result
        assert "sk-" not in result

    def test_redacts_api_key_in_config(self):
        message = "api_key: my-secret-key"
        result = redact_sensitive_data(message)
        assert "[API_KEY]" in result

    def test_redacts_password(self):
        message = "password: mysecretpass"
        result = redact_sensitive_data(message)
        assert "[PASSWORD]" in result

    def test_preserves_non_sensitive_data(self):
        message = "Processing 100 records"
        result = redact_sensitive_data(message)
        assert result == message

    def test_handles_multiple_sensitive_items(self):
        message = "Email user@test.com, phone 123-456-7890"
        result = redact_sensitive_data(message)
        assert "[EMAIL]" in result
        assert "[PHONE]" in result
        assert "user@test.com" not in result
        assert "123-456-7890" not in result


class TestGetLogger:
    def test_returns_logger(self):
        logger = get_logger("test_module")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_module"

    def test_logger_has_redacting_filter(self):
        logger = get_logger("test_filter")
        filters = [f for f in logger.filters if isinstance(f, RedactingFilter)]
        assert len(filters) == 1

    def test_logger_level_is_configurable(self):
        logger = get_logger("test_level", level="DEBUG")
        assert logger.level == logging.DEBUG

    def test_returns_same_logger_on_repeated_calls(self):
        logger1 = get_logger("test_same")
        logger2 = get_logger("test_same")
        assert logger1 is logger2
