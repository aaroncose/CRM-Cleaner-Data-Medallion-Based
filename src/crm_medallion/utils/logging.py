"""Logging configuration with sensitive data redaction."""

import logging
import re
from typing import Any


SENSITIVE_PATTERNS = [
    (re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"), "[EMAIL]"),
    (re.compile(r"\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b"), "[PHONE]"),
    (re.compile(r"\b\d{8}[A-Za-z]\b"), "[NIF]"),
    (re.compile(r"\b[A-Za-z]\d{8}\b"), "[CIF]"),
    (re.compile(r"sk-[a-zA-Z0-9]{32,}"), "[API_KEY]"),
    (re.compile(r"(?i)api[_-]?key[\"']?\s*[:=]\s*[\"']?[\w-]+"), "[API_KEY]"),
    (re.compile(r"(?i)password[\"']?\s*[:=]\s*[\"']?[\w-]+"), "[PASSWORD]"),
]


def redact_sensitive_data(message: str) -> str:
    """Redact sensitive information from log messages."""
    result = message
    for pattern, replacement in SENSITIVE_PATTERNS:
        result = pattern.sub(replacement, result)
    return result


class RedactingFormatter(logging.Formatter):
    """Formatter that redacts sensitive data from log messages."""

    def format(self, record: logging.LogRecord) -> str:
        original_msg = record.getMessage()
        record.msg = redact_sensitive_data(original_msg)
        record.args = ()
        return super().format(record)


class RedactingFilter(logging.Filter):
    """Filter that redacts sensitive data from log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = redact_sensitive_data(record.msg)
        if record.args:
            record.args = tuple(
                redact_sensitive_data(str(arg)) if isinstance(arg, str) else arg
                for arg in record.args
            )
        return True


def get_logger(
    name: str,
    level: str = "INFO",
    log_file: str | None = None,
) -> logging.Logger:
    """
    Get a configured logger with sensitive data redaction.

    Args:
        name: Logger name (typically __name__)
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional file path for logging output

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.addFilter(RedactingFilter())

    formatter = RedactingFormatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def configure_root_logger(level: str = "INFO", log_file: str | None = None) -> None:
    """Configure the root logger for the framework."""
    root_logger = logging.getLogger("crm_medallion")
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    root_logger.addFilter(RedactingFilter())

    if not root_logger.handlers:
        formatter = RedactingFormatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
