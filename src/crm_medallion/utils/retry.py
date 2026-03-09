"""Retry logic with exponential backoff."""

import time
from functools import wraps
from typing import Any, Callable, TypeVar

from crm_medallion.utils.errors import LLMError
from crm_medallion.utils.logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


def retry_with_backoff(
    max_retries: int = 5,
    initial_delay: float = 1.0,
    backoff_multiplier: float = 2.0,
    max_delay: float = 60.0,
    exceptions: tuple = (LLMError, Exception),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator for retrying a function with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds before first retry
        backoff_multiplier: Multiplier for delay after each retry
        max_delay: Maximum delay between retries
        exceptions: Tuple of exception types to catch and retry

    Returns:
        Decorated function
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            delay = initial_delay
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    if attempt == max_retries:
                        logger.error(
                            f"Function {func.__name__} failed after {max_retries + 1} attempts: {e}"
                        )
                        raise LLMError(
                            f"Max retries ({max_retries}) exceeded: {e}",
                            retry_count=attempt + 1,
                        )

                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries + 1} failed: {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )

                    time.sleep(delay)
                    delay = min(delay * backoff_multiplier, max_delay)

            raise last_exception or LLMError("Unknown error in retry logic")

        return wrapper

    return decorator


def execute_with_retry(
    func: Callable[..., T],
    *args: Any,
    max_retries: int = 5,
    initial_delay: float = 1.0,
    backoff_multiplier: float = 2.0,
    max_delay: float = 60.0,
    **kwargs: Any,
) -> T:
    """
    Execute a function with retry logic.

    Args:
        func: Function to execute
        *args: Positional arguments for the function
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds
        backoff_multiplier: Multiplier for delay
        max_delay: Maximum delay between retries
        **kwargs: Keyword arguments for the function

    Returns:
        Result of the function

    Raises:
        LLMError: If all retries fail
    """
    delay = initial_delay

    for attempt in range(max_retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries:
                raise LLMError(
                    f"Max retries ({max_retries}) exceeded: {e}",
                    retry_count=attempt + 1,
                )

            logger.warning(
                f"Attempt {attempt + 1}/{max_retries + 1} failed: {e}. "
                f"Retrying in {delay:.1f}s..."
            )

            time.sleep(delay)
            delay = min(delay * backoff_multiplier, max_delay)

    raise LLMError("Unknown error in retry logic")
