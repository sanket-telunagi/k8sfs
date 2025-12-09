import time
import functools
from typing import Callable, TypeVar, Any
from config.logging_config import LoggerFactory

logger = LoggerFactory.get_logger(__name__)

T = TypeVar("T")


def retry_with_backoff(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
):
    """
    Decorator for retrying functions with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for delay after each retry
        exceptions: Tuple of exceptions to catch
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            current_delay = delay
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_retries:
                        logger.error(
                            f"Failed after {max_retries} retries",
                            extra={
                                "function": func.__name__,
                                "error": str(e),
                                "attempt": attempt + 1,
                            },
                        )
                        raise

                    logger.warning(
                        f"Retry attempt {attempt + 1}/{max_retries}",
                        extra={
                            "function": func.__name__,
                            "error": str(e),
                            "delay": current_delay,
                        },
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff

            raise last_exception

        return wrapper

    return decorator
