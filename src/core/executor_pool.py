from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Callable, Any, TypeVar
from config.logging_config import LoggerFactory
from config.settings import Settings

logger = LoggerFactory.get_logger(__name__)

T = TypeVar("T")


class ExecutorPool:
    """
    Managed thread pool executor for parallel processing.
    Provides context manager support and error handling.
    """

    def __init__(self, settings: Settings):
        """Initialize executor pool with configured number of workers."""
        self.settings = settings
        self.max_workers = settings.max_workers
        self._executor: ThreadPoolExecutor = None

    def __enter__(self):
        """Context manager entry."""
        self._executor = ThreadPoolExecutor(max_workers=self.max_workers)
        logger.info(f"Executor pool started with {self.max_workers} workers")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self._executor:
            self._executor.shutdown(wait=True)
            logger.info("Executor pool shut down")

    def execute_parallel(
        self, func: Callable[..., T], items: List[Any], timeout: int = None
    ) -> List[T]:
        """
        Execute function in parallel for all items.

        Args:
            func: Function to execute
            items: List of items to process
            timeout: Optional timeout in seconds

        Returns:
            List of results
        """
        if not items:
            return []

        timeout = timeout or self.settings.timeout_seconds
        results = []
        futures = {self._executor.submit(func, item): item for item in items}

        for future in as_completed(futures, timeout=timeout):
            item = futures[future]
            try:
                result = future.result()
                if result is not None:
                    results.append(result)
            except Exception as e:
                logger.error(
                    f"Error processing item", extra={"item": str(item), "error": str(e)}
                )

        return results
