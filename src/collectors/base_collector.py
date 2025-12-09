from abc import ABC, abstractmethod
from typing import Any
from config.logging_config import LoggerFactory
from src.core.k8s_client import K8sClient
from src.utils.metrics import MetricsCollector
from src.utils.cache import SimpleCache

logger = LoggerFactory.get_logger(__name__)


class BaseCollector(ABC):
    """
    Abstract base class for all collectors.
    Implements Template Method pattern for collection lifecycle.
    """

    def __init__(
        self, k8s_client: K8sClient, metrics: MetricsCollector, cache: SimpleCache
    ):
        """
        Initialize base collector.

        Args:
            k8s_client: Kubernetes client instance
            metrics: Metrics collector instance
            cache: Cache instance
        """
        self.k8s_client = k8s_client
        self.metrics = metrics
        self.cache = cache
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

    def collect(self, namespace: str, *args, **kwargs) -> Any:
        """
        Template method for collection process.

        Args:
            namespace: Namespace to collect from
            *args: Additional positional arguments
            **kwargs: Additional keyword arguments

        Returns:
            Collection results
        """
        cache_key = self._get_cache_key(namespace, *args, **kwargs)

        # Check cache
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            self.logger.debug(f"Cache hit for {cache_key}")
            return cached_result

        # Start metrics timer
        timer_key = f"{self.__class__.__name__}:{namespace}"
        self.metrics.start_timer(timer_key)

        try:
            # Validate inputs
            self._validate(namespace, *args, **kwargs)

            # Perform collection
            result = self._collect_data(namespace, *args, **kwargs)

            # Process results
            processed_result = self._process_data(result)

            # Cache results
            self.cache.set(cache_key, processed_result)

            # Record metrics
            elapsed = self.metrics.stop_timer(timer_key)
            self.metrics.record_collection(namespace, self.__class__.__name__, elapsed)

            return processed_result

        except Exception as e:
            self.logger.error(
                f"Collection failed for namespace {namespace}", extra={"error": str(e)}
            )
            self.metrics.record_error(namespace, type(e).__name__)
            raise

    def _get_cache_key(self, namespace: str, *args, **kwargs) -> str:
        """Generate cache key for this collection."""
        return f"{self.__class__.__name__}:{namespace}"

    def _validate(self, namespace: str, *args, **kwargs) -> None:
        """Validate collection inputs."""
        if not namespace:
            raise ValueError("Namespace cannot be empty")

    @abstractmethod
    def _collect_data(self, namespace: str, *args, **kwargs) -> Any:
        """Collect raw data from Kubernetes."""
        pass

    @abstractmethod
    def _process_data(self, raw_data: Any) -> Any:
        """Process and transform raw data."""
        pass
