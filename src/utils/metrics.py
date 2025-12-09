import time
from typing import Dict, Optional
from threading import Lock
from prometheus_client import Counter, Histogram, Gauge
from config.logging_config import LoggerFactory

logger = LoggerFactory.get_logger(__name__)


class MetricsCollector:
    """Thread-safe metrics collector for monitoring application performance."""

    _instance: Optional["MetricsCollector"] = None
    _lock: Lock = Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize_metrics()
        return cls._instance

    def _initialize_metrics(self):
        """Initialize Prometheus metrics."""
        self.collection_duration = Histogram(
            "k8s_fs_collection_duration_seconds",
            "Time spent collecting filesystem data",
            ["namespace", "collector_type"],
        )

        self.collection_errors = Counter(
            "k8s_fs_collection_errors_total",
            "Total number of collection errors",
            ["namespace", "error_type"],
        )

        self.pods_processed = Counter(
            "k8s_fs_pods_processed_total",
            "Total number of pods processed",
            ["namespace"],
        )

        self.nodes_processed = Gauge(
            "k8s_fs_nodes_current",
            "Current number of nodes being monitored",
            ["namespace"],
        )

        self._timers: Dict[str, float] = {}

    def start_timer(self, key: str) -> None:
        """Start a timer for measuring duration."""
        self._timers[key] = time.time()

    def stop_timer(self, key: str) -> float:
        """Stop a timer and return elapsed time."""
        if key not in self._timers:
            logger.warning(f"Timer {key} was not started")
            return 0.0

        elapsed = time.time() - self._timers[key]
        del self._timers[key]
        return elapsed

    def record_collection(self, namespace: str, collector_type: str, duration: float):
        """Record a collection operation."""
        self.collection_duration.labels(
            namespace=namespace, collector_type=collector_type
        ).observe(duration)

    def record_error(self, namespace: str, error_type: str):
        """Record an error."""
        self.collection_errors.labels(namespace=namespace, error_type=error_type).inc()

    def record_pods(self, namespace: str, count: int):
        """Record number of pods processed."""
        self.pods_processed.labels(namespace=namespace).inc(count)

    def set_nodes(self, namespace: str, count: int):
        """Set current number of nodes."""
        self.nodes_processed.labels(namespace=namespace).set(count)
