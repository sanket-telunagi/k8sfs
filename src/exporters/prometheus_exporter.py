from prometheus_client import start_http_server, Gauge
from typing import Dict, List
from src.models.node_storage import NodeStorage
from config.logging_config import LoggerFactory

logger = LoggerFactory.get_logger(__name__)


class PrometheusExporter:
    """Exports metrics to Prometheus."""

    def __init__(self, port: int = 9090):
        """
        Initialize Prometheus exporter.

        Args:
            port: Port to expose metrics on
        """
        self.port = port
        self._initialize_metrics()

    def _initialize_metrics(self):
        """Initialize Prometheus gauges."""
        self.node_pod_count = Gauge(
            "k8s_fs_node_pod_count", "Number of pods on node", ["namespace", "node"]
        )

        self.node_capacity = Gauge(
            "k8s_fs_node_capacity_bytes", "Node storage capacity", ["namespace", "node"]
        )

    def start_server(self):
        """Start Prometheus HTTP server."""
        start_http_server(self.port)
        logger.info(f"Prometheus metrics server started on port {self.port}")

    def export(self, data: Dict[str, List[NodeStorage]]):
        """
        Export data as Prometheus metrics.

        Args:
            data: Dictionary mapping namespace to NodeStorage list
        """
        for namespace, node_storages in data.items():
            for node_storage in node_storages:
                self.node_pod_count.labels(
                    namespace=namespace, node=node_storage.node_name
                ).set(len(node_storage.pods))

                if node_storage.total_capacity:
                    try:
                        # Parse capacity string (e.g., "100Gi" -> bytes)
                        capacity_bytes = self._parse_capacity(
                            node_storage.total_capacity
                        )
                        self.node_capacity.labels(
                            namespace=namespace, node=node_storage.node_name
                        ).set(capacity_bytes)
                    except Exception as e:
                        logger.warning(f"Failed to parse capacity: {e}")

    def _parse_capacity(self, capacity_str: str) -> float:
        """Parse Kubernetes capacity string to bytes."""
        multipliers = {
            "Ki": 1024,
            "Mi": 1024**2,
            "Gi": 1024**3,
            "Ti": 1024**4,
            "K": 1000,
            "M": 1000**2,
            "G": 1000**3,
            "T": 1000**4,
        }

        for suffix, multiplier in multipliers.items():
            if capacity_str.endswith(suffix):
                number = float(capacity_str[: -len(suffix)])
                return number * multiplier

        return float(capacity_str)
