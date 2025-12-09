"""Database exporter for storing metrics in SQLite/DuckDB."""

from datetime import datetime
from typing import Dict, List
from src.models.node_storage import NodeStorage
from src.database.db_manager import DatabaseManager, SQLiteManager
from config.logging_config import LoggerFactory

logger = LoggerFactory.get_logger(__name__)


class DatabaseExporter:
    """Exports filesystem metrics to database."""

    def __init__(self, db_manager: DatabaseManager = None, db_type: str = "sqlite"):
        """
        Initialize database exporter.

        Args:
            db_manager: Custom database manager instance
            db_type: Type of database ('sqlite' or 'duckdb')
        """
        if db_manager:
            self.db_manager = db_manager
        elif db_type == "sqlite":
            self.db_manager = SQLiteManager()
        elif db_type == "duckdb":
            try:
                from src.database.db_manager import DuckDBManager

                self.db_manager = DuckDBManager()
            except ImportError:
                logger.warning("DuckDB not available, falling back to SQLite")
                self.db_manager = SQLiteManager()
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

        logger.info(f"Initialized {db_type} exporter")

    def export(self, data: Dict[str, List[NodeStorage]]) -> None:
        """
        Export data to database.

        Args:
            data: Dictionary mapping namespace to NodeStorage list
        """
        timestamp = datetime.now()

        for namespace, node_storages in data.items():
            for node_storage in node_storages:
                # Store node-level metrics
                node_metrics = {
                    "total_capacity": node_storage.total_capacity,
                    "total_allocatable": node_storage.total_allocatable,
                    "node_conditions": node_storage.node_conditions,
                    "kubelet_version": node_storage.kubelet_version,
                }

                self.db_manager.insert_metrics(
                    timestamp=timestamp,
                    namespace=namespace,
                    node=node_storage.node_name,
                    metrics=node_metrics,
                )

                # Store pod-level metrics
                for pod in node_storage.pods:
                    pod_metrics = {
                        "pod_name": pod.name,
                        "volumes": [v.to_dict() for v in pod.volumes],
                        "ephemeral_storage": pod.ephemeral_storage,
                    }

                    self.db_manager.insert_pod_metrics(
                        timestamp=timestamp,
                        namespace=namespace,
                        pod_name=pod.name,
                        node=node_storage.node_name,
                        metrics=pod_metrics,
                    )

        logger.info(
            f"Successfully exported metrics for {len(data)} namespace(s) to database"
        )

    def export_batch(self, data_list: List[Dict[str, List[NodeStorage]]]) -> None:
        """
        Export multiple batches of data.

        Args:
            data_list: List of data dictionaries
        """
        for data in data_list:
            self.export(data)

    def get_metrics_for_dashboard(self, hours: int = 24, namespace: str = None) -> Dict:
        """
        Get metrics for dashboard visualization.

        Args:
            hours: Number of hours to retrieve
            namespace: Optional namespace filter

        Returns:
            Dictionary with metrics ready for visualization
        """
        from datetime import timedelta

        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)

        node_metrics = self.db_manager.query_metrics(
            start_time=start_time, end_time=end_time, namespace=namespace
        )

        pod_metrics = self.db_manager.query_pod_metrics(
            start_time=start_time, end_time=end_time, namespace=namespace
        )

        # Aggregate data by node
        nodes_data = {}
        for metric in node_metrics:
            node = metric["node_name"]
            if node not in nodes_data:
                nodes_data[node] = {
                    "timestamps": [],
                    "capacities": [],
                    "allocatable": [],
                }
            nodes_data[node]["timestamps"].append(metric["timestamp"])
            nodes_data[node]["capacities"].append(metric["total_capacity"])
            nodes_data[node]["allocatable"].append(metric["total_allocatable"])

        return {
            "node_metrics": node_metrics,
            "pod_metrics": pod_metrics,
            "nodes_data": nodes_data,
            "timestamp_range": {
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
            },
        }

    def cleanup_old_data(self, retention_days: int = 30) -> None:
        """
        Clean up old metrics data.

        Args:
            retention_days: Number of days to retain
        """
        self.db_manager.cleanup_old_data(retention_days)

    def close(self) -> None:
        """Close database connection."""
        self.db_manager.close()
