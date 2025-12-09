from typing import Dict, List, Any
from src.models.node_storage import NodeStorage
from config.logging_config import LoggerFactory

logger = LoggerFactory.get_logger(__name__)


class DataAggregator:
    """Aggregates and summarizes filesystem data."""

    @staticmethod
    def aggregate_by_namespace(data: Dict[str, List[NodeStorage]]) -> Dict[str, Any]:
        """
        Create summary statistics by namespace.

        Args:
            data: Dictionary mapping namespace to NodeStorage list

        Returns:
            Aggregated summary data
        """
        summary = {
            "total_namespaces": len(data),
            "total_nodes": 0,
            "total_pods": 0,
            "namespaces": {},
        }

        for namespace, node_storages in data.items():
            namespace_summary = {
                "node_count": len(node_storages),
                "pod_count": sum(len(ns.pods) for ns in node_storages),
                "nodes": {},
            }

            for node_storage in node_storages:
                namespace_summary["nodes"][node_storage.node_name] = {
                    "pod_count": len(node_storage.pods),
                    "total_capacity": node_storage.total_capacity,
                    "total_allocatable": node_storage.total_allocatable,
                }

            summary["namespaces"][namespace] = namespace_summary
            summary["total_nodes"] += namespace_summary["node_count"]
            summary["total_pods"] += namespace_summary["pod_count"]

        return summary

    @staticmethod
    def aggregate_by_node(data: Dict[str, List[NodeStorage]]) -> Dict[str, Any]:
        """
        Create summary statistics by node across all namespaces.

        Args:
            data: Dictionary mapping namespace to NodeStorage list

        Returns:
            Node-centric aggregated data
        """
        node_map = {}

        for namespace, node_storages in data.items():
            for node_storage in node_storages:
                node_name = node_storage.node_name

                if node_name not in node_map:
                    node_map[node_name] = {
                        "namespaces": {},
                        "total_pods": 0,
                        "capacity": node_storage.total_capacity,
                        "allocatable": node_storage.total_allocatable,
                    }

                node_map[node_name]["namespaces"][namespace] = {
                    "pod_count": len(node_storage.pods),
                    "pods": [pod.name for pod in node_storage.pods],
                }
                node_map[node_name]["total_pods"] += len(node_storage.pods)

        return node_map
