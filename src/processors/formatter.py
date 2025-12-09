import json
from typing import Dict, List, Any
from src.models.node_storage import NodeStorage
from config.logging_config import LoggerFactory

logger = LoggerFactory.get_logger(__name__)


class Formatter:
    """Formats data for various output formats."""

    @staticmethod
    def to_json(data: Dict[str, List[NodeStorage]], pretty: bool = True) -> str:
        """
        Format data as JSON.

        Args:
            data: Dictionary mapping namespace to NodeStorage list
            pretty: Whether to pretty-print JSON

        Returns:
            JSON string
        """
        formatted_data = {}

        for namespace, node_storages in data.items():
            formatted_data[namespace] = [ns.to_dict() for ns in node_storages]

        indent = 2 if pretty else None
        return json.dumps(formatted_data, indent=indent, default=str)

    @staticmethod
    def to_table(data: Dict[str, List[NodeStorage]]) -> str:
        """
        Format data as ASCII table.

        Args:
            data: Dictionary mapping namespace to NodeStorage list

        Returns:
            Formatted table string
        """
        lines = []
        lines.append("=" * 100)
        lines.append(
            f"{'Namespace':<20} | {'Node':<30} | {'Pods':<10} | {'Capacity':<20}"
        )
        lines.append("=" * 100)

        for namespace, node_storages in data.items():
            for node_storage in node_storages:
                pod_count = len(node_storage.pods)
                capacity = node_storage.total_capacity or "N/A"

                lines.append(
                    f"{namespace:<20} | {node_storage.node_name:<30} | "
                    f"{pod_count:<10} | {capacity:<20}"
                )

        lines.append("=" * 100)
        return "\n".join(lines)
