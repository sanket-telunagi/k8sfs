from typing import Dict, List
from src.models.node_storage import NodeStorage
from src.processors.formatter import Formatter
from src.processors.data_aggregator import DataAggregator
from config.logging_config import LoggerFactory

logger = LoggerFactory.get_logger(__name__)


class ConsoleExporter:
    """Exports data to console."""

    def export(self, data: Dict[str, List[NodeStorage]]):
        """
        Export data to console.

        Args:
            data: Dictionary mapping namespace to NodeStorage list
        """
        print("\n" + "=" * 100)
        print("KUBERNETES FILESYSTEM MONITORING REPORT")
        print("=" * 100 + "\n")

        # Print table
        print(Formatter.to_table(data))

        # Print summary
        print("\nSUMMARY:")
        print("-" * 50)
        summary = DataAggregator.aggregate_by_namespace(data)
        print(f"Total Namespaces: {summary['total_namespaces']}")
        print(f"Total Nodes: {summary['total_nodes']}")
        print(f"Total Pods: {summary['total_pods']}")
        print("\n" + "=" * 100 + "\n")
