import json
from pathlib import Path
from typing import Dict, List
from src.models.node_storage import NodeStorage
from src.processors.formatter import Formatter
from config.logging_config import LoggerFactory

logger = LoggerFactory.get_logger(__name__)


class JsonExporter:
    """Exports data to JSON files."""

    def __init__(self, output_dir: str = "./output"):
        """
        Initialize JSON exporter.

        Args:
            output_dir: Directory to write JSON files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export(
        self, data: Dict[str, List[NodeStorage]], filename: str = "filesystem_data.json"
    ) -> str:
        """
        Export data to JSON file.

        Args:
            data: Dictionary mapping namespace to NodeStorage list
            filename: Output filename

        Returns:
            Path to exported file
        """
        output_path = self.output_dir / filename

        json_data = Formatter.to_json(data, pretty=True)

        with open(output_path, "w") as f:
            f.write(json_data)

        logger.info(f"Exported data to {output_path}")
        return str(output_path)
