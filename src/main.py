import sys
import argparse
from typing import List
from config.settings import Settings
from config.logging_config import LoggerFactory
from src.core.filesystem_collector import FilesystemCollector
from src.models.namespace_config import NamespaceConfig
from src.processors.data_aggregator import DataAggregator
from src.exporters.json_exporter import JsonExporter
from src.exporters.console_exporter import ConsoleExporter
from src.exporters.prometheus_exporter import PrometheusExporter
from src.exporters.database_exporter import DatabaseExporter

logger = LoggerFactory.get_logger(__name__)


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Kubernetes Filesystem Monitoring Tool"
    )

    parser.add_argument(
        "--namespaces", nargs="+", required=True, help="List of namespaces to monitor"
    )

    parser.add_argument(
        "--output",
        choices=["json", "console", "prometheus", "database", "all"],
        default="console",
        help="Output format",
    )

    parser.add_argument(
        "--output-dir", default="./output", help="Output directory for JSON export"
    )

    parser.add_argument(
        "--prometheus-port",
        type=int,
        default=9090,
        help="Port for Prometheus metrics server",
    )

    parser.add_argument(
        "--db-type",
        choices=["sqlite", "duckdb"],
        default="sqlite",
        help="Database type for metrics storage",
    )

    parser.add_argument(
        "--db-path",
        default="./k8s_metrics.db",
        help="Path to database file",
    )

    parser.add_argument(
        "--dashboard",
        action="store_true",
        help="Start web dashboard",
    )

    parser.add_argument(
        "--dashboard-port",
        type=int,
        default=8050,
        help="Port for dashboard server",
    )

    parser.add_argument("--label-selector", help="Label selector for filtering pods")

    parser.add_argument("--field-selector", help="Field selector for filtering pods")

    return parser.parse_args()


def main():
    """Main entry point for the application."""
    args = parse_arguments()

    # Load settings
    settings = Settings.from_env()
    settings.validate()

    # Configure logging
    LoggerFactory.configure(
        log_level=settings.log_level, log_format=settings.log_format
    )

    logger.info("Starting Kubernetes Filesystem Monitor")
    logger.info(f"Monitoring namespaces: {args.namespaces}")

    try:
        # Create namespace configurations
        namespace_configs = [
            NamespaceConfig(
                name=ns,
                include_pods=True,
                include_pvcs=True,
                label_selector=args.label_selector,
                field_selector=args.field_selector,
            )
            for ns in args.namespaces
        ]

        # Initialize exporters
        db_exporter = None
        if args.output in ["database", "all"]:
            db_exporter = DatabaseExporter(db_type=args.db_type)

        # If dashboard is requested, start it and return
        if args.dashboard:
            logger.info(f"Starting dashboard on port {args.dashboard_port}")
            from src.dashboard.app import create_dashboard_app

            app = create_dashboard_app(db_type=args.db_type)
            app.run_server(debug=False, port=args.dashboard_port, host="0.0.0.0")
            return 0

        # Collect filesystem data
        collector = FilesystemCollector(settings)
        data = collector.collect_all_namespaces(namespace_configs)

        # Export data
        if args.output in ["console", "all"]:
            console_exporter = ConsoleExporter()
            console_exporter.export(data)

        if args.output in ["json", "all"]:
            json_exporter = JsonExporter(output_dir=args.output_dir)
            json_exporter.export(data)

        if args.output in ["database", "all"]:
            db_exporter.export(data)
            logger.info(f"Metrics stored in {args.db_type} database at {args.db_path}")

        if args.output in ["prometheus", "all"]:
            prom_exporter = PrometheusExporter(port=args.prometheus_port)
            prom_exporter.start_server()
            prom_exporter.export(data)
            logger.info("Prometheus server running. Press Ctrl+C to stop.")

            # Keep running for Prometheus
            import time

            while True:
                time.sleep(60)
                # Re-collect and update metrics
                data = collector.collect_all_namespaces(namespace_configs)
                prom_exporter.export(data)

                # Also store in database if enabled
                if db_exporter:
                    db_exporter.export(data)

        logger.info("Collection completed successfully")
        return 0

    except KeyboardInterrupt:
        logger.info("Shutting down...")
        return 0
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
