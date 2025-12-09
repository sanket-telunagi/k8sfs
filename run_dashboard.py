#!/usr/bin/env python3
"""CLI runner for Kubernetes Filesystem Monitoring Dashboard."""

import sys
from src.dashboard.app import KubernetesMetricsDashboard
from src.database.db_manager import SQLiteManager, DuckDBManager
from config.logging_config import LoggerFactory
import argparse

logger = LoggerFactory.get_logger(__name__)


def main():
    """Run dashboard server."""
    parser = argparse.ArgumentParser(description="Start Kubernetes Metrics Dashboard")

    parser.add_argument(
        "--db-type",
        choices=["sqlite", "duckdb"],
        default="sqlite",
        help="Database type",
    )

    parser.add_argument(
        "--db-path",
        default="./k8s_metrics.db",
        help="Path to database file",
    )

    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind to",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=8050,
        help="Port to bind to",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode",
    )

    args = parser.parse_args()

    # Initialize database manager
    if args.db_type == "sqlite":
        db_manager = SQLiteManager(args.db_path)
    else:
        try:
            db_manager = DuckDBManager(args.db_path)
        except ImportError:
            logger.warning("DuckDB not available, using SQLite")
            db_manager = SQLiteManager(args.db_path)

    # Create and run dashboard
    dashboard = KubernetesMetricsDashboard(db_manager)
    dashboard.run(debug=args.debug, port=args.port, host=args.host)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Dashboard stopped")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)
