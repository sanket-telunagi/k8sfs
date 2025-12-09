#!/usr/bin/env python3
"""
Utility script for database operations and maintenance.

Usage:
    python db_utils.py --help
"""

import argparse
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from src.database.db_manager import SQLiteManager, DuckDBManager
from config.logging_config import LoggerFactory

logger = LoggerFactory.get_logger(__name__)


def get_db_manager(db_type: str, db_path: str):
    """Get appropriate database manager."""
    if db_type == "sqlite":
        return SQLiteManager(db_path)
    elif db_type == "duckdb":
        return DuckDBManager(db_path)
    else:
        raise ValueError(f"Unknown database type: {db_type}")


def cmd_stats(args):
    """Show database statistics."""
    print(f"\n{'Database Statistics':=^60}")

    db = get_db_manager(args.db_type, args.db_path)

    if isinstance(db, SQLiteManager):
        cursor = db.connection.cursor()

        # Node metrics stats
        cursor.execute("SELECT COUNT(*) FROM node_metrics")
        node_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM pod_metrics")
        pod_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT namespace) FROM node_metrics")
        namespace_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT node_name) FROM node_metrics")
        node_name_count = cursor.fetchone()[0]

        cursor.execute("SELECT MIN(timestamp) FROM node_metrics")
        min_timestamp = cursor.fetchone()[0]

        cursor.execute("SELECT MAX(timestamp) FROM node_metrics")
        max_timestamp = cursor.fetchone()[0]
    else:
        # DuckDB
        node_count = db.connection.execute(
            "SELECT COUNT(*) FROM node_metrics"
        ).fetchall()[0][0]
        pod_count = db.connection.execute(
            "SELECT COUNT(*) FROM pod_metrics"
        ).fetchall()[0][0]
        namespace_count = db.connection.execute(
            "SELECT COUNT(DISTINCT namespace) FROM node_metrics"
        ).fetchall()[0][0]
        node_name_count = db.connection.execute(
            "SELECT COUNT(DISTINCT node_name) FROM node_metrics"
        ).fetchall()[0][0]
        min_timestamp = db.connection.execute(
            "SELECT MIN(timestamp) FROM node_metrics"
        ).fetchall()[0][0]
        max_timestamp = db.connection.execute(
            "SELECT MAX(timestamp) FROM node_metrics"
        ).fetchall()[0][0]

    print(f"\nDatabase Path: {args.db_path}")
    print(f"Database Type: {args.db_type}")
    print(f"\nNode Metrics:      {node_count:,}")
    print(f"Pod Metrics:       {pod_count:,}")
    print(f"Namespaces:        {namespace_count}")
    print(f"Nodes:             {node_name_count}")
    print(f"\nData Range:")
    print(f"  Oldest: {min_timestamp}")
    print(f"  Newest: {max_timestamp}")

    db.close()


def cmd_list_nodes(args):
    """List all nodes in database."""
    print(f"\n{'Nodes in Database':=^60}\n")

    db = get_db_manager(args.db_type, args.db_path)

    latest = db.get_latest_metrics()

    if not latest:
        print("No metrics found in database")
        return

    nodes = {}
    for metric in latest:
        node = metric["node_name"]
        if node not in nodes:
            nodes[node] = metric

    print(f"{'Node Name':<30} {'Namespace':<20} {'Last Updated':<20}")
    print("-" * 70)

    for node_name in sorted(nodes.keys()):
        metric = nodes[node_name]
        timestamp = metric["timestamp"][:19] if metric["timestamp"] else "N/A"
        print(f"{node_name:<30} {metric['namespace']:<20} {timestamp:<20}")

    print(f"\nTotal: {len(nodes)} nodes")

    db.close()


def cmd_list_namespaces(args):
    """List all namespaces in database."""
    print(f"\n{'Namespaces in Database':=^60}\n")

    db = get_db_manager(args.db_type, args.db_path)

    end_time = datetime.now()
    start_time = end_time - timedelta(hours=24)

    metrics = db.query_metrics(start_time, end_time)

    namespaces = {}
    for metric in metrics:
        ns = metric["namespace"]
        if ns not in namespaces:
            namespaces[ns] = {"nodes": set(), "count": 0}
        namespaces[ns]["nodes"].add(metric["node_name"])
        namespaces[ns]["count"] += 1

    print(f"{'Namespace':<30} {'Nodes':<10} {'Metrics (24h)':<15}")
    print("-" * 55)

    for ns in sorted(namespaces.keys()):
        data = namespaces[ns]
        print(f"{ns:<30} {len(data['nodes']):<10} {data['count']:<15}")

    print(f"\nTotal: {len(namespaces)} namespaces")

    db.close()


def cmd_cleanup(args):
    """Clean up old metrics."""
    print(f"\n{'Cleanup Old Metrics':=^60}")

    db = get_db_manager(args.db_type, args.db_path)

    print(f"\nRemoving metrics older than {args.retention_days} days...")

    # Get count before
    if isinstance(db, SQLiteManager):
        cursor = db.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM node_metrics")
        before = cursor.fetchone()[0]
    else:
        before = db.connection.execute("SELECT COUNT(*) FROM node_metrics").fetchall()[
            0
        ][0]

    # Clean up
    db.cleanup_old_data(args.retention_days)

    # Get count after
    if isinstance(db, SQLiteManager):
        cursor = db.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM node_metrics")
        after = cursor.fetchone()[0]
    else:
        after = db.connection.execute("SELECT COUNT(*) FROM node_metrics").fetchall()[
            0
        ][0]

    print(f"\nRecords removed: {before - after}")
    print(f"Remaining records: {after}")

    db.close()


def cmd_export_csv(args):
    """Export metrics to CSV."""
    import csv

    print(f"\n{'Export to CSV':=^60}")

    db = get_db_manager(args.db_type, args.db_path)

    end_time = datetime.now()
    start_time = end_time - timedelta(hours=args.hours)

    metrics = db.query_metrics(start_time, end_time, namespace=args.namespace)

    if not metrics:
        print("No metrics found")
        return

    output_file = Path(args.output)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    print(f"\nExporting {len(metrics)} metrics to {output_file}...")

    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["timestamp", "namespace", "node_name", "total_capacity"]
        )
        writer.writeheader()

        for metric in metrics:
            writer.writerow({
                "timestamp": metric["timestamp"],
                "namespace": metric["namespace"],
                "node_name": metric["node_name"],
                "total_capacity": metric["total_capacity"],
            })

    print(f"✓ Exported successfully")

    db.close()


def cmd_backup(args):
    """Backup database."""
    import shutil

    print(f"\n{'Backup Database':=^60}")

    db_path = Path(args.db_path)

    if not db_path.exists():
        print(f"Database not found: {db_path}")
        return

    backup_path = (
        db_path.parent
        / f"{db_path.stem}.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}{db_path.suffix}"
    )

    print(f"\nBacking up: {db_path}")
    print(f"To:        {backup_path}")

    shutil.copy2(db_path, backup_path)

    print(f"✓ Backup successful")


def cmd_vacuum(args):
    """Optimize database file."""
    print(f"\n{'Optimize Database':=^60}")

    db = get_db_manager(args.db_type, args.db_path)

    print("\nOptimizing database...")

    if isinstance(db, SQLiteManager):
        db.connection.execute("VACUUM")
        print("✓ Database optimized")
    else:
        print("Optimization not needed for DuckDB")

    db.close()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Database utilities for Kubernetes Filesystem Monitor"
    )

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

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Stats command
    subparsers.add_parser("stats", help="Show database statistics")

    # List nodes
    subparsers.add_parser("nodes", help="List all nodes in database")

    # List namespaces
    subparsers.add_parser("namespaces", help="List all namespaces")

    # Cleanup command
    cleanup_parser = subparsers.add_parser("cleanup", help="Clean up old metrics")
    cleanup_parser.add_argument(
        "--retention-days",
        type=int,
        default=30,
        help="Retention days",
    )

    # Export CSV command
    export_parser = subparsers.add_parser("export", help="Export to CSV")
    export_parser.add_argument(
        "--output", default="metrics.csv", help="Output CSV file"
    )
    export_parser.add_argument("--hours", type=int, default=24, help="Hours to export")
    export_parser.add_argument("--namespace", default=None, help="Filter by namespace")

    # Backup command
    subparsers.add_parser("backup", help="Backup database")

    # Vacuum command
    subparsers.add_parser("vacuum", help="Optimize database")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    commands = {
        "stats": cmd_stats,
        "nodes": cmd_list_nodes,
        "namespaces": cmd_list_namespaces,
        "cleanup": cmd_cleanup,
        "export": cmd_export_csv,
        "backup": cmd_backup,
        "vacuum": cmd_vacuum,
    }

    try:
        commands[args.command](args)
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
