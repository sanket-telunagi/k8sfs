#!/usr/bin/env python3
"""
Example: Kubernetes Filesystem Monitoring with Database and Dashboard

This script demonstrates:
1. Collecting Kubernetes filesystem metrics
2. Storing metrics in SQLite database
3. Querying historical data
4. Starting the Grafana-style dashboard
"""

import argparse
import sys
import time
from datetime import datetime, timedelta
from config.logging_config import LoggerFactory
from config.settings import Settings
from src.core.filesystem_collector import FilesystemCollector
from src.models.namespace_config import NamespaceConfig
from src.exporters.database_exporter import DatabaseExporter
from src.exporters.console_exporter import ConsoleExporter
from src.database.db_manager import SQLiteManager

logger = LoggerFactory.get_logger(__name__)


def example_1_simple_collection():
    """Example 1: Simple metric collection and storage."""
    print("\n" + "=" * 60)
    print("EXAMPLE 1: Simple Metric Collection and Storage")
    print("=" * 60)

    settings = Settings.from_env()
    LoggerFactory.configure(log_level="INFO")

    # Create namespace configs
    namespace_configs = [
        NamespaceConfig(
            name="default",
            include_pods=True,
            include_pvcs=True,
        ),
        NamespaceConfig(
            name="kube-system",
            include_pods=True,
            include_pvcs=False,
        ),
    ]

    # Collect data
    print("\n[1] Collecting metrics from Kubernetes...")
    collector = FilesystemCollector(settings)
    data = collector.collect_all_namespaces(namespace_configs)

    # Export to console
    print("[2] Displaying metrics...")
    console_exporter = ConsoleExporter()
    console_exporter.export(data)

    # Store in database
    print("[3] Storing metrics in database...")
    db_exporter = DatabaseExporter(db_type="sqlite")
    db_exporter.export(data)

    print("\n✓ Metrics successfully stored in k8s_metrics.db")
    db_exporter.close()


def example_2_query_historical():
    """Example 2: Query and analyze historical data."""
    print("\n" + "=" * 60)
    print("EXAMPLE 2: Query Historical Metrics")
    print("=" * 60)

    # Initialize database
    db = SQLiteManager("./k8s_metrics.db")

    # Query last 24 hours
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=24)

    print(f"\n[1] Querying metrics from {start_time} to {end_time}")
    metrics = db.query_metrics(start_time, end_time)

    print(f"\n[2] Found {len(metrics)} metric records")

    # Show by node
    nodes = {}
    for metric in metrics:
        node = metric["node_name"]
        if node not in nodes:
            nodes[node] = []
        nodes[node].append(metric)

    print("\n[3] Metrics by Node:")
    for node, node_metrics in nodes.items():
        print(f"\n  Node: {node}")
        print(f"  - Total records: {len(node_metrics)}")
        if node_metrics:
            latest = node_metrics[0]
            print(f"  - Latest capacity: {latest['total_capacity']}")
            print(f"  - Latest allocatable: {latest['total_allocatable']}")
            print(f"  - Last updated: {latest['timestamp']}")

    # Show by namespace
    print("\n[4] Metrics by Namespace:")
    namespaces = set(m["namespace"] for m in metrics)
    for ns in sorted(namespaces):
        ns_metrics = [m for m in metrics if m["namespace"] == ns]
        print(f"  - {ns}: {len(ns_metrics)} records")

    db.close()


def example_3_continuous_monitoring():
    """Example 3: Continuous monitoring loop."""
    print("\n" + "=" * 60)
    print("EXAMPLE 3: Continuous Monitoring (5 iterations)")
    print("=" * 60)

    settings = Settings.from_env()
    LoggerFactory.configure(log_level="INFO")

    namespace_configs = [
        NamespaceConfig(
            name="default",
            include_pods=True,
            include_pvcs=True,
        ),
    ]

    db_exporter = DatabaseExporter(db_type="sqlite")
    collector = FilesystemCollector(settings)

    print("\n[*] Starting continuous monitoring (5 iterations, 10s interval)...")

    for i in range(5):
        print(f"\n[Iteration {i + 1}]")
        try:
            data = collector.collect_all_namespaces(namespace_configs)
            db_exporter.export(data)
            print(f"  ✓ Metrics collected and stored at {datetime.now()}")

            if i < 4:
                print("  Waiting 10 seconds for next iteration...")
                time.sleep(10)
        except KeyboardInterrupt:
            print("\n[!] Monitoring interrupted by user")
            break
        except Exception as e:
            print(f"  [!] Error: {e}")

    print("\n✓ Continuous monitoring completed")
    db_exporter.close()


def example_4_data_analysis():
    """Example 4: Advanced data analysis."""
    print("\n" + "=" * 60)
    print("EXAMPLE 4: Advanced Data Analysis")
    print("=" * 60)

    db = SQLiteManager("./k8s_metrics.db")

    # Get latest metrics
    print("\n[1] Latest Node Metrics:")
    latest = db.get_latest_metrics()

    print(f"\nFound {len(latest)} nodes")
    for metric in latest:
        print(f"\n  Node: {metric['node_name']}")
        print(f"  Namespace: {metric['namespace']}")
        print(f"  Capacity: {metric['total_capacity']}")
        print(f"  Allocatable: {metric['total_allocatable']}")
        print(f"  Timestamp: {metric['timestamp']}")

    # Calculate statistics
    print("\n[2] Capacity Statistics:")
    if latest:
        capacities = [m["total_capacity"] for m in latest if m["total_capacity"]]
        print(f"  Total nodes with metrics: {len(capacities)}")

    # Pod metrics
    print("\n[3] Pod Level Metrics:")
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=1)

    pod_metrics = db.query_pod_metrics(start_time, end_time)
    print(f"  Pod metrics in last hour: {len(pod_metrics)}")

    if pod_metrics:
        # Group by namespace
        ns_pods = {}
        for pm in pod_metrics:
            ns = pm["namespace"]
            if ns not in ns_pods:
                ns_pods[ns] = []
            ns_pods[ns].append(pm["pod_name"])

        print("\n  Pods by namespace:")
        for ns, pods in ns_pods.items():
            print(f"    {ns}: {len(set(pods))} unique pods")

    db.close()


def example_5_start_dashboard():
    """Example 5: Start the interactive dashboard."""
    print("\n" + "=" * 60)
    print("EXAMPLE 5: Starting Interactive Dashboard")
    print("=" * 60)

    print("\n[1] Initializing dashboard...")
    from src.dashboard.app import KubernetesMetricsDashboard

    db = SQLiteManager("./k8s_metrics.db")
    dashboard = KubernetesMetricsDashboard(db)

    print("[2] Dashboard ready!")
    print("\nAccess the dashboard at: http://127.0.0.1:8050")
    print("Press Ctrl+C to stop the dashboard\n")

    try:
        dashboard.run(debug=False, port=8050, host="127.0.0.1")
    except KeyboardInterrupt:
        print("\n\n✓ Dashboard stopped")
    finally:
        db.close()


def main():
    """Run examples."""
    parser = argparse.ArgumentParser(
        description="Kubernetes Filesystem Monitoring Examples"
    )

    parser.add_argument(
        "--example",
        type=int,
        choices=[1, 2, 3, 4, 5],
        default=1,
        help="Example number to run (1-5)",
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all examples in sequence (except dashboard)",
    )

    args = parser.parse_args()

    if args.all:
        try:
            example_1_simple_collection()
            time.sleep(2)
            example_2_query_historical()
            time.sleep(2)
            example_3_continuous_monitoring()
            time.sleep(2)
            example_4_data_analysis()

            print("\n" + "=" * 60)
            print("All examples completed successfully!")
            print("=" * 60)
            print("\nTo start the dashboard, run:")
            print("  python examples.py --example 5")
        except KeyboardInterrupt:
            print("\n\nExamples interrupted by user")
            sys.exit(0)
        except Exception as e:
            print(f"\nError running examples: {e}")
            sys.exit(1)
    else:
        examples = {
            1: example_1_simple_collection,
            2: example_2_query_historical,
            3: example_3_continuous_monitoring,
            4: example_4_data_analysis,
            5: example_5_start_dashboard,
        }

        try:
            examples[args.example]()
        except KeyboardInterrupt:
            print("\n\nExample interrupted by user")
            sys.exit(0)
        except Exception as e:
            print(f"\nError running example: {e}")
            import traceback

            traceback.print_exc()
            sys.exit(1)


if __name__ == "__main__":
    main()
