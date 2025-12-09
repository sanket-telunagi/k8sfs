"""Tests for database functionality."""

import unittest
import tempfile
import os
from datetime import datetime, timedelta
from src.database.db_manager import SQLiteManager
from src.exporters.database_exporter import DatabaseExporter


class TestSQLiteManager(unittest.TestCase):
    """Test SQLite database manager."""

    def setUp(self):
        """Create temporary database for testing."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.db_path = self.temp_db.name
        self.temp_db.close()
        self.db = SQLiteManager(self.db_path)

    def tearDown(self):
        """Clean up test database."""
        self.db.close()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_create_tables(self):
        """Test table creation."""
        cursor = self.db.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        table_names = [t[0] for t in tables]

        self.assertIn("node_metrics", table_names)
        self.assertIn("pod_metrics", table_names)

    def test_insert_metrics(self):
        """Test metric insertion."""
        timestamp = datetime.now()
        metrics = {"total_capacity": "100Gi", "total_allocatable": "80Gi"}

        self.db.insert_metrics(
            timestamp=timestamp,
            namespace="default",
            node="node-1",
            metrics=metrics,
        )

        # Verify insertion
        cursor = self.db.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM node_metrics")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 1)

    def test_query_metrics(self):
        """Test metric querying."""
        timestamp = datetime.now()
        metrics = {"total_capacity": "100Gi"}

        self.db.insert_metrics(
            timestamp=timestamp,
            namespace="default",
            node="node-1",
            metrics=metrics,
        )

        # Query metrics
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=1)
        results = self.db.query_metrics(start_time, end_time, namespace="default")

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["node_name"], "node-1")

    def test_insert_pod_metrics(self):
        """Test pod metric insertion."""
        timestamp = datetime.now()
        metrics = {"pod_name": "test-pod"}

        self.db.insert_pod_metrics(
            timestamp=timestamp,
            namespace="default",
            pod_name="test-pod",
            node="node-1",
            metrics=metrics,
        )

        # Verify insertion
        cursor = self.db.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM pod_metrics")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 1)

    def test_get_latest_metrics(self):
        """Test getting latest metrics."""
        now = datetime.now()

        # Insert multiple metrics for same node at different times
        self.db.insert_metrics(
            timestamp=now - timedelta(hours=2),
            namespace="default",
            node="node-1",
            metrics={"total_capacity": "50Gi"},
        )

        self.db.insert_metrics(
            timestamp=now,
            namespace="default",
            node="node-1",
            metrics={"total_capacity": "100Gi"},
        )

        # Get latest
        latest = self.db.get_latest_metrics(namespace="default")

        self.assertEqual(len(latest), 1)
        self.assertEqual(latest[0]["total_capacity"], "100Gi")


class TestDatabaseExporter(unittest.TestCase):
    """Test database exporter."""

    def setUp(self):
        """Create temporary database for testing."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.db_path = self.temp_db.name
        self.temp_db.close()
        self.exporter = DatabaseExporter(db_type="sqlite")

    def tearDown(self):
        """Clean up."""
        self.exporter.close()

    def test_export(self):
        """Test export functionality."""
        from src.models.node_storage import NodeStorage, PodStorage

        # Create test data
        pod = PodStorage(
            name="test-pod", namespace="default", node="node-1", volumes=[]
        )
        node = NodeStorage(
            node_name="node-1",
            namespace="default",
            total_capacity="100Gi",
            total_allocatable="80Gi",
            pods=[pod],
        )

        data = {"default": [node]}

        # Export
        self.exporter.export(data)

        # Verify
        cursor = self.exporter.db_manager.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM node_metrics")
        node_count = cursor.fetchone()[0]

        self.assertGreater(node_count, 0)


if __name__ == "__main__":
    unittest.main()
