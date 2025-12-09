"""Database manager for SQLite/DuckDB storage."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Any, Optional
import sqlite3
import json
from pathlib import Path
from config.logging_config import LoggerFactory

logger = LoggerFactory.get_logger(__name__)


class DatabaseManager(ABC):
    """Abstract base class for database operations."""

    @abstractmethod
    def create_tables(self) -> None:
        """Create required tables."""
        pass

    @abstractmethod
    def insert_metrics(
        self, timestamp: datetime, namespace: str, node: str, metrics: Dict[str, Any]
    ) -> None:
        """Insert metrics into database."""
        pass

    @abstractmethod
    def insert_pod_metrics(
        self,
        timestamp: datetime,
        namespace: str,
        pod_name: str,
        node: str,
        metrics: Dict[str, Any],
    ) -> None:
        """Insert pod-level metrics."""
        pass

    @abstractmethod
    def query_metrics(
        self, start_time: datetime, end_time: datetime, namespace: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query metrics within time range."""
        pass

    @abstractmethod
    def query_pod_metrics(
        self,
        start_time: datetime,
        end_time: datetime,
        namespace: Optional[str] = None,
        pod_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Query pod-level metrics."""
        pass

    @abstractmethod
    def get_latest_metrics(
        self, namespace: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get the latest metrics for each node."""
        pass

    @abstractmethod
    def cleanup_old_data(self, retention_days: int) -> None:
        """Delete metrics older than retention period."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close database connection."""
        pass


class SQLiteManager(DatabaseManager):
    """SQLite database manager."""

    def __init__(self, db_path: str = "./k8s_metrics.db"):
        """Initialize SQLite manager."""
        self.db_path = db_path
        self.connection = sqlite3.connect(db_path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        logger.info(f"Connected to SQLite database at {db_path}")
        self.create_tables()

    def create_tables(self) -> None:
        """Create required tables."""
        cursor = self.connection.cursor()

        # Node metrics table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS node_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                namespace TEXT NOT NULL,
                node_name TEXT NOT NULL,
                total_capacity TEXT,
                total_allocatable TEXT,
                metrics_json TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(timestamp, namespace, node_name)
            )
        """
        )

        # Pod metrics table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS pod_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                namespace TEXT NOT NULL,
                pod_name TEXT NOT NULL,
                node_name TEXT NOT NULL,
                metrics_json TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(timestamp, namespace, pod_name)
            )
        """
        )

        # Create indices for better query performance
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_node_metrics_timestamp 
            ON node_metrics(timestamp)
        """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_node_metrics_namespace 
            ON node_metrics(namespace)
        """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pod_metrics_timestamp 
            ON pod_metrics(timestamp)
        """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pod_metrics_namespace 
            ON pod_metrics(namespace)
        """
        )

        self.connection.commit()
        logger.info("Database tables created/verified")

    def insert_metrics(
        self, timestamp: datetime, namespace: str, node: str, metrics: Dict[str, Any]
    ) -> None:
        """Insert node metrics into database."""
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                INSERT OR REPLACE INTO node_metrics 
                (timestamp, namespace, node_name, total_capacity, total_allocatable, metrics_json)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    timestamp.isoformat(),
                    namespace,
                    node,
                    metrics.get("total_capacity"),
                    metrics.get("total_allocatable"),
                    json.dumps(metrics),
                ),
            )
            self.connection.commit()
        except Exception as e:
            logger.error(f"Error inserting metrics: {e}")

    def insert_pod_metrics(
        self,
        timestamp: datetime,
        namespace: str,
        pod_name: str,
        node: str,
        metrics: Dict[str, Any],
    ) -> None:
        """Insert pod metrics into database."""
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                INSERT OR REPLACE INTO pod_metrics 
                (timestamp, namespace, pod_name, node_name, metrics_json)
                VALUES (?, ?, ?, ?, ?)
            """,
                (
                    timestamp.isoformat(),
                    namespace,
                    pod_name,
                    node,
                    json.dumps(metrics),
                ),
            )
            self.connection.commit()
        except Exception as e:
            logger.error(f"Error inserting pod metrics: {e}")

    def query_metrics(
        self, start_time: datetime, end_time: datetime, namespace: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query node metrics within time range."""
        cursor = self.connection.cursor()

        query = "SELECT * FROM node_metrics WHERE timestamp BETWEEN ? AND ?"
        params = [start_time.isoformat(), end_time.isoformat()]

        if namespace:
            query += " AND namespace = ?"
            params.append(namespace)

        query += " ORDER BY timestamp DESC"

        cursor.execute(query, params)
        rows = cursor.fetchall()

        return [
            {
                "id": row["id"],
                "timestamp": row["timestamp"],
                "namespace": row["namespace"],
                "node_name": row["node_name"],
                "total_capacity": row["total_capacity"],
                "total_allocatable": row["total_allocatable"],
                "metrics": json.loads(row["metrics_json"]),
            }
            for row in rows
        ]

    def query_pod_metrics(
        self,
        start_time: datetime,
        end_time: datetime,
        namespace: Optional[str] = None,
        pod_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Query pod metrics within time range."""
        cursor = self.connection.cursor()

        query = "SELECT * FROM pod_metrics WHERE timestamp BETWEEN ? AND ?"
        params = [start_time.isoformat(), end_time.isoformat()]

        if namespace:
            query += " AND namespace = ?"
            params.append(namespace)

        if pod_name:
            query += " AND pod_name = ?"
            params.append(pod_name)

        query += " ORDER BY timestamp DESC"

        cursor.execute(query, params)
        rows = cursor.fetchall()

        return [
            {
                "id": row["id"],
                "timestamp": row["timestamp"],
                "namespace": row["namespace"],
                "pod_name": row["pod_name"],
                "node_name": row["node_name"],
                "metrics": json.loads(row["metrics_json"]),
            }
            for row in rows
        ]

    def get_latest_metrics(
        self, namespace: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get the latest metrics for each node."""
        cursor = self.connection.cursor()

        query = """
            SELECT DISTINCT ON (node_name, namespace) *
            FROM node_metrics
        """
        if namespace:
            query += f" WHERE namespace = '{namespace}'"

        query += " ORDER BY node_name, namespace, timestamp DESC"

        # SQLite doesn't support DISTINCT ON, use subquery instead
        query = """
            SELECT * FROM node_metrics nm
            WHERE (timestamp, namespace, node_name) IN (
                SELECT MAX(timestamp), namespace, node_name
                FROM node_metrics
        """
        if namespace:
            query += f" WHERE namespace = '{namespace}'"
        query += " GROUP BY namespace, node_name)"

        cursor.execute(query)
        rows = cursor.fetchall()

        return [
            {
                "id": row["id"],
                "timestamp": row["timestamp"],
                "namespace": row["namespace"],
                "node_name": row["node_name"],
                "total_capacity": row["total_capacity"],
                "total_allocatable": row["total_allocatable"],
                "metrics": json.loads(row["metrics_json"]),
            }
            for row in rows
        ]

    def cleanup_old_data(self, retention_days: int = 30) -> None:
        """Delete metrics older than retention period."""
        cursor = self.connection.cursor()
        cutoff_time = datetime.now().timestamp() - (retention_days * 86400)

        try:
            cursor.execute(
                "DELETE FROM node_metrics WHERE created_at < datetime(?, 'unixepoch')",
                (cutoff_time,),
            )
            deleted_node = cursor.rowcount

            cursor.execute(
                "DELETE FROM pod_metrics WHERE created_at < datetime(?, 'unixepoch')",
                (cutoff_time,),
            )
            deleted_pod = cursor.rowcount

            self.connection.commit()
            logger.info(
                f"Cleaned up old data: {deleted_node} node metrics, {deleted_pod} pod metrics"
            )
        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")

    def close(self) -> None:
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")


class DuckDBManager(DatabaseManager):
    """DuckDB database manager."""

    def __init__(self, db_path: str = "./k8s_metrics.duckdb"):
        """Initialize DuckDB manager."""
        try:
            import duckdb

            self.duckdb = duckdb
        except ImportError:
            logger.error("DuckDB not installed. Install with: pip install duckdb")
            raise

        self.db_path = db_path
        self.connection = duckdb.connect(db_path)
        logger.info(f"Connected to DuckDB database at {db_path}")
        self.create_tables()

    def create_tables(self) -> None:
        """Create required tables."""
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS node_metrics (
                id INTEGER PRIMARY KEY DEFAULT nextval('seq_node_metrics'),
                timestamp TIMESTAMP NOT NULL,
                namespace VARCHAR NOT NULL,
                node_name VARCHAR NOT NULL,
                total_capacity VARCHAR,
                total_allocatable VARCHAR,
                metrics_json VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(timestamp, namespace, node_name)
            )
        """
        )

        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS pod_metrics (
                id INTEGER PRIMARY KEY DEFAULT nextval('seq_pod_metrics'),
                timestamp TIMESTAMP NOT NULL,
                namespace VARCHAR NOT NULL,
                pod_name VARCHAR NOT NULL,
                node_name VARCHAR NOT NULL,
                metrics_json VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(timestamp, namespace, pod_name)
            )
        """
        )

        # Create indices
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_node_metrics_timestamp ON node_metrics(timestamp)"
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_node_metrics_namespace ON node_metrics(namespace)"
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_pod_metrics_timestamp ON pod_metrics(timestamp)"
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_pod_metrics_namespace ON pod_metrics(namespace)"
        )

        logger.info("DuckDB tables created/verified")

    def insert_metrics(
        self, timestamp: datetime, namespace: str, node: str, metrics: Dict[str, Any]
    ) -> None:
        """Insert node metrics into database."""
        try:
            self.connection.execute(
                """
                INSERT OR REPLACE INTO node_metrics 
                (timestamp, namespace, node_name, total_capacity, total_allocatable, metrics_json)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                [
                    timestamp,
                    namespace,
                    node,
                    metrics.get("total_capacity"),
                    metrics.get("total_allocatable"),
                    json.dumps(metrics),
                ],
            )
        except Exception as e:
            logger.error(f"Error inserting metrics: {e}")

    def insert_pod_metrics(
        self,
        timestamp: datetime,
        namespace: str,
        pod_name: str,
        node: str,
        metrics: Dict[str, Any],
    ) -> None:
        """Insert pod metrics into database."""
        try:
            self.connection.execute(
                """
                INSERT OR REPLACE INTO pod_metrics 
                (timestamp, namespace, pod_name, node_name, metrics_json)
                VALUES (?, ?, ?, ?, ?)
            """,
                [timestamp, namespace, pod_name, node, json.dumps(metrics)],
            )
        except Exception as e:
            logger.error(f"Error inserting pod metrics: {e}")

    def query_metrics(
        self, start_time: datetime, end_time: datetime, namespace: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query node metrics within time range."""
        query = "SELECT * FROM node_metrics WHERE timestamp BETWEEN ? AND ?"
        params = [start_time, end_time]

        if namespace:
            query += " AND namespace = ?"
            params.append(namespace)

        query += " ORDER BY timestamp DESC"

        result = self.connection.execute(query, params).fetchall()
        columns = [desc[0] for desc in self.connection.description]

        return [
            {
                col: (json.loads(row[i]) if col == "metrics_json" else row[i])
                for i, col in enumerate(columns)
            }
            for row in result
        ]

    def query_pod_metrics(
        self,
        start_time: datetime,
        end_time: datetime,
        namespace: Optional[str] = None,
        pod_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Query pod metrics within time range."""
        query = "SELECT * FROM pod_metrics WHERE timestamp BETWEEN ? AND ?"
        params = [start_time, end_time]

        if namespace:
            query += " AND namespace = ?"
            params.append(namespace)

        if pod_name:
            query += " AND pod_name = ?"
            params.append(pod_name)

        query += " ORDER BY timestamp DESC"

        result = self.connection.execute(query, params).fetchall()
        columns = [desc[0] for desc in self.connection.description]

        return [
            {
                col: (json.loads(row[i]) if col == "metrics_json" else row[i])
                for i, col in enumerate(columns)
            }
            for row in result
        ]

    def get_latest_metrics(
        self, namespace: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get the latest metrics for each node."""
        query = """
            SELECT * FROM node_metrics nm
            WHERE (timestamp, namespace, node_name) IN (
                SELECT MAX(timestamp), namespace, node_name
                FROM node_metrics
        """
        if namespace:
            query += f" WHERE namespace = '{namespace}'"
        query += " GROUP BY namespace, node_name)"

        result = self.connection.execute(query).fetchall()
        columns = [desc[0] for desc in self.connection.description]

        return [
            {
                col: (json.loads(row[i]) if col == "metrics_json" else row[i])
                for i, col in enumerate(columns)
            }
            for row in result
        ]

    def cleanup_old_data(self, retention_days: int = 30) -> None:
        """Delete metrics older than retention period."""
        try:
            self.connection.execute(
                f"""
                DELETE FROM node_metrics 
                WHERE created_at < NOW() - INTERVAL '{retention_days}' DAY
            """
            )
            deleted_node = self.connection.total_changes

            self.connection.execute(
                f"""
                DELETE FROM pod_metrics 
                WHERE created_at < NOW() - INTERVAL '{retention_days}' DAY
            """
            )
            deleted_pod = self.connection.total_changes - deleted_node

            logger.info(
                f"Cleaned up old data: {deleted_node} node metrics, {deleted_pod} pod metrics"
            )
        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")

    def close(self) -> None:
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("DuckDB connection closed")
