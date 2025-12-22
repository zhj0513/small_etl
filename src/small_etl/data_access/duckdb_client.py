"""DuckDB client for in-memory data validation."""

import logging
import tempfile
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

logger = logging.getLogger(__name__)


class DuckDBClient:
    """DuckDB client for in-memory data operations.

    Uses DuckDB's in-memory mode for fast data validation
    and transformation operations.
    """

    def __init__(self) -> None:
        self._conn = duckdb.connect(":memory:")
        logger.info("DuckDBClient initialized with in-memory database")

    def load_csv_bytes(self, csv_bytes: bytes, table_name: str) -> None:
        """Load CSV data from bytes into a DuckDB table.

        Args:
            csv_bytes: CSV file content as bytes.
            table_name: Name for the created table.
        """
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv", delete=False) as f:
            f.write(csv_bytes)
            temp_path = f.name

        try:
            self._conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self._conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{temp_path}', header=true, timestampformat='%Y-%m-%dT%H:%M:%S')")
            logger.info(f"Loaded CSV data into table '{table_name}'")
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def query(self, sql: str) -> pl.DataFrame:
        """Execute SQL query and return results as Polars DataFrame.

        Args:
            sql: SQL query string.

        Returns:
            Query results as Polars DataFrame.
        """
        result = self._conn.execute(sql)
        return result.pl()

    def to_polars(self, table_name: str) -> pl.DataFrame:
        """Convert a DuckDB table to Polars DataFrame.

        Args:
            table_name: Name of the table to convert.

        Returns:
            Table data as Polars DataFrame.
        """
        return self.query(f"SELECT * FROM {table_name}")

    def execute(self, sql: str, params: list[Any] | None = None) -> None:
        """Execute a SQL statement.

        Args:
            sql: SQL statement.
            params: Optional parameters for parameterized queries.
        """
        if params:
            self._conn.execute(sql, params)
        else:
            self._conn.execute(sql)

    def get_row_count(self, table_name: str) -> int:
        """Get row count for a table.

        Args:
            table_name: Name of the table.

        Returns:
            Number of rows in the table.
        """
        result = self._conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        return result[0] if result else 0

    def close(self) -> None:
        """Close the DuckDB connection."""
        self._conn.close()
        logger.info("DuckDBClient connection closed")

    def __enter__(self) -> "DuckDBClient":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
