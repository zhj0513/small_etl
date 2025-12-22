"""Unit tests for DuckDB client."""

import polars as pl
import pytest

from small_etl.data_access.duckdb_client import DuckDBClient


class TestDuckDBClient:
    """Tests for DuckDBClient."""

    @pytest.fixture
    def client(self) -> DuckDBClient:
        """Create DuckDB client instance."""
        return DuckDBClient()

    def test_load_csv_bytes(self, client: DuckDBClient) -> None:
        """Test loading CSV from bytes."""
        csv_content = b"id,name,value\n1,Alice,100\n2,Bob,200\n"

        client.load_csv_bytes(csv_content, "test_table")
        count = client.get_row_count("test_table")

        assert count == 2

    def test_query_returns_polars(self, client: DuckDBClient) -> None:
        """Test that query returns Polars DataFrame."""
        csv_content = b"id,name,value\n1,Alice,100\n2,Bob,200\n"
        client.load_csv_bytes(csv_content, "test_table")

        result = client.query("SELECT * FROM test_table WHERE value > 150")

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 1
        assert result["name"][0] == "Bob"

    def test_to_polars(self, client: DuckDBClient) -> None:
        """Test converting table to Polars DataFrame."""
        csv_content = b"id,name\n1,Alice\n2,Bob\n3,Charlie\n"
        client.load_csv_bytes(csv_content, "test_table")

        df = client.to_polars("test_table")

        assert isinstance(df, pl.DataFrame)
        assert len(df) == 3
        assert "id" in df.columns
        assert "name" in df.columns

    def test_context_manager(self) -> None:
        """Test DuckDB client as context manager."""
        with DuckDBClient() as client:
            csv_content = b"id,value\n1,100\n"
            client.load_csv_bytes(csv_content, "test")
            count = client.get_row_count("test")
            assert count == 1

    def test_execute_sql(self, client: DuckDBClient) -> None:
        """Test executing arbitrary SQL."""
        client.execute("CREATE TABLE numbers (n INTEGER)")
        client.execute("INSERT INTO numbers VALUES (1), (2), (3)")

        count = client.get_row_count("numbers")
        assert count == 3

    def test_load_csv_with_datetime(self, client: DuckDBClient) -> None:
        """Test loading CSV with datetime field."""
        csv_content = b"id,updated_at\n1,2025-12-22T14:30:00\n2,2025-12-22T15:00:00\n"

        client.load_csv_bytes(csv_content, "test_dates")
        df = client.to_polars("test_dates")

        assert len(df) == 2
        assert df["updated_at"].dtype == pl.Datetime
