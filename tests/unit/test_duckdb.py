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
        result = client.query("SELECT COUNT(*) as cnt FROM test_table")

        assert result["cnt"][0] == 2

    def test_query_returns_polars(self, client: DuckDBClient) -> None:
        """Test that query returns Polars DataFrame."""
        csv_content = b"id,name,value\n1,Alice,100\n2,Bob,200\n"
        client.load_csv_bytes(csv_content, "test_table")

        result = client.query("SELECT * FROM test_table WHERE value > 150")

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 1
        assert result["name"][0] == "Bob"

    def test_context_manager(self) -> None:
        """Test DuckDB client as context manager."""
        with DuckDBClient() as client:
            csv_content = b"id,value\n1,100\n"
            client.load_csv_bytes(csv_content, "test")
            result = client.query("SELECT COUNT(*) as cnt FROM test")
            assert result["cnt"][0] == 1

    def test_load_csv_with_datetime(self, client: DuckDBClient) -> None:
        """Test loading CSV with datetime field."""
        csv_content = b"id,updated_at\n1,2025-12-22T14:30:00\n2,2025-12-22T15:00:00\n"

        client.load_csv_bytes(csv_content, "test_dates")
        df = client.query("SELECT * FROM test_dates")

        assert len(df) == 2
        assert df["updated_at"].dtype == pl.Datetime

    def test_upsert_to_postgres_empty_df(self, client: DuckDBClient) -> None:
        """Test upsert_to_postgres with empty DataFrame returns 0."""
        empty_df = pl.DataFrame({"id": [], "name": []})

        result = client.upsert_to_postgres(empty_df, "test_table", "id")

        assert result == 0

    def test_upsert_to_postgres_without_attach_raises(self, client: DuckDBClient) -> None:
        """Test upsert_to_postgres without attach_postgres raises error."""
        df = pl.DataFrame({"id": [1], "name": ["test"]})

        with pytest.raises(RuntimeError, match="PostgreSQL not attached"):
            client.upsert_to_postgres(df, "test_table", "id")
