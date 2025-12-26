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

    def test_init(self, client: DuckDBClient) -> None:
        """Test DuckDB client initialization."""
        assert client._pg_attached is False

    def test_query_returns_polars(self, client: DuckDBClient) -> None:
        """Test that query returns Polars DataFrame."""
        client._conn.execute("CREATE TABLE test_table (id INT, name VARCHAR, value INT)")
        client._conn.execute("INSERT INTO test_table VALUES (1, 'Alice', 100), (2, 'Bob', 200)")

        result = client.query("SELECT * FROM test_table WHERE value > 150")

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 1
        assert result["name"][0] == "Bob"

    def test_context_manager(self) -> None:
        """Test DuckDB client as context manager."""
        with DuckDBClient() as client:
            client._conn.execute("CREATE TABLE test (id INT, value INT)")
            client._conn.execute("INSERT INTO test VALUES (1, 100)")
            result = client.query("SELECT COUNT(*) as cnt FROM test")
            assert result["cnt"][0] == 1

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

    def test_query_asset_statistics_without_attach_raises(self, client: DuckDBClient) -> None:
        """Test query_asset_statistics without attach_postgres raises error."""
        with pytest.raises(RuntimeError, match="PostgreSQL not attached"):
            client.query_asset_statistics()

    def test_query_trade_statistics_without_attach_raises(self, client: DuckDBClient) -> None:
        """Test query_trade_statistics without attach_postgres raises error."""
        with pytest.raises(RuntimeError, match="PostgreSQL not attached"):
            client.query_trade_statistics()
