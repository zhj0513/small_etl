"""Unit tests for DuckDB client."""

import polars as pl
import pytest
from omegaconf import OmegaConf

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


class TestDuckDBClientTransform:
    """Tests for DuckDB client transform methods."""

    @pytest.fixture
    def client(self) -> DuckDBClient:
        """Create DuckDB client instance."""
        return DuckDBClient()

    def test_register_dataframe(self, client: DuckDBClient) -> None:
        """Test register_dataframe creates a view."""
        df = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

        client.register_dataframe(df, "test_view")

        result = client.query("SELECT * FROM test_view")
        assert len(result) == 2
        assert result["name"][0] == "Alice"

        # Clean up
        client.unregister("test_view")

    def test_unregister(self, client: DuckDBClient) -> None:
        """Test unregister removes a view."""
        df = pl.DataFrame({"id": [1]})
        client.register_dataframe(df, "test_view")

        client.unregister("test_view")

        with pytest.raises(Exception):
            client.query("SELECT * FROM test_view")

    def test_create_transformed_table_basic_types(self, client: DuckDBClient) -> None:
        """Test create_transformed_table with basic type conversions."""
        df = pl.DataFrame(
            {
                "str_id": ["1", "2"],
                "str_value": ["100.5", "200.5"],
                "str_count": ["10", "20"],
            }
        )
        client.register_dataframe(df, "raw_data")

        columns_config = OmegaConf.create(
            [
                {"csv_name": "str_id", "name": "id", "dtype": "Int64"},
                {"csv_name": "str_value", "name": "value", "dtype": "Float64"},
                {"csv_name": "str_count", "name": "count", "dtype": "Int32"},
            ]
        )

        row_count = client.create_transformed_table("raw_data", "transformed_data", columns_config)

        assert row_count == 2

        result = client.query("SELECT * FROM transformed_data")
        assert "id" in result.columns
        assert "value" in result.columns
        assert "count" in result.columns

        # Clean up
        client.unregister("raw_data")
        client.drop_table("transformed_data")

    def test_create_transformed_table_datetime(self, client: DuckDBClient) -> None:
        """Test create_transformed_table with datetime conversion."""
        df = pl.DataFrame({"date_str": ["2025-12-22 14:30:00", "2025-12-23 15:00:00"]})
        client.register_dataframe(df, "raw_dates")

        columns_config = OmegaConf.create(
            [{"csv_name": "date_str", "name": "created_at", "dtype": "Datetime", "format": "%Y-%m-%d %H:%M:%S"}]
        )

        row_count = client.create_transformed_table("raw_dates", "transformed_dates", columns_config)

        assert row_count == 2

        result = client.query("SELECT * FROM transformed_dates")
        assert "created_at" in result.columns

        # Clean up
        client.unregister("raw_dates")
        client.drop_table("transformed_dates")

    def test_create_transformed_table_decimal(self, client: DuckDBClient) -> None:
        """Test create_transformed_table with decimal conversion."""
        df = pl.DataFrame({"amount_str": ["1234.56", "7890.12"]})
        client.register_dataframe(df, "raw_amounts")

        columns_config = OmegaConf.create(
            [{"csv_name": "amount_str", "name": "amount", "dtype": "Decimal", "precision": 20, "scale": 2}]
        )

        row_count = client.create_transformed_table("raw_amounts", "transformed_amounts", columns_config)

        assert row_count == 2

        result = client.query("SELECT * FROM transformed_amounts")
        assert "amount" in result.columns

        # Clean up
        client.unregister("raw_amounts")
        client.drop_table("transformed_amounts")

    def test_drop_table(self, client: DuckDBClient) -> None:
        """Test drop_table removes a table."""
        client._conn.execute("CREATE TABLE to_drop (id INT)")
        client._conn.execute("INSERT INTO to_drop VALUES (1)")

        client.drop_table("to_drop")

        with pytest.raises(Exception):
            client.query("SELECT * FROM to_drop")

    def test_drop_table_nonexistent(self, client: DuckDBClient) -> None:
        """Test drop_table on nonexistent table does not raise."""
        # Should not raise
        client.drop_table("nonexistent_table")

    def test_upsert_from_table_without_attach_raises(self, client: DuckDBClient) -> None:
        """Test upsert_from_table without attach_postgres raises error."""
        client._conn.execute("CREATE TABLE source (id INT)")

        with pytest.raises(RuntimeError, match="PostgreSQL not attached"):
            client.upsert_from_table("source", "target", "id", ["id"])
