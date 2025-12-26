"""Unit tests for ExtractorService."""

from datetime import datetime

import polars as pl
import pytest
from omegaconf import OmegaConf

from small_etl.data_access.duckdb_client import DuckDBClient
from small_etl.services.extractor import ExtractorService


class TestExtractorService:
    """Tests for ExtractorService."""

    @pytest.fixture
    def duckdb_client(self) -> DuckDBClient:
        """Create DuckDB client instance."""
        return DuckDBClient()

    @pytest.fixture
    def assets_config(self):
        """Create sample assets config."""
        return OmegaConf.create(
            {
                "assets": {
                    "columns": [
                        {"csv_name": "id", "name": "id", "dtype": "Int64"},
                        {"csv_name": "account_id", "name": "account_id", "dtype": "Utf8"},
                        {"csv_name": "account_type", "name": "account_type", "dtype": "Int32"},
                        {"csv_name": "cash", "name": "cash", "dtype": "Float64"},
                        {"csv_name": "frozen_cash", "name": "frozen_cash", "dtype": "Float64"},
                        {"csv_name": "market_value", "name": "market_value", "dtype": "Float64"},
                        {"csv_name": "total_asset", "name": "total_asset", "dtype": "Float64"},
                        {
                            "csv_name": "updated_at",
                            "name": "updated_at",
                            "dtype": "Datetime",
                            "format": "%Y-%m-%d %H:%M:%S",
                        },
                    ]
                }
            }
        )

    @pytest.fixture
    def trades_config(self):
        """Create sample trades config."""
        return OmegaConf.create(
            {
                "trades": {
                    "columns": [
                        {"csv_name": "id", "name": "id", "dtype": "Int64"},
                        {"csv_name": "account_id", "name": "account_id", "dtype": "Utf8"},
                        {"csv_name": "account_type", "name": "account_type", "dtype": "Int32"},
                        {"csv_name": "traded_id", "name": "traded_id", "dtype": "Utf8"},
                        {"csv_name": "stock_code", "name": "stock_code", "dtype": "Utf8"},
                        {
                            "csv_name": "traded_time",
                            "name": "traded_time",
                            "dtype": "Datetime",
                            "format": "%Y-%m-%d %H:%M:%S",
                        },
                        {"csv_name": "traded_price", "name": "traded_price", "dtype": "Float64"},
                        {"csv_name": "traded_volume", "name": "traded_volume", "dtype": "Int64"},
                        {"csv_name": "traded_amount", "name": "traded_amount", "dtype": "Float64"},
                        {"csv_name": "strategy_name", "name": "strategy_name", "dtype": "Utf8"},
                        {"csv_name": "order_remark", "name": "order_remark", "dtype": "Utf8"},
                        {"csv_name": "direction", "name": "direction", "dtype": "Int32"},
                        {"csv_name": "offset_flag", "name": "offset_flag", "dtype": "Int32"},
                        {
                            "csv_name": "created_at",
                            "name": "created_at",
                            "dtype": "Datetime",
                            "format": "%Y-%m-%d %H:%M:%S",
                        },
                        {
                            "csv_name": "updated_at",
                            "name": "updated_at",
                            "dtype": "Datetime",
                            "format": "%Y-%m-%d %H:%M:%S",
                        },
                    ]
                }
            }
        )

    def test_init(self, duckdb_client: DuckDBClient):
        """Test ExtractorService initialization."""
        extractor = ExtractorService(duckdb_client=duckdb_client)
        assert extractor._config is None
        assert extractor._duckdb is duckdb_client

    def test_init_with_config(self, assets_config, duckdb_client: DuckDBClient):
        """Test ExtractorService initialization with config."""
        extractor = ExtractorService(config=assets_config, duckdb_client=duckdb_client)
        assert extractor._config is not None

    def test_transform_without_duckdb_raises(self):
        """Test transform without DuckDB client raises error."""
        extractor = ExtractorService(config=None, duckdb_client=None)
        df = pl.DataFrame({"id": [1]})

        with pytest.raises(RuntimeError, match="DuckDB client is required"):
            extractor.transform(df, "asset")

    def test_transform_asset_no_config(self, duckdb_client: DuckDBClient):
        """Test transform asset without config (pass-through)."""
        df = pl.DataFrame(
            {
                "id": [1],
                "account_id": ["10000000001"],
                "account_type": [1],
                "cash": [100000.00],
                "frozen_cash": [5000.00],
                "market_value": [200000.00],
                "total_asset": [305000.00],
                "updated_at": [datetime(2025, 12, 22, 14, 30, 0)],
            }
        )

        extractor = ExtractorService(config=None, duckdb_client=duckdb_client)
        table_name = extractor.transform(df, "asset")

        assert table_name == "_transformed_asset"

        # Verify table was created
        result = duckdb_client.query(f"SELECT COUNT(*) as cnt FROM {table_name}")
        assert result["cnt"][0] == 1

        # Clean up
        duckdb_client.drop_table(table_name)

    def test_transform_asset_with_config(self, assets_config, duckdb_client: DuckDBClient):
        """Test transform asset with config transformation."""
        raw_df = pl.DataFrame(
            {
                "id": ["1"],
                "account_id": ["10000000001"],
                "account_type": ["1"],
                "cash": ["100000.00"],
                "frozen_cash": ["5000.00"],
                "market_value": ["200000.00"],
                "total_asset": ["305000.00"],
                "updated_at": ["2025-12-22 14:30:00"],
            }
        )

        extractor = ExtractorService(config=assets_config, duckdb_client=duckdb_client)
        table_name = extractor.transform(raw_df, "asset")

        assert table_name == "_transformed_asset"

        # Verify table was created with correct data
        result = duckdb_client.query(f"SELECT * FROM {table_name}")
        assert len(result) == 1
        assert "account_id" in result.columns
        assert "id" in result.columns

        # Clean up
        duckdb_client.drop_table(table_name)

    def test_transform_trade_no_config(self, duckdb_client: DuckDBClient):
        """Test transform trade without config (pass-through)."""
        df = pl.DataFrame(
            {
                "id": [1],
                "account_id": ["10000000001"],
                "account_type": [1],
                "traded_id": ["T001"],
                "stock_code": ["600000"],
                "traded_time": [datetime(2025, 12, 22, 10, 30, 0)],
                "traded_price": [15.50],
                "traded_volume": [1000],
                "traded_amount": [15500.00],
                "strategy_name": ["策略A"],
                "order_remark": ["正常交易"],
                "direction": [0],
                "offset_flag": [48],
                "created_at": [datetime(2025, 12, 22, 10, 30, 0)],
                "updated_at": [datetime(2025, 12, 22, 14, 30, 0)],
            }
        )

        extractor = ExtractorService(config=None, duckdb_client=duckdb_client)
        table_name = extractor.transform(df, "trade")

        assert table_name == "_transformed_trade"

        # Verify table was created
        result = duckdb_client.query(f"SELECT COUNT(*) as cnt FROM {table_name}")
        assert result["cnt"][0] == 1

        # Clean up
        duckdb_client.drop_table(table_name)

    def test_transform_trade_with_config(self, trades_config, duckdb_client: DuckDBClient):
        """Test transform trade with config transformation."""
        raw_df = pl.DataFrame(
            {
                "id": ["1"],
                "account_id": ["10000000001"],
                "account_type": ["1"],
                "traded_id": ["T001"],
                "stock_code": ["600000"],
                "traded_time": ["2025-12-22 10:30:00"],
                "traded_price": ["15.50"],
                "traded_volume": ["1000"],
                "traded_amount": ["15500.00"],
                "strategy_name": ["策略A"],
                "order_remark": ["正常交易"],
                "direction": ["0"],
                "offset_flag": ["48"],
                "created_at": ["2025-12-22 10:30:00"],
                "updated_at": ["2025-12-22 14:30:00"],
            }
        )

        extractor = ExtractorService(config=trades_config, duckdb_client=duckdb_client)
        table_name = extractor.transform(raw_df, "trade")

        assert table_name == "_transformed_trade"

        # Verify table was created with correct data
        result = duckdb_client.query(f"SELECT * FROM {table_name}")
        assert len(result) == 1
        assert "traded_id" in result.columns

        # Clean up
        duckdb_client.drop_table(table_name)

    def test_transform_returns_table_name(self, assets_config, duckdb_client: DuckDBClient):
        """Test that transform returns the correct table name."""
        df = pl.DataFrame(
            {
                "id": ["1"],
                "account_id": ["test"],
                "account_type": ["1"],
                "cash": ["100"],
                "frozen_cash": ["10"],
                "market_value": ["200"],
                "total_asset": ["310"],
                "updated_at": ["2025-12-22 14:30:00"],
            }
        )

        extractor = ExtractorService(config=assets_config, duckdb_client=duckdb_client)

        asset_table = extractor.transform(df, "asset")
        assert asset_table == "_transformed_asset"
        duckdb_client.drop_table(asset_table)

    def test_transform_cleans_up_raw_view(self, assets_config, duckdb_client: DuckDBClient):
        """Test that transform cleans up the raw view after creating transformed table."""
        df = pl.DataFrame(
            {
                "id": ["1"],
                "account_id": ["test"],
                "account_type": ["1"],
                "cash": ["100"],
                "frozen_cash": ["10"],
                "market_value": ["200"],
                "total_asset": ["310"],
                "updated_at": ["2025-12-22 14:30:00"],
            }
        )

        extractor = ExtractorService(config=assets_config, duckdb_client=duckdb_client)
        table_name = extractor.transform(df, "asset")

        # The raw view should be unregistered
        # Trying to query it should fail
        with pytest.raises(Exception):
            duckdb_client.query("SELECT * FROM _raw_asset")

        # But the transformed table should exist
        result = duckdb_client.query(f"SELECT COUNT(*) as cnt FROM {table_name}")
        assert result["cnt"][0] == 1

        # Clean up
        duckdb_client.drop_table(table_name)
