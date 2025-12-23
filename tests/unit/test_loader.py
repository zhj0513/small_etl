"""Tests for loader service."""

from datetime import datetime
from unittest.mock import MagicMock

import polars as pl
import pytest

from small_etl.domain.enums import AccountType, Direction, OffsetFlag
from small_etl.services.loader import LoaderService, LoadResult


class TestLoaderService:
    """Tests for LoaderService."""

    @pytest.fixture
    def mock_repository(self):
        """Create a mock repository."""
        return MagicMock()

    @pytest.fixture
    def mock_duckdb(self):
        """Create a mock DuckDB client."""
        return MagicMock()

    @pytest.fixture
    def loader(self, mock_repository, mock_duckdb):
        """Create a LoaderService with mock repository and DuckDB client."""
        return LoaderService(
            repository=mock_repository,
            duckdb_client=mock_duckdb,
            database_url="postgresql://test:test@localhost/test",
        )

    @pytest.fixture
    def sample_asset_df(self):
        """Sample asset DataFrame."""
        return pl.DataFrame({
            "account_id": ["10000000001", "10000000002"],
            "account_type": [AccountType.SECURITY, AccountType.CREDIT],
            "cash": [100000.00, 200000.00],
            "frozen_cash": [5000.00, 10000.00],
            "market_value": [200000.00, 300000.00],
            "total_asset": [305000.00, 510000.00],
            "updated_at": [datetime(2025, 12, 22, 14, 30), datetime(2025, 12, 22, 14, 30)],
        })

    @pytest.fixture
    def sample_trade_df(self):
        """Sample trade DataFrame."""
        return pl.DataFrame({
            "account_id": ["10000000001"],
            "account_type": [AccountType.SECURITY],
            "traded_id": ["T20251222000001"],
            "stock_code": ["600000"],
            "traded_time": [datetime(2025, 12, 22, 10, 30)],
            "traded_price": [15.50],
            "traded_volume": [1000],
            "traded_amount": [15500.00],
            "strategy_name": ["策略A"],
            "order_remark": ["正常交易"],
            "direction": [Direction.NA],
            "offset_flag": [OffsetFlag.OPEN],
            "created_at": [datetime(2025, 12, 22, 10, 30)],
            "updated_at": [datetime(2025, 12, 22, 14, 30)],
        })

    def test_load_assets_empty_dataframe(self, loader, mock_duckdb):
        """Test loading empty asset DataFrame."""
        empty_df = pl.DataFrame({
            "account_id": [],
            "account_type": [],
            "cash": [],
            "frozen_cash": [],
            "market_value": [],
            "total_asset": [],
            "updated_at": [],
        })

        result = loader.load_assets(empty_df)

        assert result.success is True
        assert result.total_rows == 0
        assert result.loaded_count == 0
        mock_duckdb.upsert_to_postgres.assert_not_called()

    def test_load_trades_empty_dataframe(self, loader, mock_duckdb):
        """Test loading empty trade DataFrame."""
        empty_df = pl.DataFrame({
            "account_id": [],
            "account_type": [],
            "traded_id": [],
            "stock_code": [],
            "traded_time": [],
            "traded_price": [],
            "traded_volume": [],
            "traded_amount": [],
            "strategy_name": [],
            "order_remark": [],
            "direction": [],
            "offset_flag": [],
            "created_at": [],
            "updated_at": [],
        })

        result = loader.load_trades(empty_df)

        assert result.success is True
        assert result.total_rows == 0
        assert result.loaded_count == 0
        mock_duckdb.upsert_to_postgres.assert_not_called()

    def test_load_assets_error(self, loader, mock_duckdb, sample_asset_df):
        """Test loading assets with error."""
        mock_duckdb.upsert_to_postgres.side_effect = Exception("Database error")

        result = loader.load_assets(sample_asset_df)

        assert result.success is False
        assert result.error_message == "Database error"
        assert result.failed_count > 0

    def test_load_trades_error(self, loader, mock_duckdb, sample_trade_df):
        """Test loading trades with error."""
        mock_duckdb.upsert_to_postgres.side_effect = Exception("Database error")

        result = loader.load_trades(sample_trade_df)

        assert result.success is False
        assert result.error_message == "Database error"
        assert result.failed_count > 0

    def test_load_assets_with_batching(self, loader, mock_duckdb, sample_asset_df):
        """Test loading assets with small batch size."""
        mock_duckdb.upsert_to_postgres.return_value = 1

        result = loader.load_assets(sample_asset_df, batch_size=1)

        assert result.success is True
        assert result.loaded_count == 2
        assert mock_duckdb.upsert_to_postgres.call_count == 2

    def test_load_trades_with_batching(self, loader, mock_duckdb, sample_trade_df):
        """Test loading trades with small batch size."""
        mock_duckdb.upsert_to_postgres.return_value = 1

        result = loader.load_trades(sample_trade_df, batch_size=1)

        assert result.success is True
        assert result.loaded_count == 1


class TestLoadResult:
    """Tests for LoadResult dataclass."""

    def test_load_result_defaults(self):
        """Test LoadResult default values."""
        result = LoadResult(success=True, total_rows=100, loaded_count=100)

        assert result.failed_count == 0
        assert result.error_message is None

    def test_load_result_with_error(self):
        """Test LoadResult with error."""
        result = LoadResult(
            success=False,
            total_rows=100,
            loaded_count=50,
            failed_count=50,
            error_message="Database connection failed",
        )

        assert result.success is False
        assert result.failed_count == 50
        assert result.error_message == "Database connection failed"
