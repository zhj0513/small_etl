"""Unit tests for PostgresRepository."""

from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from sqlmodel import Session

from small_etl.data_access.postgres_repository import (
    PostgresRepository,
    polars_to_assets,
    polars_to_trades,
)
from small_etl.domain.enums import AccountType, Direction, OffsetFlag
from small_etl.domain.models import Asset, Trade


class TestPolarsToAssets:
    """Tests for polars_to_assets function."""

    def test_convert_valid_dataframe(self):
        """Test converting a valid Polars DataFrame to Asset list."""
        df = pl.DataFrame(
            {
                "account_id": ["10000000001", "10000000002"],
                "account_type": [1, 2],
                "cash": [100000.00, 200000.00],
                "frozen_cash": [5000.00, 10000.00],
                "market_value": [200000.00, 300000.00],
                "total_asset": [305000.00, 510000.00],
                "updated_at": [
                    datetime(2025, 12, 22, 14, 30, 0),
                    datetime(2025, 12, 22, 14, 25, 0),
                ],
            }
        )

        assets = polars_to_assets(df)

        assert len(assets) == 2
        assert assets[0].account_id == "10000000001"
        assert assets[0].account_type == 1
        assert assets[0].cash == Decimal("100000.00")
        assert assets[0].frozen_cash == Decimal("5000.00")
        assert assets[0].market_value == Decimal("200000.00")
        assert assets[0].total_asset == Decimal("305000.00")
        assert assets[0].updated_at == datetime(2025, 12, 22, 14, 30, 0)

    def test_convert_empty_dataframe(self):
        """Test converting empty DataFrame returns empty list."""
        df = pl.DataFrame(
            schema={
                "account_id": pl.Utf8,
                "account_type": pl.Int32,
                "cash": pl.Float64,
                "frozen_cash": pl.Float64,
                "market_value": pl.Float64,
                "total_asset": pl.Float64,
                "updated_at": pl.Datetime,
            }
        )

        assets = polars_to_assets(df)

        assert len(assets) == 0


class TestPolarsToTrades:
    """Tests for polars_to_trades function."""

    def test_convert_valid_dataframe(self):
        """Test converting a valid Polars DataFrame to Trade list."""
        df = pl.DataFrame(
            {
                "account_id": ["10000000001"],
                "account_type": [1],
                "traded_id": ["T20251222000001"],
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

        trades = polars_to_trades(df)

        assert len(trades) == 1
        assert trades[0].account_id == "10000000001"
        assert trades[0].traded_id == "T20251222000001"
        assert trades[0].stock_code == "600000"
        assert trades[0].traded_price == Decimal("15.50")
        assert trades[0].traded_volume == 1000
        assert trades[0].traded_amount == Decimal("15500.00")

    def test_convert_with_none_order_remark(self):
        """Test converting DataFrame with None order_remark."""
        df = pl.DataFrame(
            {
                "account_id": ["10000000001"],
                "account_type": [1],
                "traded_id": ["T20251222000001"],
                "stock_code": ["600000"],
                "traded_time": [datetime(2025, 12, 22, 10, 30, 0)],
                "traded_price": [15.50],
                "traded_volume": [1000],
                "traded_amount": [15500.00],
                "strategy_name": ["策略A"],
                "order_remark": [None],
                "direction": [0],
                "offset_flag": [48],
                "created_at": [datetime(2025, 12, 22, 10, 30, 0)],
                "updated_at": [datetime(2025, 12, 22, 14, 30, 0)],
            }
        )

        trades = polars_to_trades(df)

        assert len(trades) == 1
        assert trades[0].order_remark is None


class TestPostgresRepositoryMocked:
    """Unit tests for PostgresRepository using mocks."""

    @pytest.fixture
    def mock_engine(self):
        """Create a mock database engine."""
        return MagicMock()

    @pytest.fixture
    def repo_with_mock(self, mock_engine):
        """Create a PostgresRepository with mocked engine."""
        with patch("small_etl.data_access.postgres_repository.create_engine") as mock_create:
            mock_create.return_value = mock_engine
            repo = PostgresRepository("postgresql://test:test@localhost/test")
            return repo

    def test_init(self, repo_with_mock):
        """Test repository initialization."""
        assert repo_with_mock._engine is not None

    def test_get_session(self, repo_with_mock, mock_engine):
        """Test get_session returns a Session."""
        with patch("small_etl.data_access.postgres_repository.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value = mock_session

            session = repo_with_mock.get_session()

            mock_session_class.assert_called_once_with(repo_with_mock._engine)

    def test_close(self, repo_with_mock, mock_engine):
        """Test close disposes of the engine."""
        repo_with_mock.close()
        mock_engine.dispose.assert_called_once()

    def test_bulk_insert_assets(self, repo_with_mock):
        """Test bulk_insert_assets inserts records."""
        with patch("small_etl.data_access.postgres_repository.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_session_class.return_value.__exit__ = MagicMock(return_value=False)

            assets = [
                Asset(
                    account_id="10000000001",
                    account_type=1,
                    cash=Decimal("100000"),
                    frozen_cash=Decimal("5000"),
                    market_value=Decimal("200000"),
                    total_asset=Decimal("305000"),
                    updated_at=datetime.now(),
                )
            ]

            result = repo_with_mock.bulk_insert_assets(assets)

            assert result == 1
            mock_session.add_all.assert_called_once_with(assets)
            mock_session.commit.assert_called_once()

    def test_bulk_insert_trades(self, repo_with_mock):
        """Test bulk_insert_trades inserts records."""
        with patch("small_etl.data_access.postgres_repository.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_session_class.return_value.__exit__ = MagicMock(return_value=False)

            trades = [
                Trade(
                    account_id="10000000001",
                    account_type=1,
                    traded_id="T001",
                    stock_code="600000",
                    traded_time=datetime.now(),
                    traded_price=Decimal("15.50"),
                    traded_volume=1000,
                    traded_amount=Decimal("15500"),
                    strategy_name="策略A",
                    direction=0,
                    offset_flag=48,
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            ]

            result = repo_with_mock.bulk_insert_trades(trades)

            assert result == 1
            mock_session.add_all.assert_called_once_with(trades)
            mock_session.commit.assert_called_once()

    def test_upsert_assets_empty_list(self, repo_with_mock):
        """Test upsert_assets with empty list returns 0."""
        result = repo_with_mock.upsert_assets([])
        assert result == 0

    def test_upsert_trades_empty_list(self, repo_with_mock):
        """Test upsert_trades with empty list returns 0."""
        result = repo_with_mock.upsert_trades([])
        assert result == 0

    def test_upsert_assets_insert_new(self, repo_with_mock):
        """Test upsert_assets inserts new records."""
        with patch("small_etl.data_access.postgres_repository.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_session_class.return_value.__exit__ = MagicMock(return_value=False)

            # Mock exec to return None (no existing record)
            mock_result = MagicMock()
            mock_result.first.return_value = None
            mock_session.exec.return_value = mock_result

            assets = [
                Asset(
                    account_id="10000000001",
                    account_type=1,
                    cash=Decimal("100000"),
                    frozen_cash=Decimal("5000"),
                    market_value=Decimal("200000"),
                    total_asset=Decimal("305000"),
                    updated_at=datetime.now(),
                )
            ]

            result = repo_with_mock.upsert_assets(assets)

            assert result == 1
            mock_session.add.assert_called_once()
            mock_session.commit.assert_called_once()

    def test_upsert_assets_update_existing(self, repo_with_mock):
        """Test upsert_assets updates existing records."""
        with patch("small_etl.data_access.postgres_repository.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_session_class.return_value.__exit__ = MagicMock(return_value=False)

            # Mock existing record
            existing_asset = MagicMock(spec=Asset)
            mock_result = MagicMock()
            mock_result.first.return_value = existing_asset
            mock_session.exec.return_value = mock_result

            new_asset = Asset(
                account_id="10000000001",
                account_type=2,
                cash=Decimal("200000"),
                frozen_cash=Decimal("10000"),
                market_value=Decimal("400000"),
                total_asset=Decimal("610000"),
                updated_at=datetime.now(),
            )

            result = repo_with_mock.upsert_assets([new_asset])

            assert result == 1
            assert existing_asset.account_type == 2
            assert existing_asset.cash == Decimal("200000")
            mock_session.commit.assert_called_once()

    def test_upsert_trades_insert_new(self, repo_with_mock):
        """Test upsert_trades inserts new records."""
        with patch("small_etl.data_access.postgres_repository.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_session_class.return_value.__exit__ = MagicMock(return_value=False)

            # Mock exec to return None (no existing record)
            mock_result = MagicMock()
            mock_result.first.return_value = None
            mock_session.exec.return_value = mock_result

            trades = [
                Trade(
                    account_id="10000000001",
                    account_type=1,
                    traded_id="T001",
                    stock_code="600000",
                    traded_time=datetime.now(),
                    traded_price=Decimal("15.50"),
                    traded_volume=1000,
                    traded_amount=Decimal("15500"),
                    strategy_name="策略A",
                    direction=0,
                    offset_flag=48,
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            ]

            result = repo_with_mock.upsert_trades(trades)

            assert result == 1
            mock_session.add.assert_called_once()
            mock_session.commit.assert_called_once()

    def test_upsert_trades_update_existing(self, repo_with_mock):
        """Test upsert_trades updates existing records."""
        with patch("small_etl.data_access.postgres_repository.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_session_class.return_value.__exit__ = MagicMock(return_value=False)

            # Mock existing record
            existing_trade = MagicMock(spec=Trade)
            mock_result = MagicMock()
            mock_result.first.return_value = existing_trade
            mock_session.exec.return_value = mock_result

            new_trade = Trade(
                account_id="10000000001",
                account_type=1,
                traded_id="T001",
                stock_code="600001",
                traded_time=datetime.now(),
                traded_price=Decimal("20.00"),
                traded_volume=2000,
                traded_amount=Decimal("40000"),
                strategy_name="策略B",
                direction=48,
                offset_flag=49,
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )

            result = repo_with_mock.upsert_trades([new_trade])

            assert result == 1
            mock_session.commit.assert_called_once()

    def test_get_asset_by_account_id(self, repo_with_mock):
        """Test get_asset_by_account_id returns asset."""
        with patch("small_etl.data_access.postgres_repository.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_session_class.return_value.__exit__ = MagicMock(return_value=False)

            expected_asset = Asset(
                account_id="10000000001",
                account_type=1,
                cash=Decimal("100000"),
                frozen_cash=Decimal("5000"),
                market_value=Decimal("200000"),
                total_asset=Decimal("305000"),
                updated_at=datetime.now(),
            )
            mock_result = MagicMock()
            mock_result.first.return_value = expected_asset
            mock_session.exec.return_value = mock_result

            result = repo_with_mock.get_asset_by_account_id("10000000001")

            assert result == expected_asset

    def test_get_all_assets_empty(self, repo_with_mock):
        """Test get_all_assets returns empty DataFrame when no data."""
        with patch("small_etl.data_access.postgres_repository.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_session_class.return_value.__exit__ = MagicMock(return_value=False)

            mock_result = MagicMock()
            mock_result.all.return_value = []
            mock_session.exec.return_value = mock_result

            result = repo_with_mock.get_all_assets()

            assert isinstance(result, pl.DataFrame)
            assert len(result) == 0
            assert "account_id" in result.columns

    def test_get_all_assets_with_data(self, repo_with_mock):
        """Test get_all_assets returns DataFrame with data."""
        with patch("small_etl.data_access.postgres_repository.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_session_class.return_value.__exit__ = MagicMock(return_value=False)

            asset = MagicMock()
            asset.id = 1
            asset.account_id = "10000000001"
            asset.account_type = 1
            asset.cash = Decimal("100000")
            asset.frozen_cash = Decimal("5000")
            asset.market_value = Decimal("200000")
            asset.total_asset = Decimal("305000")
            asset.updated_at = datetime(2025, 12, 22, 14, 30, 0)

            mock_result = MagicMock()
            mock_result.all.return_value = [asset]
            mock_session.exec.return_value = mock_result

            result = repo_with_mock.get_all_assets()

            assert isinstance(result, pl.DataFrame)
            assert len(result) == 1
            assert result["account_id"][0] == "10000000001"

    def test_get_all_trades_empty(self, repo_with_mock):
        """Test get_all_trades returns empty DataFrame when no data."""
        with patch("small_etl.data_access.postgres_repository.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_session_class.return_value.__exit__ = MagicMock(return_value=False)

            mock_result = MagicMock()
            mock_result.all.return_value = []
            mock_session.exec.return_value = mock_result

            result = repo_with_mock.get_all_trades()

            assert isinstance(result, pl.DataFrame)
            assert len(result) == 0
            assert "traded_id" in result.columns

    def test_get_all_trades_with_data(self, repo_with_mock):
        """Test get_all_trades returns DataFrame with data."""
        with patch("small_etl.data_access.postgres_repository.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_session_class.return_value.__exit__ = MagicMock(return_value=False)

            trade = MagicMock()
            trade.id = 1
            trade.account_id = "10000000001"
            trade.account_type = 1
            trade.traded_id = "T001"
            trade.stock_code = "600000"
            trade.traded_time = datetime(2025, 12, 22, 10, 30, 0)
            trade.traded_price = Decimal("15.50")
            trade.traded_volume = 1000
            trade.traded_amount = Decimal("15500")
            trade.strategy_name = "策略A"
            trade.order_remark = "正常交易"
            trade.direction = 0
            trade.offset_flag = 48
            trade.created_at = datetime(2025, 12, 22, 10, 30, 0)
            trade.updated_at = datetime(2025, 12, 22, 14, 30, 0)

            mock_result = MagicMock()
            mock_result.all.return_value = [trade]
            mock_session.exec.return_value = mock_result

            result = repo_with_mock.get_all_trades()

            assert isinstance(result, pl.DataFrame)
            assert len(result) == 1
            assert result["traded_id"][0] == "T001"
