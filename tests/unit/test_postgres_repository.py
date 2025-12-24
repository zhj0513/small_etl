"""Unit tests for PostgresRepository."""

from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from small_etl.data_access.postgres_repository import PostgresRepository


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
