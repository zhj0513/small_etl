"""Unit tests for analytics service."""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from small_etl.domain.enums import OffsetFlag
from small_etl.services.analytics import AnalyticsService


class TestAnalyticsService:
    """Tests for AnalyticsService."""

    @pytest.fixture
    def mock_duckdb(self) -> MagicMock:
        """Create mock DuckDB client."""
        return MagicMock()

    @pytest.fixture
    def analytics(self, mock_duckdb: MagicMock) -> AnalyticsService:
        """Create analytics instance with mock DuckDB."""
        return AnalyticsService(duckdb_client=mock_duckdb)

    def test_asset_statistics_from_db(self, analytics: AnalyticsService, mock_duckdb: MagicMock) -> None:
        """Test computing asset statistics from database via DuckDB."""
        mock_duckdb.query_asset_statistics.return_value = {
            "total_records": 3,
            "total_cash": 850000.00,
            "total_frozen_cash": 50000.00,
            "total_market_value": 1380000.00,
            "total_assets": 2280000.00,
            "avg_cash": 283333.33,
            "avg_total_asset": 760000.00,
            "by_account_type": {
                1: {"count": 2, "sum_cash": 600000.00, "sum_total": 1655000.00, "avg_total": 827500.00},
                2: {"count": 1, "sum_cash": 250000.00, "sum_total": 625000.00, "avg_total": 625000.00},
            },
        }

        stats = analytics.asset_statistics_from_db()

        assert stats.total_records == 3
        assert stats.total_cash == Decimal("850000.0")
        assert stats.total_assets == Decimal("2280000.0")
        assert stats.avg_cash > 0
        assert stats.avg_total_asset > 0
        assert len(stats.by_account_type) == 2
        mock_duckdb.query_asset_statistics.assert_called_once()

    def test_asset_statistics_from_db_empty(self, analytics: AnalyticsService, mock_duckdb: MagicMock) -> None:
        """Test computing statistics for empty database."""
        mock_duckdb.query_asset_statistics.return_value = {
            "total_records": 0,
            "total_cash": 0,
            "total_frozen_cash": 0,
            "total_market_value": 0,
            "total_assets": 0,
            "avg_cash": 0,
            "avg_total_asset": 0,
            "by_account_type": {},
        }

        stats = analytics.asset_statistics_from_db()

        assert stats.total_records == 0
        assert stats.total_cash == Decimal("0")
        assert stats.total_assets == Decimal("0")

    def test_trade_statistics_from_db(self, analytics: AnalyticsService, mock_duckdb: MagicMock) -> None:
        """Test computing trade statistics from database via DuckDB."""
        mock_duckdb.query_trade_statistics.return_value = {
            "total_records": 3,
            "total_volume": 3500,
            "total_amount": 115250.00,
            "avg_price": 32.50,
            "avg_volume": 1166.67,
            "by_account_type": {
                1: {"count": 2, "sum_volume": 1500, "sum_amount": 29650.00},
                2: {"count": 1, "sum_volume": 2000, "sum_amount": 85600.00},
            },
            "by_offset_flag": {
                OffsetFlag.OPEN: {"count": 2, "sum_volume": 3000, "sum_amount": 101100.00},
                OffsetFlag.CLOSE: {"count": 1, "sum_volume": 500, "sum_amount": 14150.00},
            },
            "by_strategy": {
                "策略A": {"count": 1, "sum_volume": 1000, "sum_amount": 15500.00},
                "量化1号": {"count": 1, "sum_volume": 500, "sum_amount": 14150.00},
                "趋势跟踪": {"count": 1, "sum_volume": 2000, "sum_amount": 85600.00},
            },
        }

        stats = analytics.trade_statistics_from_db()

        assert stats.total_records == 3
        assert stats.total_volume == 3500
        assert stats.total_amount == Decimal("115250.0")
        assert stats.avg_price > 0
        assert stats.avg_volume > 0
        assert len(stats.by_strategy) == 3
        mock_duckdb.query_trade_statistics.assert_called_once()

    def test_trade_statistics_by_offset_flag(self, analytics: AnalyticsService, mock_duckdb: MagicMock) -> None:
        """Test trade statistics breakdown by offset flag."""
        mock_duckdb.query_trade_statistics.return_value = {
            "total_records": 3,
            "total_volume": 3500,
            "total_amount": 115250.00,
            "avg_price": 32.50,
            "avg_volume": 1166.67,
            "by_account_type": {},
            "by_offset_flag": {
                OffsetFlag.OPEN: {"count": 2, "sum_volume": 3000, "sum_amount": 101100.00},
                OffsetFlag.CLOSE: {"count": 1, "sum_volume": 500, "sum_amount": 14150.00},
            },
            "by_strategy": {},
        }

        stats = analytics.trade_statistics_from_db()

        assert OffsetFlag.OPEN in stats.by_offset_flag
        assert OffsetFlag.CLOSE in stats.by_offset_flag
        assert stats.by_offset_flag[OffsetFlag.OPEN]["count"] == 2
        assert stats.by_offset_flag[OffsetFlag.CLOSE]["count"] == 1

    def test_trade_statistics_from_db_empty(self, analytics: AnalyticsService, mock_duckdb: MagicMock) -> None:
        """Test computing statistics for empty trade database."""
        mock_duckdb.query_trade_statistics.return_value = {
            "total_records": 0,
            "total_volume": 0,
            "total_amount": 0,
            "avg_price": 0,
            "avg_volume": 0,
            "by_account_type": {},
            "by_offset_flag": {},
            "by_strategy": {},
        }

        stats = analytics.trade_statistics_from_db()

        assert stats.total_records == 0
        assert stats.total_volume == 0
        assert stats.total_amount == Decimal("0")
