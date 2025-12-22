"""Unit tests for analytics service."""

from decimal import Decimal

import polars as pl
import pytest

from small_etl.domain.enums import OffsetFlag
from small_etl.services.analytics import AnalyticsService


class TestAnalyticsService:
    """Tests for AnalyticsService."""

    @pytest.fixture
    def analytics(self) -> AnalyticsService:
        """Create analytics instance."""
        return AnalyticsService()

    def test_asset_statistics(self, analytics: AnalyticsService, sample_asset_data: pl.DataFrame) -> None:
        """Test computing asset statistics."""
        stats = analytics.asset_statistics(sample_asset_data)

        assert stats.total_records == 3
        assert stats.total_cash == Decimal("850000.00")  # 100000 + 500000 + 250000
        assert stats.total_assets == Decimal("2280000.00")  # 305000 + 1350000 + 625000
        assert stats.avg_cash > 0
        assert stats.avg_total_asset > 0
        assert len(stats.by_account_type) == 2  # SECURITY and CREDIT

    def test_asset_statistics_empty(self, analytics: AnalyticsService) -> None:
        """Test computing statistics for empty DataFrame."""
        empty_df = pl.DataFrame(
            {
                "id": [],
                "account_id": [],
                "account_type": [],
                "cash": [],
                "frozen_cash": [],
                "market_value": [],
                "total_asset": [],
                "updated_at": [],
            }
        ).cast(
            {
                "id": pl.Int64,
                "cash": pl.Float64,
                "frozen_cash": pl.Float64,
                "market_value": pl.Float64,
                "total_asset": pl.Float64,
            }
        )

        stats = analytics.asset_statistics(empty_df)

        assert stats.total_records == 0
        assert stats.total_cash == Decimal("0")
        assert stats.total_assets == Decimal("0")

    def test_trade_statistics(self, analytics: AnalyticsService, sample_trade_data: pl.DataFrame) -> None:
        """Test computing trade statistics."""
        stats = analytics.trade_statistics(sample_trade_data)

        assert stats.total_records == 3
        assert stats.total_volume == 1000 + 500 + 2000
        assert stats.total_amount == Decimal("115250.00")  # 15500 + 14150 + 85600
        assert stats.avg_price > 0
        assert stats.avg_volume > 0
        assert len(stats.by_strategy) == 3  # 策略A, 量化1号, 趋势跟踪

    def test_trade_statistics_by_offset_flag(self, analytics: AnalyticsService, sample_trade_data: pl.DataFrame) -> None:
        """Test trade statistics breakdown by offset flag."""
        stats = analytics.trade_statistics(sample_trade_data)

        assert OffsetFlag.OPEN in stats.by_offset_flag
        assert OffsetFlag.CLOSE in stats.by_offset_flag
        assert stats.by_offset_flag[OffsetFlag.OPEN]["count"] == 2
        assert stats.by_offset_flag[OffsetFlag.CLOSE]["count"] == 1

    def test_trade_statistics_empty(self, analytics: AnalyticsService) -> None:
        """Test computing statistics for empty trade DataFrame."""
        empty_df = pl.DataFrame(
            {
                "id": [],
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
            }
        ).cast(
            {
                "id": pl.Int64,
                "traded_price": pl.Float64,
                "traded_volume": pl.Int64,
                "traded_amount": pl.Float64,
            }
        )

        stats = analytics.trade_statistics(empty_df)

        assert stats.total_records == 0
        assert stats.total_volume == 0
        assert stats.total_amount == Decimal("0")
