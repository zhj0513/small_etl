"""Analytics service for data statistics."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from small_etl.data_access.duckdb_client import DuckDBClient

logger = logging.getLogger(__name__)


@dataclass
class AssetStatistics:
    """Statistics for asset data.

    Attributes:
        total_records: Total number of asset records.
        total_cash: Sum of all cash balances.
        total_frozen_cash: Sum of all frozen cash.
        total_market_value: Sum of all market values.
        total_assets: Sum of all total assets.
        avg_cash: Average cash balance.
        avg_total_asset: Average total asset.
        by_account_type: Breakdown by account type.
    """

    total_records: int
    total_cash: Decimal
    total_frozen_cash: Decimal
    total_market_value: Decimal
    total_assets: Decimal
    avg_cash: Decimal
    avg_total_asset: Decimal
    by_account_type: dict[int, dict[str, Any]]


@dataclass
class TradeStatistics:
    """Statistics for trade data.

    Attributes:
        total_records: Total number of trade records.
        total_volume: Total traded volume.
        total_amount: Total traded amount.
        avg_price: Average trade price.
        avg_volume: Average trade volume.
        by_account_type: Breakdown by account type.
        by_offset_flag: Breakdown by offset flag (buy/sell).
        by_strategy: Breakdown by strategy name.
    """

    total_records: int
    total_volume: int
    total_amount: Decimal
    avg_price: Decimal
    avg_volume: float
    by_account_type: dict[int, dict[str, Any]]
    by_offset_flag: dict[int, dict[str, Any]]
    by_strategy: dict[str, dict[str, Any]]


class AnalyticsService:
    """Service for computing data statistics via DuckDB.

    Args:
        duckdb_client: DuckDBClient for statistics via DuckDB.
                      Must have attached PostgreSQL for database statistics.
    """

    def __init__(
        self,
        duckdb_client: DuckDBClient,
    ) -> None:
        self._duckdb = duckdb_client

    def asset_statistics_from_db(self) -> AssetStatistics:
        """Compute statistics from database records via DuckDB.

        Returns:
            AssetStatistics computed from database.
        """
        logger.info("Computing asset statistics via DuckDB")
        stats = self._duckdb.query_asset_statistics()
        by_account_type = {}
        for acct_type, data in stats["by_account_type"].items():
            by_account_type[acct_type] = {
                "count": data["count"],
                "sum_cash": Decimal(str(data["sum_cash"])),
                "sum_total": Decimal(str(data["sum_total"])),
                "avg_total": Decimal(str(data["avg_total"])),
            }
        return AssetStatistics(
            total_records=stats["total_records"],
            total_cash=Decimal(str(stats["total_cash"])),
            total_frozen_cash=Decimal(str(stats["total_frozen_cash"])),
            total_market_value=Decimal(str(stats["total_market_value"])),
            total_assets=Decimal(str(stats["total_assets"])),
            avg_cash=Decimal(str(stats["avg_cash"])),
            avg_total_asset=Decimal(str(stats["avg_total_asset"])),
            by_account_type=by_account_type,
        )

    def trade_statistics_from_db(self) -> TradeStatistics:
        """Compute statistics from database records via DuckDB.

        Returns:
            TradeStatistics computed from database.
        """
        logger.info("Computing trade statistics via DuckDB")
        stats = self._duckdb.query_trade_statistics()

        by_account_type = {}
        for acct_type, data in stats["by_account_type"].items():
            by_account_type[acct_type] = {
                "count": data["count"],
                "sum_volume": data["sum_volume"],
                "sum_amount": Decimal(str(data["sum_amount"])),
            }

        by_offset_flag = {}
        for flag, data in stats["by_offset_flag"].items():
            by_offset_flag[flag] = {
                "count": data["count"],
                "sum_volume": data["sum_volume"],
                "sum_amount": Decimal(str(data["sum_amount"])),
            }

        by_strategy = {}
        for strategy, data in stats["by_strategy"].items():
            by_strategy[strategy] = {
                "count": data["count"],
                "sum_volume": data["sum_volume"],
                "sum_amount": Decimal(str(data["sum_amount"])),
            }

        return TradeStatistics(
            total_records=stats["total_records"],
            total_volume=stats["total_volume"],
            total_amount=Decimal(str(stats["total_amount"])),
            avg_price=Decimal(str(stats["avg_price"])),
            avg_volume=float(stats["avg_volume"]),
            by_account_type=by_account_type,
            by_offset_flag=by_offset_flag,
            by_strategy=by_strategy,
        )
