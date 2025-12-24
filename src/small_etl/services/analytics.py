"""Analytics service for data statistics."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import polars as pl

if TYPE_CHECKING:
    from small_etl.data_access.duckdb_client import DuckDBClient
    from small_etl.data_access.postgres_repository import PostgresRepository

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
    """Service for computing data statistics.

    Args:
        repository: Optional PostgresRepository for database queries.
                   If provided, enables statistics from database.
        duckdb_client: Optional DuckDBClient for statistics via DuckDB.
                      If provided with attached PostgreSQL, enables efficient
                      aggregation queries directly on the database.
    """

    def __init__(
        self,
        repository: PostgresRepository | None = None,
        duckdb_client: DuckDBClient | None = None,
    ) -> None:
        self._repo = repository
        self._duckdb = duckdb_client

    def asset_statistics(self, df: pl.DataFrame) -> AssetStatistics:
        """Compute statistics for asset data.

        Args:
            df: DataFrame with asset data.

        Returns:
            AssetStatistics with computed metrics.
        """
        logger.info(f"Computing statistics for {len(df)} asset records")

        if len(df) == 0:
            return AssetStatistics(
                total_records=0,
                total_cash=Decimal("0"),
                total_frozen_cash=Decimal("0"),
                total_market_value=Decimal("0"),
                total_assets=Decimal("0"),
                avg_cash=Decimal("0"),
                avg_total_asset=Decimal("0"),
                by_account_type={},
            )

        agg = df.select(
            pl.len().alias("count"),
            pl.col("cash").sum().alias("sum_cash"),
            pl.col("frozen_cash").sum().alias("sum_frozen"),
            pl.col("market_value").sum().alias("sum_market"),
            pl.col("total_asset").sum().alias("sum_total"),
            pl.col("cash").mean().alias("avg_cash"),
            pl.col("total_asset").mean().alias("avg_total"),
        ).row(0, named=True)

        by_type = df.group_by("account_type").agg(
            pl.len().alias("count"),
            pl.col("cash").sum().alias("sum_cash"),
            pl.col("total_asset").sum().alias("sum_total"),
            pl.col("total_asset").mean().alias("avg_total"),
        )

        by_account_type = {}
        for row in by_type.iter_rows(named=True):
            by_account_type[row["account_type"]] = {
                "count": row["count"],
                "sum_cash": Decimal(str(row["sum_cash"])),
                "sum_total": Decimal(str(row["sum_total"])),
                "avg_total": Decimal(str(row["avg_total"])),
            }

        return AssetStatistics(
            total_records=agg["count"],
            total_cash=Decimal(str(agg["sum_cash"])),
            total_frozen_cash=Decimal(str(agg["sum_frozen"])),
            total_market_value=Decimal(str(agg["sum_market"])),
            total_assets=Decimal(str(agg["sum_total"])),
            avg_cash=Decimal(str(agg["avg_cash"])),
            avg_total_asset=Decimal(str(agg["avg_total"])),
            by_account_type=by_account_type,
        )

    def trade_statistics(self, df: pl.DataFrame) -> TradeStatistics:
        """Compute statistics for trade data.

        Args:
            df: DataFrame with trade data.

        Returns:
            TradeStatistics with computed metrics.
        """
        logger.info(f"Computing statistics for {len(df)} trade records")

        if len(df) == 0:
            return TradeStatistics(
                total_records=0,
                total_volume=0,
                total_amount=Decimal("0"),
                avg_price=Decimal("0"),
                avg_volume=0.0,
                by_account_type={},
                by_offset_flag={},
                by_strategy={},
            )

        agg = df.select(
            pl.len().alias("count"),
            pl.col("traded_volume").sum().alias("sum_volume"),
            pl.col("traded_amount").sum().alias("sum_amount"),
            pl.col("traded_price").mean().alias("avg_price"),
            pl.col("traded_volume").mean().alias("avg_volume"),
        ).row(0, named=True)

        by_type = df.group_by("account_type").agg(
            pl.len().alias("count"),
            pl.col("traded_volume").sum().alias("sum_volume"),
            pl.col("traded_amount").sum().alias("sum_amount"),
        )
        by_account_type = {}
        for row in by_type.iter_rows(named=True):
            by_account_type[row["account_type"]] = {
                "count": row["count"],
                "sum_volume": row["sum_volume"],
                "sum_amount": Decimal(str(row["sum_amount"])),
            }

        by_flag = df.group_by("offset_flag").agg(
            pl.len().alias("count"),
            pl.col("traded_volume").sum().alias("sum_volume"),
            pl.col("traded_amount").sum().alias("sum_amount"),
        )
        by_offset_flag = {}
        for row in by_flag.iter_rows(named=True):
            by_offset_flag[row["offset_flag"]] = {
                "count": row["count"],
                "sum_volume": row["sum_volume"],
                "sum_amount": Decimal(str(row["sum_amount"])),
            }

        by_strat = df.group_by("strategy_name").agg(
            pl.len().alias("count"),
            pl.col("traded_volume").sum().alias("sum_volume"),
            pl.col("traded_amount").sum().alias("sum_amount"),
        )
        by_strategy = {}
        for row in by_strat.iter_rows(named=True):
            by_strategy[row["strategy_name"]] = {
                "count": row["count"],
                "sum_volume": row["sum_volume"],
                "sum_amount": Decimal(str(row["sum_amount"])),
            }

        return TradeStatistics(
            total_records=agg["count"],
            total_volume=agg["sum_volume"],
            total_amount=Decimal(str(agg["sum_amount"])),
            avg_price=Decimal(str(agg["avg_price"])),
            avg_volume=agg["avg_volume"],
            by_account_type=by_account_type,
            by_offset_flag=by_offset_flag,
            by_strategy=by_strategy,
        )

    def asset_statistics_from_db(self) -> AssetStatistics:
        """Compute statistics from database records.

        Uses DuckDB if available for efficient aggregation queries,
        otherwise falls back to loading all data via repository.

        Returns:
            AssetStatistics computed from database.

        Raises:
            ValueError: If neither duckdb_client nor repository is configured.
        """
        if self._duckdb is not None:
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

        if self._repo is None:
            raise ValueError("Neither DuckDB client nor repository configured for database statistics")

        df = self._repo.get_all_assets()
        return self.asset_statistics(df)

    def trade_statistics_from_db(self) -> TradeStatistics:
        """Compute statistics from database records.

        Uses DuckDB if available for efficient aggregation queries,
        otherwise falls back to loading all data via repository.

        Returns:
            TradeStatistics computed from database.

        Raises:
            ValueError: If neither duckdb_client nor repository is configured.
        """
        if self._duckdb is not None:
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

        if self._repo is None:
            raise ValueError("Neither DuckDB client nor repository configured for database statistics")

        df = self._repo.get_all_trades()
        return self.trade_statistics(df)
