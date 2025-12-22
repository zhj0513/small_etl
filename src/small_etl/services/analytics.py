"""Analytics service for data statistics."""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import polars as pl

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
    """Service for computing data statistics."""

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
