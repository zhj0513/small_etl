"""PostgreSQL repository for data persistence."""

import logging
from collections.abc import Sequence
from decimal import Decimal

import polars as pl
from sqlalchemy import func, text
from sqlmodel import Session, create_engine, select

from small_etl.domain.models import Asset, Trade

logger = logging.getLogger(__name__)


class PostgresRepository:
    """PostgreSQL repository for asset and trade data operations.

    Args:
        database_url: PostgreSQL connection URL.
        echo: Whether to echo SQL statements (default: False).
    """

    def __init__(self, database_url: str, echo: bool = False) -> None:
        self._engine = create_engine(database_url, echo=echo)
        logger.info("PostgresRepository initialized")

    def create_tables(self) -> None:
        """Create all tables defined in SQLModel metadata."""
        from sqlmodel import SQLModel

        SQLModel.metadata.create_all(self._engine)
        logger.info("Database tables created")

    def get_session(self) -> Session:
        """Get a new database session.

        Returns:
            SQLModel Session instance.
        """
        return Session(self._engine)

    def bulk_insert_assets(self, assets: Sequence[Asset]) -> int:
        """Bulk insert asset records.

        Args:
            assets: List of Asset instances to insert.

        Returns:
            Number of records inserted.
        """
        with Session(self._engine) as session:
            session.add_all(assets)
            session.commit()
            logger.info(f"Inserted {len(assets)} asset records")
            return len(assets)

    def bulk_insert_trades(self, trades: Sequence[Trade]) -> int:
        """Bulk insert trade records.

        Args:
            trades: List of Trade instances to insert.

        Returns:
            Number of records inserted.
        """
        with Session(self._engine) as session:
            session.add_all(trades)
            session.commit()
            logger.info(f"Inserted {len(trades)} trade records")
            return len(trades)

    def upsert_assets(self, assets: Sequence[Asset]) -> int:
        """Upsert (insert or update) asset records.

        Uses PostgreSQL ON CONFLICT for efficient upserts.

        Args:
            assets: List of Asset instances to upsert.

        Returns:
            Number of records affected.
        """
        if not assets:
            return 0

        with Session(self._engine) as session:
            for asset in assets:
                existing = session.exec(select(Asset).where(Asset.account_id == asset.account_id)).first()
                if existing:
                    existing.account_type = asset.account_type
                    existing.cash = asset.cash
                    existing.frozen_cash = asset.frozen_cash
                    existing.market_value = asset.market_value
                    existing.total_asset = asset.total_asset
                    existing.updated_at = asset.updated_at
                else:
                    session.add(asset)
            session.commit()
            logger.info(f"Upserted {len(assets)} asset records")
            return len(assets)

    def upsert_trades(self, trades: Sequence[Trade]) -> int:
        """Upsert (insert or update) trade records.

        Args:
            trades: List of Trade instances to upsert.

        Returns:
            Number of records affected.
        """
        if not trades:
            return 0

        with Session(self._engine) as session:
            for trade in trades:
                existing = session.exec(select(Trade).where(Trade.traded_id == trade.traded_id)).first()
                if existing:
                    for key, value in trade.model_dump(exclude={"id"}).items():
                        setattr(existing, key, value)
                else:
                    session.add(trade)
            session.commit()
            logger.info(f"Upserted {len(trades)} trade records")
            return len(trades)

    def get_asset_by_account_id(self, account_id: str) -> Asset | None:
        """Get asset record by account ID.

        Args:
            account_id: Account identifier.

        Returns:
            Asset instance or None if not found.
        """
        with Session(self._engine) as session:
            return session.exec(select(Asset).where(Asset.account_id == account_id)).first()

    def get_all_account_ids(self) -> set[str]:
        """Get all account IDs from asset table.

        Returns:
            Set of account IDs.
        """
        with Session(self._engine) as session:
            result = session.exec(select(Asset.account_id))
            return set(result.all())

    def get_asset_count(self) -> int:
        """Get total count of asset records.

        Returns:
            Number of asset records.
        """
        with Session(self._engine) as session:
            result = session.exec(select(func.count()).select_from(Asset))
            return result.one()

    def get_trade_count(self) -> int:
        """Get total count of trade records.

        Returns:
            Number of trade records.
        """
        with Session(self._engine) as session:
            result = session.exec(select(func.count()).select_from(Trade))
            return result.one()

    def truncate_tables(self) -> None:
        """Truncate asset and trade tables."""
        with Session(self._engine) as session:
            session.execute(text("TRUNCATE TABLE trade CASCADE"))  # pyrefly: ignore[deprecated]
            session.execute(text("TRUNCATE TABLE asset CASCADE"))  # pyrefly: ignore[deprecated]
            session.commit()
            logger.info("Truncated asset and trade tables")

    def close(self) -> None:
        """Dispose of the database engine."""
        self._engine.dispose()
        logger.info("PostgresRepository connection closed")

    def get_all_assets(self) -> pl.DataFrame:
        """Get all asset records as Polars DataFrame.

        Returns:
            Polars DataFrame with all asset data.
        """
        with Session(self._engine) as session:
            result = session.exec(select(Asset))
            assets = result.all()

            if not assets:
                return pl.DataFrame(
                    schema={
                        "id": pl.Int64,
                        "account_id": pl.Utf8,
                        "account_type": pl.Int32,
                        "cash": pl.Float64,
                        "frozen_cash": pl.Float64,
                        "market_value": pl.Float64,
                        "total_asset": pl.Float64,
                        "updated_at": pl.Datetime,
                    }
                )

            return pl.DataFrame([
                {
                    "id": a.id,
                    "account_id": a.account_id,
                    "account_type": a.account_type,
                    "cash": float(a.cash),
                    "frozen_cash": float(a.frozen_cash),
                    "market_value": float(a.market_value),
                    "total_asset": float(a.total_asset),
                    "updated_at": a.updated_at,
                }
                for a in assets
            ])

    def get_all_trades(self) -> pl.DataFrame:
        """Get all trade records as Polars DataFrame.

        Returns:
            Polars DataFrame with all trade data.
        """
        with Session(self._engine) as session:
            result = session.exec(select(Trade))
            trades = result.all()

            if not trades:
                return pl.DataFrame(
                    schema={
                        "id": pl.Int64,
                        "account_id": pl.Utf8,
                        "account_type": pl.Int32,
                        "traded_id": pl.Utf8,
                        "stock_code": pl.Utf8,
                        "traded_time": pl.Datetime,
                        "traded_price": pl.Float64,
                        "traded_volume": pl.Int64,
                        "traded_amount": pl.Float64,
                        "strategy_name": pl.Utf8,
                        "order_remark": pl.Utf8,
                        "direction": pl.Int32,
                        "offset_flag": pl.Int32,
                        "created_at": pl.Datetime,
                        "updated_at": pl.Datetime,
                    }
                )

            return pl.DataFrame([
                {
                    "id": t.id,
                    "account_id": t.account_id,
                    "account_type": t.account_type,
                    "traded_id": t.traded_id,
                    "stock_code": t.stock_code,
                    "traded_time": t.traded_time,
                    "traded_price": float(t.traded_price),
                    "traded_volume": t.traded_volume,
                    "traded_amount": float(t.traded_amount),
                    "strategy_name": t.strategy_name,
                    "order_remark": t.order_remark,
                    "direction": t.direction,
                    "offset_flag": t.offset_flag,
                    "created_at": t.created_at,
                    "updated_at": t.updated_at,
                }
                for t in trades
            ])


def polars_to_assets(df: pl.DataFrame) -> list[Asset]:
    """Convert Polars DataFrame to list of Asset instances.

    Args:
        df: DataFrame with asset data.

    Returns:
        List of Asset instances.
    """
    assets = []
    for row in df.iter_rows(named=True):
        asset = Asset(
            account_id=str(row["account_id"]),
            account_type=int(row["account_type"]),
            cash=Decimal(str(row["cash"])),
            frozen_cash=Decimal(str(row["frozen_cash"])),
            market_value=Decimal(str(row["market_value"])),
            total_asset=Decimal(str(row["total_asset"])),
            updated_at=row["updated_at"],
        )
        assets.append(asset)
    return assets


def polars_to_trades(df: pl.DataFrame) -> list[Trade]:
    """Convert Polars DataFrame to list of Trade instances.

    Args:
        df: DataFrame with trade data.

    Returns:
        List of Trade instances.
    """
    trades = []
    for row in df.iter_rows(named=True):
        trade = Trade(
            account_id=str(row["account_id"]),
            account_type=int(row["account_type"]),
            traded_id=str(row["traded_id"]),
            stock_code=str(row["stock_code"]),
            traded_time=row["traded_time"],
            traded_price=Decimal(str(row["traded_price"])),
            traded_volume=int(row["traded_volume"]),
            traded_amount=Decimal(str(row["traded_amount"])),
            strategy_name=str(row["strategy_name"]),
            order_remark=str(row["order_remark"]) if row.get("order_remark") else None,
            direction=int(row["direction"]),
            offset_flag=int(row["offset_flag"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
        trades.append(trade)
    return trades
