"""Loader service for data persistence."""

import logging
from dataclasses import dataclass

import polars as pl

from small_etl.data_access.postgres_repository import PostgresRepository, polars_to_assets, polars_to_trades

logger = logging.getLogger(__name__)


@dataclass
class LoadResult:
    """Result of data loading operation.

    Attributes:
        success: True if loading completed without errors.
        total_rows: Total number of rows to load.
        loaded_count: Number of rows successfully loaded.
        failed_count: Number of rows that failed to load.
        error_message: Error message if loading failed.
    """

    success: bool
    total_rows: int
    loaded_count: int
    failed_count: int = 0
    error_message: str | None = None


class LoaderService:
    """Service for loading validated data into PostgreSQL.

    Args:
        repository: PostgreSQL repository instance.
    """

    def __init__(self, repository: PostgresRepository) -> None:
        self._repo = repository

    def load_assets(self, df: pl.DataFrame, batch_size: int = 10000) -> LoadResult:
        """Load asset data into PostgreSQL.

        Args:
            df: DataFrame with validated asset data.
            batch_size: Number of records per batch (default: 10000).

        Returns:
            LoadResult with loading statistics.
        """
        logger.info(f"Loading {len(df)} asset records (batch_size={batch_size})")

        if len(df) == 0:
            return LoadResult(success=True, total_rows=0, loaded_count=0)

        total_loaded = 0

        try:
            for batch_start in range(0, len(df), batch_size):
                batch_end = min(batch_start + batch_size, len(df))
                batch_df = df.slice(batch_start, batch_end - batch_start)

                assets = polars_to_assets(batch_df)
                loaded = self._repo.upsert_assets(assets)
                total_loaded += loaded

                logger.debug(f"Loaded batch {batch_start}-{batch_end}: {loaded} records")

            logger.info(f"Successfully loaded {total_loaded} asset records")
            return LoadResult(
                success=True,
                total_rows=len(df),
                loaded_count=total_loaded,
            )

        except Exception as e:
            logger.exception(f"Error loading assets: {e}")
            return LoadResult(
                success=False,
                total_rows=len(df),
                loaded_count=total_loaded,
                failed_count=len(df) - total_loaded,
                error_message=str(e),
            )

    def load_trades(self, df: pl.DataFrame, batch_size: int = 10000) -> LoadResult:
        """Load trade data into PostgreSQL.

        Args:
            df: DataFrame with validated trade data.
            batch_size: Number of records per batch (default: 10000).

        Returns:
            LoadResult with loading statistics.
        """
        logger.info(f"Loading {len(df)} trade records (batch_size={batch_size})")

        if len(df) == 0:
            return LoadResult(success=True, total_rows=0, loaded_count=0)

        total_loaded = 0

        try:
            for batch_start in range(0, len(df), batch_size):
                batch_end = min(batch_start + batch_size, len(df))
                batch_df = df.slice(batch_start, batch_end - batch_start)

                trades = polars_to_trades(batch_df)
                loaded = self._repo.upsert_trades(trades)
                total_loaded += loaded

                logger.debug(f"Loaded batch {batch_start}-{batch_end}: {loaded} records")

            logger.info(f"Successfully loaded {total_loaded} trade records")
            return LoadResult(
                success=True,
                total_rows=len(df),
                loaded_count=total_loaded,
            )

        except Exception as e:
            logger.exception(f"Error loading trades: {e}")
            return LoadResult(
                success=False,
                total_rows=len(df),
                loaded_count=total_loaded,
                failed_count=len(df) - total_loaded,
                error_message=str(e),
            )
