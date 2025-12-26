"""Loader service for data persistence."""

import logging
from dataclasses import dataclass

import polars as pl

from small_etl.data_access.duckdb_client import DuckDBClient
from small_etl.data_access.postgres_repository import PostgresRepository
from small_etl.domain.registry import DataTypeRegistry

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
    """Service for loading validated data into PostgreSQL using DuckDB postgres extension.

    Args:
        repository: PostgreSQL repository instance (for database URL).
        duckdb_client: DuckDB client instance.
        database_url: PostgreSQL connection URL.
    """

    def __init__(
        self,
        repository: PostgresRepository,
        duckdb_client: DuckDBClient | None = None,
        database_url: str | None = None,
    ) -> None:
        self._repo = repository
        self._duckdb = duckdb_client
        self._database_url = database_url
        self._pg_attached = False

    def _ensure_pg_attached(self) -> DuckDBClient:
        """Ensure PostgreSQL is attached to DuckDB.

        Returns:
            DuckDB client instance.

        Raises:
            RuntimeError: If DuckDB client or database_url is not provided.
        """
        if self._duckdb is None or self._database_url is None:
            raise RuntimeError("DuckDB client and database_url required for bulk loading")
        if not self._pg_attached:
            self._duckdb.attach_postgres(self._database_url)
            self._pg_attached = True
        return self._duckdb

    def load(self, df: pl.DataFrame, data_type: str, batch_size: int = 10000) -> LoadResult:
        """Load data into PostgreSQL using DuckDB postgres extension.

        Generic method that uses DataTypeRegistry for configuration.

        Args:
            df: DataFrame with validated data.
            data_type: Data type name (e.g., "asset", "trade").
            batch_size: Number of records per batch (default: 10000).

        Returns:
            LoadResult with loading statistics.
        """
        config = DataTypeRegistry.get(data_type)
        logger.info(f"Loading {len(df)} {data_type} records (batch_size={batch_size})")

        if len(df) == 0:
            return LoadResult(success=True, total_rows=0, loaded_count=0)

        total_loaded = 0

        try:
            duckdb = self._ensure_pg_attached()

            for batch_start in range(0, len(df), batch_size):
                batch_end = min(batch_start + batch_size, len(df))
                batch_df = df.slice(batch_start, batch_end - batch_start)

                # Select only columns that exist in the database table
                batch_df = batch_df.select([c for c in config.db_columns if c in batch_df.columns])

                loaded = duckdb.upsert_to_postgres(batch_df, config.table_name, config.unique_key)
                total_loaded += loaded

                logger.debug(f"Loaded batch {batch_start}-{batch_end}: {loaded} records")

            logger.info(f"Successfully loaded {total_loaded} {data_type} records")
            return LoadResult(
                success=True,
                total_rows=len(df),
                loaded_count=total_loaded,
            )

        except Exception as e:
            logger.exception(f"Error loading {data_type}: {e}")
            return LoadResult(
                success=False,
                total_rows=len(df),
                loaded_count=total_loaded,
                failed_count=len(df) - total_loaded,
                error_message=str(e),
            )
