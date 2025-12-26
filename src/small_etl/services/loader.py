"""Loader service for data persistence."""

import logging
from dataclasses import dataclass

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
    """Service for loading data from DuckDB table into PostgreSQL.

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

    def load(self, source_table: str, data_type: str) -> LoadResult:
        """Load data from DuckDB table into PostgreSQL.

        Args:
            source_table: Name of the DuckDB table (e.g., "_transformed_asset").
            data_type: Data type name (e.g., "asset", "trade").

        Returns:
            LoadResult with loading statistics.
        """
        config = DataTypeRegistry.get(data_type)
        logger.info(f"Loading {data_type} from DuckDB table {source_table}")

        try:
            duckdb = self._ensure_pg_attached()

            total_loaded = duckdb.upsert_from_table(
                source_table=source_table,
                pg_table=config.table_name,
                conflict_column=config.unique_key,
                columns=config.db_columns,
            )

            logger.info(f"Successfully loaded {total_loaded} {data_type} records")
            return LoadResult(
                success=True,
                total_rows=total_loaded,
                loaded_count=total_loaded,
            )

        except Exception as e:
            logger.exception(f"Error loading {data_type}: {e}")
            return LoadResult(
                success=False,
                total_rows=0,
                loaded_count=0,
                error_message=str(e),
            )
