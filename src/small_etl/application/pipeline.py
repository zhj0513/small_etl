"""ETL Pipeline for orchestrating the data flow."""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from omegaconf import DictConfig

from small_etl.data_access.duckdb_client import DuckDBClient
from small_etl.data_access.postgres_repository import PostgresRepository
from small_etl.services.analytics import AnalyticsService
from small_etl.services.extractor import ExtractorService
from small_etl.services.loader import LoaderService
from small_etl.services.validator import ValidatorService

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    """Return current UTC datetime."""
    return datetime.now(UTC)


@dataclass
class PipelineResult:
    """Result of ETL pipeline execution."""

    success: bool
    started_at: datetime
    completed_at: datetime | None = None
    error_message: str | None = None
    assets_loaded: int = 0
    trades_loaded: int = 0
    assets_stats: Any = None
    trades_stats: Any = None


class ETLPipeline:
    """ETL Pipeline for S3 CSV to PostgreSQL data synchronization.

    Data flow:
    1. Read CSV from S3 using Polars (via ValidatorService)
    2. Validate data using Pandera schemas - fail fast if invalid
    3. Transform data types (via ExtractorService)
    4. Load data into PostgreSQL
    5. Compute analytics on the loaded data
    """

    def __init__(self, config: DictConfig) -> None:
        self._config = config

        self._duckdb = DuckDBClient()

        echo = getattr(config.db, "echo", False)
        self._repo = PostgresRepository(config.db.url, echo=echo)

        self._validator = ValidatorService(
            tolerance=config.etl.validation.tolerance,
            s3_config=config.s3,
        )
        self._extractor = ExtractorService(config=getattr(config, "extractor", None))
        self._loader = LoaderService(
            repository=self._repo,
            duckdb_client=self._duckdb,
            database_url=config.db.url,
        )
        self._analytics = AnalyticsService(duckdb_client=self._duckdb)

        self._duckdb.attach_postgres(config.db.url)

        logger.info("ETLPipeline initialized")

    def run(self) -> PipelineResult:
        """Run the complete ETL pipeline for both assets and trades.

        Returns:
            PipelineResult with results from all stages.
        """
        started_at = _utc_now()
        logger.info("Starting ETL pipeline")

        try:
            # === Phase 1: Read + Validate Assets ===
            logger.info("=== Phase 1: Read and Validate Assets ===")
            assets_validation = self._validator.fetch_and_validate(
                bucket=self._config.s3.bucket,
                object_name=self._config.s3.assets_file,
                data_type="asset",
            )

            if not assets_validation.is_valid:
                return PipelineResult(
                    success=False,
                    started_at=started_at,
                    completed_at=_utc_now(),
                    error_message=f"Asset validation failed: {assets_validation.error_message}",
                )

            # === Phase 2: Transform Assets ===
            logger.info("=== Phase 2: Transform Assets ===")
            assets_df = self._extractor.transform(assets_validation.data, "asset")

            # === Phase 3: Load Assets ===
            logger.info("=== Phase 3: Load Assets ===")
            assets_load = self._loader.load_assets(assets_df, batch_size=self._config.etl.batch_size)

            if not assets_load.success:
                return PipelineResult(
                    success=False,
                    started_at=started_at,
                    completed_at=_utc_now(),
                    error_message=f"Asset loading failed: {assets_load.error_message}",
                )

            valid_account_ids = self._repo.get_all_account_ids()
            logger.info(f"Loaded {len(valid_account_ids)} accounts for trade validation")

            # === Phase 4: Read + Validate Trades ===
            logger.info("=== Phase 4: Read and Validate Trades ===")
            trades_validation = self._validator.fetch_and_validate(
                bucket=self._config.s3.bucket,
                object_name=self._config.s3.trades_file,
                data_type="trade",
                valid_foreign_keys=valid_account_ids,
            )

            if not trades_validation.is_valid:
                return PipelineResult(
                    success=False,
                    started_at=started_at,
                    completed_at=_utc_now(),
                    error_message=f"Trade validation failed: {trades_validation.error_message}",
                    assets_loaded=assets_load.loaded_count,
                )

            # === Phase 5: Transform Trades ===
            logger.info("=== Phase 5: Transform Trades ===")
            trades_df = self._extractor.transform(trades_validation.data, "trade")

            # === Phase 6: Load Trades ===
            logger.info("=== Phase 6: Load Trades ===")
            trades_load = self._loader.load_trades(trades_df, batch_size=self._config.etl.batch_size)

            if not trades_load.success:
                return PipelineResult(
                    success=False,
                    started_at=started_at,
                    completed_at=_utc_now(),
                    error_message=f"Trade loading failed: {trades_load.error_message}",
                    assets_loaded=assets_load.loaded_count,
                )

            # === Phase 7: Analytics ===
            logger.info("=== Phase 7: Compute Analytics from Database ===")
            assets_stats = self._analytics.asset_statistics_from_db()
            trades_stats = self._analytics.trade_statistics_from_db()

            completed_at = _utc_now()
            duration = (completed_at - started_at).total_seconds()
            logger.info(f"ETL pipeline completed successfully in {duration:.2f}s")

            return PipelineResult(
                success=True,
                started_at=started_at,
                completed_at=completed_at,
                assets_loaded=assets_load.loaded_count,
                trades_loaded=trades_load.loaded_count,
                assets_stats=assets_stats,
                trades_stats=trades_stats,
            )

        except Exception as e:
            logger.exception(f"ETL pipeline failed: {e}")
            return PipelineResult(
                success=False,
                started_at=started_at,
                completed_at=_utc_now(),
                error_message=str(e),
            )

    def run_assets_only(self) -> PipelineResult:
        """Run ETL pipeline for assets only."""
        started_at = _utc_now()
        logger.info("Starting ETL pipeline (assets only)")

        try:
            assets_validation = self._validator.fetch_and_validate(
                bucket=self._config.s3.bucket,
                object_name=self._config.s3.assets_file,
                data_type="asset",
            )

            if not assets_validation.is_valid:
                return PipelineResult(
                    success=False,
                    started_at=started_at,
                    completed_at=_utc_now(),
                    error_message=f"Asset validation failed: {assets_validation.error_message}",
                )

            assets_df = self._extractor.transform(assets_validation.data, "asset")
            assets_load = self._loader.load_assets(assets_df, batch_size=self._config.etl.batch_size)

            if not assets_load.success:
                return PipelineResult(
                    success=False,
                    started_at=started_at,
                    completed_at=_utc_now(),
                    error_message=f"Asset loading failed: {assets_load.error_message}",
                )

            assets_stats = self._analytics.asset_statistics_from_db()

            return PipelineResult(
                success=True,
                started_at=started_at,
                completed_at=_utc_now(),
                assets_loaded=assets_load.loaded_count,
                assets_stats=assets_stats,
            )

        except Exception as e:
            logger.exception(f"Asset ETL failed: {e}")
            return PipelineResult(
                success=False,
                started_at=started_at,
                completed_at=_utc_now(),
                error_message=str(e),
            )

    def run_trades_only(self) -> PipelineResult:
        """Run ETL pipeline for trades only. Requires assets to be already loaded."""
        started_at = _utc_now()
        logger.info("Starting ETL pipeline (trades only)")

        try:
            valid_account_ids = self._repo.get_all_account_ids()
            if not valid_account_ids:
                return PipelineResult(
                    success=False,
                    started_at=started_at,
                    completed_at=_utc_now(),
                    error_message="No accounts found. Load assets first.",
                )

            trades_validation = self._validator.fetch_and_validate(
                bucket=self._config.s3.bucket,
                object_name=self._config.s3.trades_file,
                data_type="trade",
                valid_foreign_keys=valid_account_ids,
            )

            if not trades_validation.is_valid:
                return PipelineResult(
                    success=False,
                    started_at=started_at,
                    completed_at=_utc_now(),
                    error_message=f"Trade validation failed: {trades_validation.error_message}",
                )

            trades_df = self._extractor.transform(trades_validation.data, "trade")
            trades_load = self._loader.load_trades(trades_df, batch_size=self._config.etl.batch_size)

            if not trades_load.success:
                return PipelineResult(
                    success=False,
                    started_at=started_at,
                    completed_at=_utc_now(),
                    error_message=f"Trade loading failed: {trades_load.error_message}",
                )

            trades_stats = self._analytics.trade_statistics_from_db()

            return PipelineResult(
                success=True,
                started_at=started_at,
                completed_at=_utc_now(),
                trades_loaded=trades_load.loaded_count,
                trades_stats=trades_stats,
            )

        except Exception as e:
            logger.exception(f"Trade ETL failed: {e}")
            return PipelineResult(
                success=False,
                started_at=started_at,
                completed_at=_utc_now(),
                error_message=str(e),
            )

    def close(self) -> None:
        """Clean up resources."""
        self._duckdb.close()
        self._repo.close()
        logger.info("ETLPipeline resources cleaned up")

    def __enter__(self) -> "ETLPipeline":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
