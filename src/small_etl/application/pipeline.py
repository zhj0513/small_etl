"""ETL Pipeline for orchestrating the data flow."""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from omegaconf import DictConfig

from small_etl.data_access.duckdb_client import DuckDBClient
from small_etl.data_access.postgres_repository import PostgresRepository
from small_etl.data_access.s3_connector import S3Connector
from small_etl.services.analytics import AnalyticsService, AssetStatistics, TradeStatistics
from small_etl.services.extractor import ExtractorService
from small_etl.services.loader import LoaderService, LoadResult
from small_etl.services.validator import ValidationResult, ValidatorService

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    """Return current UTC datetime."""
    return datetime.now(UTC)


@dataclass
class PipelineResult:
    """Result of ETL pipeline execution.

    Attributes:
        success: True if pipeline completed successfully.
        started_at: Pipeline start timestamp.
        completed_at: Pipeline completion timestamp.
        assets_validation: Asset validation result.
        trades_validation: Trade validation result.
        assets_load: Asset loading result.
        trades_load: Trade loading result.
        assets_stats: Asset statistics.
        trades_stats: Trade statistics.
        error_message: Error message if pipeline failed.
    """

    success: bool
    started_at: datetime
    completed_at: datetime | None = None
    assets_validation: ValidationResult | None = None
    trades_validation: ValidationResult | None = None
    assets_load: LoadResult | None = None
    trades_load: LoadResult | None = None
    assets_stats: AssetStatistics | None = None
    trades_stats: TradeStatistics | None = None
    error_message: str | None = None


class ETLPipeline:
    """ETL Pipeline for S3 CSV to PostgreSQL data synchronization.

    Orchestrates the full ETL process:
    1. Extract data from S3 CSV files
    2. Validate data using DuckDB and validation rules
    3. Load valid data into PostgreSQL
    4. Compute analytics on the loaded data

    Args:
        config: Hydra configuration with db, s3, and etl settings.
    """

    def __init__(self, config: DictConfig) -> None:
        self._config = config

        self._s3 = S3Connector(
            endpoint=config.s3.endpoint,
            access_key=config.s3.access_key,
            secret_key=config.s3.secret_key,
            secure=config.s3.secure,
        )

        self._duckdb = DuckDBClient()

        echo = getattr(config.db, "echo", False)
        self._repo = PostgresRepository(config.db.url, echo=echo)

        self._extractor = ExtractorService(
            self._s3, self._duckdb, config=getattr(config, "extractor", None)
        )
        self._validator = ValidatorService(tolerance=config.etl.validation.tolerance)
        self._loader = LoaderService(
            repository=self._repo,
            duckdb_client=self._duckdb,
            database_url=config.db.url,
        )
        self._analytics = AnalyticsService(repository=self._repo, duckdb_client=self._duckdb)

        # Attach PostgreSQL to DuckDB for analytics
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
            logger.info("=== Phase 1: Extract Assets ===")
            assets_df = self._extractor.extract_assets(
                bucket=self._config.s3.bucket,
                object_name=self._config.s3.assets_file,
            )

            logger.info("=== Phase 2: Validate Assets ===")
            assets_validation = self._validator.validate_assets(assets_df)

            if not assets_validation.is_valid:
                logger.warning(f"Asset validation has {assets_validation.invalid_count} invalid records")

            logger.info("=== Phase 3: Load Assets ===")
            assets_load = self._loader.load_assets(
                assets_validation.valid_rows,
                batch_size=self._config.etl.batch_size,
            )

            if not assets_load.success:
                raise RuntimeError(f"Asset loading failed: {assets_load.error_message}")

            valid_account_ids = self._repo.get_all_account_ids()
            logger.info(f"Loaded {len(valid_account_ids)} accounts for trade validation")

            logger.info("=== Phase 4: Extract Trades ===")
            trades_df = self._extractor.extract_trades(
                bucket=self._config.s3.bucket,
                object_name=self._config.s3.trades_file,
            )

            logger.info("=== Phase 5: Validate Trades ===")
            trades_validation = self._validator.validate_trades(trades_df, valid_account_ids)

            if not trades_validation.is_valid:
                logger.warning(f"Trade validation has {trades_validation.invalid_count} invalid records")

            logger.info("=== Phase 6: Load Trades ===")
            trades_load = self._loader.load_trades(
                trades_validation.valid_rows,
                batch_size=self._config.etl.batch_size,
            )

            if not trades_load.success:
                raise RuntimeError(f"Trade loading failed: {trades_load.error_message}")

            logger.info("=== Phase 7: Compute Analytics from Database (via DuckDB) ===")
            assets_stats = self._analytics.asset_statistics_from_db()
            trades_stats = self._analytics.trade_statistics_from_db()

            completed_at = _utc_now()
            duration = (completed_at - started_at).total_seconds()
            logger.info("--- ETL statistics ---")
            logger.info(f"Asset statistics: \n{assets_stats}")
            logger.info(f"Trade statistics: \n{trades_stats}")
            logger.info(f"ETL pipeline completed successfully in {duration:.2f}s")

            return PipelineResult(
                success=True,
                started_at=started_at,
                completed_at=completed_at,
                assets_validation=assets_validation,
                trades_validation=trades_validation,
                assets_load=assets_load,
                trades_load=trades_load,
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
        """Run ETL pipeline for assets only.

        Returns:
            PipelineResult with asset processing results.
        """
        started_at = _utc_now()
        logger.info("Starting ETL pipeline (assets only)")

        try:
            assets_df = self._extractor.extract_assets(
                bucket=self._config.s3.bucket,
                object_name=self._config.s3.assets_file,
            )

            assets_validation = self._validator.validate_assets(assets_df)

            assets_load = self._loader.load_assets(
                assets_validation.valid_rows,
                batch_size=self._config.etl.batch_size,
            )

            assets_stats = self._analytics.asset_statistics_from_db()

            return PipelineResult(
                success=assets_load.success,
                started_at=started_at,
                completed_at=_utc_now(),
                assets_validation=assets_validation,
                assets_load=assets_load,
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
        """Run ETL pipeline for trades only.

        Requires assets to be already loaded for foreign key validation.

        Returns:
            PipelineResult with trade processing results.
        """
        started_at = _utc_now()
        logger.info("Starting ETL pipeline (trades only)")

        try:
            valid_account_ids = self._repo.get_all_account_ids()
            if not valid_account_ids:
                raise RuntimeError("No accounts found. Load assets first.")

            trades_df = self._extractor.extract_trades(
                bucket=self._config.s3.bucket,
                object_name=self._config.s3.trades_file,
            )

            trades_validation = self._validator.validate_trades(trades_df, valid_account_ids)

            trades_load = self._loader.load_trades(
                trades_validation.valid_rows,
                batch_size=self._config.etl.batch_size,
            )

            trades_stats = self._analytics.trade_statistics_from_db()

            return PipelineResult(
                success=trades_load.success,
                started_at=started_at,
                completed_at=_utc_now(),
                trades_validation=trades_validation,
                trades_load=trades_load,
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
