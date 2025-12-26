"""ETL Pipeline for orchestrating the data flow."""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from omegaconf import DictConfig

from small_etl.data_access.duckdb_client import DuckDBClient
from small_etl.data_access.postgres_repository import PostgresRepository
from small_etl.domain.registry import DataTypeRegistry
from small_etl.services.analytics import AnalyticsService
from small_etl.services.extractor import ExtractorService
from small_etl.services.loader import LoaderService
from small_etl.services.validator import ValidatorService

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    """Return current UTC datetime."""
    return datetime.now(UTC)


@dataclass
class StepResult:
    """Result of a single pipeline step."""

    data_type: str
    success: bool
    loaded_count: int = 0
    error_message: str | None = None
    statistics: Any = None


@dataclass
class PipelineResult:
    """Result of ETL pipeline execution."""

    success: bool
    started_at: datetime
    completed_at: datetime | None = None
    error_message: str | None = None
    step_results: list[StepResult] = field(default_factory=list)

    # Legacy compatibility properties
    @property
    def assets_loaded(self) -> int:
        """Get asset loaded count for backward compatibility."""
        for step in self.step_results:
            if step.data_type == "asset":
                return step.loaded_count
        return 0

    @property
    def trades_loaded(self) -> int:
        """Get trade loaded count for backward compatibility."""
        for step in self.step_results:
            if step.data_type == "trade":
                return step.loaded_count
        return 0

    @property
    def assets_stats(self) -> Any:
        """Get asset statistics for backward compatibility."""
        for step in self.step_results:
            if step.data_type == "asset":
                return step.statistics
        return None

    @property
    def trades_stats(self) -> Any:
        """Get trade statistics for backward compatibility."""
        for step in self.step_results:
            if step.data_type == "trade":
                return step.statistics
        return None


class ETLPipeline:
    """ETL Pipeline for S3 CSV to PostgreSQL data synchronization.

    Configuration-driven pipeline that processes steps defined in pipeline config.
    Each step follows: Validate -> Transform -> Load -> (optional) Analytics
    """

    def __init__(self, config: DictConfig) -> None:
        self._config = config

        self._duckdb = DuckDBClient()

        echo = getattr(config.db, "echo", False)
        self._repo = PostgresRepository(config.db.url, echo=echo)

        self._validator = ValidatorService(s3_config=config.s3)
        self._extractor = ExtractorService(
            config=getattr(config, "extractor", None),
            duckdb_client=self._duckdb,
        )
        self._loader = LoaderService(
            repository=self._repo,
            duckdb_client=self._duckdb,
            database_url=config.db.url,
        )
        self._analytics = AnalyticsService(duckdb_client=self._duckdb)

        self._duckdb.attach_postgres(config.db.url)

        # Parse pipeline configuration
        pipeline_cfg = getattr(config, "pipeline", None)
        self._steps = self._parse_steps(pipeline_cfg)
        self._compute_analytics = getattr(pipeline_cfg, "compute_analytics", True) if pipeline_cfg else True

        logger.info(f"ETLPipeline initialized with {len(self._steps)} steps")

    def _parse_steps(self, pipeline_cfg: DictConfig | None) -> list[str]:
        """Parse pipeline steps from configuration.

        Args:
            pipeline_cfg: Pipeline configuration section.

        Returns:
            List of enabled data type names in order.
        """
        if not pipeline_cfg or not hasattr(pipeline_cfg, "steps"):
            # Default to asset -> trade for backward compatibility
            return ["asset", "trade"]

        steps = []
        for step in pipeline_cfg.steps:
            if getattr(step, "enabled", True):
                data_type = step.data_type
                if DataTypeRegistry.is_registered(data_type):
                    steps.append(data_type)
                else:
                    logger.warning(f"Skipping unregistered data type: {data_type}")
        return steps

    def _get_foreign_keys(self, data_type: str) -> set[str] | None:
        """Get valid foreign keys for a data type if required.

        Args:
            data_type: Data type to check for foreign key requirements.

        Returns:
            Set of valid foreign key values, or None if not required.
        """
        config = DataTypeRegistry.get(data_type)
        if not config.foreign_key_reference:
            return None

        # Query foreign keys from database
        ref_config = DataTypeRegistry.get(config.foreign_key_reference)
        return self._duckdb.query_column_values(ref_config.table_name, ref_config.unique_key)

    def _get_s3_file(self, data_type: str) -> str:
        """Get S3 file path for a data type.

        Args:
            data_type: Data type name.

        Returns:
            S3 file path from config.
        """
        config = DataTypeRegistry.get(data_type)
        return getattr(self._config.s3, config.s3_file_key)

    def _compute_statistics(self, data_type: str) -> Any:
        """Compute statistics for a data type.

        Args:
            data_type: Data type name.

        Returns:
            Statistics dictionary.
        """
        if data_type == "asset":
            return self._analytics.asset_statistics_from_db()
        elif data_type == "trade":
            return self._analytics.trade_statistics_from_db()
        return None

    def _run_step(self, data_type: str, step_num: int) -> StepResult:
        """Run a single pipeline step for a data type.

        Args:
            data_type: Data type to process.
            step_num: Step number for logging.

        Returns:
            StepResult for this step.
        """
        logger.info(f"=== Step {step_num}: Processing {data_type} ===")

        # Phase 1: Validate
        logger.info(f"  Validating {data_type}...")
        valid_foreign_keys = self._get_foreign_keys(data_type)

        if valid_foreign_keys is not None and not valid_foreign_keys:
            return StepResult(
                data_type=data_type,
                success=False,
                error_message=f"No foreign keys found for {data_type}. Load referenced data first.",
            )

        validation = self._validator.fetch_and_validate(
            bucket=self._config.s3.bucket,
            object_name=self._get_s3_file(data_type),
            data_type=data_type,
            valid_foreign_keys=valid_foreign_keys,
        )

        if not validation.is_valid:
            return StepResult(
                data_type=data_type,
                success=False,
                error_message=f"Validation failed: {validation.error_message}",
            )

        # Phase 2: Extract And Transform (using DuckDB)
        logger.info(f"  Transforming {data_type}...")
        transformed_table = self._extractor.transform(validation.data, data_type)

        # Phase 3: Load (from DuckDB table)
        logger.info(f"  Loading {data_type}...")
        load_result = self._loader.load(transformed_table, data_type)

        # Clean up transformed table
        self._duckdb.drop_table(transformed_table)

        if not load_result.success:
            return StepResult(
                data_type=data_type,
                success=False,
                loaded_count=load_result.loaded_count,
                error_message=f"Loading failed: {load_result.error_message}",
            )

        # Phase 4: Analytics (optional)
        statistics = None
        if self._compute_analytics:
            logger.info(f"  Computing analytics for {data_type}...")
            statistics = self._compute_statistics(data_type)

        logger.info(f"  Completed {data_type}: {load_result.loaded_count} records")
        return StepResult(
            data_type=data_type,
            success=True,
            loaded_count=load_result.loaded_count,
            statistics=statistics,
        )

    def run(self, data_types: list[str] | None = None) -> PipelineResult:
        """Run the ETL pipeline for specified data types.

        Args:
            data_types: List of data types to process. If None, uses configured steps.

        Returns:
            PipelineResult with results from all steps.
        """
        started_at = _utc_now()
        steps_to_run = data_types if data_types else self._steps

        logger.info(f"Starting ETL pipeline with steps: {steps_to_run}")

        step_results: list[StepResult] = []

        try:
            for i, data_type in enumerate(steps_to_run, 1):
                step_result = self._run_step(data_type, i)
                step_results.append(step_result)

                if not step_result.success:
                    return PipelineResult(
                        success=False,
                        started_at=started_at,
                        completed_at=_utc_now(),
                        error_message=f"{data_type}: {step_result.error_message}",
                        step_results=step_results,
                    )

            completed_at = _utc_now()
            duration = (completed_at - started_at).total_seconds()
            total_loaded = sum(r.loaded_count for r in step_results)
            logger.info(f"ETL pipeline completed successfully in {duration:.2f}s ({total_loaded} total records)")

            return PipelineResult(
                success=True,
                started_at=started_at,
                completed_at=completed_at,
                step_results=step_results,
            )

        except Exception as e:
            logger.exception(f"ETL pipeline failed: {e}")
            return PipelineResult(
                success=False,
                started_at=started_at,
                completed_at=_utc_now(),
                error_message=str(e),
                step_results=step_results,
            )

    def run_assets_only(self) -> PipelineResult:
        """Run ETL pipeline for assets only.

        Returns:
            PipelineResult with asset loading results.
        """
        return self.run(data_types=["asset"])

    def run_trades_only(self) -> PipelineResult:
        """Run ETL pipeline for trades only. Requires assets to be already loaded.

        Returns:
            PipelineResult with trade loading results.
        """
        return self.run(data_types=["trade"])

    def close(self) -> None:
        """Clean up resources."""
        self._duckdb.close()
        self._repo.close()
        logger.info("ETLPipeline resources cleaned up")

    def __enter__(self) -> "ETLPipeline":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
