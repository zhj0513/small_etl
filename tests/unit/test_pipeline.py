"""Unit tests for ETLPipeline."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from omegaconf import OmegaConf

from small_etl.application.pipeline import ETLPipeline, PipelineResult
from small_etl.services.loader import LoadResult
from small_etl.services.validator import ValidationResult


@pytest.fixture
def mock_config():
    """Create mock Hydra configuration matching test config structure."""
    return OmegaConf.create(
        {
            "db": {
                "url": "postgresql://etl:etlpass@localhost:15432/etl_test_db",
                "echo": False,
            },
            "s3": {
                "endpoint": "localhost:19000",
                "access_key": "minioadmin",
                "secret_key": "minioadmin123",
                "secure": False,
                "bucket": "fake-data-for-training",
                "assets_file": "account_assets.csv",
                "trades_file": "trades.csv",
            },
            "etl": {
                "batch_size": 100,
            },
        }
    )


@pytest.fixture
def mock_asset_df() -> pl.DataFrame:
    """Create mock asset DataFrame."""
    return pl.DataFrame(
        {
            "id": [1, 2],
            "account_id": ["10000000001", "10000000002"],
            "account_type": [1, 1],
            "cash": [100000.0, 200000.0],
            "frozen_cash": [5000.0, 10000.0],
            "market_value": [200000.0, 300000.0],
            "total_asset": [305000.0, 510000.0],
            "updated_at": [datetime(2025, 12, 22), datetime(2025, 12, 22)],
        }
    )


@pytest.fixture
def mock_trade_df() -> pl.DataFrame:
    """Create mock trade DataFrame."""
    return pl.DataFrame(
        {
            "id": [1],
            "account_id": ["10000000001"],
            "account_type": [1],
            "traded_id": ["T001"],
            "stock_code": ["600000"],
            "traded_time": [datetime(2025, 12, 22)],
            "traded_price": [15.5],
            "traded_volume": [1000],
            "traded_amount": [15500.0],
            "strategy_name": ["策略A"],
            "order_remark": [""],
            "direction": [0],
            "offset_flag": [48],
            "created_at": [datetime(2025, 12, 22)],
            "updated_at": [datetime(2025, 12, 22)],
        }
    )


@pytest.fixture
def mock_validation_result_valid(mock_asset_df: pl.DataFrame) -> ValidationResult:
    """Create valid ValidationResult."""
    return ValidationResult(is_valid=True, data=mock_asset_df)


@pytest.fixture
def mock_validation_result_invalid() -> ValidationResult:
    """Create invalid ValidationResult."""
    return ValidationResult(
        is_valid=False,
        data=pl.DataFrame(),
        error_message="Validation failed: invalid data",
    )


@pytest.fixture
def mock_load_result_success() -> LoadResult:
    """Create successful LoadResult."""
    return LoadResult(success=True, total_rows=2, loaded_count=2)


@pytest.fixture
def mock_load_result_failure() -> LoadResult:
    """Create failed LoadResult."""
    return LoadResult(
        success=False,
        total_rows=2,
        loaded_count=0,
        failed_count=2,
        error_message="Database connection failed",
    )


class TestETLPipeline:
    """Tests for ETLPipeline."""

    @patch("small_etl.application.pipeline.DuckDBClient")
    @patch("small_etl.application.pipeline.PostgresRepository")
    @patch("small_etl.application.pipeline.ExtractorService")
    @patch("small_etl.application.pipeline.ValidatorService")
    @patch("small_etl.application.pipeline.LoaderService")
    @patch("small_etl.application.pipeline.AnalyticsService")
    def test_run_success(
        self,
        mock_analytics_cls: MagicMock,
        mock_loader_cls: MagicMock,
        mock_validator_cls: MagicMock,
        mock_extractor_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_duckdb_cls: MagicMock,
        mock_config: MagicMock,
        mock_asset_df: pl.DataFrame,
        mock_trade_df: pl.DataFrame,
        mock_validation_result_valid: ValidationResult,
        mock_load_result_success: LoadResult,
    ) -> None:
        """Test successful complete pipeline run."""
        mock_validator = mock_validator_cls.return_value
        mock_validator.fetch_and_validate.side_effect = [
            mock_validation_result_valid,
            ValidationResult(is_valid=True, data=mock_trade_df),
        ]

        mock_extractor = mock_extractor_cls.return_value
        mock_extractor.transform.side_effect = [mock_asset_df, mock_trade_df]

        mock_loader = mock_loader_cls.return_value
        mock_loader.load.return_value = mock_load_result_success

        mock_duckdb = mock_duckdb_cls.return_value
        mock_duckdb.query_column_values.return_value = {"10000000001", "10000000002"}

        mock_analytics = mock_analytics_cls.return_value
        mock_analytics.asset_statistics_from_db.return_value = {}
        mock_analytics.trade_statistics_from_db.return_value = {}

        pipeline = ETLPipeline(mock_config)
        result = pipeline.run()

        assert result.success is True
        assert result.error_message is None
        assert result.assets_loaded == 2
        assert result.trades_loaded == 2

        assert mock_validator.fetch_and_validate.call_count == 2
        assert mock_extractor.transform.call_count == 2

    @patch("small_etl.application.pipeline.DuckDBClient")
    @patch("small_etl.application.pipeline.PostgresRepository")
    @patch("small_etl.application.pipeline.ExtractorService")
    @patch("small_etl.application.pipeline.ValidatorService")
    @patch("small_etl.application.pipeline.LoaderService")
    @patch("small_etl.application.pipeline.AnalyticsService")
    def test_run_asset_validation_failure(
        self,
        mock_analytics_cls: MagicMock,
        mock_loader_cls: MagicMock,
        mock_validator_cls: MagicMock,
        mock_extractor_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_duckdb_cls: MagicMock,
        mock_config: MagicMock,
        mock_validation_result_invalid: ValidationResult,
    ) -> None:
        """Test pipeline fails when asset validation fails."""
        mock_validator = mock_validator_cls.return_value
        mock_validator.fetch_and_validate.return_value = mock_validation_result_invalid

        pipeline = ETLPipeline(mock_config)
        result = pipeline.run()

        assert result.success is False
        assert "Validation failed" in result.error_message
        mock_validator.fetch_and_validate.assert_called_once()

    @patch("small_etl.application.pipeline.DuckDBClient")
    @patch("small_etl.application.pipeline.PostgresRepository")
    @patch("small_etl.application.pipeline.ExtractorService")
    @patch("small_etl.application.pipeline.ValidatorService")
    @patch("small_etl.application.pipeline.LoaderService")
    @patch("small_etl.application.pipeline.AnalyticsService")
    def test_run_asset_load_failure(
        self,
        mock_analytics_cls: MagicMock,
        mock_loader_cls: MagicMock,
        mock_validator_cls: MagicMock,
        mock_extractor_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_duckdb_cls: MagicMock,
        mock_config: MagicMock,
        mock_asset_df: pl.DataFrame,
        mock_validation_result_valid: ValidationResult,
        mock_load_result_failure: LoadResult,
    ) -> None:
        """Test pipeline fails when asset loading fails."""
        mock_validator = mock_validator_cls.return_value
        mock_validator.fetch_and_validate.return_value = mock_validation_result_valid

        mock_extractor = mock_extractor_cls.return_value
        mock_extractor.transform.return_value = mock_asset_df

        mock_loader = mock_loader_cls.return_value
        mock_loader.load.return_value = mock_load_result_failure

        pipeline = ETLPipeline(mock_config)
        result = pipeline.run()

        assert result.success is False
        assert "Loading failed" in result.error_message

    @patch("small_etl.application.pipeline.DuckDBClient")
    @patch("small_etl.application.pipeline.PostgresRepository")
    @patch("small_etl.application.pipeline.ExtractorService")
    @patch("small_etl.application.pipeline.ValidatorService")
    @patch("small_etl.application.pipeline.LoaderService")
    @patch("small_etl.application.pipeline.AnalyticsService")
    def test_run_trade_validation_failure(
        self,
        mock_analytics_cls: MagicMock,
        mock_loader_cls: MagicMock,
        mock_validator_cls: MagicMock,
        mock_extractor_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_duckdb_cls: MagicMock,
        mock_config: MagicMock,
        mock_asset_df: pl.DataFrame,
        mock_validation_result_valid: ValidationResult,
        mock_validation_result_invalid: ValidationResult,
        mock_load_result_success: LoadResult,
    ) -> None:
        """Test pipeline fails when trade validation fails."""
        mock_validator = mock_validator_cls.return_value
        mock_validator.fetch_and_validate.side_effect = [
            mock_validation_result_valid,
            mock_validation_result_invalid,
        ]

        mock_extractor = mock_extractor_cls.return_value
        mock_extractor.transform.return_value = mock_asset_df

        mock_loader = mock_loader_cls.return_value
        mock_loader.load.return_value = mock_load_result_success

        mock_duckdb = mock_duckdb_cls.return_value
        mock_duckdb.query_column_values.return_value = {"10000000001", "10000000002"}

        pipeline = ETLPipeline(mock_config)
        result = pipeline.run()

        assert result.success is False
        assert "Validation failed" in result.error_message
        assert result.assets_loaded == 2

    @patch("small_etl.application.pipeline.DuckDBClient")
    @patch("small_etl.application.pipeline.PostgresRepository")
    @patch("small_etl.application.pipeline.ExtractorService")
    @patch("small_etl.application.pipeline.ValidatorService")
    @patch("small_etl.application.pipeline.LoaderService")
    @patch("small_etl.application.pipeline.AnalyticsService")
    def test_run_exception(
        self,
        mock_analytics_cls: MagicMock,
        mock_loader_cls: MagicMock,
        mock_validator_cls: MagicMock,
        mock_extractor_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_duckdb_cls: MagicMock,
        mock_config: MagicMock,
    ) -> None:
        """Test pipeline handles exceptions."""
        mock_validator = mock_validator_cls.return_value
        mock_validator.fetch_and_validate.side_effect = Exception("S3 connection failed")

        pipeline = ETLPipeline(mock_config)
        result = pipeline.run()

        assert result.success is False
        assert "S3 connection failed" in result.error_message

    @patch("small_etl.application.pipeline.DuckDBClient")
    @patch("small_etl.application.pipeline.PostgresRepository")
    @patch("small_etl.application.pipeline.ExtractorService")
    @patch("small_etl.application.pipeline.ValidatorService")
    @patch("small_etl.application.pipeline.LoaderService")
    @patch("small_etl.application.pipeline.AnalyticsService")
    def test_run_assets_only_success(
        self,
        mock_analytics_cls: MagicMock,
        mock_loader_cls: MagicMock,
        mock_validator_cls: MagicMock,
        mock_extractor_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_duckdb_cls: MagicMock,
        mock_config: MagicMock,
        mock_asset_df: pl.DataFrame,
        mock_validation_result_valid: ValidationResult,
        mock_load_result_success: LoadResult,
    ) -> None:
        """Test assets-only pipeline run."""
        mock_validator = mock_validator_cls.return_value
        mock_validator.fetch_and_validate.return_value = mock_validation_result_valid

        mock_extractor = mock_extractor_cls.return_value
        mock_extractor.transform.return_value = mock_asset_df

        mock_loader = mock_loader_cls.return_value
        mock_loader.load.return_value = mock_load_result_success

        mock_analytics = mock_analytics_cls.return_value
        mock_analytics.asset_statistics_from_db.return_value = {}

        pipeline = ETLPipeline(mock_config)
        result = pipeline.run_assets_only()

        assert result.success is True
        assert result.assets_loaded == 2
        assert result.trades_loaded == 0

    @patch("small_etl.application.pipeline.DuckDBClient")
    @patch("small_etl.application.pipeline.PostgresRepository")
    @patch("small_etl.application.pipeline.ExtractorService")
    @patch("small_etl.application.pipeline.ValidatorService")
    @patch("small_etl.application.pipeline.LoaderService")
    @patch("small_etl.application.pipeline.AnalyticsService")
    def test_run_trades_only_success(
        self,
        mock_analytics_cls: MagicMock,
        mock_loader_cls: MagicMock,
        mock_validator_cls: MagicMock,
        mock_extractor_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_duckdb_cls: MagicMock,
        mock_config: MagicMock,
        mock_trade_df: pl.DataFrame,
        mock_load_result_success: LoadResult,
    ) -> None:
        """Test trades-only pipeline run."""
        mock_duckdb = mock_duckdb_cls.return_value
        mock_duckdb.query_column_values.return_value = {"10000000001", "10000000002"}

        mock_validator = mock_validator_cls.return_value
        mock_validator.fetch_and_validate.return_value = ValidationResult(is_valid=True, data=mock_trade_df)

        mock_extractor = mock_extractor_cls.return_value
        mock_extractor.transform.return_value = mock_trade_df

        mock_loader = mock_loader_cls.return_value
        mock_loader.load.return_value = mock_load_result_success

        mock_analytics = mock_analytics_cls.return_value
        mock_analytics.trade_statistics_from_db.return_value = {}

        pipeline = ETLPipeline(mock_config)
        result = pipeline.run_trades_only()

        assert result.success is True
        assert result.trades_loaded == 2

    @patch("small_etl.application.pipeline.DuckDBClient")
    @patch("small_etl.application.pipeline.PostgresRepository")
    @patch("small_etl.application.pipeline.ExtractorService")
    @patch("small_etl.application.pipeline.ValidatorService")
    @patch("small_etl.application.pipeline.LoaderService")
    @patch("small_etl.application.pipeline.AnalyticsService")
    def test_run_trades_only_no_accounts(
        self,
        mock_analytics_cls: MagicMock,
        mock_loader_cls: MagicMock,
        mock_validator_cls: MagicMock,
        mock_extractor_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_duckdb_cls: MagicMock,
        mock_config: MagicMock,
    ) -> None:
        """Test trades-only pipeline fails when no accounts exist."""
        mock_duckdb = mock_duckdb_cls.return_value
        mock_duckdb.query_column_values.return_value = set()

        pipeline = ETLPipeline(mock_config)
        result = pipeline.run_trades_only()

        assert result.success is False
        assert "No foreign keys found" in result.error_message

    @patch("small_etl.application.pipeline.DuckDBClient")
    @patch("small_etl.application.pipeline.PostgresRepository")
    @patch("small_etl.application.pipeline.ExtractorService")
    @patch("small_etl.application.pipeline.ValidatorService")
    @patch("small_etl.application.pipeline.LoaderService")
    @patch("small_etl.application.pipeline.AnalyticsService")
    def test_context_manager(
        self,
        mock_analytics_cls: MagicMock,
        mock_loader_cls: MagicMock,
        mock_validator_cls: MagicMock,
        mock_extractor_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_duckdb_cls: MagicMock,
        mock_config: MagicMock,
    ) -> None:
        """Test pipeline works as context manager."""
        mock_duckdb = mock_duckdb_cls.return_value
        mock_repo = mock_repo_cls.return_value

        with ETLPipeline(mock_config) as pipeline:
            assert isinstance(pipeline, ETLPipeline)

        mock_duckdb.close.assert_called_once()
        mock_repo.close.assert_called_once()


class TestPipelineResult:
    """Tests for PipelineResult dataclass."""

    def test_pipeline_result_success(self) -> None:
        """Test PipelineResult with success."""
        from small_etl.application.pipeline import StepResult

        result = PipelineResult(
            success=True,
            started_at=datetime(2025, 12, 22, 10, 0, 0),
            completed_at=datetime(2025, 12, 22, 10, 5, 0),
            step_results=[
                StepResult(data_type="asset", success=True, loaded_count=100),
                StepResult(data_type="trade", success=True, loaded_count=500),
            ],
        )

        assert result.success is True
        assert result.error_message is None
        assert result.assets_loaded == 100
        assert result.trades_loaded == 500

    def test_pipeline_result_failure(self) -> None:
        """Test PipelineResult with failure."""
        result = PipelineResult(
            success=False,
            started_at=datetime(2025, 12, 22, 10, 0, 0),
            completed_at=datetime(2025, 12, 22, 10, 1, 0),
            error_message="Pipeline failed",
        )

        assert result.success is False
        assert result.error_message == "Pipeline failed"
        assert result.assets_loaded == 0
        assert result.trades_loaded == 0
