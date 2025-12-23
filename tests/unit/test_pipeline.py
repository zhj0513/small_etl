"""Unit tests for ETLPipeline."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from omegaconf import OmegaConf

from small_etl.application.pipeline import ETLPipeline, PipelineResult
from small_etl.services.analytics import AssetStatistics, TradeStatistics
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
                "validation": {"tolerance": 0.01},
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
    return ValidationResult(
        is_valid=True,
        total_rows=2,
        valid_count=2,
        invalid_count=0,
        valid_rows=mock_asset_df,
        invalid_rows=pl.DataFrame(),
        errors=[],
    )


@pytest.fixture
def mock_validation_result_invalid(mock_asset_df: pl.DataFrame) -> ValidationResult:
    """Create invalid ValidationResult with some invalid rows."""
    return ValidationResult(
        is_valid=False,
        total_rows=2,
        valid_count=1,
        invalid_count=1,
        valid_rows=mock_asset_df.head(1),
        invalid_rows=mock_asset_df.tail(1),
        errors=[],
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


@pytest.fixture
def mock_asset_stats() -> AssetStatistics:
    """Create mock asset statistics."""
    from decimal import Decimal

    return AssetStatistics(
        total_records=2,
        total_cash=Decimal("300000.0"),
        total_frozen_cash=Decimal("15000.0"),
        total_market_value=Decimal("500000.0"),
        total_assets=Decimal("815000.0"),
        avg_cash=Decimal("150000.0"),
        avg_total_asset=Decimal("407500.0"),
        by_account_type={},
    )


@pytest.fixture
def mock_trade_stats() -> TradeStatistics:
    """Create mock trade statistics."""
    from decimal import Decimal

    return TradeStatistics(
        total_records=1,
        total_volume=1000,
        total_amount=Decimal("15500.0"),
        avg_price=Decimal("15.5"),
        avg_volume=1000.0,
        by_account_type={},
        by_offset_flag={},
        by_strategy={},
    )


class TestETLPipeline:
    """Tests for ETLPipeline."""

    @patch("small_etl.application.pipeline.S3Connector")
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
        mock_s3_cls: MagicMock,
        mock_config: MagicMock,
        mock_asset_df: pl.DataFrame,
        mock_trade_df: pl.DataFrame,
        mock_validation_result_valid: ValidationResult,
        mock_load_result_success: LoadResult,
        mock_asset_stats: AssetStatistics,
        mock_trade_stats: TradeStatistics,
    ) -> None:
        """Test successful complete pipeline run."""
        # Configure mocks
        mock_extractor = mock_extractor_cls.return_value
        mock_extractor.extract_assets.return_value = mock_asset_df
        mock_extractor.extract_trades.return_value = mock_trade_df

        mock_validator = mock_validator_cls.return_value
        mock_validator.validate_assets.return_value = mock_validation_result_valid
        mock_validator.validate_trades.return_value = mock_validation_result_valid

        mock_loader = mock_loader_cls.return_value
        mock_loader.load_assets.return_value = mock_load_result_success
        mock_loader.load_trades.return_value = mock_load_result_success

        mock_repo = mock_repo_cls.return_value
        mock_repo.get_all_account_ids.return_value = {"10000000001", "10000000002"}

        mock_analytics = mock_analytics_cls.return_value
        mock_analytics.asset_statistics.return_value = mock_asset_stats
        mock_analytics.trade_statistics.return_value = mock_trade_stats

        # Run pipeline
        pipeline = ETLPipeline(mock_config)
        result = pipeline.run()

        # Assertions
        assert result.success is True
        assert result.error_message is None
        assert result.assets_validation is not None
        assert result.trades_validation is not None
        assert result.assets_load is not None
        assert result.assets_load.success is True
        assert result.trades_load is not None
        assert result.trades_load.success is True
        assert result.assets_stats is not None
        assert result.trades_stats is not None

        # Verify method calls
        mock_extractor.extract_assets.assert_called_once()
        mock_extractor.extract_trades.assert_called_once()
        mock_validator.validate_assets.assert_called_once()
        mock_validator.validate_trades.assert_called_once()
        mock_loader.load_assets.assert_called_once()
        mock_loader.load_trades.assert_called_once()

    @patch("small_etl.application.pipeline.S3Connector")
    @patch("small_etl.application.pipeline.DuckDBClient")
    @patch("small_etl.application.pipeline.PostgresRepository")
    @patch("small_etl.application.pipeline.ExtractorService")
    @patch("small_etl.application.pipeline.ValidatorService")
    @patch("small_etl.application.pipeline.LoaderService")
    @patch("small_etl.application.pipeline.AnalyticsService")
    def test_run_with_invalid_assets(
        self,
        mock_analytics_cls: MagicMock,
        mock_loader_cls: MagicMock,
        mock_validator_cls: MagicMock,
        mock_extractor_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_duckdb_cls: MagicMock,
        mock_s3_cls: MagicMock,
        mock_config: MagicMock,
        mock_asset_df: pl.DataFrame,
        mock_trade_df: pl.DataFrame,
        mock_validation_result_invalid: ValidationResult,
        mock_validation_result_valid: ValidationResult,
        mock_load_result_success: LoadResult,
        mock_asset_stats: AssetStatistics,
        mock_trade_stats: TradeStatistics,
    ) -> None:
        """Test pipeline continues with invalid asset data (logs warning)."""
        mock_extractor = mock_extractor_cls.return_value
        mock_extractor.extract_assets.return_value = mock_asset_df
        mock_extractor.extract_trades.return_value = mock_trade_df

        mock_validator = mock_validator_cls.return_value
        mock_validator.validate_assets.return_value = mock_validation_result_invalid
        mock_validator.validate_trades.return_value = mock_validation_result_valid

        mock_loader = mock_loader_cls.return_value
        mock_loader.load_assets.return_value = mock_load_result_success
        mock_loader.load_trades.return_value = mock_load_result_success

        mock_repo = mock_repo_cls.return_value
        mock_repo.get_all_account_ids.return_value = {"10000000001"}

        mock_analytics = mock_analytics_cls.return_value
        mock_analytics.asset_statistics.return_value = mock_asset_stats
        mock_analytics.trade_statistics.return_value = mock_trade_stats

        pipeline = ETLPipeline(mock_config)
        result = pipeline.run()

        # Pipeline should still succeed even with invalid data
        assert result.success is True
        assert result.assets_validation is not None
        assert result.assets_validation.is_valid is False
        assert result.assets_validation.invalid_count == 1

    @patch("small_etl.application.pipeline.S3Connector")
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
        mock_s3_cls: MagicMock,
        mock_config: MagicMock,
        mock_asset_df: pl.DataFrame,
        mock_validation_result_valid: ValidationResult,
        mock_load_result_failure: LoadResult,
    ) -> None:
        """Test pipeline fails when asset loading fails."""
        mock_extractor = mock_extractor_cls.return_value
        mock_extractor.extract_assets.return_value = mock_asset_df

        mock_validator = mock_validator_cls.return_value
        mock_validator.validate_assets.return_value = mock_validation_result_valid

        mock_loader = mock_loader_cls.return_value
        mock_loader.load_assets.return_value = mock_load_result_failure

        pipeline = ETLPipeline(mock_config)
        result = pipeline.run()

        assert result.success is False
        assert result.error_message is not None
        assert "Asset loading failed" in result.error_message

        # Trades should not be processed
        mock_extractor.extract_trades.assert_not_called()

    @patch("small_etl.application.pipeline.S3Connector")
    @patch("small_etl.application.pipeline.DuckDBClient")
    @patch("small_etl.application.pipeline.PostgresRepository")
    @patch("small_etl.application.pipeline.ExtractorService")
    @patch("small_etl.application.pipeline.ValidatorService")
    @patch("small_etl.application.pipeline.LoaderService")
    @patch("small_etl.application.pipeline.AnalyticsService")
    def test_run_trade_load_failure(
        self,
        mock_analytics_cls: MagicMock,
        mock_loader_cls: MagicMock,
        mock_validator_cls: MagicMock,
        mock_extractor_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_duckdb_cls: MagicMock,
        mock_s3_cls: MagicMock,
        mock_config: MagicMock,
        mock_asset_df: pl.DataFrame,
        mock_trade_df: pl.DataFrame,
        mock_validation_result_valid: ValidationResult,
        mock_load_result_success: LoadResult,
        mock_load_result_failure: LoadResult,
    ) -> None:
        """Test pipeline fails when trade loading fails."""
        mock_extractor = mock_extractor_cls.return_value
        mock_extractor.extract_assets.return_value = mock_asset_df
        mock_extractor.extract_trades.return_value = mock_trade_df

        mock_validator = mock_validator_cls.return_value
        mock_validator.validate_assets.return_value = mock_validation_result_valid
        mock_validator.validate_trades.return_value = mock_validation_result_valid

        mock_loader = mock_loader_cls.return_value
        mock_loader.load_assets.return_value = mock_load_result_success
        mock_loader.load_trades.return_value = mock_load_result_failure

        mock_repo = mock_repo_cls.return_value
        mock_repo.get_all_account_ids.return_value = {"10000000001", "10000000002"}

        pipeline = ETLPipeline(mock_config)
        result = pipeline.run()

        assert result.success is False
        assert result.error_message is not None
        assert "Trade loading failed" in result.error_message

    @patch("small_etl.application.pipeline.S3Connector")
    @patch("small_etl.application.pipeline.DuckDBClient")
    @patch("small_etl.application.pipeline.PostgresRepository")
    @patch("small_etl.application.pipeline.ExtractorService")
    @patch("small_etl.application.pipeline.ValidatorService")
    @patch("small_etl.application.pipeline.LoaderService")
    @patch("small_etl.application.pipeline.AnalyticsService")
    def test_run_extraction_failure(
        self,
        mock_analytics_cls: MagicMock,
        mock_loader_cls: MagicMock,
        mock_validator_cls: MagicMock,
        mock_extractor_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_duckdb_cls: MagicMock,
        mock_s3_cls: MagicMock,
        mock_config: MagicMock,
    ) -> None:
        """Test pipeline fails when extraction fails."""
        mock_extractor = mock_extractor_cls.return_value
        mock_extractor.extract_assets.side_effect = Exception("S3 connection failed")

        pipeline = ETLPipeline(mock_config)
        result = pipeline.run()

        assert result.success is False
        assert result.error_message is not None
        assert "S3 connection failed" in result.error_message

    @patch("small_etl.application.pipeline.S3Connector")
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
        mock_s3_cls: MagicMock,
        mock_config: MagicMock,
        mock_asset_df: pl.DataFrame,
        mock_validation_result_valid: ValidationResult,
        mock_load_result_success: LoadResult,
        mock_asset_stats: AssetStatistics,
    ) -> None:
        """Test assets-only pipeline run."""
        mock_extractor = mock_extractor_cls.return_value
        mock_extractor.extract_assets.return_value = mock_asset_df

        mock_validator = mock_validator_cls.return_value
        mock_validator.validate_assets.return_value = mock_validation_result_valid

        mock_loader = mock_loader_cls.return_value
        mock_loader.load_assets.return_value = mock_load_result_success

        mock_analytics = mock_analytics_cls.return_value
        mock_analytics.asset_statistics.return_value = mock_asset_stats

        pipeline = ETLPipeline(mock_config)
        result = pipeline.run_assets_only()

        assert result.success is True
        assert result.assets_validation is not None
        assert result.assets_load is not None
        assert result.assets_stats is not None
        assert result.trades_validation is None
        assert result.trades_load is None
        assert result.trades_stats is None

        # Trades should not be processed
        mock_extractor.extract_trades.assert_not_called()

    @patch("small_etl.application.pipeline.S3Connector")
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
        mock_s3_cls: MagicMock,
        mock_config: MagicMock,
        mock_trade_df: pl.DataFrame,
        mock_validation_result_valid: ValidationResult,
        mock_load_result_success: LoadResult,
        mock_trade_stats: TradeStatistics,
    ) -> None:
        """Test trades-only pipeline run."""
        mock_repo = mock_repo_cls.return_value
        mock_repo.get_all_account_ids.return_value = {"10000000001", "10000000002"}

        mock_extractor = mock_extractor_cls.return_value
        mock_extractor.extract_trades.return_value = mock_trade_df

        mock_validator = mock_validator_cls.return_value
        mock_validator.validate_trades.return_value = mock_validation_result_valid

        mock_loader = mock_loader_cls.return_value
        mock_loader.load_trades.return_value = mock_load_result_success

        mock_analytics = mock_analytics_cls.return_value
        mock_analytics.trade_statistics.return_value = mock_trade_stats

        pipeline = ETLPipeline(mock_config)
        result = pipeline.run_trades_only()

        assert result.success is True
        assert result.trades_validation is not None
        assert result.trades_load is not None
        assert result.trades_stats is not None
        assert result.assets_validation is None
        assert result.assets_load is None
        assert result.assets_stats is None

        # Assets should not be processed
        mock_extractor.extract_assets.assert_not_called()

    @patch("small_etl.application.pipeline.S3Connector")
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
        mock_s3_cls: MagicMock,
        mock_config: MagicMock,
    ) -> None:
        """Test trades-only pipeline fails when no accounts exist."""
        mock_repo = mock_repo_cls.return_value
        mock_repo.get_all_account_ids.return_value = set()

        pipeline = ETLPipeline(mock_config)
        result = pipeline.run_trades_only()

        assert result.success is False
        assert result.error_message is not None
        assert "No accounts found" in result.error_message

    @patch("small_etl.application.pipeline.S3Connector")
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
        mock_s3_cls: MagicMock,
        mock_config: MagicMock,
    ) -> None:
        """Test pipeline works as context manager."""
        mock_duckdb = mock_duckdb_cls.return_value
        mock_repo = mock_repo_cls.return_value

        with ETLPipeline(mock_config) as pipeline:
            assert isinstance(pipeline, ETLPipeline)

        # Verify cleanup was called
        mock_duckdb.close.assert_called_once()
        mock_repo.close.assert_called_once()

    @patch("small_etl.application.pipeline.S3Connector")
    @patch("small_etl.application.pipeline.DuckDBClient")
    @patch("small_etl.application.pipeline.PostgresRepository")
    @patch("small_etl.application.pipeline.ExtractorService")
    @patch("small_etl.application.pipeline.ValidatorService")
    @patch("small_etl.application.pipeline.LoaderService")
    @patch("small_etl.application.pipeline.AnalyticsService")
    def test_close_cleanup(
        self,
        mock_analytics_cls: MagicMock,
        mock_loader_cls: MagicMock,
        mock_validator_cls: MagicMock,
        mock_extractor_cls: MagicMock,
        mock_repo_cls: MagicMock,
        mock_duckdb_cls: MagicMock,
        mock_s3_cls: MagicMock,
        mock_config: MagicMock,
    ) -> None:
        """Test close method cleans up resources."""
        mock_duckdb = mock_duckdb_cls.return_value
        mock_repo = mock_repo_cls.return_value

        pipeline = ETLPipeline(mock_config)
        pipeline.close()

        mock_duckdb.close.assert_called_once()
        mock_repo.close.assert_called_once()


class TestPipelineResult:
    """Tests for PipelineResult dataclass."""

    def test_pipeline_result_success(self) -> None:
        """Test PipelineResult with success."""
        result = PipelineResult(
            success=True,
            started_at=datetime(2025, 12, 22, 10, 0, 0),
            completed_at=datetime(2025, 12, 22, 10, 5, 0),
        )

        assert result.success is True
        assert result.error_message is None
        assert result.assets_validation is None

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
