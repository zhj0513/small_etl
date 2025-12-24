"""Tests for CLI module."""

import argparse
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from small_etl.cli import (
    EXIT_ARGS_ERROR,
    EXIT_CONNECTION_ERROR,
    EXIT_ERROR,
    EXIT_SUCCESS,
    EXIT_VALIDATION_ERROR,
    build_config,
    get_config_dir,
    parse_args,
    print_result,
    run_clean,
    run_pipeline,
    setup_logging,
)


class TestParseArgs:
    """Tests for parse_args function."""

    def test_parse_run_command(self):
        """Test parsing run command."""
        args = parse_args(["run"])
        assert args.command == "run"
        assert args.env == "dev"
        assert args.verbose is False
        assert args.dry_run is False

    def test_parse_assets_command(self):
        """Test parsing assets command."""
        args = parse_args(["assets"])
        assert args.command == "assets"

    def test_parse_trades_command(self):
        """Test parsing trades command."""
        args = parse_args(["trades"])
        assert args.command == "trades"

    def test_parse_clean_command(self):
        """Test parsing clean command."""
        args = parse_args(["clean"])
        assert args.command == "clean"
        assert args.env == "dev"

    def test_parse_env_option(self):
        """Test parsing environment option."""
        args = parse_args(["run", "--env", "test"])
        assert args.env == "test"

        args = parse_args(["run", "-e", "test"])
        assert args.env == "test"

    def test_parse_batch_size_option(self):
        """Test parsing batch size option."""
        args = parse_args(["run", "--batch-size", "5000"])
        assert args.batch_size == 5000

        args = parse_args(["run", "-b", "1000"])
        assert args.batch_size == 1000

    def test_parse_verbose_option(self):
        """Test parsing verbose option."""
        args = parse_args(["run", "--verbose"])
        assert args.verbose is True

        args = parse_args(["run", "-v"])
        assert args.verbose is True

    def test_parse_dry_run_option(self):
        """Test parsing dry-run option."""
        args = parse_args(["run", "--dry-run"])
        assert args.dry_run is True

    def test_parse_s3_options(self):
        """Test parsing S3 override options."""
        args = parse_args([
            "run",
            "--s3-endpoint", "localhost:9000",
            "--s3-bucket", "my-bucket",
            "--assets-file", "assets.csv",
            "--trades-file", "trades.csv",
        ])
        assert args.s3_endpoint == "localhost:9000"
        assert args.s3_bucket == "my-bucket"
        assert args.assets_file == "assets.csv"
        assert args.trades_file == "trades.csv"

    def test_parse_db_options(self):
        """Test parsing database override options."""
        args = parse_args([
            "run",
            "--db-host", "192.168.1.100",
            "--db-port", "5432",
            "--db-name", "mydb",
            "--db-user", "admin",
            "--db-password", "secret",
        ])
        assert args.db_host == "192.168.1.100"
        assert args.db_port == 5432
        assert args.db_name == "mydb"
        assert args.db_user == "admin"
        assert args.db_password == "secret"

    def test_parse_clean_with_db_options(self):
        """Test parsing clean command with database options."""
        args = parse_args([
            "clean",
            "--env", "test",
            "--db-host", "localhost",
            "--db-port", "5432",
        ])
        assert args.command == "clean"
        assert args.env == "test"
        assert args.db_host == "localhost"
        assert args.db_port == 5432

    def test_no_command_exits(self):
        """Test that no command shows help and exits."""
        with pytest.raises(SystemExit) as exc_info:
            parse_args([])
        assert exc_info.value.code == EXIT_ARGS_ERROR


class TestGetConfigDir:
    """Tests for get_config_dir function."""

    def test_get_config_dir_returns_path(self):
        """Test that get_config_dir returns a valid path."""
        config_dir = get_config_dir()
        assert config_dir.exists()
        assert config_dir.name == "configs"


class TestBuildConfig:
    """Tests for build_config function."""

    def test_build_config_default(self):
        """Test building config with default values."""
        args = parse_args(["run"])
        config = build_config(args)

        assert "db" in config
        assert "s3" in config
        assert "etl" in config

    def test_build_config_with_batch_size_override(self):
        """Test building config with batch_size override."""
        args = parse_args(["run", "--batch-size", "5000"])
        config = build_config(args)

        assert config["etl"]["batch_size"] == 5000

    def test_build_config_with_s3_overrides(self):
        """Test building config with S3 overrides."""
        args = parse_args([
            "run",
            "--s3-endpoint", "custom:9000",
            "--s3-bucket", "custom-bucket",
        ])
        config = build_config(args)

        assert config["s3"]["endpoint"] == "custom:9000"
        assert config["s3"]["bucket"] == "custom-bucket"

    def test_build_config_with_db_overrides(self):
        """Test building config with database overrides."""
        args = parse_args([
            "run",
            "--db-host", "custom-host",
            "--db-port", "5432",
            "--db-name", "custom-db",
            "--db-user", "custom-user",
            "--db-password", "custom-pass",
        ])
        config = build_config(args)

        assert config["db"]["host"] == "custom-host"
        assert config["db"]["port"] == 5432
        assert config["db"]["database"] == "custom-db"
        assert config["db"]["user"] == "custom-user"
        assert config["db"]["password"] == "custom-pass"
        assert "custom-host" in config["db"]["url"]
        assert "5432" in config["db"]["url"]

    def test_build_config_clean_command(self):
        """Test building config for clean command (no batch_size)."""
        args = parse_args(["clean"])
        config = build_config(args)

        # Should not raise, even though clean doesn't have batch_size
        assert "db" in config

    def test_build_config_test_env(self):
        """Test building config with test environment."""
        args = parse_args(["run", "--env", "test"])
        config = build_config(args)

        # Should use test database config
        assert "db" in config


class TestSetupLogging:
    """Tests for setup_logging function."""

    def test_setup_logging_default(self):
        """Test setup_logging with default (INFO) level."""
        import logging
        # Reset logging config
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        setup_logging(verbose=False)
        # Check that basicConfig was called (handlers were added)
        assert len(logging.root.handlers) > 0

    def test_setup_logging_verbose(self):
        """Test setup_logging with verbose (DEBUG) level."""
        import logging
        # Reset logging config
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        setup_logging(verbose=True)
        # Check that basicConfig was called
        assert len(logging.root.handlers) > 0


class TestPrintResult:
    """Tests for print_result function."""

    def test_print_result_success(self, capsys):
        """Test printing successful result."""
        from small_etl.application.pipeline import PipelineResult

        result = PipelineResult(
            success=True,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
        )
        print_result(result)

        captured = capsys.readouterr()
        assert "SUCCESS" in captured.out

    def test_print_result_failure(self, capsys):
        """Test printing failed result."""
        from small_etl.application.pipeline import PipelineResult

        result = PipelineResult(
            success=False,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            error_message="Test error",
        )
        print_result(result)

        captured = capsys.readouterr()
        assert "FAILED" in captured.out
        assert "Test error" in captured.out

    def test_print_result_with_assets_validation(self, capsys):
        """Test printing result with assets validation."""
        from small_etl.application.pipeline import PipelineResult
        from small_etl.services.validator import ValidationResult

        import polars as pl

        assets_validation = ValidationResult(
            is_valid=True,
            valid_rows=pl.DataFrame(),
            invalid_rows=pl.DataFrame(),
            errors=[],
            total_rows=100,
            valid_count=95,
            invalid_count=5,
        )

        result = PipelineResult(
            success=True,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            assets_validation=assets_validation,
        )
        print_result(result)

        captured = capsys.readouterr()
        assert "Assets Validation" in captured.out
        assert "100" in captured.out

    def test_print_result_with_load_result(self, capsys):
        """Test printing result with load result."""
        from small_etl.application.pipeline import PipelineResult
        from small_etl.services.loader import LoadResult

        assets_load = LoadResult(
            success=True,
            total_rows=100,
            loaded_count=100,
        )

        result = PipelineResult(
            success=True,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            assets_load=assets_load,
        )
        print_result(result)

        captured = capsys.readouterr()
        assert "Assets Load" in captured.out
        assert "100" in captured.out

    def test_print_result_verbose_with_stats(self, capsys):
        """Test printing result with verbose stats."""
        from decimal import Decimal

        from small_etl.application.pipeline import PipelineResult
        from small_etl.services.analytics import AssetStatistics

        assets_stats = AssetStatistics(
            total_records=100,
            total_cash=Decimal("1000000.00"),
            total_frozen_cash=Decimal("50000.00"),
            total_market_value=Decimal("2000000.00"),
            total_assets=Decimal("3050000.00"),
            avg_cash=Decimal("10000.00"),
            avg_total_asset=Decimal("30500.00"),
            by_account_type={},
        )

        result = PipelineResult(
            success=True,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            assets_stats=assets_stats,
        )
        print_result(result, verbose=True)

        captured = capsys.readouterr()
        assert "Assets Stats" in captured.out

    def test_print_result_verbose_with_trades_stats(self, capsys):
        """Test printing result with verbose trades stats."""
        from decimal import Decimal

        from small_etl.application.pipeline import PipelineResult
        from small_etl.services.analytics import TradeStatistics

        trades_stats = TradeStatistics(
            total_records=500,
            total_volume=100000,
            total_amount=Decimal("5000000.00"),
            avg_price=Decimal("50.00"),
            avg_volume=200.0,
            by_account_type={},
            by_offset_flag={},
            by_strategy={},
        )

        result = PipelineResult(
            success=True,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            trades_stats=trades_stats,
        )
        print_result(result, verbose=True)

        captured = capsys.readouterr()
        assert "Trades Stats" in captured.out

    def test_print_result_verbose_with_validation_errors(self, capsys):
        """Test printing result with verbose validation errors."""
        from small_etl.application.pipeline import PipelineResult
        from small_etl.services.validator import ValidationError, ValidationResult

        import polars as pl

        errors = [
            ValidationError(row_index=1, field="cash", message="Negative value", value="-100"),
            ValidationError(row_index=2, field="total_asset", message="Mismatch", value="999"),
        ]

        assets_validation = ValidationResult(
            is_valid=False,
            valid_rows=pl.DataFrame(),
            invalid_rows=pl.DataFrame(),
            errors=errors,
            total_rows=100,
            valid_count=98,
            invalid_count=2,
        )

        result = PipelineResult(
            success=True,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            assets_validation=assets_validation,
        )
        print_result(result, verbose=True)

        captured = capsys.readouterr()
        assert "Errors" in captured.out
        assert "cash" in captured.out


class TestRunClean:
    """Tests for run_clean function."""

    def test_run_clean_success(self, capsys):
        """Test successful clean operation."""
        import logging

        mock_repo = MagicMock()
        mock_repo.get_asset_count.return_value = 10
        mock_repo.get_trade_count.return_value = 20

        config = {"db": {"url": "postgresql://test:test@localhost/test"}}
        logger = logging.getLogger("test_clean")

        with patch("small_etl.data_access.postgres_repository.PostgresRepository", return_value=mock_repo):
            result = run_clean(config, logger)

        assert result == EXIT_SUCCESS
        mock_repo.truncate_tables.assert_called_once()
        mock_repo.close.assert_called_once()

        captured = capsys.readouterr()
        assert "SUCCESS" in captured.out
        assert "10" in captured.out
        assert "20" in captured.out

    def test_run_clean_failure(self, capsys):
        """Test failed clean operation."""
        import logging

        config = {"db": {"url": "postgresql://test:test@localhost/test"}}
        logger = logging.getLogger("test_clean_fail")

        with patch("small_etl.data_access.postgres_repository.PostgresRepository", side_effect=Exception("Connection failed")):
            result = run_clean(config, logger)

        assert result == EXIT_ERROR
        captured = capsys.readouterr()
        assert "FAILED" in captured.out


class TestRunPipeline:
    """Tests for run_pipeline function."""

    def test_run_pipeline_clean_command(self):
        """Test run_pipeline with clean command."""
        args = argparse.Namespace(
            command="clean",
            env="dev",
            verbose=False,
            db_host=None,
            db_port=None,
            db_name=None,
            db_user=None,
            db_password=None,
        )

        with patch("small_etl.cli.run_clean", return_value=EXIT_SUCCESS) as mock_clean:
            result = run_pipeline(args)

        assert result == EXIT_SUCCESS
        mock_clean.assert_called_once()

    def test_run_pipeline_run_command(self):
        """Test run_pipeline with run command."""
        args = argparse.Namespace(
            command="run",
            env="dev",
            verbose=False,
            dry_run=False,
            batch_size=None,
            s3_endpoint=None,
            s3_bucket=None,
            assets_file=None,
            trades_file=None,
            db_host=None,
            db_port=None,
            db_name=None,
            db_user=None,
            db_password=None,
        )

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.assets_validation = None
        mock_result.trades_validation = None
        mock_result.started_at = datetime.now(UTC)
        mock_result.completed_at = datetime.now(UTC)
        mock_result.assets_load = None
        mock_result.trades_load = None
        mock_result.assets_stats = None
        mock_result.trades_stats = None
        mock_result.error_message = None

        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = mock_result
        mock_pipeline.__enter__ = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__exit__ = MagicMock(return_value=False)

        with patch("small_etl.cli.ETLPipeline", return_value=mock_pipeline):
            result = run_pipeline(args)

        assert result == EXIT_SUCCESS

    def test_run_pipeline_assets_command(self):
        """Test run_pipeline with assets command."""
        args = argparse.Namespace(
            command="assets",
            env="dev",
            verbose=False,
            dry_run=False,
            batch_size=None,
            s3_endpoint=None,
            s3_bucket=None,
            assets_file=None,
            trades_file=None,
            db_host=None,
            db_port=None,
            db_name=None,
            db_user=None,
            db_password=None,
        )

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.assets_validation = None
        mock_result.trades_validation = None
        mock_result.started_at = datetime.now(UTC)
        mock_result.completed_at = datetime.now(UTC)
        mock_result.assets_load = None
        mock_result.trades_load = None
        mock_result.assets_stats = None
        mock_result.trades_stats = None
        mock_result.error_message = None

        mock_pipeline = MagicMock()
        mock_pipeline.run_assets_only.return_value = mock_result
        mock_pipeline.__enter__ = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__exit__ = MagicMock(return_value=False)

        with patch("small_etl.cli.ETLPipeline", return_value=mock_pipeline):
            result = run_pipeline(args)

        assert result == EXIT_SUCCESS
        mock_pipeline.run_assets_only.assert_called_once()

    def test_run_pipeline_trades_command(self):
        """Test run_pipeline with trades command."""
        args = argparse.Namespace(
            command="trades",
            env="dev",
            verbose=False,
            dry_run=False,
            batch_size=None,
            s3_endpoint=None,
            s3_bucket=None,
            assets_file=None,
            trades_file=None,
            db_host=None,
            db_port=None,
            db_name=None,
            db_user=None,
            db_password=None,
        )

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.assets_validation = None
        mock_result.trades_validation = None
        mock_result.started_at = datetime.now(UTC)
        mock_result.completed_at = datetime.now(UTC)
        mock_result.assets_load = None
        mock_result.trades_load = None
        mock_result.assets_stats = None
        mock_result.trades_stats = None
        mock_result.error_message = None

        mock_pipeline = MagicMock()
        mock_pipeline.run_trades_only.return_value = mock_result
        mock_pipeline.__enter__ = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__exit__ = MagicMock(return_value=False)

        with patch("small_etl.cli.ETLPipeline", return_value=mock_pipeline):
            result = run_pipeline(args)

        assert result == EXIT_SUCCESS
        mock_pipeline.run_trades_only.assert_called_once()

    def test_run_pipeline_with_validation_errors(self):
        """Test run_pipeline returns validation error code."""
        args = argparse.Namespace(
            command="run",
            env="dev",
            verbose=False,
            dry_run=False,
            batch_size=None,
            s3_endpoint=None,
            s3_bucket=None,
            assets_file=None,
            trades_file=None,
            db_host=None,
            db_port=None,
            db_name=None,
            db_user=None,
            db_password=None,
        )

        mock_validation = MagicMock()
        mock_validation.invalid_count = 5
        mock_validation.total_rows = 100
        mock_validation.valid_count = 95
        mock_validation.errors = []

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.assets_validation = mock_validation
        mock_result.trades_validation = None
        mock_result.started_at = datetime.now(UTC)
        mock_result.completed_at = datetime.now(UTC)
        mock_result.assets_load = None
        mock_result.trades_load = None
        mock_result.assets_stats = None
        mock_result.trades_stats = None
        mock_result.error_message = None

        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = mock_result
        mock_pipeline.__enter__ = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__exit__ = MagicMock(return_value=False)

        with patch("small_etl.cli.ETLPipeline", return_value=mock_pipeline):
            result = run_pipeline(args)

        assert result == EXIT_VALIDATION_ERROR

    def test_run_pipeline_failure(self):
        """Test run_pipeline returns error on failure."""
        args = argparse.Namespace(
            command="run",
            env="dev",
            verbose=False,
            dry_run=False,
            batch_size=None,
            s3_endpoint=None,
            s3_bucket=None,
            assets_file=None,
            trades_file=None,
            db_host=None,
            db_port=None,
            db_name=None,
            db_user=None,
            db_password=None,
        )

        mock_result = MagicMock()
        mock_result.success = False
        mock_result.assets_validation = None
        mock_result.trades_validation = None
        mock_result.started_at = datetime.now(UTC)
        mock_result.completed_at = datetime.now(UTC)
        mock_result.assets_load = None
        mock_result.trades_load = None
        mock_result.assets_stats = None
        mock_result.trades_stats = None
        mock_result.error_message = "Pipeline failed"

        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = mock_result
        mock_pipeline.__enter__ = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__exit__ = MagicMock(return_value=False)

        with patch("small_etl.cli.ETLPipeline", return_value=mock_pipeline):
            result = run_pipeline(args)

        assert result == EXIT_ERROR

    def test_run_pipeline_file_not_found(self):
        """Test run_pipeline handles FileNotFoundError."""
        args = argparse.Namespace(
            command="run",
            env="dev",
            verbose=False,
            dry_run=False,
            batch_size=None,
            s3_endpoint=None,
            s3_bucket=None,
            assets_file=None,
            trades_file=None,
            db_host=None,
            db_port=None,
            db_name=None,
            db_user=None,
            db_password=None,
        )

        with patch("small_etl.cli.build_config", side_effect=FileNotFoundError("Config not found")):
            result = run_pipeline(args)

        assert result == EXIT_ARGS_ERROR

    def test_run_pipeline_connection_error(self):
        """Test run_pipeline handles ConnectionError."""
        args = argparse.Namespace(
            command="run",
            env="dev",
            verbose=False,
            dry_run=False,
            batch_size=None,
            s3_endpoint=None,
            s3_bucket=None,
            assets_file=None,
            trades_file=None,
            db_host=None,
            db_port=None,
            db_name=None,
            db_user=None,
            db_password=None,
        )

        with patch("small_etl.cli.ETLPipeline", side_effect=ConnectionError("Cannot connect")):
            result = run_pipeline(args)

        assert result == EXIT_CONNECTION_ERROR

    def test_run_pipeline_general_exception(self):
        """Test run_pipeline handles general exceptions."""
        args = argparse.Namespace(
            command="run",
            env="dev",
            verbose=False,
            dry_run=False,
            batch_size=None,
            s3_endpoint=None,
            s3_bucket=None,
            assets_file=None,
            trades_file=None,
            db_host=None,
            db_port=None,
            db_name=None,
            db_user=None,
            db_password=None,
        )

        with patch("small_etl.cli.ETLPipeline", side_effect=Exception("Unexpected error")):
            result = run_pipeline(args)

        assert result == EXIT_ERROR

    def test_run_pipeline_dry_run(self):
        """Test run_pipeline with dry-run mode."""
        args = argparse.Namespace(
            command="run",
            env="dev",
            verbose=False,
            dry_run=True,
            batch_size=None,
            s3_endpoint=None,
            s3_bucket=None,
            assets_file=None,
            trades_file=None,
            db_host=None,
            db_port=None,
            db_name=None,
            db_user=None,
            db_password=None,
        )

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.assets_validation = None
        mock_result.trades_validation = None
        mock_result.started_at = datetime.now(UTC)
        mock_result.completed_at = datetime.now(UTC)
        mock_result.assets_load = None
        mock_result.trades_load = None
        mock_result.assets_stats = None
        mock_result.trades_stats = None
        mock_result.error_message = None

        mock_pipeline = MagicMock()
        mock_pipeline.__enter__ = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__exit__ = MagicMock(return_value=False)

        with patch("small_etl.cli.ETLPipeline", return_value=mock_pipeline), \
             patch("small_etl.cli._run_dry", return_value=mock_result):
            result = run_pipeline(args)

        assert result == EXIT_SUCCESS

    def test_run_pipeline_unknown_command(self):
        """Test run_pipeline with unknown command."""
        args = argparse.Namespace(
            command="unknown",
            env="dev",
            verbose=False,
            dry_run=False,
            batch_size=None,
            s3_endpoint=None,
            s3_bucket=None,
            assets_file=None,
            trades_file=None,
            db_host=None,
            db_port=None,
            db_name=None,
            db_user=None,
            db_password=None,
        )

        mock_pipeline = MagicMock()
        mock_pipeline.__enter__ = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__exit__ = MagicMock(return_value=False)

        with patch("small_etl.cli.ETLPipeline", return_value=mock_pipeline):
            result = run_pipeline(args)

        assert result == EXIT_ARGS_ERROR

    def test_run_pipeline_trades_validation_errors(self):
        """Test run_pipeline returns validation error when trades have errors."""
        args = argparse.Namespace(
            command="run",
            env="dev",
            verbose=False,
            dry_run=False,
            batch_size=None,
            s3_endpoint=None,
            s3_bucket=None,
            assets_file=None,
            trades_file=None,
            db_host=None,
            db_port=None,
            db_name=None,
            db_user=None,
            db_password=None,
        )

        mock_validation = MagicMock()
        mock_validation.invalid_count = 3
        mock_validation.total_rows = 50
        mock_validation.valid_count = 47
        mock_validation.errors = []

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.assets_validation = None
        mock_result.trades_validation = mock_validation
        mock_result.started_at = datetime.now(UTC)
        mock_result.completed_at = datetime.now(UTC)
        mock_result.assets_load = None
        mock_result.trades_load = None
        mock_result.assets_stats = None
        mock_result.trades_stats = None
        mock_result.error_message = None

        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = mock_result
        mock_pipeline.__enter__ = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__exit__ = MagicMock(return_value=False)

        with patch("small_etl.cli.ETLPipeline", return_value=mock_pipeline):
            result = run_pipeline(args)

        assert result == EXIT_VALIDATION_ERROR

    def test_run_pipeline_schedule_command(self):
        """Test run_pipeline with schedule command."""
        args = argparse.Namespace(
            command="schedule",
            schedule_command="list",
            env="dev",
            verbose=False,
            db_host=None,
            db_port=None,
            db_name=None,
            db_user=None,
            db_password=None,
        )

        with patch("small_etl.cli.run_schedule", return_value=EXIT_SUCCESS) as mock_schedule:
            result = run_pipeline(args)

        assert result == EXIT_SUCCESS
        mock_schedule.assert_called_once()


class TestRunDry:
    """Tests for _run_dry function."""

    def test_run_dry_run_command(self):
        """Test _run_dry with run command (assets + trades)."""
        from small_etl.cli import _run_dry

        import polars as pl

        mock_pipeline = MagicMock()
        mock_extractor = MagicMock()
        mock_validator = MagicMock()

        mock_pipeline._extractor = mock_extractor
        mock_pipeline._validator = mock_validator

        mock_extractor.extract_assets.return_value = pl.DataFrame({"account_id": ["001"]})
        mock_extractor.extract_trades.return_value = pl.DataFrame({"traded_id": ["T001"]})

        mock_assets_validation = MagicMock()
        mock_trades_validation = MagicMock()
        mock_validator.validate_assets.return_value = mock_assets_validation
        mock_validator.validate_trades.return_value = mock_trades_validation

        config = {
            "s3": {"bucket": "test-bucket", "assets_file": "assets.csv", "trades_file": "trades.csv"}
        }

        result = _run_dry(mock_pipeline, "run", config)

        assert result.success is True
        assert result.assets_validation == mock_assets_validation
        assert result.trades_validation == mock_trades_validation
        mock_extractor.extract_assets.assert_called_once()
        mock_extractor.extract_trades.assert_called_once()

    def test_run_dry_assets_command(self):
        """Test _run_dry with assets command."""
        from small_etl.cli import _run_dry

        import polars as pl

        mock_pipeline = MagicMock()
        mock_extractor = MagicMock()
        mock_validator = MagicMock()

        mock_pipeline._extractor = mock_extractor
        mock_pipeline._validator = mock_validator

        mock_extractor.extract_assets.return_value = pl.DataFrame({"account_id": ["001"]})
        mock_assets_validation = MagicMock()
        mock_validator.validate_assets.return_value = mock_assets_validation

        config = {
            "s3": {"bucket": "test-bucket", "assets_file": "assets.csv", "trades_file": "trades.csv"}
        }

        result = _run_dry(mock_pipeline, "assets", config)

        assert result.success is True
        assert result.assets_validation == mock_assets_validation
        mock_extractor.extract_assets.assert_called_once()
        mock_extractor.extract_trades.assert_not_called()

    def test_run_dry_trades_command(self):
        """Test _run_dry with trades command."""
        from small_etl.cli import _run_dry

        import polars as pl

        mock_pipeline = MagicMock()
        mock_extractor = MagicMock()
        mock_validator = MagicMock()

        mock_pipeline._extractor = mock_extractor
        mock_pipeline._validator = mock_validator

        mock_extractor.extract_trades.return_value = pl.DataFrame({"traded_id": ["T001"]})
        mock_trades_validation = MagicMock()
        mock_validator.validate_trades.return_value = mock_trades_validation

        config = {
            "s3": {"bucket": "test-bucket", "assets_file": "assets.csv", "trades_file": "trades.csv"}
        }

        result = _run_dry(mock_pipeline, "trades", config)

        assert result.success is True
        assert result.trades_validation == mock_trades_validation
        mock_extractor.extract_trades.assert_called_once()
        mock_extractor.extract_assets.assert_not_called()

    def test_run_dry_exception(self):
        """Test _run_dry handles exceptions."""
        from small_etl.cli import _run_dry

        mock_pipeline = MagicMock()
        mock_extractor = MagicMock()
        mock_pipeline._extractor = mock_extractor
        mock_extractor.extract_assets.side_effect = Exception("S3 error")

        config = {
            "s3": {"bucket": "test-bucket", "assets_file": "assets.csv", "trades_file": "trades.csv"}
        }

        result = _run_dry(mock_pipeline, "run", config)

        assert result.success is False
        assert "S3 error" in result.error_message


class TestGetConfigDirFallback:
    """Tests for get_config_dir fallback behavior."""

    def test_get_config_dir_fallback_to_cwd(self):
        """Test get_config_dir falls back to cwd."""
        from pathlib import Path

        from small_etl.cli import get_config_dir

        # The normal case should return the project configs directory
        config_dir = get_config_dir()
        assert config_dir.exists()
        assert config_dir.name == "configs"

    def test_get_config_dir_not_found(self):
        """Test get_config_dir raises FileNotFoundError when no configs found."""
        from pathlib import Path
        from unittest.mock import PropertyMock

        # This tests the edge case where neither path exists
        with patch("pathlib.Path.exists", return_value=False):
            with pytest.raises(FileNotFoundError, match="Cannot find configs directory"):
                get_config_dir()


class TestPrintResultTradesValidation:
    """Tests for print_result with trades validation."""

    def test_print_result_with_trades_validation(self, capsys):
        """Test printing result with trades validation."""
        from small_etl.application.pipeline import PipelineResult
        from small_etl.services.validator import ValidationResult

        import polars as pl

        trades_validation = ValidationResult(
            is_valid=True,
            valid_rows=pl.DataFrame(),
            invalid_rows=pl.DataFrame(),
            errors=[],
            total_rows=50,
            valid_count=48,
            invalid_count=2,
        )

        result = PipelineResult(
            success=True,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            trades_validation=trades_validation,
        )
        print_result(result)

        captured = capsys.readouterr()
        assert "Trades Validation" in captured.out
        assert "50" in captured.out

    def test_print_result_with_trades_load(self, capsys):
        """Test printing result with trades load result."""
        from small_etl.application.pipeline import PipelineResult
        from small_etl.services.loader import LoadResult

        trades_load = LoadResult(
            success=True,
            total_rows=50,
            loaded_count=50,
        )

        result = PipelineResult(
            success=True,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            trades_load=trades_load,
        )
        print_result(result)

        captured = capsys.readouterr()
        assert "Trades Load" in captured.out
        assert "50" in captured.out

    def test_print_result_verbose_with_trades_validation_errors(self, capsys):
        """Test printing result with verbose trades validation errors."""
        from small_etl.application.pipeline import PipelineResult
        from small_etl.services.validator import ValidationError, ValidationResult

        import polars as pl

        errors = [
            ValidationError(row_index=1, field="traded_price", message="Zero price", value="0"),
            ValidationError(row_index=2, field="traded_amount", message="Mismatch", value="999"),
        ]

        trades_validation = ValidationResult(
            is_valid=False,
            valid_rows=pl.DataFrame(),
            invalid_rows=pl.DataFrame(),
            errors=errors,
            total_rows=50,
            valid_count=48,
            invalid_count=2,
        )

        result = PipelineResult(
            success=True,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            trades_validation=trades_validation,
        )
        print_result(result, verbose=True)

        captured = capsys.readouterr()
        assert "Errors" in captured.out
        assert "traded_price" in captured.out


class TestParseArgsSchedule:
    """Tests for parse_args with schedule commands."""

    def test_parse_schedule_start(self):
        """Test parsing schedule start command."""
        args = parse_args(["schedule", "start"])
        assert args.command == "schedule"
        assert args.schedule_command == "start"

    def test_parse_schedule_add(self):
        """Test parsing schedule add command."""
        args = parse_args([
            "schedule", "add",
            "--job-id", "daily-etl",
            "--etl-command", "run",
            "--interval", "day",
            "--at", "02:00",
        ])
        assert args.command == "schedule"
        assert args.schedule_command == "add"
        assert args.job_id == "daily-etl"
        assert args.etl_command == "run"
        assert args.interval == "day"
        assert getattr(args, "at") == "02:00"

    def test_parse_schedule_list(self):
        """Test parsing schedule list command."""
        args = parse_args(["schedule", "list"])
        assert args.command == "schedule"
        assert args.schedule_command == "list"

    def test_parse_schedule_remove(self):
        """Test parsing schedule remove command."""
        args = parse_args(["schedule", "remove", "--job-id", "daily-etl"])
        assert args.command == "schedule"
        assert args.schedule_command == "remove"
        assert args.job_id == "daily-etl"

    def test_parse_schedule_pause(self):
        """Test parsing schedule pause command."""
        args = parse_args(["schedule", "pause", "--job-id", "daily-etl"])
        assert args.command == "schedule"
        assert args.schedule_command == "pause"
        assert args.job_id == "daily-etl"

    def test_parse_schedule_resume(self):
        """Test parsing schedule resume command."""
        args = parse_args(["schedule", "resume", "--job-id", "daily-etl"])
        assert args.command == "schedule"
        assert args.schedule_command == "resume"
        assert args.job_id == "daily-etl"


class TestRunSchedule:
    """Tests for run_schedule function."""

    def test_run_schedule_no_command(self, capsys):
        """Test run_schedule with no schedule command."""
        from small_etl.cli import run_schedule

        import logging

        args = argparse.Namespace(schedule_command=None)
        config = {"db": {"url": "postgresql://test:test@localhost/test"}}
        logger = logging.getLogger("test_schedule")

        result = run_schedule(args, config, logger)

        assert result == EXIT_ARGS_ERROR
        captured = capsys.readouterr()
        assert "Please specify" in captured.out

    def test_run_schedule_list(self, capsys):
        """Test run_schedule list command."""
        from small_etl.cli import run_schedule

        import logging

        args = argparse.Namespace(schedule_command="list", env="dev", verbose=False)
        config = {"db": {"url": "postgresql://test:test@localhost/test"}}
        logger = logging.getLogger("test_schedule_list")

        mock_scheduler = MagicMock()
        mock_scheduler.list_jobs.return_value = []

        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(args, config, logger)

        assert result == EXIT_SUCCESS
        captured = capsys.readouterr()
        assert "Scheduled Jobs" in captured.out

    def test_run_schedule_list_with_jobs(self, capsys):
        """Test run_schedule list command with jobs."""
        from small_etl.cli import run_schedule

        import logging

        args = argparse.Namespace(schedule_command="list", env="dev", verbose=False)
        config = {"db": {"url": "postgresql://test:test@localhost/test"}}
        logger = logging.getLogger("test_schedule_list")

        mock_job = MagicMock()
        mock_job.job_id = "daily-etl"
        mock_job.command = "run"
        mock_job.interval = "day"
        mock_job.at_time = "02:00"
        mock_job.enabled = True
        mock_job.last_run = None

        mock_scheduler = MagicMock()
        mock_scheduler.list_jobs.return_value = [mock_job]

        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(args, config, logger)

        assert result == EXIT_SUCCESS
        captured = capsys.readouterr()
        assert "daily-etl" in captured.out

    def test_run_schedule_add(self, capsys):
        """Test run_schedule add command."""
        from small_etl.cli import run_schedule

        import logging

        args = argparse.Namespace(
            schedule_command="add",
            env="dev",
            verbose=False,
            job_id="daily-etl",
            etl_command="run",
            interval="day",
            at="02:00",
        )
        config = {"db": {"url": "postgresql://test:test@localhost/test"}}
        logger = logging.getLogger("test_schedule_add")

        mock_job = MagicMock()
        mock_job.job_id = "daily-etl"
        mock_job.command = "run"
        mock_job.interval = "day"
        mock_job.at_time = "02:00"

        mock_scheduler = MagicMock()
        mock_scheduler.add_job.return_value = mock_job

        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(args, config, logger)

        assert result == EXIT_SUCCESS
        captured = capsys.readouterr()
        assert "Job Added" in captured.out

    def test_run_schedule_remove_success(self, capsys):
        """Test run_schedule remove command success."""
        from small_etl.cli import run_schedule

        import logging

        args = argparse.Namespace(
            schedule_command="remove",
            env="dev",
            verbose=False,
            job_id="daily-etl",
        )
        config = {"db": {"url": "postgresql://test:test@localhost/test"}}
        logger = logging.getLogger("test_schedule_remove")

        mock_scheduler = MagicMock()
        mock_scheduler.remove_job.return_value = True

        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(args, config, logger)

        assert result == EXIT_SUCCESS
        captured = capsys.readouterr()
        assert "removed successfully" in captured.out

    def test_run_schedule_remove_not_found(self, capsys):
        """Test run_schedule remove command when job not found."""
        from small_etl.cli import run_schedule

        import logging

        args = argparse.Namespace(
            schedule_command="remove",
            env="dev",
            verbose=False,
            job_id="nonexistent",
        )
        config = {"db": {"url": "postgresql://test:test@localhost/test"}}
        logger = logging.getLogger("test_schedule_remove")

        mock_scheduler = MagicMock()
        mock_scheduler.remove_job.return_value = False

        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(args, config, logger)

        assert result == EXIT_ERROR
        captured = capsys.readouterr()
        assert "not found" in captured.out

    def test_run_schedule_pause_success(self, capsys):
        """Test run_schedule pause command success."""
        from small_etl.cli import run_schedule

        import logging

        args = argparse.Namespace(
            schedule_command="pause",
            env="dev",
            verbose=False,
            job_id="daily-etl",
        )
        config = {"db": {"url": "postgresql://test:test@localhost/test"}}
        logger = logging.getLogger("test_schedule_pause")

        mock_scheduler = MagicMock()
        mock_scheduler.pause_job.return_value = True

        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(args, config, logger)

        assert result == EXIT_SUCCESS
        captured = capsys.readouterr()
        assert "paused successfully" in captured.out

    def test_run_schedule_pause_not_found(self):
        """Test run_schedule pause command when job not found."""
        from small_etl.cli import run_schedule

        import logging

        args = argparse.Namespace(
            schedule_command="pause",
            env="dev",
            verbose=False,
            job_id="nonexistent",
        )
        config = {"db": {"url": "postgresql://test:test@localhost/test"}}
        logger = logging.getLogger("test_schedule_pause")

        mock_scheduler = MagicMock()
        mock_scheduler.pause_job.return_value = False

        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(args, config, logger)

        assert result == EXIT_ERROR

    def test_run_schedule_resume_success(self, capsys):
        """Test run_schedule resume command success."""
        from small_etl.cli import run_schedule

        import logging

        args = argparse.Namespace(
            schedule_command="resume",
            env="dev",
            verbose=False,
            job_id="daily-etl",
        )
        config = {"db": {"url": "postgresql://test:test@localhost/test"}}
        logger = logging.getLogger("test_schedule_resume")

        mock_scheduler = MagicMock()
        mock_scheduler.resume_job.return_value = True

        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(args, config, logger)

        assert result == EXIT_SUCCESS
        captured = capsys.readouterr()
        assert "resumed successfully" in captured.out

    def test_run_schedule_resume_not_found(self):
        """Test run_schedule resume command when job not found."""
        from small_etl.cli import run_schedule

        import logging

        args = argparse.Namespace(
            schedule_command="resume",
            env="dev",
            verbose=False,
            job_id="nonexistent",
        )
        config = {"db": {"url": "postgresql://test:test@localhost/test"}}
        logger = logging.getLogger("test_schedule_resume")

        mock_scheduler = MagicMock()
        mock_scheduler.resume_job.return_value = False

        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(args, config, logger)

        assert result == EXIT_ERROR

    def test_run_schedule_unknown_command(self):
        """Test run_schedule with unknown schedule command."""
        from small_etl.cli import run_schedule

        import logging

        args = argparse.Namespace(
            schedule_command="unknown",
            env="dev",
            verbose=False,
        )
        config = {"db": {"url": "postgresql://test:test@localhost/test"}}
        logger = logging.getLogger("test_schedule_unknown")

        mock_scheduler = MagicMock()

        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(args, config, logger)

        assert result == EXIT_ARGS_ERROR

    def test_run_schedule_value_error(self):
        """Test run_schedule handles ValueError."""
        from small_etl.cli import run_schedule

        import logging

        args = argparse.Namespace(
            schedule_command="add",
            env="dev",
            verbose=False,
            job_id="daily-etl",
            etl_command="run",
            interval="invalid",
            at=None,
        )
        config = {"db": {"url": "postgresql://test:test@localhost/test"}}
        logger = logging.getLogger("test_schedule_error")

        mock_scheduler = MagicMock()
        mock_scheduler.add_job.side_effect = ValueError("Invalid interval")

        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(args, config, logger)

        assert result == EXIT_ARGS_ERROR

    def test_run_schedule_exception(self):
        """Test run_schedule handles general exceptions."""
        from small_etl.cli import run_schedule

        import logging

        args = argparse.Namespace(
            schedule_command="list",
            env="dev",
            verbose=False,
        )
        config = {"db": {"url": "postgresql://test:test@localhost/test"}}
        logger = logging.getLogger("test_schedule_exception")

        with patch("small_etl.scheduler.scheduler.ETLScheduler", side_effect=Exception("Scheduler error")):
            result = run_schedule(args, config, logger)

        assert result == EXIT_ERROR


class TestMain:
    """Tests for main function."""

    def test_main_function(self):
        """Test main function."""
        from small_etl.cli import main

        with patch("small_etl.cli.parse_args") as mock_parse, \
             patch("small_etl.cli.run_pipeline", return_value=EXIT_SUCCESS) as mock_run:
            mock_parse.return_value = argparse.Namespace(command="run")

            result = main()

            assert result == EXIT_SUCCESS
            mock_parse.assert_called_once()
            mock_run.assert_called_once()
