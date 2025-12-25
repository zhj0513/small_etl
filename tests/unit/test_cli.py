"""Tests for CLI module."""

import argparse
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from small_etl.cli import (
    EXIT_ARGS_ERROR,
    EXIT_ERROR,
    EXIT_SUCCESS,
    build_config,
    get_config_dir,
    parse_args,
    print_result,
    run_clean,
    run_pipeline,
    run_schedule,
    setup_logging,
)


class TestParseArgs:
    """Tests for parse_args function."""

    def test_parse_run_command(self):
        """Test parsing run command."""
        args, overrides = parse_args(["run"])
        assert args.command == "run"
        assert overrides == []

    def test_parse_clean_command(self):
        """Test parsing clean command."""
        args, overrides = parse_args(["clean"])
        assert args.command == "clean"
        assert overrides == []

    def test_parse_hydra_overrides(self):
        """Test parsing Hydra override arguments."""
        args, overrides = parse_args(["run", "etl.batch_size=5000", "db.host=localhost"])
        assert overrides == ["etl.batch_size=5000", "db.host=localhost"]

    def test_parse_hydra_db_overrides(self):
        """Test parsing database override via Hydra syntax."""
        args, overrides = parse_args([
            "run",
            "db.host=192.168.1.100",
            "db.port=5432",
            "db=test",
        ])
        assert "db.host=192.168.1.100" in overrides
        assert "db.port=5432" in overrides
        assert "db=test" in overrides

    def test_no_command_exits(self):
        """Test that no command shows help and exits."""
        with pytest.raises(SystemExit) as exc_info:
            parse_args([])
        assert exc_info.value.code == EXIT_ARGS_ERROR

    def test_parse_hydra_special_overrides(self):
        """Test parsing special Hydra override syntax (+, ~)."""
        args, overrides = parse_args(["run", "+new.key=value", "~remove.key"])
        assert "+new.key=value" in overrides
        assert "~remove.key" in overrides


class TestParseArgsSchedule:
    """Tests for parse_args with schedule commands."""

    def test_parse_schedule_start(self):
        """Test parsing schedule start command."""
        args, overrides = parse_args(["schedule", "start"])
        assert args.command == "schedule"
        assert args.schedule_command == "start"

    def test_parse_schedule_add(self):
        """Test parsing schedule add command."""
        args, overrides = parse_args([
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
        args, overrides = parse_args(["schedule", "list"])
        assert args.command == "schedule"
        assert args.schedule_command == "list"

    def test_parse_schedule_remove(self):
        """Test parsing schedule remove command."""
        args, overrides = parse_args(["schedule", "remove", "--job-id", "daily-etl"])
        assert args.command == "schedule"
        assert args.schedule_command == "remove"
        assert args.job_id == "daily-etl"

    def test_parse_schedule_pause(self):
        """Test parsing schedule pause command."""
        args, overrides = parse_args(["schedule", "pause", "--job-id", "daily-etl"])
        assert args.command == "schedule"
        assert args.schedule_command == "pause"
        assert args.job_id == "daily-etl"

    def test_parse_schedule_resume(self):
        """Test parsing schedule resume command."""
        args, overrides = parse_args(["schedule", "resume", "--job-id", "daily-etl"])
        assert args.command == "schedule"
        assert args.schedule_command == "resume"
        assert args.job_id == "daily-etl"


class TestGetConfigDir:
    """Tests for get_config_dir function."""

    def test_get_config_dir_returns_path(self):
        """Test that get_config_dir returns a valid path."""
        config_dir = get_config_dir()
        assert config_dir.exists()
        assert config_dir.name == "configs"

    def test_get_config_dir_not_found(self):
        """Test get_config_dir raises FileNotFoundError when no configs found."""
        with patch("pathlib.Path.exists", return_value=False):
            with pytest.raises(FileNotFoundError, match="Cannot find configs directory"):
                get_config_dir()


class TestBuildConfig:
    """Tests for build_config function."""

    def test_build_config_default(self):
        """Test building config with default values."""
        config = build_config([])
        assert "db" in config
        assert "s3" in config
        assert "etl" in config

    def test_build_config_with_overrides(self):
        """Test building config with Hydra overrides."""
        config = build_config(["etl.batch_size=5000"])
        assert config["etl"]["batch_size"] == 5000

    def test_build_config_test_env(self):
        """Test building config with test environment."""
        config = build_config(["db=test"])
        assert config["db"]["database"] == "etl_test_db"


class TestSetupLogging:
    """Tests for setup_logging function."""

    def test_setup_logging(self):
        """Test setup_logging configures handlers."""
        import logging
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        setup_logging()
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

    def test_print_result_with_validation(self, capsys):
        """Test printing result with validation info."""
        from small_etl.application.pipeline import DataTypeResult, PipelineResult
        from small_etl.services.validator import ValidationResult

        import polars as pl

        validation = ValidationResult(
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
            results={"asset": DataTypeResult(data_type="asset", validation=validation)},
        )
        print_result(result)

        captured = capsys.readouterr()
        assert "Asset" in captured.out
        assert "Total=100" in captured.out
        assert "Valid=95" in captured.out
        assert "Invalid=5" in captured.out

    def test_print_result_with_multiple_data_types(self, capsys):
        """Test printing result with multiple data types."""
        from small_etl.application.pipeline import DataTypeResult, PipelineResult
        from small_etl.services.loader import LoadResult
        from small_etl.services.validator import ValidationResult

        import polars as pl

        asset_validation = ValidationResult(
            is_valid=True,
            valid_rows=pl.DataFrame(),
            invalid_rows=pl.DataFrame(),
            errors=[],
            total_rows=100,
            valid_count=100,
            invalid_count=0,
        )
        trade_validation = ValidationResult(
            is_valid=True,
            valid_rows=pl.DataFrame(),
            invalid_rows=pl.DataFrame(),
            errors=[],
            total_rows=500,
            valid_count=495,
            invalid_count=5,
        )

        result = PipelineResult(
            success=True,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            results={
                "asset": DataTypeResult(
                    data_type="asset",
                    validation=asset_validation,
                    load=LoadResult(success=True, total_rows=100, loaded_count=100),
                ),
                "trade": DataTypeResult(
                    data_type="trade",
                    validation=trade_validation,
                    load=LoadResult(success=True, total_rows=495, loaded_count=495),
                ),
            },
        )
        print_result(result)

        captured = capsys.readouterr()
        assert "Asset" in captured.out
        assert "Trade" in captured.out
        assert "100 loaded" in captured.out
        assert "495 loaded" in captured.out


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

    def test_run_clean_failure(self, capsys):
        """Test failed clean operation."""
        import logging

        config = {"db": {"url": "postgresql://test:test@localhost/test"}}
        logger = logging.getLogger("test_clean_fail")

        with patch("small_etl.data_access.postgres_repository.PostgresRepository", side_effect=Exception("Connection failed")):
            result = run_clean(config, logger)

        assert result == EXIT_ERROR


class TestRunSchedule:
    """Tests for run_schedule function."""

    def test_run_schedule_no_command(self, capsys):
        """Test run_schedule with no schedule command."""
        import logging

        args = argparse.Namespace(schedule_command=None)
        config = {"db": {"url": "postgresql://test:test@localhost/test"}}
        logger = logging.getLogger("test_schedule")

        result = run_schedule(args, config, logger)

        assert result == EXIT_ARGS_ERROR

    def test_run_schedule_list(self, capsys):
        """Test run_schedule list command."""
        import logging

        args = argparse.Namespace(schedule_command="list")
        config = {"db": {"url": "postgresql://test:test@localhost/test"}}
        logger = logging.getLogger("test_schedule_list")

        mock_scheduler = MagicMock()
        mock_scheduler.list_jobs.return_value = []

        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(args, config, logger)

        assert result == EXIT_SUCCESS

    def test_run_schedule_add(self, capsys):
        """Test run_schedule add command."""
        import logging

        args = argparse.Namespace(
            schedule_command="add",
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

        mock_scheduler = MagicMock()
        mock_scheduler.add_job.return_value = mock_job

        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(args, config, logger)

        assert result == EXIT_SUCCESS

    def test_run_schedule_remove_success(self):
        """Test run_schedule remove command success."""
        import logging

        args = argparse.Namespace(schedule_command="remove", job_id="daily-etl")
        config = {"db": {"url": "postgresql://test:test@localhost/test"}}
        logger = logging.getLogger("test_schedule_remove")

        mock_scheduler = MagicMock()
        mock_scheduler.remove_job.return_value = True

        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(args, config, logger)

        assert result == EXIT_SUCCESS

    def test_run_schedule_remove_not_found(self):
        """Test run_schedule remove command when job not found."""
        import logging

        args = argparse.Namespace(schedule_command="remove", job_id="nonexistent")
        config = {"db": {"url": "postgresql://test:test@localhost/test"}}
        logger = logging.getLogger("test_schedule_remove")

        mock_scheduler = MagicMock()
        mock_scheduler.remove_job.return_value = False

        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(args, config, logger)

        assert result == EXIT_ERROR

    def test_run_schedule_exception(self):
        """Test run_schedule handles general exceptions."""
        import logging

        args = argparse.Namespace(schedule_command="list")
        config = {"db": {"url": "postgresql://test:test@localhost/test"}}
        logger = logging.getLogger("test_schedule_exception")

        with patch("small_etl.scheduler.scheduler.ETLScheduler", side_effect=Exception("Scheduler error")):
            result = run_schedule(args, config, logger)

        assert result == EXIT_ERROR


class TestRunPipeline:
    """Tests for run_pipeline function."""

    def test_run_pipeline_clean_command(self):
        """Test run_pipeline with clean command."""
        args = argparse.Namespace(command="clean")
        hydra_overrides: list[str] = []

        with patch("small_etl.cli.run_clean", return_value=EXIT_SUCCESS) as mock_clean:
            result = run_pipeline(args, hydra_overrides)

        assert result == EXIT_SUCCESS
        mock_clean.assert_called_once()

    def test_run_pipeline_schedule_command(self):
        """Test run_pipeline with schedule command."""
        args = argparse.Namespace(command="schedule", schedule_command="list")
        hydra_overrides: list[str] = []

        with patch("small_etl.cli.run_schedule", return_value=EXIT_SUCCESS) as mock_schedule:
            result = run_pipeline(args, hydra_overrides)

        assert result == EXIT_SUCCESS
        mock_schedule.assert_called_once()

    def test_run_pipeline_run_command(self):
        """Test run_pipeline with run command."""
        args = argparse.Namespace(command="run")
        hydra_overrides: list[str] = []

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.assets_validation = None
        mock_result.trades_validation = None
        mock_result.started_at = datetime.now(UTC)
        mock_result.completed_at = datetime.now(UTC)
        mock_result.assets_load = None
        mock_result.trades_load = None
        mock_result.error_message = None

        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = mock_result
        mock_pipeline.__enter__ = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__exit__ = MagicMock(return_value=False)

        with patch("small_etl.cli.ETLPipeline", return_value=mock_pipeline):
            result = run_pipeline(args, hydra_overrides)

        assert result == EXIT_SUCCESS

    def test_run_pipeline_failure(self):
        """Test run_pipeline returns error on failure."""
        args = argparse.Namespace(command="run")
        hydra_overrides: list[str] = []

        mock_result = MagicMock()
        mock_result.success = False
        mock_result.error_message = "Pipeline failed"
        mock_result.started_at = datetime.now(UTC)
        mock_result.completed_at = datetime.now(UTC)
        mock_result.assets_validation = None
        mock_result.trades_validation = None
        mock_result.assets_load = None
        mock_result.trades_load = None

        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = mock_result
        mock_pipeline.__enter__ = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__exit__ = MagicMock(return_value=False)

        with patch("small_etl.cli.ETLPipeline", return_value=mock_pipeline):
            result = run_pipeline(args, hydra_overrides)

        assert result == EXIT_ERROR

    def test_run_pipeline_file_not_found(self):
        """Test run_pipeline handles FileNotFoundError."""
        args = argparse.Namespace(command="run")
        hydra_overrides: list[str] = []

        with patch("small_etl.cli.build_config", side_effect=FileNotFoundError("Config not found")):
            result = run_pipeline(args, hydra_overrides)

        assert result == EXIT_ARGS_ERROR

    def test_run_pipeline_general_exception(self):
        """Test run_pipeline handles general exceptions."""
        args = argparse.Namespace(command="run")
        hydra_overrides: list[str] = []

        with patch("small_etl.cli.ETLPipeline", side_effect=Exception("Unexpected error")):
            result = run_pipeline(args, hydra_overrides)

        assert result == EXIT_ERROR


class TestMain:
    """Tests for main function."""

    def test_main_function(self):
        """Test main function."""
        from small_etl.cli import main

        with patch("small_etl.cli.parse_args") as mock_parse, \
             patch("small_etl.cli.run_pipeline", return_value=EXIT_SUCCESS) as mock_run:
            mock_parse.return_value = (argparse.Namespace(command="run"), [])

            result = main()

            assert result == EXIT_SUCCESS
            mock_parse.assert_called_once()
            mock_run.assert_called_once()
