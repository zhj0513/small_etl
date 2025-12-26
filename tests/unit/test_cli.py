"""Tests for CLI module using Hydra."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from omegaconf import DictConfig, OmegaConf

from small_etl.cli import (
    EXIT_ERROR,
    EXIT_SUCCESS,
    print_result,
    run_clean,
    run_etl,
    run_schedule,
)


@pytest.fixture
def mock_config() -> DictConfig:
    """Create a mock Hydra config."""
    return OmegaConf.create({
        "db": {"url": "postgresql://test:test@localhost/test"},
        "s3": {"bucket": "test"},
        "etl": {"batch_size": 1000},
        "command": "run",
        "job": {
            "id": "",
            "command": "run",
            "interval": "day",
            "at": "",
            "action": "start",
        },
    })


class TestPrintResult:
    """Tests for print_result function."""

    def test_print_result_success(self, capsys):
        """Test printing successful result."""
        from small_etl.application.pipeline import PipelineResult

        result = PipelineResult(
            success=True,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            assets_loaded=100,
            trades_loaded=500,
        )
        print_result(result)

        captured = capsys.readouterr()
        assert "SUCCESS" in captured.out
        assert "Assets: 100" in captured.out
        assert "Trades: 500" in captured.out

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

    def test_print_result_with_assets_only(self, capsys):
        """Test printing result with only assets loaded."""
        from small_etl.application.pipeline import PipelineResult

        result = PipelineResult(
            success=True,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            assets_loaded=100,
        )
        print_result(result)

        captured = capsys.readouterr()
        assert "SUCCESS" in captured.out
        assert "Assets: 100" in captured.out
        assert "Trades" not in captured.out

    def test_print_result_no_loaded_counts(self, capsys):
        """Test printing result with no loaded counts."""
        from small_etl.application.pipeline import PipelineResult

        result = PipelineResult(
            success=True,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
        )
        print_result(result)

        captured = capsys.readouterr()
        assert "SUCCESS" in captured.out
        assert "Assets" not in captured.out
        assert "Trades" not in captured.out


class TestRunClean:
    """Tests for run_clean function."""

    def test_run_clean_success(self, capsys, mock_config):
        """Test successful clean operation."""
        mock_duckdb = MagicMock()
        mock_duckdb.query_count.side_effect = [10, 20]
        mock_duckdb.__enter__ = MagicMock(return_value=mock_duckdb)
        mock_duckdb.__exit__ = MagicMock(return_value=False)

        mock_repo = MagicMock()

        with patch("small_etl.data_access.duckdb_client.DuckDBClient", return_value=mock_duckdb):
            with patch("small_etl.data_access.postgres_repository.PostgresRepository", return_value=mock_repo):
                result = run_clean(mock_config)

        assert result == EXIT_SUCCESS
        mock_repo.truncate_tables.assert_called_once()

    def test_run_clean_failure(self, capsys, mock_config):
        """Test failed clean operation."""
        with patch("small_etl.data_access.duckdb_client.DuckDBClient", side_effect=Exception("Connection failed")):
            result = run_clean(mock_config)

        assert result == EXIT_ERROR


class TestRunSchedule:
    """Tests for run_schedule function."""

    def test_run_schedule_list(self, capsys, mock_config):
        """Test run_schedule list action."""
        mock_config.job.action = "list"

        mock_scheduler = MagicMock()
        mock_scheduler.list_jobs.return_value = []

        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(mock_config)

        assert result == EXIT_SUCCESS

    def test_run_schedule_add(self, capsys, mock_config):
        """Test run_schedule add action."""
        mock_config.job.action = "add"
        mock_config.job.id = "daily-etl"
        mock_config.job.command = "run"
        mock_config.job.interval = "day"
        mock_config.job.at = "02:00"

        mock_job = MagicMock()
        mock_job.job_id = "daily-etl"
        mock_job.command = "run"
        mock_job.interval = "day"

        mock_scheduler = MagicMock()
        mock_scheduler.add_job.return_value = mock_job

        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(mock_config)

        assert result == EXIT_SUCCESS

    def test_run_schedule_add_missing_id(self, mock_config):
        """Test run_schedule add action without job.id."""
        mock_config.job.action = "add"
        mock_config.job.id = ""

        mock_scheduler = MagicMock()
        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(mock_config)

        assert result == EXIT_ERROR

    def test_run_schedule_remove_success(self, mock_config):
        """Test run_schedule remove action success."""
        mock_config.job.action = "remove"
        mock_config.job.id = "daily-etl"

        mock_scheduler = MagicMock()
        mock_scheduler.remove_job.return_value = True

        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(mock_config)

        assert result == EXIT_SUCCESS

    def test_run_schedule_remove_not_found(self, mock_config):
        """Test run_schedule remove action when job not found."""
        mock_config.job.action = "remove"
        mock_config.job.id = "nonexistent"

        mock_scheduler = MagicMock()
        mock_scheduler.remove_job.return_value = False

        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(mock_config)

        assert result == EXIT_ERROR

    def test_run_schedule_pause_success(self, mock_config):
        """Test run_schedule pause action success."""
        mock_config.job.action = "pause"
        mock_config.job.id = "daily-etl"

        mock_scheduler = MagicMock()
        mock_scheduler.pause_job.return_value = True

        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(mock_config)

        assert result == EXIT_SUCCESS

    def test_run_schedule_resume_success(self, mock_config):
        """Test run_schedule resume action success."""
        mock_config.job.action = "resume"
        mock_config.job.id = "daily-etl"

        mock_scheduler = MagicMock()
        mock_scheduler.resume_job.return_value = True

        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(mock_config)

        assert result == EXIT_SUCCESS

    def test_run_schedule_exception(self, mock_config):
        """Test run_schedule handles general exceptions."""
        mock_config.job.action = "list"

        with patch("small_etl.scheduler.scheduler.ETLScheduler", side_effect=Exception("Scheduler error")):
            result = run_schedule(mock_config)

        assert result == EXIT_ERROR

    def test_run_schedule_unknown_action(self, mock_config):
        """Test run_schedule with unknown action."""
        mock_config.job.action = "unknown"

        mock_scheduler = MagicMock()
        with patch("small_etl.scheduler.scheduler.ETLScheduler", return_value=mock_scheduler):
            result = run_schedule(mock_config)

        assert result == EXIT_ERROR


class TestRunETL:
    """Tests for run_etl function."""

    def test_run_etl_success(self, mock_config):
        """Test run_etl with success."""
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.started_at = datetime.now(UTC)
        mock_result.completed_at = datetime.now(UTC)
        mock_result.error_message = None
        mock_result.assets_loaded = 100
        mock_result.trades_loaded = 500

        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = mock_result
        mock_pipeline.__enter__ = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__exit__ = MagicMock(return_value=False)

        with patch("small_etl.cli.ETLPipeline", return_value=mock_pipeline):
            result = run_etl(mock_config)

        assert result == EXIT_SUCCESS

    def test_run_etl_failure(self, mock_config):
        """Test run_etl returns error on failure."""
        mock_result = MagicMock()
        mock_result.success = False
        mock_result.error_message = "Pipeline failed"
        mock_result.started_at = datetime.now(UTC)
        mock_result.completed_at = datetime.now(UTC)
        mock_result.assets_loaded = 0
        mock_result.trades_loaded = 0

        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = mock_result
        mock_pipeline.__enter__ = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__exit__ = MagicMock(return_value=False)

        with patch("small_etl.cli.ETLPipeline", return_value=mock_pipeline):
            result = run_etl(mock_config)

        assert result == EXIT_ERROR

    def test_run_etl_exception(self, mock_config):
        """Test run_etl handles general exceptions."""
        with patch("small_etl.cli.ETLPipeline", side_effect=Exception("Unexpected error")):
            result = run_etl(mock_config)

        assert result == EXIT_ERROR
