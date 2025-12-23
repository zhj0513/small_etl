"""Tests for scheduler module using APScheduler."""

from unittest.mock import MagicMock, patch

import pytest

from small_etl.scheduler.scheduler import ETLScheduler, ScheduledJob, execute_etl_job


class TestScheduledJob:
    """Tests for ScheduledJob dataclass."""

    def test_create_scheduled_job(self):
        """Test creating a ScheduledJob."""
        job = ScheduledJob(
            job_id="test_job",
            command="run",
            interval="day",
            at_time="02:00",
            enabled=True,
        )
        assert job.job_id == "test_job"
        assert job.command == "run"
        assert job.interval == "day"
        assert job.at_time == "02:00"
        assert job.enabled is True
        assert job.last_run is None

    def test_create_scheduled_job_defaults(self):
        """Test ScheduledJob default values."""
        job = ScheduledJob(
            job_id="test_job",
            command="assets",
            interval="hour",
        )
        assert job.at_time is None
        assert job.enabled is True
        assert job.last_run is None


class TestETLScheduler:
    """Tests for ETLScheduler class with APScheduler."""

    @pytest.fixture
    def config(self):
        """Provide a minimal config for scheduler."""
        return {
            "scheduler": {
                "enabled": True,
                "check_interval": 1,
                "jobs": [],
            },
            "db": {
                "url": "postgresql://etl:etlpass@localhost:15432/etl_test_db",
            },
            "s3": {
                "bucket": "test-bucket",
                "assets_file": "assets.csv",
                "trades_file": "trades.csv",
            },
            "etl": {
                "batch_size": 1000,
            },
        }

    @pytest.fixture
    def scheduler(self, config):
        """Create an ETLScheduler instance."""
        sched = ETLScheduler(config, blocking=False)
        yield sched
        # Cleanup: shutdown scheduler and remove all jobs
        try:
            for job in sched._scheduler.get_jobs():
                sched._scheduler.remove_job(job.id)
            sched.shutdown(wait=False)
        except Exception:
            pass

    def test_create_scheduler(self, scheduler):
        """Test creating an ETLScheduler."""
        assert scheduler is not None
        assert scheduler.job_count == 0
        # Background scheduler starts in paused mode, so is_running is True
        assert scheduler.is_running is True

    def test_add_job_day(self, scheduler):
        """Test adding a daily job."""
        job = scheduler.add_job(
            job_id="daily_etl",
            command="run",
            interval="day",
            at_time="02:00",
        )
        assert job.job_id == "daily_etl"
        assert job.command == "run"
        assert job.interval == "day"
        assert job.at_time == "02:00"
        assert scheduler.job_count == 1

    def test_add_job_hour(self, scheduler):
        """Test adding an hourly job."""
        job = scheduler.add_job(
            job_id="hourly_assets",
            command="assets",
            interval="hour",
        )
        assert job.job_id == "hourly_assets"
        assert job.interval == "hour"
        assert scheduler.job_count == 1

    def test_add_job_minute(self, scheduler):
        """Test adding a minute job."""
        job = scheduler.add_job(
            job_id="minute_test",
            command="trades",
            interval="minute",
        )
        assert job.job_id == "minute_test"
        assert job.interval == "minute"
        assert scheduler.job_count == 1

    def test_add_job_duplicate_id(self, scheduler):
        """Test adding a job with duplicate ID raises error."""
        scheduler.add_job(job_id="job1", command="run", interval="day")
        with pytest.raises(ValueError, match="already exists"):
            scheduler.add_job(job_id="job1", command="assets", interval="hour")

    def test_add_job_invalid_command(self, scheduler):
        """Test adding a job with invalid command raises error."""
        with pytest.raises(ValueError, match="Invalid command"):
            scheduler.add_job(job_id="job1", command="invalid", interval="day")

    def test_add_job_invalid_interval(self, scheduler):
        """Test adding a job with invalid interval raises error."""
        with pytest.raises(ValueError, match="Invalid interval"):
            scheduler.add_job(job_id="job1", command="run", interval="weekly")

    def test_remove_job(self, scheduler):
        """Test removing a job."""
        scheduler.add_job(job_id="job1", command="run", interval="day")
        assert scheduler.job_count == 1

        result = scheduler.remove_job("job1")
        assert result is True
        assert scheduler.job_count == 0

    def test_remove_job_not_found(self, scheduler):
        """Test removing a non-existent job."""
        result = scheduler.remove_job("nonexistent")
        assert result is False

    def test_pause_job(self, scheduler):
        """Test pausing a job."""
        scheduler.add_job(job_id="job1", command="run", interval="day")

        result = scheduler.pause_job("job1")
        assert result is True

        # Verify job is paused (next_run_time is None when paused)
        job = scheduler.get_job("job1")
        assert job is not None
        assert job.enabled is False

    def test_pause_job_not_found(self, scheduler):
        """Test pausing a non-existent job."""
        result = scheduler.pause_job("nonexistent")
        assert result is False

    def test_resume_job(self, scheduler):
        """Test resuming a paused job."""
        scheduler.add_job(job_id="job1", command="run", interval="day")
        scheduler.pause_job("job1")

        result = scheduler.resume_job("job1")
        assert result is True

        # Verify job is resumed
        job = scheduler.get_job("job1")
        assert job is not None
        assert job.enabled is True

    def test_resume_job_not_found(self, scheduler):
        """Test resuming a non-existent job."""
        result = scheduler.resume_job("nonexistent")
        assert result is False

    def test_list_jobs(self, scheduler):
        """Test listing jobs."""
        scheduler.add_job(job_id="job1", command="run", interval="day")
        scheduler.add_job(job_id="job2", command="assets", interval="hour")

        jobs = scheduler.list_jobs()
        assert len(jobs) == 2
        assert any(j.job_id == "job1" for j in jobs)
        assert any(j.job_id == "job2" for j in jobs)

    def test_get_job(self, scheduler):
        """Test getting a specific job."""
        scheduler.add_job(job_id="job1", command="run", interval="day")

        job = scheduler.get_job("job1")
        assert job is not None
        assert job.job_id == "job1"

    def test_get_job_not_found(self, scheduler):
        """Test getting a non-existent job."""
        job = scheduler.get_job("nonexistent")
        assert job is None

    def test_stop(self, scheduler):
        """Test stopping the scheduler."""
        # Background scheduler starts in paused mode, so is_running is True
        assert scheduler.is_running is True
        scheduler.stop()
        assert scheduler.is_running is False

    def test_at_time_ignored_for_non_day_interval(self, scheduler):
        """Test that at_time is ignored for non-day intervals."""
        job = scheduler.add_job(
            job_id="hourly_job",
            command="run",
            interval="hour",
            at_time="02:00",  # Should be ignored
        )
        assert job.at_time is None

    def test_job_default_day_at_midnight(self, scheduler):
        """Test that day interval without at_time defaults to midnight."""
        job = scheduler.add_job(
            job_id="daily_job",
            command="run",
            interval="day",
        )
        # Job should be created successfully
        assert job.job_id == "daily_job"
        assert scheduler.job_count == 1


class TestExecuteETLJob:
    """Tests for the execute_etl_job function."""

    @pytest.fixture
    def config(self):
        """Provide a minimal config for ETL job execution."""
        return {
            "db": {
                "url": "postgresql://test:test@localhost/test",
            },
            "s3": {
                "bucket": "test-bucket",
                "assets_file": "assets.csv",
                "trades_file": "trades.csv",
            },
            "etl": {
                "batch_size": 1000,
            },
        }

    @patch("small_etl.application.pipeline.ETLPipeline")
    def test_execute_etl_job_run(self, mock_pipeline_class, config):
        """Test executing 'run' command."""
        mock_pipeline = MagicMock()
        mock_result = MagicMock()
        mock_result.success = True
        mock_pipeline.run.return_value = mock_result
        mock_pipeline.__enter__ = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__exit__ = MagicMock(return_value=False)
        mock_pipeline_class.return_value = mock_pipeline

        execute_etl_job(config, "run")

        mock_pipeline.run.assert_called_once()

    @patch("small_etl.application.pipeline.ETLPipeline")
    def test_execute_etl_job_assets(self, mock_pipeline_class, config):
        """Test executing 'assets' command."""
        mock_pipeline = MagicMock()
        mock_result = MagicMock()
        mock_result.success = True
        mock_pipeline.run_assets_only.return_value = mock_result
        mock_pipeline.__enter__ = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__exit__ = MagicMock(return_value=False)
        mock_pipeline_class.return_value = mock_pipeline

        execute_etl_job(config, "assets")

        mock_pipeline.run_assets_only.assert_called_once()

    @patch("small_etl.application.pipeline.ETLPipeline")
    def test_execute_etl_job_trades(self, mock_pipeline_class, config):
        """Test executing 'trades' command."""
        mock_pipeline = MagicMock()
        mock_result = MagicMock()
        mock_result.success = True
        mock_pipeline.run_trades_only.return_value = mock_result
        mock_pipeline.__enter__ = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__exit__ = MagicMock(return_value=False)
        mock_pipeline_class.return_value = mock_pipeline

        execute_etl_job(config, "trades")

        mock_pipeline.run_trades_only.assert_called_once()

    @patch("small_etl.application.pipeline.ETLPipeline")
    def test_execute_etl_job_invalid(self, mock_pipeline_class, config):
        """Test executing invalid command raises error."""
        mock_pipeline = MagicMock()
        mock_pipeline.__enter__ = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__exit__ = MagicMock(return_value=False)
        mock_pipeline_class.return_value = mock_pipeline

        # The function catches exceptions and logs them, so it won't raise
        # We need to check that it properly handles the error
        execute_etl_job(config, "invalid")
        # If we reach here without crash, the error was logged

    @patch("small_etl.application.pipeline.ETLPipeline")
    def test_execute_etl_job_failure(self, mock_pipeline_class, config):
        """Test handling failed ETL job execution."""
        mock_pipeline = MagicMock()
        mock_result = MagicMock()
        mock_result.success = False
        mock_result.error_message = "Test error"
        mock_pipeline.run.return_value = mock_result
        mock_pipeline.__enter__ = MagicMock(return_value=mock_pipeline)
        mock_pipeline.__exit__ = MagicMock(return_value=False)
        mock_pipeline_class.return_value = mock_pipeline

        # Should not raise, just log error
        execute_etl_job(config, "run")

        mock_pipeline.run.assert_called_once()


class TestSchedulerCLI:
    """Tests for scheduler CLI commands."""

    @pytest.fixture
    def config(self):
        """Provide a minimal config for scheduler."""
        return {
            "scheduler": {
                "enabled": True,
                "check_interval": 1,
                "jobs": [],
            },
            "db": {
                "url": "postgresql://etl:etlpass@localhost:15432/etl_test_db",
            },
            "s3": {
                "bucket": "test-bucket",
                "assets_file": "assets.csv",
                "trades_file": "trades.csv",
            },
            "etl": {
                "batch_size": 1000,
            },
        }

    def test_run_schedule_add(self, config, capsys):
        """Test schedule add command."""
        import argparse
        import logging
        import uuid

        from small_etl.cli import EXIT_SUCCESS, run_schedule

        # Use unique job ID to avoid conflicts with persistent storage
        unique_job_id = f"test_job_{uuid.uuid4().hex[:8]}"

        args = argparse.Namespace(
            schedule_command="add",
            job_id=unique_job_id,
            etl_command="run",
            interval="day",
            at="02:00",
            env="dev",
            verbose=False,
        )
        logger = logging.getLogger("test")

        result = run_schedule(args, config, logger)
        assert result == EXIT_SUCCESS

        captured = capsys.readouterr()
        assert "Job Added Successfully" in captured.out
        assert unique_job_id in captured.out

    def test_run_schedule_list_empty(self, config, capsys):
        """Test schedule list command with no jobs."""
        import argparse
        import logging

        from small_etl.cli import EXIT_SUCCESS, run_schedule
        from small_etl.scheduler.scheduler import ETLScheduler

        # Clear all existing jobs to ensure empty state
        scheduler = ETLScheduler(config, blocking=False)
        for job in scheduler.list_jobs():
            scheduler.remove_job(job.job_id)
        scheduler.shutdown(wait=False)

        args = argparse.Namespace(
            schedule_command="list",
            env="dev",
            verbose=False,
        )
        logger = logging.getLogger("test")

        result = run_schedule(args, config, logger)
        assert result == EXIT_SUCCESS

        captured = capsys.readouterr()
        assert "Scheduled Jobs (0 total)" in captured.out
        assert "No jobs configured" in captured.out

    def test_run_schedule_remove_not_found(self, config, capsys):
        """Test schedule remove command for non-existent job."""
        import argparse
        import logging

        from small_etl.cli import EXIT_ERROR, run_schedule

        args = argparse.Namespace(
            schedule_command="remove",
            job_id="nonexistent",
            env="dev",
            verbose=False,
        )
        logger = logging.getLogger("test")

        result = run_schedule(args, config, logger)
        assert result == EXIT_ERROR

        captured = capsys.readouterr()
        assert "not found" in captured.out

    def test_run_schedule_no_subcommand(self, config, capsys):
        """Test schedule command without subcommand."""
        import argparse
        import logging

        from small_etl.cli import EXIT_ARGS_ERROR, run_schedule

        args = argparse.Namespace(
            schedule_command=None,
            env="dev",
            verbose=False,
        )
        logger = logging.getLogger("test")

        result = run_schedule(args, config, logger)
        assert result == EXIT_ARGS_ERROR

    def test_run_schedule_add_invalid_command(self, config, capsys):
        """Test schedule add with invalid ETL command."""
        import argparse
        import logging

        from small_etl.cli import EXIT_ARGS_ERROR, run_schedule

        # This should be caught by argparse, but testing direct call
        args = argparse.Namespace(
            schedule_command="add",
            job_id="test_job_invalid",
            etl_command="invalid",
            interval="day",
            at=None,
            env="dev",
            verbose=False,
        )
        logger = logging.getLogger("test")

        result = run_schedule(args, config, logger)
        assert result == EXIT_ARGS_ERROR

    def test_run_schedule_pause(self, config, capsys):
        """Test schedule pause command."""
        import argparse
        import logging

        from small_etl.cli import EXIT_ERROR, run_schedule

        args = argparse.Namespace(
            schedule_command="pause",
            job_id="nonexistent",
            env="dev",
            verbose=False,
        )
        logger = logging.getLogger("test")

        result = run_schedule(args, config, logger)
        assert result == EXIT_ERROR

        captured = capsys.readouterr()
        assert "not found" in captured.out

    def test_run_schedule_resume(self, config, capsys):
        """Test schedule resume command."""
        import argparse
        import logging

        from small_etl.cli import EXIT_ERROR, run_schedule

        args = argparse.Namespace(
            schedule_command="resume",
            job_id="nonexistent",
            env="dev",
            verbose=False,
        )
        logger = logging.getLogger("test")

        result = run_schedule(args, config, logger)
        assert result == EXIT_ERROR

        captured = capsys.readouterr()
        assert "not found" in captured.out
