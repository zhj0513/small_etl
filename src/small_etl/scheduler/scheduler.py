"""ETL Scheduler using APScheduler with PostgreSQL job store.

This module provides a scheduler based on APScheduler with persistent
job storage in PostgreSQL, supporting cron/interval triggers.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

logger = logging.getLogger(__name__)


@dataclass
class ScheduledJob:
    """Definition of a scheduled ETL job.

    Attributes:
        job_id: Unique identifier for the job.
        command: ETL command to execute (run/assets/trades).
        interval: Scheduling interval (day/hour/minute).
        at_time: Execution time point (e.g., "02:00"), only for day interval.
        enabled: Whether the job is enabled (running state).
        next_run: Next scheduled run time.
        last_run: Timestamp of the last execution.
    """

    job_id: str
    command: str
    interval: str
    at_time: str | None = None
    enabled: bool = True
    next_run: datetime | None = None
    last_run: datetime | None = None


def execute_etl_job(config: dict[str, Any], command: str) -> None:
    """Execute an ETL job.

    This function is called by APScheduler when a job is triggered.

    Args:
        config: Configuration dictionary.
        command: ETL command to execute (run/assets/trades).
    """
    from omegaconf import OmegaConf

    from small_etl.application.pipeline import ETLPipeline

    logger.info(f"[Scheduler] Executing ETL job: command={command}")

    try:
        # Convert dict to DictConfig for ETLPipeline
        cfg = OmegaConf.create(config)

        with ETLPipeline(cfg) as pipeline:
            if command == "run":
                result = pipeline.run()
            elif command == "assets":
                result = pipeline.run_assets_only()
            elif command == "trades":
                result = pipeline.run_trades_only()
            else:
                raise ValueError(f"Unknown command: {command}")

            if result.success:
                logger.info(f"[Scheduler] ETL job '{command}' completed successfully")
            else:
                logger.error(f"[Scheduler] ETL job '{command}' failed: {result.error_message}")

    except Exception as e:
        logger.exception(f"[Scheduler] ETL job '{command}' error: {e}")


class ETLScheduler:
    """ETL task scheduler using APScheduler with PostgreSQL persistence.

    Jobs are stored in PostgreSQL and survive process restarts.

    Args:
        config: Hydra configuration dictionary with db and scheduler settings.
        blocking: If True, use BlockingScheduler; otherwise BackgroundScheduler.
    """

    VALID_COMMANDS = {"run", "assets", "trades"}
    VALID_INTERVALS = {"day", "hour", "minute"}

    def __init__(self, config: dict[str, Any], blocking: bool = True) -> None:
        self._config = config
        self._blocking = blocking

        # Get database URL for job store
        db_url = config.get("db", {}).get("url", "")
        if not db_url:
            raise ValueError("Database URL is required for scheduler job store")

        # Configure job stores
        jobstores = {
            "default": SQLAlchemyJobStore(url=db_url),
        }

        # Configure executors
        executors = {
            "default": ThreadPoolExecutor(max_workers=3),
        }

        # Configure job defaults
        job_defaults = {
            "coalesce": True,  # Combine missed runs into one
            "max_instances": 1,  # Only one instance of each job at a time
            "misfire_grace_time": 60 * 60,  # 1 hour grace time
        }

        # Create scheduler
        scheduler_config = config.get("scheduler", {})
        self._check_interval = scheduler_config.get("check_interval", 1)

        if blocking:
            self._scheduler = BlockingScheduler(
                jobstores=jobstores,
                executors=executors,
                job_defaults=job_defaults,
            )
        else:
            self._scheduler = BackgroundScheduler(
                jobstores=jobstores,
                executors=executors,
                job_defaults=job_defaults,
            )
            # Start background scheduler immediately to enable job persistence
            self._scheduler.start(paused=True)

        self._running = False

    def add_job(
        self,
        job_id: str,
        command: str,
        interval: str,
        at_time: str | None = None,
    ) -> ScheduledJob:
        """Add a new scheduled job.

        Args:
            job_id: Unique identifier for the job.
            command: ETL command (run/assets/trades).
            interval: Scheduling interval (day/hour/minute).
            at_time: Execution time point (e.g., "02:00"), only for day interval.

        Returns:
            The created ScheduledJob instance.

        Raises:
            ValueError: If job_id already exists or invalid parameters.
        """
        # Check if job already exists
        existing = self._scheduler.get_job(job_id)
        if existing:
            raise ValueError(f"Job with id '{job_id}' already exists")

        if command not in self.VALID_COMMANDS:
            raise ValueError(f"Invalid command '{command}'. Must be one of: {self.VALID_COMMANDS}")

        if interval not in self.VALID_INTERVALS:
            raise ValueError(f"Invalid interval '{interval}'. Must be one of: {self.VALID_INTERVALS}")

        # Create trigger based on interval
        if interval == "day":
            if at_time:
                hour, minute = map(int, at_time.split(":"))
                trigger = CronTrigger(hour=hour, minute=minute)
            else:
                trigger = CronTrigger(hour=0, minute=0)  # Default to midnight
        elif interval == "hour":
            trigger = IntervalTrigger(hours=1)
            at_time = None  # Clear at_time for non-day intervals
        elif interval == "minute":
            trigger = IntervalTrigger(minutes=1)
            at_time = None
        else:
            raise ValueError(f"Invalid interval: {interval}")

        # Add job to scheduler
        # Store metadata in job args for retrieval
        job = self._scheduler.add_job(
            func=execute_etl_job,
            trigger=trigger,
            args=[self._config, command],
            id=job_id,
            name=f"ETL {command} ({interval})",
            replace_existing=False,
        )

        logger.info(f"Added job: {job_id} (command={command}, interval={interval}, at_time={at_time})")

        return ScheduledJob(
            job_id=job_id,
            command=command,
            interval=interval,
            at_time=at_time,
            enabled=True,
            next_run=getattr(job, "next_run_time", None),
        )

    def remove_job(self, job_id: str) -> bool:
        """Remove a scheduled job.

        Args:
            job_id: The ID of the job to remove.

        Returns:
            True if the job was removed, False if not found.
        """
        try:
            self._scheduler.remove_job(job_id)
            logger.info(f"Removed job: {job_id}")
            return True
        except Exception:
            return False

    def pause_job(self, job_id: str) -> bool:
        """Pause a scheduled job.

        Args:
            job_id: The ID of the job to pause.

        Returns:
            True if successful, False otherwise.
        """
        try:
            self._scheduler.pause_job(job_id)
            logger.info(f"Paused job: {job_id}")
            return True
        except Exception:
            return False

    def resume_job(self, job_id: str) -> bool:
        """Resume a paused job.

        Args:
            job_id: The ID of the job to resume.

        Returns:
            True if successful, False otherwise.
        """
        try:
            self._scheduler.resume_job(job_id)
            logger.info(f"Resumed job: {job_id}")
            return True
        except Exception:
            return False

    def list_jobs(self) -> list[ScheduledJob]:
        """List all scheduled jobs.

        Returns:
            List of all ScheduledJob instances.
        """
        jobs = []
        for job in self._scheduler.get_jobs():
            # Extract command from job args
            command = job.args[1] if len(job.args) > 1 else "unknown"

            # Parse interval from job name or trigger
            interval = "unknown"
            at_time = None
            if isinstance(job.trigger, CronTrigger):
                interval = "day"
                # Extract time from cron trigger
                for field in job.trigger.fields:
                    if field.name == "hour" and str(field) != "*":
                        hour = str(field)
                        for f2 in job.trigger.fields:
                            if f2.name == "minute":
                                minute = str(f2)
                                at_time = f"{hour.zfill(2)}:{minute.zfill(2)}"
                                break
            elif isinstance(job.trigger, IntervalTrigger):
                if job.trigger.interval.total_seconds() == 3600:
                    interval = "hour"
                elif job.trigger.interval.total_seconds() == 60:
                    interval = "minute"

            jobs.append(
                ScheduledJob(
                    job_id=job.id,
                    command=command,
                    interval=interval,
                    at_time=at_time,
                    enabled=getattr(job, "next_run_time", None) is not None,
                    next_run=getattr(job, "next_run_time", None),
                )
            )

        return jobs

    def get_job(self, job_id: str) -> ScheduledJob | None:
        """Get a specific job by ID.

        Args:
            job_id: The ID of the job to retrieve.

        Returns:
            The ScheduledJob if found, None otherwise.
        """
        job = self._scheduler.get_job(job_id)
        if not job:
            return None

        command = job.args[1] if len(job.args) > 1 else "unknown"
        interval = "day" if isinstance(job.trigger, CronTrigger) else "hour"

        return ScheduledJob(
            job_id=job.id,
            command=command,
            interval=interval,
            enabled=getattr(job, "next_run_time", None) is not None,
            next_run=getattr(job, "next_run_time", None),
        )

    def start(self) -> None:
        """Start the scheduler.

        If blocking=True (default), this method blocks until shutdown.
        If blocking=False, scheduler runs in background thread.
        """
        self._running = True
        logger.info(f"Starting scheduler (blocking={self._blocking})")

        try:
            self._scheduler.start()
        except KeyboardInterrupt:
            self.stop()

    def stop(self) -> None:
        """Stop the scheduler."""
        logger.info("Stopping scheduler...")
        self._running = False
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the scheduler.

        Args:
            wait: If True, wait for running jobs to complete.
        """
        self._running = False
        if self._scheduler.running:
            self._scheduler.shutdown(wait=wait)

    @property
    def is_running(self) -> bool:
        """Check if the scheduler is running."""
        return self._scheduler.running

    @property
    def job_count(self) -> int:
        """Get the number of registered jobs."""
        return len(self._scheduler.get_jobs())
