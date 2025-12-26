"""Command-line interface for Small ETL pipeline using Hydra.

Usage:
    pixi run python -m small_etl [hydra_overrides...]

Commands (via command= override):
    command=run       Run ETL pipeline (default)
    command=clean     Truncate tables
    command=schedule  Manage scheduled tasks

Examples:
    pixi run python -m small_etl
    pixi run python -m small_etl command=clean db=test
    pixi run python -m small_etl command=schedule job.action=list
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import hydra
from omegaconf import DictConfig, OmegaConf

from small_etl.application.pipeline import ETLPipeline

EXIT_SUCCESS, EXIT_ERROR = 0, 1
logger = logging.getLogger(__name__)

# Calculate absolute config path at module load time
_CONFIG_PATH = str(Path(__file__).resolve().parent.parent.parent / "configs")


def print_result(result: Any) -> None:
    """Print pipeline result."""
    status = "SUCCESS" if result.success else "FAILED"
    print(f"\n{'=' * 50}\nETL Pipeline: {status}")
    if not result.success and result.error_message:
        print(f"Error: {result.error_message}")
    if result.completed_at and result.started_at:
        print(f"Duration: {(result.completed_at - result.started_at).total_seconds():.2f}s")
    if result.assets_loaded > 0:
        print(f"Assets: {result.assets_loaded}")
    if result.trades_loaded > 0:
        print(f"Trades: {result.trades_loaded}")
    print("=" * 50 + "\n")


def run_clean(cfg: DictConfig) -> int:
    """Truncate tables."""
    from small_etl.data_access.postgres_repository import PostgresRepository
    try:
        repo = PostgresRepository(cfg.db.url)
        a, t = repo.get_asset_count(), repo.get_trade_count()
        repo.truncate_tables()
        repo.close()
        print(f"\nClean: SUCCESS - Deleted {a} assets, {t} trades\n")
        return EXIT_SUCCESS
    except Exception as e:
        logger.exception(f"Clean failed: {e}")
        print(f"\nClean: FAILED - {e}\n")
        return EXIT_ERROR


def run_schedule(cfg: DictConfig) -> int:
    """Execute scheduler commands."""
    from small_etl.scheduler.scheduler import ETLScheduler

    action, job = cfg.job.action, cfg.job
    config_dict: dict[str, Any] = OmegaConf.to_container(cfg, resolve=True)  # type: ignore[assignment]

    try:
        scheduler = ETLScheduler(config_dict, blocking=(action == "start"))

        if action == "start":
            print(f"\nStarting Scheduler ({scheduler.job_count} jobs)... Ctrl+C to stop\n")
            try:
                scheduler.start()
            except KeyboardInterrupt:
                scheduler.stop()
                print("\nStopped")
            return EXIT_SUCCESS

        if action == "add":
            if not job.id:
                return print("Error: job.id required") or EXIT_ERROR  # type: ignore[return-value]
            j = scheduler.add_job(job.id, job.command, job.interval, job.at or None)
            print(f"\nJob added: {j.job_id} ({j.command} every {j.interval})\n")
            return EXIT_SUCCESS

        if action == "list":
            jobs = scheduler.list_jobs()
            print(f"\nScheduled Jobs ({len(jobs)}):")
            for j in jobs:
                at = f" at {j.at_time}" if j.at_time else ""
                last = j.last_run.strftime("%Y-%m-%d %H:%M") if j.last_run else "never"
                print(f"  [{j.job_id}] {j.command} every {j.interval}{at} - last: {last}")
            print()
            return EXIT_SUCCESS

        if action in ("remove", "pause", "resume"):
            if not job.id:
                return print(f"Error: job.id required for {action}") or EXIT_ERROR  # type: ignore[return-value]
            fn = getattr(scheduler, f"{action}_job")
            if fn(job.id):
                print(f"\nJob '{job.id}' {action}d\n")
                return EXIT_SUCCESS
            print(f"\nJob '{job.id}' not found\n")
            return EXIT_ERROR

        print(f"Error: Unknown action: {action}")
        return EXIT_ERROR
    except Exception as e:
        logger.exception(f"Schedule error: {e}")
        print(f"\nSchedule: FAILED - {e}\n")
        return EXIT_ERROR


def run_etl(cfg: DictConfig) -> int:
    """Run ETL pipeline."""
    try:
        with ETLPipeline(cfg) as pipeline:
            result = pipeline.run()
        print_result(result)
        return EXIT_SUCCESS if result.success else EXIT_ERROR
    except Exception as e:
        logger.exception(f"Pipeline error: {e}")
        return EXIT_ERROR


@hydra.main(config_path=_CONFIG_PATH, config_name="config", version_base=None)
def main(cfg: DictConfig) -> None:
    """CLI entry point."""
    command = cfg.get("command", "run")
    logger.info(f"Running: {command}")

    if command == "clean":
        run_clean(cfg)
    elif command == "schedule":
        run_schedule(cfg)
    else:
        run_etl(cfg)


if __name__ == "__main__":
    main()
