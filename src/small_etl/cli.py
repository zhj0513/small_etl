"""Command-line interface for Small ETL pipeline.

Usage:
    pixi run python -m small_etl <command> [hydra_overrides...]

Commands:
    run       Run complete ETL pipeline (assets + trades)
    clean     Truncate asset and trade tables
    schedule  Manage scheduled ETL tasks

Examples:
    # Run ETL
    pixi run python -m small_etl run
    pixi run python -m small_etl run db=test
    pixi run python -m small_etl run db.host=192.168.1.100 etl.batch_size=5000

    # Clean tables
    pixi run python -m small_etl clean
    pixi run python -m small_etl clean db=test

    # Schedule commands
    pixi run python -m small_etl schedule start
    pixi run python -m small_etl schedule add --job-id daily_etl --etl-command run --interval day --at 02:00
    pixi run python -m small_etl schedule list
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import TYPE_CHECKING, Any, cast

from hydra import compose, initialize_config_dir
from omegaconf import OmegaConf

from small_etl.application.pipeline import ETLPipeline, PipelineResult

if TYPE_CHECKING:
    from pathlib import Path

# Exit codes
EXIT_SUCCESS = 0
EXIT_ERROR = 1
EXIT_ARGS_ERROR = 2


def get_config_dir() -> Path:
    """Get the configs directory path."""
    from pathlib import Path

    cli_path = Path(__file__).resolve()
    project_root = cli_path.parent.parent.parent
    config_dir = project_root / "configs"

    if config_dir.exists():
        return config_dir

    cwd_config = Path.cwd() / "configs"
    if cwd_config.exists():
        return cwd_config

    raise FileNotFoundError("Cannot find configs directory")


def parse_args(args: list[str] | None = None) -> tuple[argparse.Namespace, list[str]]:
    """Parse command-line arguments and separate Hydra overrides."""
    parser = argparse.ArgumentParser(
        prog="small_etl",
        description="Small ETL - S3 CSV to PostgreSQL data pipeline",
        epilog="""
Hydra config overrides (key=value):
    db=test                   Use test database config
    db.host=192.168.1.100     Override database host
    etl.batch_size=5000       Override batch size
    s3.bucket=my-bucket       Override S3 bucket
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # run command
    subparsers.add_parser("run", help="Run complete ETL pipeline (assets + trades)")

    # clean command
    subparsers.add_parser("clean", help="Truncate asset and trade tables")

    # schedule command
    schedule_parser = subparsers.add_parser("schedule", help="Manage scheduled ETL tasks")
    schedule_subparsers = schedule_parser.add_subparsers(dest="schedule_command", help="Schedule operations")

    schedule_subparsers.add_parser("start", help="Start the scheduler (blocking)")

    schedule_add = schedule_subparsers.add_parser("add", help="Add a scheduled job")
    schedule_add.add_argument("--job-id", required=True, help="Unique job identifier")
    schedule_add.add_argument("--etl-command", required=True, choices=["run"], dest="etl_command", help="ETL command")
    schedule_add.add_argument("--interval", required=True, choices=["day", "hour", "minute"], help="Scheduling interval")
    schedule_add.add_argument("--at", help="Execution time (e.g., '02:00'), only for day interval")

    schedule_subparsers.add_parser("list", help="List all scheduled jobs")

    schedule_remove = schedule_subparsers.add_parser("remove", help="Remove a scheduled job")
    schedule_remove.add_argument("--job-id", required=True, help="Job ID to remove")

    schedule_pause = schedule_subparsers.add_parser("pause", help="Pause a scheduled job")
    schedule_pause.add_argument("--job-id", required=True, help="Job ID to pause")

    schedule_resume = schedule_subparsers.add_parser("resume", help="Resume a paused job")
    schedule_resume.add_argument("--job-id", required=True, help="Job ID to resume")

    parsed, remaining = parser.parse_known_args(args)

    if parsed.command is None:
        parser.print_help()
        sys.exit(EXIT_ARGS_ERROR)

    # Filter Hydra overrides
    hydra_overrides = [arg for arg in remaining if "=" in arg or arg.startswith("+") or arg.startswith("~")]

    unrecognized = [arg for arg in remaining if arg not in hydra_overrides]
    if unrecognized:
        print(f"Warning: Unrecognized arguments: {unrecognized}", file=sys.stderr)

    return parsed, hydra_overrides


def build_config(hydra_overrides: list[str]) -> dict[str, Any]:
    """Build Hydra config with command-line overrides."""
    config_dir = get_config_dir()

    with initialize_config_dir(config_dir=str(config_dir), version_base=None):
        config = compose(config_name="config", overrides=hydra_overrides)
        return cast(dict[str, Any], OmegaConf.to_container(config, resolve=True))


def setup_logging() -> None:
    """Configure logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def print_result(result: PipelineResult) -> None:
    """Print pipeline execution result."""
    print("\n" + "=" * 60)

    if result.success:
        print("ETL Pipeline: SUCCESS")
    else:
        print("ETL Pipeline: FAILED")
        if result.error_message:
            print(f"Error: {result.error_message}")

    print("=" * 60)

    if result.completed_at and result.started_at:
        duration = (result.completed_at - result.started_at).total_seconds()
        print(f"Duration: {duration:.2f}s")

    if result.assets_loaded > 0:
        print(f"Assets loaded: {result.assets_loaded}")
    if result.trades_loaded > 0:
        print(f"Trades loaded: {result.trades_loaded}")

    print("=" * 60 + "\n")


def run_clean(config: dict[str, Any], logger: logging.Logger) -> int:
    """Truncate asset and trade tables."""
    from small_etl.data_access.postgres_repository import PostgresRepository

    try:
        repo = PostgresRepository(config["db"]["url"])
        asset_count = repo.get_asset_count()
        trade_count = repo.get_trade_count()

        logger.info(f"Current records - Assets: {asset_count}, Trades: {trade_count}")
        repo.truncate_tables()
        logger.info("Tables truncated successfully")
        repo.close()

        print("\n" + "=" * 60)
        print("Clean: SUCCESS")
        print(f"Deleted {asset_count} assets and {trade_count} trades")
        print("=" * 60 + "\n")

        return EXIT_SUCCESS

    except Exception as e:
        logger.exception(f"Clean failed: {e}")
        print(f"\nClean: FAILED - {e}\n")
        return EXIT_ERROR


def run_schedule(args: argparse.Namespace, config: dict[str, Any], logger: logging.Logger) -> int:
    """Execute scheduler commands."""
    from small_etl.scheduler.scheduler import ETLScheduler

    schedule_cmd = getattr(args, "schedule_command", None)

    if not schedule_cmd:
        print("Error: Please specify a schedule operation (start, add, list, remove, pause, resume)")
        return EXIT_ARGS_ERROR

    try:
        blocking = (schedule_cmd == "start")
        scheduler = ETLScheduler(config, blocking=blocking)

        if schedule_cmd == "start":
            print(f"\nStarting ETL Scheduler with {scheduler.job_count} jobs... (Ctrl+C to stop)\n")
            try:
                scheduler.start()
            except KeyboardInterrupt:
                scheduler.stop()
                print("\nScheduler stopped")
            return EXIT_SUCCESS

        elif schedule_cmd == "add":
            job = scheduler.add_job(
                job_id=args.job_id,
                command=args.etl_command,
                interval=args.interval,
                at_time=getattr(args, "at", None),
            )
            print(f"\nJob added: {job.job_id} ({job.command} every {job.interval})\n")
            return EXIT_SUCCESS

        elif schedule_cmd == "list":
            jobs = scheduler.list_jobs()
            print(f"\nScheduled Jobs ({len(jobs)}):")
            for job in jobs:
                status = "enabled" if job.enabled else "disabled"
                at_str = f" at {job.at_time}" if job.at_time else ""
                last_run = job.last_run.strftime("%Y-%m-%d %H:%M:%S") if job.last_run else "never"
                print(f"  [{job.job_id}] {job.command} every {job.interval}{at_str} ({status}) - last: {last_run}")
            print()
            return EXIT_SUCCESS

        elif schedule_cmd == "remove":
            if scheduler.remove_job(args.job_id):
                print(f"\nJob '{args.job_id}' removed\n")
                return EXIT_SUCCESS
            print(f"\nJob '{args.job_id}' not found\n")
            return EXIT_ERROR

        elif schedule_cmd == "pause":
            if scheduler.pause_job(args.job_id):
                print(f"\nJob '{args.job_id}' paused\n")
                return EXIT_SUCCESS
            print(f"\nJob '{args.job_id}' not found\n")
            return EXIT_ERROR

        elif schedule_cmd == "resume":
            if scheduler.resume_job(args.job_id):
                print(f"\nJob '{args.job_id}' resumed\n")
                return EXIT_SUCCESS
            print(f"\nJob '{args.job_id}' not found\n")
            return EXIT_ERROR

        else:
            print(f"Error: Unknown schedule command: {schedule_cmd}")
            return EXIT_ARGS_ERROR

    except ValueError as e:
        logger.error(f"Schedule error: {e}")
        print(f"\nSchedule: FAILED - {e}\n")
        return EXIT_ARGS_ERROR
    except Exception as e:
        logger.exception(f"Schedule error: {e}")
        print(f"\nSchedule: FAILED - {e}\n")
        return EXIT_ERROR


def run_pipeline(args: argparse.Namespace, hydra_overrides: list[str]) -> int:
    """Execute ETL pipeline based on command-line arguments."""
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        config_dict = build_config(hydra_overrides)
        config = OmegaConf.create(config_dict)

        logger.info(f"Running command: {args.command}")
        if hydra_overrides:
            logger.info(f"Config overrides: {hydra_overrides}")

        if args.command == "clean":
            return run_clean(config_dict, logger)

        if args.command == "schedule":
            return run_schedule(args, config_dict, logger)

        # Run ETL pipeline
        with ETLPipeline(config) as pipeline:
            result = pipeline.run()

        print_result(result)

        if not result.success:
            return EXIT_ERROR

        return EXIT_SUCCESS

    except FileNotFoundError as e:
        logger.error(f"Configuration error: {e}")
        return EXIT_ARGS_ERROR
    except Exception as e:
        logger.exception(f"Pipeline error: {e}")
        return EXIT_ERROR


def main() -> int:
    """CLI entry point."""
    args, hydra_overrides = parse_args()
    return run_pipeline(args, hydra_overrides)


if __name__ == "__main__":
    sys.exit(main())
