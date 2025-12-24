"""Command-line interface for Small ETL pipeline.

Usage:
    pixi run python -m small_etl run [options]
    pixi run python -m small_etl assets [options]
    pixi run python -m small_etl trades [options]
    pixi run python -m small_etl clean [options]
    pixi run python -m small_etl schedule start|add|list|remove [options]
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
EXIT_CONNECTION_ERROR = 3
EXIT_VALIDATION_ERROR = 4


def get_config_dir() -> Path:
    """Get the configs directory path."""
    from pathlib import Path

    # Try to find configs directory relative to this file
    cli_path = Path(__file__).resolve()
    # src/small_etl/cli.py -> project_root/configs
    project_root = cli_path.parent.parent.parent
    config_dir = project_root / "configs"

    if config_dir.exists():
        return config_dir

    # Fallback: try current working directory
    cwd_config = Path.cwd() / "configs"
    if cwd_config.exists():
        return cwd_config

    raise FileNotFoundError("Cannot find configs directory")


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        args: Command-line arguments. If None, uses sys.argv.

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(
        prog="small_etl",
        description="Small ETL - S3 CSV to PostgreSQL data pipeline",
    )

    # Subcommands
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Common arguments for all commands
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument(
        "-e",
        "--env",
        choices=["dev", "test"],
        default="dev",
        help="Environment configuration (default: dev)",
    )
    common_parser.add_argument(
        "-b",
        "--batch-size",
        type=int,
        default=None,
        help="Batch size for loading (default: from config)",
    )
    common_parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    common_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate only, do not load to database",
    )

    # S3 override arguments
    common_parser.add_argument("--s3-endpoint", help="S3/MinIO endpoint")
    common_parser.add_argument("--s3-bucket", help="S3 bucket name")
    common_parser.add_argument("--assets-file", help="Assets CSV file name")
    common_parser.add_argument("--trades-file", help="Trades CSV file name")

    # Database override arguments
    common_parser.add_argument("--db-host", help="Database host")
    common_parser.add_argument("--db-port", type=int, help="Database port")
    common_parser.add_argument("--db-name", help="Database name")
    common_parser.add_argument("--db-user", help="Database user")
    common_parser.add_argument("--db-password", help="Database password")

    # run command - full ETL
    subparsers.add_parser(
        "run",
        parents=[common_parser],
        help="Run complete ETL pipeline (assets + trades)",
    )

    # assets command - assets only
    subparsers.add_parser(
        "assets",
        parents=[common_parser],
        help="Run ETL for assets only",
    )

    # trades command - trades only
    subparsers.add_parser(
        "trades",
        parents=[common_parser],
        help="Run ETL for trades only (requires assets loaded)",
    )

    # clean command - truncate tables
    clean_parser = argparse.ArgumentParser(add_help=False)
    clean_parser.add_argument(
        "-e",
        "--env",
        choices=["dev", "test"],
        default="dev",
        help="Environment configuration (default: dev)",
    )
    clean_parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    clean_parser.add_argument("--db-host", help="Database host")
    clean_parser.add_argument("--db-port", type=int, help="Database port")
    clean_parser.add_argument("--db-name", help="Database name")
    clean_parser.add_argument("--db-user", help="Database user")
    clean_parser.add_argument("--db-password", help="Database password")

    subparsers.add_parser(
        "clean",
        parents=[clean_parser],
        help="Truncate asset and trade tables",
    )

    # schedule command - scheduler management
    schedule_parser = subparsers.add_parser(
        "schedule",
        help="Manage scheduled ETL tasks",
    )
    schedule_subparsers = schedule_parser.add_subparsers(
        dest="schedule_command",
        help="Schedule operations",
    )

    # Schedule common arguments
    schedule_common = argparse.ArgumentParser(add_help=False)
    schedule_common.add_argument(
        "-e",
        "--env",
        choices=["dev", "test"],
        default="dev",
        help="Environment configuration (default: dev)",
    )
    schedule_common.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose output",
    )

    # schedule start - start scheduler
    schedule_subparsers.add_parser(
        "start",
        parents=[schedule_common],
        help="Start the scheduler (blocking)",
    )

    # schedule add - add a job
    schedule_add = schedule_subparsers.add_parser(
        "add",
        parents=[schedule_common],
        help="Add a scheduled job",
    )
    schedule_add.add_argument(
        "--job-id",
        required=True,
        help="Unique job identifier",
    )
    schedule_add.add_argument(
        "--etl-command",
        required=True,
        choices=["run", "assets", "trades"],
        dest="etl_command",
        help="ETL command to execute",
    )
    schedule_add.add_argument(
        "--interval",
        required=True,
        choices=["day", "hour", "minute"],
        help="Scheduling interval",
    )
    schedule_add.add_argument(
        "--at",
        help="Execution time (e.g., '02:00'), only for day interval",
    )

    # schedule list - list jobs
    schedule_subparsers.add_parser(
        "list",
        parents=[schedule_common],
        help="List all scheduled jobs",
    )

    # schedule remove - remove a job
    schedule_remove = schedule_subparsers.add_parser(
        "remove",
        parents=[schedule_common],
        help="Remove a scheduled job",
    )
    schedule_remove.add_argument(
        "--job-id",
        required=True,
        help="Job ID to remove",
    )

    # schedule pause - pause a job
    schedule_pause = schedule_subparsers.add_parser(
        "pause",
        parents=[schedule_common],
        help="Pause a scheduled job",
    )
    schedule_pause.add_argument(
        "--job-id",
        required=True,
        help="Job ID to pause",
    )

    # schedule resume - resume a job
    schedule_resume = schedule_subparsers.add_parser(
        "resume",
        parents=[schedule_common],
        help="Resume a paused job",
    )
    schedule_resume.add_argument(
        "--job-id",
        required=True,
        help="Job ID to resume",
    )

    parsed = parser.parse_args(args)

    # If no command specified, show help
    if parsed.command is None:
        parser.print_help()
        sys.exit(EXIT_ARGS_ERROR)

    return parsed


def build_config(args: argparse.Namespace) -> dict[str, Any]:
    """Build Hydra config with command-line overrides.

    Args:
        args: Parsed command-line arguments.

    Returns:
        Merged configuration dictionary.
    """
    config_dir = get_config_dir()

    # Initialize Hydra with the config directory
    with initialize_config_dir(config_dir=str(config_dir), version_base=None):
        # Select db config based on environment
        overrides = [f"db={args.env}"]

        config = compose(config_name="config", overrides=overrides)

        # Convert to mutable dict for overrides
        config_dict = cast(dict[str, Any], OmegaConf.to_container(config, resolve=True))

        # Apply command-line overrides (only for ETL commands)
        if getattr(args, "batch_size", None) is not None:
            config_dict["etl"]["batch_size"] = args.batch_size

        # S3 overrides
        if getattr(args, "s3_endpoint", None):
            config_dict["s3"]["endpoint"] = args.s3_endpoint
        if getattr(args, "s3_bucket", None):
            config_dict["s3"]["bucket"] = args.s3_bucket
        if getattr(args, "assets_file", None):
            config_dict["s3"]["assets_file"] = args.assets_file
        if getattr(args, "trades_file", None):
            config_dict["s3"]["trades_file"] = args.trades_file

        # Database overrides
        if getattr(args, "db_host", None):
            config_dict["db"]["host"] = args.db_host
        if getattr(args, "db_port", None):
            config_dict["db"]["port"] = args.db_port
        if getattr(args, "db_name", None):
            config_dict["db"]["database"] = args.db_name
        if getattr(args, "db_user", None):
            config_dict["db"]["user"] = args.db_user
        if getattr(args, "db_password", None):
            config_dict["db"]["password"] = args.db_password

        # Rebuild URL if any db params changed
        if any([getattr(args, "db_host", None), getattr(args, "db_port", None),
                getattr(args, "db_name", None), getattr(args, "db_user", None),
                getattr(args, "db_password", None)]):
            db = config_dict["db"]
            config_dict["db"]["url"] = f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}"

        return config_dict


def setup_logging(verbose: bool = False) -> None:
    """Configure logging based on verbosity level.

    Args:
        verbose: If True, set DEBUG level; otherwise INFO.
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def print_result(result: PipelineResult, verbose: bool = False) -> None:
    """Print pipeline execution result.

    Args:
        result: Pipeline execution result.
        verbose: If True, print detailed information.
    """
    print("\n" + "=" * 60)

    if result.success:
        print("ETL Pipeline: SUCCESS")
    else:
        print("ETL Pipeline: FAILED")
        if result.error_message:
            print(f"Error: {result.error_message}")

    print("=" * 60)

    # Duration
    if result.completed_at and result.started_at:
        duration = (result.completed_at - result.started_at).total_seconds()
        print(f"Duration: {duration:.2f}s")

    # Assets summary
    if result.assets_validation:
        av = result.assets_validation
        print("\nAssets Validation:")
        print(f"  Total: {av.total_rows}, Valid: {av.valid_count}, Invalid: {av.invalid_count}")

        if verbose and av.errors:
            print("  Errors (first 5):")
            for err in av.errors[:5]:
                print(f"    - Row {err.row_index}: {err.field} - {err.message}")

    if result.assets_load:
        al = result.assets_load
        print(f"Assets Load: {al.loaded_count} loaded")

    if result.assets_stats and verbose:
        stats = result.assets_stats
        print("Assets Stats:")
        print(f"  Total Records: {stats.total_records}")
        print(f"  Total Assets: {stats.total_assets:,.2f}")
        print(f"  Avg Total Asset: {stats.avg_total_asset:,.2f}")

    # Trades summary
    if result.trades_validation:
        tv = result.trades_validation
        print("\nTrades Validation:")
        print(f"  Total: {tv.total_rows}, Valid: {tv.valid_count}, Invalid: {tv.invalid_count}")

        if verbose and tv.errors:
            print("  Errors (first 5):")
            for err in tv.errors[:5]:
                print(f"    - Row {err.row_index}: {err.field} - {err.message}")

    if result.trades_load:
        tl = result.trades_load
        print(f"Trades Load: {tl.loaded_count} loaded")

    if result.trades_stats and verbose:
        stats = result.trades_stats
        print("Trades Stats:")
        print(f"  Total Records: {stats.total_records}")
        print(f"  Total Volume: {stats.total_volume:,}")
        print(f"  Total Amount: {stats.total_amount:,.2f}")

    print("=" * 60 + "\n")


def run_clean(config: dict[str, Any], logger: logging.Logger) -> int:
    """Truncate asset and trade tables.

    Args:
        config: Configuration dictionary.
        logger: Logger instance.

    Returns:
        Exit code.
    """
    from small_etl.data_access.postgres_repository import PostgresRepository

    try:
        repo = PostgresRepository(config["db"]["url"])

        # Get counts before truncate
        asset_count = repo.get_asset_count()
        trade_count = repo.get_trade_count()

        logger.info(f"Current records - Assets: {asset_count}, Trades: {trade_count}")

        # Truncate tables
        repo.truncate_tables()
        logger.info("Tables truncated successfully")

        repo.close()

        print("\n" + "=" * 60)
        print("Clean: SUCCESS")
        print("=" * 60)
        print(f"Deleted {asset_count} assets and {trade_count} trades")
        print("=" * 60 + "\n")

        return EXIT_SUCCESS

    except Exception as e:
        logger.exception(f"Clean failed: {e}")
        print(f"\nClean: FAILED - {e}\n")
        return EXIT_ERROR


def run_schedule(args: argparse.Namespace, config: dict[str, Any], logger: logging.Logger) -> int:
    """Execute scheduler commands.

    Args:
        args: Parsed command-line arguments.
        config: Configuration dictionary.
        logger: Logger instance.

    Returns:
        Exit code.
    """
    from small_etl.scheduler.scheduler import ETLScheduler

    schedule_cmd = getattr(args, "schedule_command", None)

    if not schedule_cmd:
        print("Error: Please specify a schedule operation (start, add, list, remove)")
        print("Usage: small_etl schedule <start|add|list|remove> [options]")
        return EXIT_ARGS_ERROR

    try:
        # Use blocking=True only for 'start' command, otherwise use background mode for persistence
        blocking = (schedule_cmd == "start")
        scheduler = ETLScheduler(config, blocking=blocking)

        if schedule_cmd == "start":
            print("\n" + "=" * 60)
            print(f"Starting ETL Scheduler with {scheduler.job_count} jobs...")
            print("Press Ctrl+C to stop")
            print("=" * 60 + "\n")

            try:
                scheduler.start()
            except KeyboardInterrupt:
                scheduler.stop()
                print("\nScheduler stopped by user")

            return EXIT_SUCCESS

        elif schedule_cmd == "add":
            job = scheduler.add_job(
                job_id=args.job_id,
                command=args.etl_command,
                interval=args.interval,
                at_time=getattr(args, "at", None),
            )

            print("\n" + "=" * 60)
            print("Job Added Successfully")
            print("=" * 60)
            print(f"  Job ID: {job.job_id}")
            print(f"  Command: {job.command}")
            print(f"  Interval: {job.interval}")
            if job.at_time:
                print(f"  At Time: {job.at_time}")
            print("=" * 60 + "\n")

            return EXIT_SUCCESS

        elif schedule_cmd == "list":
            jobs = scheduler.list_jobs()

            print("\n" + "=" * 60)
            print(f"Scheduled Jobs ({len(jobs)} total)")
            print("=" * 60)

            if not jobs:
                print("  No jobs configured")
            else:
                for job in jobs:
                    status = "enabled" if job.enabled else "disabled"
                    at_str = f" at {job.at_time}" if job.at_time else ""
                    last_run = job.last_run.strftime("%Y-%m-%d %H:%M:%S") if job.last_run else "never"
                    print(f"  [{job.job_id}] {job.command} every {job.interval}{at_str} ({status}) - last run: {last_run}")

            print("=" * 60 + "\n")

            return EXIT_SUCCESS

        elif schedule_cmd == "remove":
            removed = scheduler.remove_job(args.job_id)

            print("\n" + "=" * 60)
            if removed:
                print(f"Job '{args.job_id}' removed successfully")
            else:
                print(f"Job '{args.job_id}' not found")
            print("=" * 60 + "\n")

            return EXIT_SUCCESS if removed else EXIT_ERROR

        elif schedule_cmd == "pause":
            paused = scheduler.pause_job(args.job_id)

            print("\n" + "=" * 60)
            if paused:
                print(f"Job '{args.job_id}' paused successfully")
            else:
                print(f"Job '{args.job_id}' not found")
            print("=" * 60 + "\n")

            return EXIT_SUCCESS if paused else EXIT_ERROR

        elif schedule_cmd == "resume":
            resumed = scheduler.resume_job(args.job_id)

            print("\n" + "=" * 60)
            if resumed:
                print(f"Job '{args.job_id}' resumed successfully")
            else:
                print(f"Job '{args.job_id}' not found")
            print("=" * 60 + "\n")

            return EXIT_SUCCESS if resumed else EXIT_ERROR

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


def run_pipeline(args: argparse.Namespace) -> int:
    """Execute ETL pipeline based on command-line arguments.

    Args:
        args: Parsed command-line arguments.

    Returns:
        Exit code (0 for success, non-zero for errors).
    """
    setup_logging(getattr(args, "verbose", False))
    logger = logging.getLogger(__name__)

    try:
        # Build configuration
        config_dict = build_config(args)
        config = OmegaConf.create(config_dict)

        logger.info(f"Running command: {args.command}")
        logger.info(f"Environment: {args.env}")

        # Handle clean command separately
        if args.command == "clean":
            return run_clean(config_dict, logger)

        # Handle schedule command
        if args.command == "schedule":
            return run_schedule(args, config_dict, logger)

        if getattr(args, "dry_run", False):
            logger.info("DRY RUN mode - will validate only, not load to database")

        # Create and run pipeline
        with ETLPipeline(config) as pipeline:
            if getattr(args, "dry_run", False):
                # For dry-run, we only extract and validate
                result = _run_dry(pipeline, args.command, config_dict)
            elif args.command == "run":
                result = pipeline.run()
            elif args.command == "assets":
                result = pipeline.run_assets_only()
            elif args.command == "trades":
                result = pipeline.run_trades_only()
            else:
                logger.error(f"Unknown command: {args.command}")
                return EXIT_ARGS_ERROR

        # Print result
        print_result(result, args.verbose)

        # Determine exit code
        if not result.success:
            return EXIT_ERROR

        # Check for validation errors (partial success)
        has_validation_errors = False
        if result.assets_validation and result.assets_validation.invalid_count > 0:
            has_validation_errors = True
        if result.trades_validation and result.trades_validation.invalid_count > 0:
            has_validation_errors = True

        if has_validation_errors:
            return EXIT_VALIDATION_ERROR

        return EXIT_SUCCESS

    except FileNotFoundError as e:
        logger.error(f"Configuration error: {e}")
        return EXIT_ARGS_ERROR
    except ConnectionError as e:
        logger.error(f"Connection error: {e}")
        return EXIT_CONNECTION_ERROR
    except Exception as e:
        logger.exception(f"Pipeline error: {e}")
        return EXIT_ERROR


def _run_dry(pipeline: ETLPipeline, command: str, config: dict[str, Any]) -> PipelineResult:
    """Run pipeline in dry-run mode (validate only).

    Args:
        pipeline: ETL pipeline instance.
        command: Command to run (run/assets/trades).
        config: Configuration dictionary.

    Returns:
        PipelineResult with validation results only.
    """
    from datetime import UTC, datetime

    from small_etl.application.pipeline import DataTypeResult

    started_at = datetime.now(UTC)

    try:
        results: dict[str, DataTypeResult] = {}

        # Extract and validate based on command
        if command in ("run", "assets"):
            assets_df = pipeline._extractor.extract_assets(
                bucket=config["s3"]["bucket"],
                object_name=config["s3"]["assets_file"],
            )
            assets_validation = pipeline._validator.validate_assets(assets_df)
            results["asset"] = DataTypeResult(
                data_type="asset",
                validation=assets_validation,
            )

        if command in ("run", "trades"):
            trades_df = pipeline._extractor.extract_trades(
                bucket=config["s3"]["bucket"],
                object_name=config["s3"]["trades_file"],
            )
            # For dry-run trades, we don't check FK since assets aren't loaded
            trades_validation = pipeline._validator.validate_trades(trades_df, valid_account_ids=None)
            results["trade"] = DataTypeResult(
                data_type="trade",
                validation=trades_validation,
            )

        return PipelineResult(
            success=True,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            results=results,
        )

    except Exception as e:
        return PipelineResult(
            success=False,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            error_message=str(e),
        )


def main() -> int:
    """CLI entry point.

    Returns:
        Exit code.
    """
    args = parse_args()
    return run_pipeline(args)


if __name__ == "__main__":
    sys.exit(main())
