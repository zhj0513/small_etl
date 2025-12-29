# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Development Commands

```bash
# Install dependencies
pixi install

# Run all tests
pixi run pytest tests/ -v --cov=src/small_etl

# Run all unit tests
pixi run pytest tests/unit/ -v --no-cov

# Run integration tests (requires PostgreSQL running)
pixi run pytest tests/integration/ -v --no-cov

# Run a single test file
pixi run pytest tests/unit/test_validator.py -v --no-cov

# Run a single test
pixi run pytest tests/unit/test_validator.py::TestValidatorService::test_validate_valid_assets -v --no-cov

# Linting
pixi run ruff check src/
pixi run ruff check src/ --fix  # Auto-fix issues

# Type checking
pixi run pyright src/
pixi run pyrefly check src/

# Database migrations (dev environment)
pixi run alembic upgrade head
pixi run alembic revision --autogenerate -m "description"

# Database migrations (test environment)
ETL_ENV=test pixi run alembic upgrade head
```

## Test Database Setup

Test database setup is automated via the test fixtures in `conftest.py`, which will automatically create the database if it doesn't exist.

```bash
# Run Alembic migrations for test database
ETL_ENV=test pixi run alembic upgrade head
```

**Prerequisites**: PostgreSQL must be running (via podman: `podman start postgres`)

## Architecture Overview

This is a batch ETL system for syncing S3 CSV files to PostgreSQL with DuckDB-based validation.

### Layer Structure

```
src/small_etl/
├── domain/          # Data models (SQLModel), enums, Pandera schemas, DataTypeRegistry
├── data_access/     # DuckDBClient, PostgresRepository
├── services/        # ValidatorService, ExtractorService, LoaderService, AnalyticsService
├── config/          # Configuration utilities
└── application/     # ETLPipeline orchestration
```

### Data Flow

```mermaid
flowchart LR
    S3[S3 CSV] --> Polars[Polars]
    Polars --> Validator[ValidatorService]
    Validator --> DuckDB1[DuckDB Register]
    DuckDB1 --> Transform[SQL Transform]
    Transform --> Table[DuckDB Table]
    Table --> PostgreSQL
    PostgreSQL --> Analytics[AnalyticsService]
```

**Pipeline Phases:**
1. **Validate**: Polars reads S3 CSV → Pandera validates
2. **Transform**: DuckDB registers DataFrame → SQL transforms → DuckDB table (`_transformed_{type}`)
3. **Load**: DuckDB UPSERT from table → PostgreSQL
4. **Analytics**: DuckDB queries PostgreSQL statistics

### Key Components

- **ETLPipeline** (`application/pipeline.py`): Main orchestrator, use `with ETLPipeline(config) as pipeline`. Configuration-driven via `pipeline/default.yaml`
- **ValidatorService** (`services/validator.py`): Uses Polars to read CSV from S3, validates with Pandera schemas
- **ExtractorService** (`services/extractor.py`): Registers DataFrame to DuckDB, uses SQL for type conversions, returns table name
- **LoaderService** (`services/loader.py`): Loads data from DuckDB table to PostgreSQL using `upsert_from_table`
- **AnalyticsService** (`services/analytics.py`): Queries statistics from PostgreSQL via DuckDB
- **PostgresRepository** (`data_access/postgres_repository.py`): Database operations (queries, truncate)
- **DuckDBClient** (`data_access/duckdb_client.py`): In-memory operations, DataFrame registration, SQL transforms, PostgreSQL attachment, upsert operations
- **DataTypeRegistry** (`domain/registry.py`): Centralized registry for data type configurations

### Data Models

Two tables: `asset` and `trade` (Trade.account_id → Asset.account_id FK)

**Asset fields**: `account_id`, `account_type`, `cash`, `frozen_cash`, `market_value`, `total_asset`, `updated_at`

**Trade fields**: `account_id`, `account_type`, `traded_id`, `stock_code`, `traded_time`, `traded_price`, `traded_volume`, `traded_amount`, `strategy_name`, `order_remark`, `direction`, `offset_flag`, `created_at`, `updated_at`

Validation rules:
- `total_asset = cash + frozen_cash + market_value`
- `traded_amount = traded_price * traded_volume`
- Enum fields: `account_type` (1,2,3,5,6,7,11), `offset_flag` (48-54), `direction` (0,48,49)

### Configuration

Hydra configs in `configs/`:
- `config.yaml`: Main config entry point
- `db/dev.yaml`, `db/test.yaml`: Database connection (supports env vars: `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`)
- `s3/dev.yaml`: MinIO/S3 connection (used by Polars storage_options)
- `etl/default.yaml`: batch_size
- `extractor/default.yaml`: CSV column mappings and data type conversions
- `pipeline/default.yaml`: Pipeline steps configuration (data types to process in order)

Default test database: `postgresql://etl:etlpass@localhost:15432/etl_test_db`

### CLI Commands

CLI uses pure Hydra configuration (no argparse). All parameters via Hydra overrides.

**Available commands (via `command=` override):**
- `command=run` - Run complete ETL pipeline (default)
- `command=clean` - Truncate asset and trade tables
- `command=schedule` - Manage scheduled tasks

**Examples:**
```bash
# Run ETL (default command)
pixi run python -m small_etl
pixi run python -m small_etl db=test
pixi run python -m small_etl db.host=192.168.1.100 etl.batch_size=5000

# Clean tables
pixi run python -m small_etl command=clean
pixi run python -m small_etl command=clean db=test

# Schedule commands
pixi run python -m small_etl command=schedule job.action=start
pixi run python -m small_etl command=schedule job.action=add job.id=daily_etl job.interval=day job.at=02:00
pixi run python -m small_etl command=schedule job.action=list
pixi run python -m small_etl command=schedule job.action=remove job.id=daily_etl
pixi run python -m small_etl command=schedule job.action=pause job.id=daily_etl
pixi run python -m small_etl command=schedule job.action=resume job.id=daily_etl
```

**Schedule job parameters:**
- `job.action` - Action: start/add/list/remove/pause/resume
- `job.id` - Job identifier (required for add/remove/pause/resume)
- `job.command` - ETL command: run (default)
- `job.interval` - Interval: day/hour/minute
- `job.at` - Execution time for day interval (e.g., "02:00")

**Hydra override syntax:**
- `key=value` - Override a config value
- `key.subkey=value` - Override nested config value
- `+key=value` - Add a new key
- `~key` - Remove a key

## Test Structure

```
tests/
├── conftest.py              # Shared fixtures (test_db_engine, sample data)
├── unit/                    # Unit tests (no external dependencies)
│   ├── test_analytics.py
│   ├── test_cli.py
│   ├── test_duckdb.py
│   ├── test_extractor.py
│   ├── test_models.py
│   ├── test_pipeline.py
│   └── test_validator.py
└── integration/             # Integration tests (requires PostgreSQL)
    └── test_full_pipeline.py
```

## Code Conventions

- Use `Decimal` for monetary fields in models, but `float` in Polars DataFrames
- Use `datetime.now(UTC)` instead of deprecated `datetime.utcnow()`
- For raw SQL in SQLModel sessions, use `session.execute(text(...))` not `session.exec()`
- pyrefly `bad-override` and `deprecated` warnings are suppressed for SQLModel/Pandera compatibility
- Ruff is configured with line-length=180 and target-version=py312

## Dependencies

Key dependencies (managed via pixi):
- Python 3.12
- SQLModel, Alembic (ORM and migrations)
- DuckDB, Polars, PyArrow (data processing, S3 access via httpfs)
- Pandera (schema validation)
- Hydra (configuration)
- psycopg2-binary (PostgreSQL driver)

Dev tools: pytest, pytest-cov, ruff, pyright, pyrefly
