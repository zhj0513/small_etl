# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Development Commands

```bash
# Install dependencies
pixi install

# Run all unit tests
pixi run pytest tests/unit/ -v --no-cov

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

Test database setup is automated via Python. The test fixtures in `conftest.py` will automatically create the database if it doesn't exist.

```bash
# Option 1: Run via Python module directly
pixi run python -m small_etl.data_access.db_setup

# Option 2: Use in Python code
from small_etl.data_access.db_setup import setup_test_database
db_url = setup_test_database()  # Creates etl_test_db and tables

# Option 3: Run Alembic migrations for test database
ETL_ENV=test pixi run alembic upgrade head
```

**Prerequisites**: PostgreSQL must be running (via podman: `podman start postgres`)

## Architecture Overview

This is a batch ETL system for syncing S3 CSV files to PostgreSQL with DuckDB-based validation.

### Layer Structure

```
src/small_etl/
├── domain/          # Data models (SQLModel), enums, Pandera schemas
├── data_access/     # S3Connector, DuckDBClient, PostgresRepository
├── services/        # ExtractorService, ValidatorService, LoaderService, AnalyticsService
└── application/     # ETLPipeline orchestration
```

### Data Flow

```
S3 (CSV) → Extractor → DuckDB (validation) → Validator → Loader → PostgreSQL
                                                              ↓
                                                         Analytics
```

### Key Components

- **ETLPipeline** (`application/pipeline.py`): Main orchestrator, use `with ETLPipeline(config) as pipeline`
- **ValidatorService** (`services/validator.py`): Field-level + business rule validation with tolerance (±0.01)
- **PostgresRepository** (`data_access/postgres_repository.py`): Bulk upsert with `polars_to_assets()` / `polars_to_trades()`

### Data Models

Two tables: `asset` and `trade` (Trade.account_id → Asset.account_id FK)

Validation rules:
- `total_asset = cash + frozen_cash + market_value` (±0.01 tolerance)
- `traded_amount = traded_price * traded_volume` (±0.01 tolerance)
- Enum fields: `account_type` (1,2,3,5,6,7,11), `offset_flag` (48-54), `direction` (0,48,49)

### Configuration

Hydra configs in `configs/`:
- `db/dev.yaml`, `db/test.yaml`: Database connection (supports env vars: `DB_HOST`, `DB_PORT`, etc.)
- `s3/dev.yaml`: MinIO/S3 connection
- `etl/default.yaml`: batch_size, validation tolerance

## Code Conventions

- Use `Decimal` for monetary fields in models, but `float` in Polars DataFrames
- Use `datetime.now(UTC)` instead of deprecated `datetime.utcnow()`
- For raw SQL in SQLModel sessions, use `session.execute(text(...))` not `session.exec()`
- pyrefly `bad-override` and `deprecated` warnings are suppressed for SQLModel/Pandera compatibility
