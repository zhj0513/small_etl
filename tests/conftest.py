"""Test fixtures and configuration for Small ETL."""

import logging
import os
from datetime import datetime
from decimal import Decimal
from typing import Generator

import polars as pl
import pytest
from sqlalchemy import text
from sqlmodel import Session, create_engine

from small_etl.domain.enums import AccountType, Direction, OffsetFlag
from small_etl.domain.models import Asset, Trade
from tests.container_manager import (
    MINIO_CONTAINER,
    POSTGRES_CONTAINER,
    POSTGRES_DB,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    copy_test_data_from_dev,
    setup_minio_bucket,
    start_minio_container,
    start_postgres_container,
    stop_container,
    wait_for_minio,
    wait_for_postgres,
)

# Test database configuration (uses test container ports)
TEST_DB_HOST = os.getenv("DB_HOST", "localhost")
TEST_DB_PORT = POSTGRES_PORT  # Use port from container_manager
TEST_DB_USER = POSTGRES_USER
TEST_DB_PASSWORD = POSTGRES_PASSWORD
TEST_DB_NAME = POSTGRES_DB

TEST_DATABASE_URL = (
    f"postgresql://{TEST_DB_USER}:{TEST_DB_PASSWORD}"
    f"@{TEST_DB_HOST}:{TEST_DB_PORT}/{TEST_DB_NAME}"
)

logger = logging.getLogger(__name__)


def create_database_if_not_exists(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
) -> bool:
    """Create a PostgreSQL database if it doesn't exist.

    Connects to the 'postgres' database to check and create the target database.

    Returns:
        True if database was created, False if it already exists.
    """
    admin_url = f"postgresql://{user}:{password}@{host}:{port}/postgres"
    engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :dbname"),
                {"dbname": database},
            )
            if result.fetchone():
                logger.info(f"Database '{database}' already exists")
                return False

            conn.execute(text(f'CREATE DATABASE "{database}" OWNER "{user}"'))
            logger.info(f"Database '{database}' created successfully")
            return True
    finally:
        engine.dispose()


@pytest.fixture(scope="session")
def test_db_engine():
    """Create test database engine.

    This fixture ensures the test database exists and creates all tables.
    Scope is 'session' to reuse across all tests.
    """
    from sqlmodel import SQLModel

    # Create database if not exists
    try:
        create_database_if_not_exists(
            host=TEST_DB_HOST,
            port=TEST_DB_PORT,
            user=TEST_DB_USER,
            password=TEST_DB_PASSWORD,
            database=TEST_DB_NAME,
        )
    except Exception:
        # If we can't create the database, assume it exists or tests will fail later
        pass

    # Create the test engine
    engine = create_engine(TEST_DATABASE_URL, echo=False)

    # Create all tables
    SQLModel.metadata.create_all(engine)

    yield engine

    # Cleanup: drop all tables after tests
    SQLModel.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture
def test_db_session(test_db_engine) -> Generator[Session, None, None]:
    """Create a test database session.

    Each test gets a fresh session with transaction rollback.
    """
    with Session(test_db_engine) as session:
        yield session
        session.rollback()


@pytest.fixture
def sample_asset_data() -> pl.DataFrame:
    """Sample valid asset data as Polars DataFrame."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3],
            "account_id": ["10000000001", "10000000002", "10000000003"],
            "account_type": [AccountType.SECURITY, AccountType.SECURITY, AccountType.CREDIT],
            "cash": [100000.00, 500000.00, 250000.00],
            "frozen_cash": [5000.00, 50000.00, 75000.00],
            "market_value": [200000.00, 800000.00, 300000.00],
            "total_asset": [305000.00, 1350000.00, 625000.00],
            "updated_at": [
                datetime(2025, 12, 22, 14, 30, 0),
                datetime(2025, 12, 22, 14, 25, 0),
                datetime(2025, 12, 22, 14, 20, 0),
            ],
        }
    )


@pytest.fixture
def sample_trade_data() -> pl.DataFrame:
    """Sample valid trade data as Polars DataFrame."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3],
            "account_id": ["10000000001", "10000000002", "10000000001"],
            "account_type": [AccountType.SECURITY, AccountType.SECURITY, AccountType.CREDIT],
            "traded_id": ["T20251222000001", "T20251222000002", "T20251222000003"],
            "stock_code": ["600000", "000001", "600036"],
            "traded_time": [
                datetime(2025, 12, 22, 10, 30, 0),
                datetime(2025, 12, 22, 11, 15, 0),
                datetime(2025, 12, 22, 13, 45, 0),
            ],
            "traded_price": [15.50, 28.30, 42.80],
            "traded_volume": [1000, 500, 2000],
            "traded_amount": [15500.00, 14150.00, 85600.00],
            "strategy_name": ["策略A", "量化1号", "趋势跟踪"],
            "order_remark": ["正常交易", "建仓", "止盈"],
            "direction": [Direction.NA, Direction.NA, Direction.NA],
            "offset_flag": [OffsetFlag.OPEN, OffsetFlag.OPEN, OffsetFlag.CLOSE],
            "created_at": [
                datetime(2025, 12, 22, 10, 30, 0),
                datetime(2025, 12, 22, 11, 15, 0),
                datetime(2025, 12, 22, 13, 45, 0),
            ],
            "updated_at": [
                datetime(2025, 12, 22, 14, 30, 0),
                datetime(2025, 12, 22, 14, 25, 0),
                datetime(2025, 12, 22, 14, 20, 0),
            ],
        }
    )


@pytest.fixture
def invalid_asset_data() -> pl.DataFrame:
    """Sample invalid asset data for testing validation."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3],
            "account_id": ["10000000001", "", "10000000003"],  # Empty account_id
            "account_type": [AccountType.SECURITY, 99, AccountType.CREDIT],  # Invalid account_type
            "cash": [100000.00, -500.00, 250000.00],  # Negative cash
            "frozen_cash": [5000.00, 50000.00, 75000.00],
            "market_value": [200000.00, 800000.00, 300000.00],
            "total_asset": [305000.00, 1350000.00, 999999.00],  # Wrong total
            "updated_at": [
                datetime(2025, 12, 22, 14, 30, 0),
                datetime(2025, 12, 22, 14, 25, 0),
                datetime(2025, 12, 22, 14, 20, 0),
            ],
        }
    )


@pytest.fixture
def invalid_trade_data() -> pl.DataFrame:
    """Sample invalid trade data for testing validation."""
    return pl.DataFrame(
        {
            "id": [1, 2],
            "account_id": ["10000000001", "10000000002"],
            "account_type": [AccountType.SECURITY, 99],  # Invalid account_type
            "traded_id": ["T20251222000001", ""],  # Empty traded_id
            "stock_code": ["600000", "000001"],
            "traded_time": [
                datetime(2025, 12, 22, 10, 30, 0),
                datetime(2025, 12, 22, 11, 15, 0),
            ],
            "traded_price": [15.50, 0.00],  # Zero price
            "traded_volume": [1000, -100],  # Negative volume
            "traded_amount": [15500.00, 999.00],  # Wrong amount
            "strategy_name": ["策略A", "量化1号"],
            "order_remark": ["正常交易", "建仓"],
            "direction": [Direction.NA, Direction.NA],
            "offset_flag": [OffsetFlag.OPEN, 99],  # Invalid offset_flag
            "created_at": [
                datetime(2025, 12, 22, 10, 30, 0),
                datetime(2025, 12, 22, 11, 15, 0),
            ],
            "updated_at": [
                datetime(2025, 12, 22, 14, 30, 0),
                datetime(2025, 12, 22, 14, 25, 0),
            ],
        }
    )


@pytest.fixture
def sample_assets() -> list[Asset]:
    """Sample Asset model instances."""
    return [
        Asset(
            account_id="10000000001",
            account_type=AccountType.SECURITY,
            cash=Decimal("100000.00"),
            frozen_cash=Decimal("5000.00"),
            market_value=Decimal("200000.00"),
            total_asset=Decimal("305000.00"),
            updated_at=datetime(2025, 12, 22, 14, 30, 0),
        ),
        Asset(
            account_id="10000000002",
            account_type=AccountType.SECURITY,
            cash=Decimal("500000.00"),
            frozen_cash=Decimal("50000.00"),
            market_value=Decimal("800000.00"),
            total_asset=Decimal("1350000.00"),
            updated_at=datetime(2025, 12, 22, 14, 25, 0),
        ),
    ]


@pytest.fixture
def sample_trades() -> list[Trade]:
    """Sample Trade model instances."""
    return [
        Trade(
            account_id="10000000001",
            account_type=AccountType.SECURITY,
            traded_id="T20251222000001",
            stock_code="600000",
            traded_time=datetime(2025, 12, 22, 10, 30, 0),
            traded_price=Decimal("15.50"),
            traded_volume=1000,
            traded_amount=Decimal("15500.00"),
            strategy_name="策略A",
            order_remark="正常交易",
            direction=Direction.NA,
            offset_flag=OffsetFlag.OPEN,
            created_at=datetime(2025, 12, 22, 10, 30, 0),
            updated_at=datetime(2025, 12, 22, 14, 30, 0),
        ),
    ]


# ============================================================================
# Container Management Fixtures (for integration tests)
# ============================================================================


@pytest.fixture(scope="session")
def postgres_container():
    """Start PostgreSQL container and stop it after tests.

    This fixture manages the lifecycle of a PostgreSQL container for integration tests.
    The container is started before tests and stopped after all tests complete.
    """
    start_postgres_container()
    if not wait_for_postgres():
        pytest.fail("PostgreSQL container failed to start")
    yield
    stop_container(POSTGRES_CONTAINER)


@pytest.fixture(scope="session")
def minio_container():
    """Start MinIO container and stop it after tests.

    This fixture manages the lifecycle of a MinIO container for integration tests.
    The container is started before tests and stopped after all tests complete.
    A default bucket is also created and test data is copied from dev MinIO.
    """
    start_minio_container()
    if not wait_for_minio():
        pytest.fail("MinIO container failed to start")
    setup_minio_bucket("fake-data-for-training")
    copy_test_data_from_dev("fake-data-for-training")
    yield
    stop_container(MINIO_CONTAINER)


@pytest.fixture(scope="session")
def integration_services(postgres_container, minio_container):
    """Composite fixture ensuring all integration services are ready.

    Use this fixture in integration tests that require both PostgreSQL and MinIO.
    """
    yield
