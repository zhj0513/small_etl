"""Database setup utilities for Small ETL.

This module provides functions to create and initialize databases,
primarily used for test environment setup.
"""

import logging

from sqlalchemy import text
from sqlmodel import SQLModel, create_engine

logger = logging.getLogger(__name__)


def create_database_if_not_exists(
    host: str = "localhost",
    port: int = 15432,
    user: str = "etl",
    password: str = "etlpass",
    database: str = "etl_test_db",
) -> bool:
    """Create a PostgreSQL database if it doesn't exist.

    Connects to the 'postgres' database to check and create the target database.

    Args:
        host: Database host.
        port: Database port.
        user: Database user.
        password: Database password.
        database: Name of the database to create.

    Returns:
        True if database was created, False if it already exists.

    Raises:
        Exception: If connection or creation fails.
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


def create_tables(database_url: str, echo: bool = False) -> None:
    """Create all SQLModel tables in the database.

    Args:
        database_url: PostgreSQL connection URL.
        echo: Whether to echo SQL statements.
    """
    engine = create_engine(database_url, echo=echo)
    try:
        SQLModel.metadata.create_all(engine)
        logger.info("All tables created successfully")
    finally:
        engine.dispose()


def drop_tables(database_url: str, echo: bool = False) -> None:
    """Drop all SQLModel tables from the database.

    Args:
        database_url: PostgreSQL connection URL.
        echo: Whether to echo SQL statements.
    """
    engine = create_engine(database_url, echo=echo)
    try:
        SQLModel.metadata.drop_all(engine)
        logger.info("All tables dropped successfully")
    finally:
        engine.dispose()


def setup_test_database(
    host: str = "localhost",
    port: int = 15432,
    user: str = "etl",
    password: str = "etlpass",
    database: str = "etl_test_db",
    echo: bool = False,
) -> str:
    """Setup test database: create if not exists and create tables.

    This is a convenience function that combines database creation
    and table setup for test environments.

    Args:
        host: Database host.
        port: Database port.
        user: Database user.
        password: Database password.
        database: Name of the test database.
        echo: Whether to echo SQL statements.

    Returns:
        Database URL for the created/existing database.
    """
    # Import models to ensure they're registered with SQLModel
    from small_etl.domain.models import Asset, Trade  # noqa: F401

    create_database_if_not_exists(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )

    database_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    create_tables(database_url, echo=echo)

    return database_url


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Default test database settings
    db_url = setup_test_database()
    print(f"Test database ready: {db_url}")
    sys.exit(0)
