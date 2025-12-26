"""PostgreSQL repository for data persistence."""

import logging

from sqlalchemy import text
from sqlmodel import Session, create_engine

logger = logging.getLogger(__name__)


class PostgresRepository:
    """PostgreSQL repository for asset and trade data operations.

    Args:
        database_url: PostgreSQL connection URL.
        echo: Whether to echo SQL statements (default: False).
    """

    def __init__(self, database_url: str, echo: bool = False) -> None:
        self._engine = create_engine(database_url, echo=echo)
        logger.info("PostgresRepository initialized")

    def create_tables(self) -> None:
        """Create all tables defined in SQLModel metadata."""
        from sqlmodel import SQLModel

        SQLModel.metadata.create_all(self._engine)
        logger.info("Database tables created")

    def get_session(self) -> Session:
        """Get a new database session.

        Returns:
            SQLModel Session instance.
        """
        return Session(self._engine)

    def truncate_tables(self) -> None:
        """Truncate asset and trade tables."""
        with Session(self._engine) as session:
            session.execute(text("TRUNCATE TABLE trade CASCADE"))  # pyrefly: ignore[deprecated]
            session.execute(text("TRUNCATE TABLE asset CASCADE"))  # pyrefly: ignore[deprecated]
            session.commit()
            logger.info("Truncated asset and trade tables")

    def close(self) -> None:
        """Dispose of the database engine."""
        self._engine.dispose()
        logger.info("PostgresRepository connection closed")
