"""Data access layer - database connectors."""

from small_etl.data_access.duckdb_client import DuckDBClient
from small_etl.data_access.postgres_repository import PostgresRepository

__all__ = [
    "DuckDBClient",
    "PostgresRepository",
]
