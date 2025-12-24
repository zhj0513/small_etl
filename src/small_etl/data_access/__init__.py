"""Data access layer - database and storage connectors."""

from small_etl.data_access.duckdb_client import DuckDBClient
from small_etl.data_access.postgres_repository import PostgresRepository
from small_etl.data_access.s3_connector import S3Connector

__all__ = [
    "S3Connector",
    "DuckDBClient",
    "PostgresRepository",
]
