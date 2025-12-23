"""Extractor service for data extraction from S3."""

import logging

import polars as pl

from small_etl.data_access.duckdb_client import DuckDBClient
from small_etl.data_access.s3_connector import S3Connector

logger = logging.getLogger(__name__)


class ExtractorService:
    """Service for extracting data from S3 and loading into DuckDB.

    Args:
        s3_connector: S3/MinIO connector instance.
        duckdb_client: DuckDB client instance.
    """

    def __init__(self, s3_connector: S3Connector, duckdb_client: DuckDBClient) -> None:
        self._s3 = s3_connector
        self._duckdb = duckdb_client

    def extract_assets(self, bucket: str, object_name: str) -> pl.DataFrame:
        """Extract asset data from S3 CSV file.

        Args:
            bucket: S3 bucket name.
            object_name: CSV file object name.

        Returns:
            Polars DataFrame with asset data.
        """
        logger.info(f"Extracting assets from {bucket}/{object_name}")
        csv_bytes = self._s3.download_csv(bucket, object_name)
        self._duckdb.load_csv_bytes(csv_bytes, "raw_assets")

        df = self._duckdb.query("""
            SELECT
                id,
                CAST(account_id AS VARCHAR) as account_id,
                account_type,
                CAST(cash AS DECIMAL(20, 2)) as cash,
                CAST(frozen_cash AS DECIMAL(20, 2)) as frozen_cash,
                CAST(market_value AS DECIMAL(20, 2)) as market_value,
                CAST(total_asset AS DECIMAL(20, 2)) as total_asset,
                updated_at
            FROM raw_assets
        """)

        logger.info(f"Extracted {len(df)} asset records")
        return df

    def extract_trades(self, bucket: str, object_name: str) -> pl.DataFrame:
        """Extract trade data from S3 CSV file.

        Args:
            bucket: S3 bucket name.
            object_name: CSV file object name.

        Returns:
            Polars DataFrame with trade data.
        """
        logger.info(f"Extracting trades from {bucket}/{object_name}")
        csv_bytes = self._s3.download_csv(bucket, object_name)
        self._duckdb.load_csv_bytes(csv_bytes, "raw_trades")

        df = self._duckdb.query("""
            SELECT
                id,
                CAST(account_id AS VARCHAR) as account_id,
                account_type,
                traded_id,
                stock_code,
                traded_time,
                CAST(traded_price AS DECIMAL(20, 2)) as traded_price,
                traded_volume,
                CAST(traded_amount AS DECIMAL(20, 2)) as traded_amount,
                strategy_name,
                order_remark,
                direction,
                offset_flag,
                created_at,
                updated_at
            FROM raw_trades
        """)

        logger.info(f"Extracted {len(df)} trade records")
        return df
