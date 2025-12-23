"""Extractor service for data extraction from S3."""

from __future__ import annotations

import io
import logging
from typing import TYPE_CHECKING, Any

import polars as pl

from small_etl.data_access.duckdb_client import DuckDBClient
from small_etl.data_access.s3_connector import S3Connector

if TYPE_CHECKING:
    from omegaconf import DictConfig

logger = logging.getLogger(__name__)


class ExtractorService:
    """Service for extracting data from S3 with config-driven transformations.

    Args:
        s3_connector: S3/MinIO connector instance.
        duckdb_client: DuckDB client instance.
        config: Optional Hydra configuration for CSV format definitions.
    """

    def __init__(
        self,
        s3_connector: S3Connector,
        duckdb_client: DuckDBClient,
        config: DictConfig | None = None,
    ) -> None:
        self._s3 = s3_connector
        self._duckdb = duckdb_client
        self._config = config

    def _get_polars_dtype(self, dtype_str: str) -> pl.DataType:
        """Convert config dtype string to Polars dtype.

        Args:
            dtype_str: Data type string from config.

        Returns:
            Polars DataType instance.
        """
        dtype_map: dict[str, pl.DataType] = {
            "Int32": pl.Int32(),
            "Int64": pl.Int64(),
            "Utf8": pl.Utf8(),
            "Float64": pl.Float64(),
            "Datetime": pl.Datetime(),
        }
        return dtype_map.get(dtype_str, pl.Utf8())

    def _transform_dataframe(
        self,
        df: pl.DataFrame,
        columns_config: list[Any],
    ) -> pl.DataFrame:
        """Transform DataFrame according to column configurations.

        Args:
            df: Input DataFrame from CSV.
            columns_config: Column configuration list from Hydra config.

        Returns:
            Transformed DataFrame with proper types and column names.
        """
        expressions = []
        for col_cfg in columns_config:
            csv_name = col_cfg.csv_name
            target_name = col_cfg.name
            dtype = col_cfg.dtype

            if csv_name not in df.columns:
                continue

            if dtype == "Datetime":
                fmt = getattr(col_cfg, "format", "%Y-%m-%dT%H:%M:%S")
                expr = pl.col(csv_name).str.to_datetime(fmt).alias(target_name)
            elif dtype == "Float64":
                expr = pl.col(csv_name).cast(pl.Float64).alias(target_name)
            elif dtype == "Utf8":
                expr = pl.col(csv_name).cast(pl.Utf8).alias(target_name)
            elif dtype == "Int32":
                expr = pl.col(csv_name).cast(pl.Int32).alias(target_name)
            elif dtype == "Int64":
                expr = pl.col(csv_name).cast(pl.Int64).alias(target_name)
            else:
                expr = pl.col(csv_name).alias(target_name)

            expressions.append(expr)

        return df.select(expressions)

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

        if self._config is not None and hasattr(self._config, "assets"):
            df = pl.read_csv(
                io.BytesIO(csv_bytes),
                has_header=True,
                infer_schema_length=10000,
            )
            df = self._transform_dataframe(df, self._config.assets.columns)
            logger.info(f"Extracted {len(df)} asset records (config-driven)")
        else:
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
            logger.info(f"Extracted {len(df)} asset records (DuckDB fallback)")

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

        if self._config is not None and hasattr(self._config, "trades"):
            df = pl.read_csv(
                io.BytesIO(csv_bytes),
                has_header=True,
                infer_schema_length=10000,
            )
            df = self._transform_dataframe(df, self._config.trades.columns)
            logger.info(f"Extracted {len(df)} trade records (config-driven)")
        else:
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
            logger.info(f"Extracted {len(df)} trade records (DuckDB fallback)")

        return df
