"""Extractor service for data extraction from S3 using DuckDB."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import polars as pl

from small_etl.data_access.duckdb_client import DuckDBClient
from small_etl.domain.registry import DataTypeRegistry

if TYPE_CHECKING:
    from omegaconf import DictConfig

logger = logging.getLogger(__name__)


class ExtractorService:
    """Service for extracting data from S3 CSV files using DuckDB.

    Args:
        duckdb_client: DuckDB client instance with S3 configured.
        config: Optional Hydra configuration for CSV format definitions.
    """

    def __init__(
        self,
        duckdb_client: DuckDBClient,
        config: DictConfig | None = None,
    ) -> None:
        self._duckdb = duckdb_client
        self._config = config

    def _transform_dataframe(
        self,
        df: pl.DataFrame,
        columns_config: list[Any],
    ) -> pl.DataFrame:
        """Transform DataFrame according to column configurations.

        Args:
            df: Input DataFrame from DuckDB.
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

            col_dtype = df[csv_name].dtype

            if dtype == "Datetime":
                # If already datetime, just rename; otherwise parse from string
                if col_dtype == pl.Datetime or str(col_dtype).startswith("Datetime"):
                    expr = pl.col(csv_name).alias(target_name)
                else:
                    fmt = getattr(col_cfg, "format", "%Y-%m-%dT%H:%M:%S")
                    expr = pl.col(csv_name).str.to_datetime(fmt).alias(target_name)
            elif dtype == "Float64":
                if col_dtype == pl.Float64:
                    expr = pl.col(csv_name).alias(target_name)
                else:
                    expr = pl.col(csv_name).cast(pl.Float64).alias(target_name)
            elif dtype == "Utf8":
                if col_dtype == pl.Utf8:
                    expr = pl.col(csv_name).alias(target_name)
                else:
                    expr = pl.col(csv_name).cast(pl.Utf8).alias(target_name)
            elif dtype == "Int32":
                if col_dtype == pl.Int32:
                    expr = pl.col(csv_name).alias(target_name)
                else:
                    expr = pl.col(csv_name).cast(pl.Int32).alias(target_name)
            elif dtype == "Int64":
                if col_dtype == pl.Int64:
                    expr = pl.col(csv_name).alias(target_name)
                else:
                    expr = pl.col(csv_name).cast(pl.Int64).alias(target_name)
            else:
                expr = pl.col(csv_name).alias(target_name)

            expressions.append(expr)

        return df.select(expressions)

    def extract(
        self,
        bucket: str,
        object_name: str,
        data_type: str,
    ) -> pl.DataFrame:
        """Extract data from S3 CSV file using DuckDB.

        DuckDB reads CSV directly from S3, then transforms according to Hydra config.

        Args:
            bucket: S3 bucket name.
            object_name: CSV file object name.
            data_type: Data type name (e.g., "asset", "trade").

        Returns:
            Polars DataFrame with extracted data.
        """
        config = DataTypeRegistry.get(data_type)
        logger.info(f"Extracting {data_type} from s3://{bucket}/{object_name}")

        # DuckDB reads CSV directly from S3
        self._duckdb.load_csv_from_s3(bucket, object_name, config.raw_table_name)
        df = self._duckdb.query(f"SELECT * FROM {config.raw_table_name}")

        # Apply Hydra config transformations if available
        config_attr = f"{data_type}s"  # e.g., "assets", "trades"
        if self._config is not None and hasattr(self._config, config_attr):
            type_config = getattr(self._config, config_attr)
            df = self._transform_dataframe(df, type_config.columns)

        logger.info(f"Extracted {len(df)} {data_type} records")
        return df
