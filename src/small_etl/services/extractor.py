"""Extractor service for data type conversion using DuckDB."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from omegaconf import DictConfig

    from small_etl.data_access.duckdb_client import DuckDBClient

logger = logging.getLogger(__name__)


class ExtractorService:
    """Service for transforming validated data with type conversions using DuckDB SQL.

    Args:
        config: Optional Hydra configuration for column type definitions.
        duckdb_client: DuckDB client for SQL-based transformations.
    """

    def __init__(
        self,
        config: "DictConfig | None" = None,
        duckdb_client: "DuckDBClient | None" = None,
    ) -> None:
        self._config = config
        self._duckdb = duckdb_client

    def transform(self, df: pl.DataFrame, data_type: str) -> str:
        """Transform validated DataFrame using DuckDB SQL.

        Registers the DataFrame in DuckDB, applies SQL-based type conversions,
        and creates a transformed table that can be used by LoaderService.

        Args:
            df: Validated DataFrame from ValidatorService.
            data_type: Data type name (e.g., "asset", "trade").

        Returns:
            Name of the transformed DuckDB table (e.g., "_transformed_asset").

        Raises:
            RuntimeError: If DuckDB client is not configured.
        """
        if self._duckdb is None:
            raise RuntimeError("DuckDB client is required for transformation")

        logger.info(f"Transforming {len(df)} {data_type} records using DuckDB")

        raw_table = f"_raw_{data_type}"
        transformed_table = f"_transformed_{data_type}"

        # Register original DataFrame as a view
        self._duckdb.register_dataframe(df, raw_table)

        # Get column configuration
        config_attr = f"{data_type}s"  # e.g., "assets", "trades"
        if self._config is not None and hasattr(self._config, config_attr):
            type_config = getattr(self._config, config_attr)
            columns_config = type_config.columns

            # Create transformed table using SQL
            row_count = self._duckdb.create_transformed_table(
                raw_table, transformed_table, columns_config
            )
        else:
            # No config: create table as-is
            self._duckdb._conn.execute(
                f"CREATE TABLE {transformed_table} AS SELECT * FROM {raw_table}"
            )
            row_count = len(df)

        # Clean up raw view
        self._duckdb.unregister(raw_table)

        logger.info(f"Transformed {row_count} {data_type} records -> {transformed_table}")
        return transformed_table
