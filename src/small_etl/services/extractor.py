"""Extractor service for data type conversion."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import polars as pl

if TYPE_CHECKING:
    from omegaconf import DictConfig

logger = logging.getLogger(__name__)


class ExtractorService:
    """Service for transforming validated data with type conversions.

    Args:
        config: Optional Hydra configuration for column type definitions.
    """

    def __init__(self, config: "DictConfig | None" = None) -> None:
        self._config = config

    def _transform_dataframe(
        self,
        df: pl.DataFrame,
        columns_config: list[Any],
    ) -> pl.DataFrame:
        """Transform DataFrame according to column configurations.

        Args:
            df: Input DataFrame.
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

    def transform(self, df: pl.DataFrame, data_type: str) -> pl.DataFrame:
        """Transform validated DataFrame with type conversions.

        Args:
            df: Validated DataFrame from ValidatorService.
            data_type: Data type name (e.g., "asset", "trade").

        Returns:
            Transformed DataFrame with proper types.
        """
        logger.info(f"Transforming {len(df)} {data_type} records")

        config_attr = f"{data_type}s"  # e.g., "assets", "trades"
        if self._config is not None and hasattr(self._config, config_attr):
            type_config = getattr(self._config, config_attr)
            df = self._transform_dataframe(df, type_config.columns)

        logger.info(f"Transformed {len(df)} {data_type} records")
        return df
