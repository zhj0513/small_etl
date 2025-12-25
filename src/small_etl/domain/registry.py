"""Data type registry for extensible ETL processing.

This module provides a centralized registry for data type configurations,
enabling generic processing of different data types (Asset, Trade, etc.)
without hardcoding type-specific logic in services.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import polars as pl

if TYPE_CHECKING:
    import pandera.polars as pa
    from sqlmodel import SQLModel


@dataclass
class DataTypeConfig:
    """Configuration for a data type in the ETL pipeline.

    Attributes:
        name: Unique identifier for the data type (e.g., "asset", "trade").
        table_name: PostgreSQL table name.
        unique_key: Column used for upsert conflict detection.
        db_columns: List of columns to load into database.
        s3_file_key: Config key for S3 file path (e.g., "assets_file").
        raw_table_name: DuckDB raw table name for extraction.
        foreign_key_column: Column to validate as foreign key (optional).
        foreign_key_reference: Data type that foreign key references (optional).
        statistics_config: Configuration for statistics computation.
    """

    name: str
    table_name: str
    unique_key: str
    db_columns: list[str]
    s3_file_key: str
    raw_table_name: str = ""
    model_class: type["SQLModel"] | None = None
    schema_class: type["pa.DataFrameModel"] | None = None
    foreign_key_column: str | None = None
    foreign_key_reference: str | None = None
    statistics_config: dict[str, Any] = field(default_factory=dict)

    def get_db_schema(self) -> dict[str, pl.DataType]:
        """Get Polars schema for empty DataFrame creation.

        Returns:
            Dictionary mapping column names to Polars data types.
        """
        # Default schema based on common column patterns
        schema: dict[str, pl.DataType] = {}
        for col in self.db_columns:
            if col == "id":
                schema[col] = pl.Int64
            elif col.endswith("_id") or col.endswith("_code") or col.endswith("_name") or col.endswith("_remark"):
                schema[col] = pl.Utf8
            elif col.endswith("_type") or col == "direction" or col.endswith("_flag") or col == "traded_volume":
                schema[col] = pl.Int64
            elif col.endswith("_at") or col.endswith("_time"):
                schema[col] = pl.Datetime
            else:
                schema[col] = pl.Float64
        return schema


class DataTypeRegistry:
    """Registry for data type configurations.

    Provides centralized access to data type configurations for all services.
    """

    _registry: dict[str, DataTypeConfig] = {}

    @classmethod
    def register(cls, config: DataTypeConfig) -> None:
        """Register a data type configuration.

        Args:
            config: DataTypeConfig to register.
        """
        cls._registry[config.name] = config

    @classmethod
    def get(cls, name: str) -> DataTypeConfig:
        """Get a data type configuration by name.

        Args:
            name: Data type name.

        Returns:
            DataTypeConfig for the specified type.

        Raises:
            KeyError: If data type is not registered.
        """
        if name not in cls._registry:
            raise KeyError(f"Data type '{name}' not registered. Available: {list(cls._registry.keys())}")
        return cls._registry[name]

    @classmethod
    def get_all(cls) -> dict[str, DataTypeConfig]:
        """Get all registered data type configurations.

        Returns:
            Dictionary of all registered configurations.
        """
        return cls._registry.copy()

    @classmethod
    def is_registered(cls, name: str) -> bool:
        """Check if a data type is registered.

        Args:
            name: Data type name.

        Returns:
            True if registered, False otherwise.
        """
        return name in cls._registry


def _register_default_types() -> None:
    """Register default data types (Asset and Trade)."""
    # Import here to avoid circular imports
    from small_etl.domain.models import Asset, Trade
    from small_etl.domain.schemas import AssetSchema, TradeSchema

    # Asset configuration
    DataTypeRegistry.register(
        DataTypeConfig(
            name="asset",
            table_name="asset",
            unique_key="account_id",
            db_columns=[
                "account_id",
                "account_type",
                "cash",
                "frozen_cash",
                "market_value",
                "total_asset",
                "updated_at",
            ],
            s3_file_key="assets_file",
            raw_table_name="raw_assets",
            model_class=Asset,
            schema_class=AssetSchema,
            statistics_config={
                "aggregates": {
                    "cash": ["sum", "avg"],
                    "frozen_cash": ["sum"],
                    "market_value": ["sum"],
                    "total_asset": ["sum", "avg"],
                },
                "group_by": ["account_type"],
            },
        )
    )

    # Trade configuration
    DataTypeRegistry.register(
        DataTypeConfig(
            name="trade",
            table_name="trade",
            unique_key="traded_id",
            db_columns=[
                "account_id",
                "account_type",
                "traded_id",
                "stock_code",
                "traded_time",
                "traded_price",
                "traded_volume",
                "traded_amount",
                "strategy_name",
                "order_remark",
                "direction",
                "offset_flag",
                "created_at",
                "updated_at",
            ],
            s3_file_key="trades_file",
            raw_table_name="raw_trades",
            model_class=Trade,
            schema_class=TradeSchema,
            foreign_key_column="account_id",
            foreign_key_reference="asset",
            statistics_config={
                "aggregates": {
                    "traded_volume": ["sum", "avg"],
                    "traded_amount": ["sum"],
                    "traded_price": ["avg"],
                },
                "group_by": ["account_type", "offset_flag", "strategy_name"],
            },
        )
    )


# Register default types on module import
_register_default_types()
