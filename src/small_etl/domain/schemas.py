"""Pandera validation schemas for Small ETL data validation."""

import pandera.polars as pa
import polars as pl
from pandera.polars import PolarsData

from small_etl.domain.enums import VALID_ACCOUNT_TYPES, VALID_DIRECTIONS, VALID_OFFSET_FLAGS


class AssetSchema(pa.DataFrameModel):
    """Pandera schema for Asset data validation.

    Validates:
        - Required fields presence
        - Numeric fields >= 0
        - Account type in valid enum range
        - Timestamp format
        - total_asset = cash + frozen_cash + market_value
    """

    account_id: str = pa.Field(str_length={"min_value": 1, "max_value": 20})
    account_type: int = pa.Field(isin=list(VALID_ACCOUNT_TYPES))
    cash: float = pa.Field(ge=0)
    frozen_cash: float = pa.Field(ge=0)
    market_value: float = pa.Field(ge=0)
    total_asset: float = pa.Field(ge=0)
    updated_at: pl.Datetime = pa.Field()

    class Config:  # type: ignore[override]  # pyrefly: ignore[bad-override]
        """Pandera configuration."""

        strict = False  # Allow extra fields like id, _original_index
        coerce = True

    @pa.dataframe_check  # type: ignore[misc]
    @classmethod
    def total_asset_equals_sum(cls, data: PolarsData) -> pl.LazyFrame:
        """Check that total_asset = cash + frozen_cash + market_value.

        Uses Decimal type for precise comparison (converted in ValidatorService).

        Args:
            data: PolarsData containing the LazyFrame to validate.

        Returns:
            LazyFrame with boolean column indicating which rows pass validation.
        """
        return data.lazyframe.select(
            pl.col("total_asset") == (pl.col("cash") + pl.col("frozen_cash") + pl.col("market_value"))
        )


class TradeSchema(pa.DataFrameModel):
    """Pandera schema for Trade data validation.

    Validates:
        - Required fields presence
        - Price and amount > 0
        - Volume > 0
        - Enum fields in valid ranges
        - Timestamp format
        - traded_amount = traded_price * traded_volume
    """

    account_id: str = pa.Field(str_length={"min_value": 1, "max_value": 20})
    account_type: int = pa.Field(isin=list(VALID_ACCOUNT_TYPES))
    traded_id: str = pa.Field(str_length={"min_value": 1, "max_value": 50})
    stock_code: str = pa.Field(str_length={"min_value": 1, "max_value": 10})
    traded_time: pl.Datetime = pa.Field()
    traded_price: float = pa.Field(gt=0)
    traded_volume: int = pa.Field(gt=0)
    traded_amount: float = pa.Field(gt=0)
    strategy_name: str = pa.Field(str_length={"min_value": 1, "max_value": 50})
    order_remark: str = pa.Field(nullable=True)
    direction: int = pa.Field(isin=list(VALID_DIRECTIONS))
    offset_flag: int = pa.Field(isin=list(VALID_OFFSET_FLAGS))
    created_at: pl.Datetime = pa.Field()
    updated_at: pl.Datetime = pa.Field()

    class Config:  # type: ignore[override] # pyrefly: ignore[bad-override]
        """Pandera configuration."""

        strict = False  # Allow extra fields like id, _original_index
        coerce = True

    @pa.dataframe_check  # type: ignore[misc]
    @classmethod
    def traded_amount_equals_product(cls, data: PolarsData) -> pl.LazyFrame:
        """Check that traded_amount = traded_price * traded_volume.

        Uses Decimal type for precise comparison (converted in ValidatorService).

        Args:
            data: PolarsData containing the LazyFrame to validate.

        Returns:
            LazyFrame with boolean column indicating which rows pass validation.
        """
        return data.lazyframe.select(
            pl.col("traded_amount") == (pl.col("traded_price") * pl.col("traded_volume"))
        )
