"""Pandera validation schemas for Small ETL data validation."""

from decimal import Decimal

import pandera.polars as pa
import polars as pl

from small_etl.domain.enums import VALID_ACCOUNT_TYPES, VALID_DIRECTIONS, VALID_OFFSET_FLAGS

# Default tolerance for calculated field comparisons
DEFAULT_TOLERANCE = Decimal("0.01")


class AssetSchema(pa.DataFrameModel):
    """Pandera schema for Asset data validation.

    Validates:
        - Required fields presence
        - Decimal fields >= 0
        - Account type in valid enum range
        - Timestamp format
        - total_asset = cash + frozen_cash + market_value (within tolerance)
    """

    id: int = pa.Field(ge=0)
    account_id: str = pa.Field(str_length={"min_value": 1, "max_value": 20})
    account_type: int = pa.Field(isin=list(VALID_ACCOUNT_TYPES))
    cash: pl.Decimal = pa.Field(ge=Decimal("0"))
    frozen_cash: pl.Decimal = pa.Field(ge=Decimal("0"))
    market_value: pl.Decimal = pa.Field(ge=Decimal("0"))
    total_asset: pl.Decimal = pa.Field(ge=Decimal("0"))
    updated_at: pl.Datetime = pa.Field()

    class Config:  # type: ignore[override]  # pyrefly: ignore[bad-override]
        """Pandera configuration."""

        strict = True
        coerce = True

    @pa.dataframe_check  # type: ignore[misc]
    @staticmethod
    def total_asset_equals_sum(df: pl.DataFrame) -> pl.Series:
        """Check that total_asset = cash + frozen_cash + market_value (within tolerance).

        Args:
            df: DataFrame to validate.

        Returns:
            Boolean Series indicating which rows pass validation.
        """
        calc_total = df["cash"] + df["frozen_cash"] + df["market_value"]
        diff = (df["total_asset"] - calc_total).abs()
        return diff <= float(DEFAULT_TOLERANCE)


class TradeSchema(pa.DataFrameModel):
    """Pandera schema for Trade data validation.

    Validates:
        - Required fields presence
        - Price and amount > 0
        - Volume > 0
        - Enum fields in valid ranges
        - Timestamp format
        - traded_amount = traded_price * traded_volume (within tolerance)
    """

    id: int = pa.Field(ge=0)
    account_id: str = pa.Field(str_length={"min_value": 1, "max_value": 20})
    account_type: int = pa.Field(isin=list(VALID_ACCOUNT_TYPES))
    traded_id: str = pa.Field(str_length={"min_value": 1, "max_value": 50})
    stock_code: str = pa.Field(str_length={"min_value": 1, "max_value": 10})
    traded_time: pl.Datetime = pa.Field()
    traded_price: pl.Decimal = pa.Field(gt=Decimal("0"))
    traded_volume: int = pa.Field(gt=0)
    traded_amount: pl.Decimal = pa.Field(gt=Decimal("0"))
    strategy_name: str = pa.Field(str_length={"min_value": 1, "max_value": 50})
    order_remark: str = pa.Field(nullable=True)
    direction: int = pa.Field(isin=list(VALID_DIRECTIONS))
    offset_flag: int = pa.Field(isin=list(VALID_OFFSET_FLAGS))
    created_at: pl.Datetime = pa.Field()
    updated_at: pl.Datetime = pa.Field()

    class Config:  # type: ignore[override] # pyrefly: ignore[bad-override]
        """Pandera configuration."""

        strict = True
        coerce = True

    @pa.dataframe_check  # type: ignore[misc]
    @staticmethod
    def traded_amount_equals_product(df: pl.DataFrame) -> pl.Series:
        """Check that traded_amount = traded_price * traded_volume (within tolerance).

        Args:
            df: DataFrame to validate.

        Returns:
            Boolean Series indicating which rows pass validation.
        """
        calc_amount = df["traded_price"] * df["traded_volume"]
        diff = (df["traded_amount"] - calc_amount).abs()
        return diff <= float(DEFAULT_TOLERANCE)
