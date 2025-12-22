"""Pandera validation schemas for Small ETL data validation."""

from decimal import Decimal

import pandera.polars as pa
import polars as pl

from small_etl.domain.enums import VALID_ACCOUNT_TYPES, VALID_DIRECTIONS, VALID_OFFSET_FLAGS


class AssetSchema(pa.DataFrameModel):
    """Pandera schema for Asset data validation.

    Validates:
        - Required fields presence
        - Decimal fields >= 0
        - Account type in valid enum range
        - Timestamp format
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


class TradeSchema(pa.DataFrameModel):
    """Pandera schema for Trade data validation.

    Validates:
        - Required fields presence
        - Price and amount > 0
        - Volume > 0
        - Enum fields in valid ranges
        - Timestamp format
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
