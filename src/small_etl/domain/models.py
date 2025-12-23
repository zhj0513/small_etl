"""SQLModel data models for Small ETL."""

from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlmodel import Field, SQLModel


class Asset(SQLModel, table=True):
    """Asset model representing account asset information.

    Attributes:
        id: Primary key, auto-generated.
        account_id: Unique account identifier.
        account_type: Account type enum value.
        cash: Available cash balance.
        frozen_cash: Frozen/locked cash.
        market_value: Current market value of holdings.
        total_asset: Total asset value (cash + frozen_cash + market_value).
        updated_at: Last update timestamp (UTC).
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    account_id: str = Field(unique=True, index=True, max_length=20)
    account_type: int = Field(index=True)
    cash: Decimal = Field(max_digits=20, decimal_places=2)
    frozen_cash: Decimal = Field(max_digits=20, decimal_places=2)
    market_value: Decimal = Field(max_digits=20, decimal_places=2)
    total_asset: Decimal = Field(max_digits=20, decimal_places=2)
    updated_at: datetime


class Trade(SQLModel, table=True):
    """Trade model representing trading records.

    Attributes:
        id: Primary key, auto-generated.
        account_id: Account identifier (foreign key to Asset).
        account_type: Account type enum value.
        traded_id: Unique trade identifier.
        stock_code: Stock/security code.
        traded_time: Trade execution timestamp (UTC).
        traded_price: Execution price.
        traded_volume: Traded quantity.
        traded_amount: Total trade amount (price * volume).
        strategy_name: Strategy identifier.
        order_remark: Optional order remarks.
        direction: Trade direction (for futures).
        offset_flag: Open/close flag.
        created_at: Record creation timestamp (UTC).
        updated_at: Last update timestamp (UTC).
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    account_id: str = Field(index=True, max_length=20, foreign_key="asset.account_id")
    account_type: int = Field(index=True)
    traded_id: str = Field(unique=True, index=True, max_length=50)
    stock_code: str = Field(index=True, max_length=10)
    traded_time: datetime
    traded_price: Decimal = Field(max_digits=20, decimal_places=2)
    traded_volume: int
    traded_amount: Decimal = Field(max_digits=20, decimal_places=2)
    strategy_name: str = Field(max_length=50)
    order_remark: Optional[str] = Field(default=None, max_length=100)
    direction: int
    offset_flag: int
    created_at: datetime
    updated_at: datetime
