"""Test fixtures and configuration for Small ETL."""

from datetime import datetime
from decimal import Decimal

import polars as pl
import pytest

from small_etl.domain.enums import AccountType, Direction, OffsetFlag
from small_etl.domain.models import Asset, Trade


@pytest.fixture
def sample_asset_data() -> pl.DataFrame:
    """Sample valid asset data as Polars DataFrame."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3],
            "account_id": ["10000000001", "10000000002", "10000000003"],
            "account_type": [AccountType.SECURITY, AccountType.SECURITY, AccountType.CREDIT],
            "cash": [100000.00, 500000.00, 250000.00],
            "frozen_cash": [5000.00, 50000.00, 75000.00],
            "market_value": [200000.00, 800000.00, 300000.00],
            "total_asset": [305000.00, 1350000.00, 625000.00],
            "updated_at": [
                datetime(2025, 12, 22, 14, 30, 0),
                datetime(2025, 12, 22, 14, 25, 0),
                datetime(2025, 12, 22, 14, 20, 0),
            ],
        }
    )


@pytest.fixture
def sample_trade_data() -> pl.DataFrame:
    """Sample valid trade data as Polars DataFrame."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3],
            "account_id": ["10000000001", "10000000002", "10000000001"],
            "account_type": [AccountType.SECURITY, AccountType.SECURITY, AccountType.CREDIT],
            "traded_id": ["T20251222000001", "T20251222000002", "T20251222000003"],
            "stock_code": ["600000", "000001", "600036"],
            "traded_time": [
                datetime(2025, 12, 22, 10, 30, 0),
                datetime(2025, 12, 22, 11, 15, 0),
                datetime(2025, 12, 22, 13, 45, 0),
            ],
            "traded_price": [15.50, 28.30, 42.80],
            "traded_volume": [1000, 500, 2000],
            "traded_amount": [15500.00, 14150.00, 85600.00],
            "strategy_name": ["策略A", "量化1号", "趋势跟踪"],
            "order_remark": ["正常交易", "建仓", "止盈"],
            "direction": [Direction.NA, Direction.NA, Direction.NA],
            "offset_flag": [OffsetFlag.OPEN, OffsetFlag.OPEN, OffsetFlag.CLOSE],
            "created_at": [
                datetime(2025, 12, 22, 10, 30, 0),
                datetime(2025, 12, 22, 11, 15, 0),
                datetime(2025, 12, 22, 13, 45, 0),
            ],
            "updated_at": [
                datetime(2025, 12, 22, 14, 30, 0),
                datetime(2025, 12, 22, 14, 25, 0),
                datetime(2025, 12, 22, 14, 20, 0),
            ],
        }
    )


@pytest.fixture
def invalid_asset_data() -> pl.DataFrame:
    """Sample invalid asset data for testing validation."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3],
            "account_id": ["10000000001", "", "10000000003"],  # Empty account_id
            "account_type": [AccountType.SECURITY, 99, AccountType.CREDIT],  # Invalid account_type
            "cash": [100000.00, -500.00, 250000.00],  # Negative cash
            "frozen_cash": [5000.00, 50000.00, 75000.00],
            "market_value": [200000.00, 800000.00, 300000.00],
            "total_asset": [305000.00, 1350000.00, 999999.00],  # Wrong total
            "updated_at": [
                datetime(2025, 12, 22, 14, 30, 0),
                datetime(2025, 12, 22, 14, 25, 0),
                datetime(2025, 12, 22, 14, 20, 0),
            ],
        }
    )


@pytest.fixture
def invalid_trade_data() -> pl.DataFrame:
    """Sample invalid trade data for testing validation."""
    return pl.DataFrame(
        {
            "id": [1, 2],
            "account_id": ["10000000001", "10000000002"],
            "account_type": [AccountType.SECURITY, 99],  # Invalid account_type
            "traded_id": ["T20251222000001", ""],  # Empty traded_id
            "stock_code": ["600000", "000001"],
            "traded_time": [
                datetime(2025, 12, 22, 10, 30, 0),
                datetime(2025, 12, 22, 11, 15, 0),
            ],
            "traded_price": [15.50, 0.00],  # Zero price
            "traded_volume": [1000, -100],  # Negative volume
            "traded_amount": [15500.00, 999.00],  # Wrong amount
            "strategy_name": ["策略A", "量化1号"],
            "order_remark": ["正常交易", "建仓"],
            "direction": [Direction.NA, Direction.NA],
            "offset_flag": [OffsetFlag.OPEN, 99],  # Invalid offset_flag
            "created_at": [
                datetime(2025, 12, 22, 10, 30, 0),
                datetime(2025, 12, 22, 11, 15, 0),
            ],
            "updated_at": [
                datetime(2025, 12, 22, 14, 30, 0),
                datetime(2025, 12, 22, 14, 25, 0),
            ],
        }
    )


@pytest.fixture
def sample_assets() -> list[Asset]:
    """Sample Asset model instances."""
    return [
        Asset(
            account_id="10000000001",
            account_type=AccountType.SECURITY,
            cash=Decimal("100000.00"),
            frozen_cash=Decimal("5000.00"),
            market_value=Decimal("200000.00"),
            total_asset=Decimal("305000.00"),
            updated_at=datetime(2025, 12, 22, 14, 30, 0),
        ),
        Asset(
            account_id="10000000002",
            account_type=AccountType.SECURITY,
            cash=Decimal("500000.00"),
            frozen_cash=Decimal("50000.00"),
            market_value=Decimal("800000.00"),
            total_asset=Decimal("1350000.00"),
            updated_at=datetime(2025, 12, 22, 14, 25, 0),
        ),
    ]


@pytest.fixture
def sample_trades() -> list[Trade]:
    """Sample Trade model instances."""
    return [
        Trade(
            account_id="10000000001",
            account_type=AccountType.SECURITY,
            traded_id="T20251222000001",
            stock_code="600000",
            traded_time=datetime(2025, 12, 22, 10, 30, 0),
            traded_price=Decimal("15.50"),
            traded_volume=1000,
            traded_amount=Decimal("15500.00"),
            strategy_name="策略A",
            order_remark="正常交易",
            direction=Direction.NA,
            offset_flag=OffsetFlag.OPEN,
            created_at=datetime(2025, 12, 22, 10, 30, 0),
            updated_at=datetime(2025, 12, 22, 14, 30, 0),
        ),
    ]
