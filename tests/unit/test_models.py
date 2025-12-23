"""Unit tests for domain models."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from small_etl.domain.enums import AccountType, Direction, OffsetFlag, VALID_ACCOUNT_TYPES, VALID_DIRECTIONS, VALID_OFFSET_FLAGS
from small_etl.domain.models import Asset, Trade


class TestEnums:
    """Tests for enum definitions."""

    def test_account_type_values(self) -> None:
        """Test AccountType enum has correct values."""
        assert AccountType.FUTURE == 1
        assert AccountType.SECURITY == 2
        assert AccountType.CREDIT == 3
        assert AccountType.FUTURE_OPTION == 5
        assert AccountType.STOCK_OPTION == 6
        assert AccountType.HUGANGTONG == 7
        assert AccountType.SHENGANGTONG == 11

    def test_offset_flag_values(self) -> None:
        """Test OffsetFlag enum has correct ASCII values."""
        assert OffsetFlag.OPEN == 48
        assert OffsetFlag.CLOSE == 49
        assert OffsetFlag.FORCECLOSE == 50

    def test_direction_values(self) -> None:
        """Test Direction enum has correct values."""
        assert Direction.NA == 0
        assert Direction.LONG == 48
        assert Direction.SHORT == 49

    def test_valid_sets_contain_all_values(self) -> None:
        """Test that valid value sets contain all enum members."""
        assert len(VALID_ACCOUNT_TYPES) == len(AccountType)
        assert len(VALID_OFFSET_FLAGS) == len(OffsetFlag)
        assert len(VALID_DIRECTIONS) == len(Direction)


class TestAssetModel:
    """Tests for Asset model."""

    def test_asset_creation(self) -> None:
        """Test creating an Asset instance."""
        asset = Asset(
            account_id="10000000001",
            account_type=AccountType.SECURITY,
            cash=Decimal("100000.00"),
            frozen_cash=Decimal("5000.00"),
            market_value=Decimal("200000.00"),
            total_asset=Decimal("305000.00"),
            updated_at=datetime(2025, 12, 22, 14, 30, 0),
        )

        assert asset.account_id == "10000000001"
        assert asset.account_type == 2
        assert asset.cash == Decimal("100000.00")
        assert asset.frozen_cash == Decimal("5000.00")
        assert asset.market_value == Decimal("200000.00")
        assert asset.total_asset == Decimal("305000.00")
        assert asset.id is None

    def test_asset_total_calculation(self) -> None:
        """Test that total_asset equals sum of components."""
        cash = Decimal("100000.00")
        frozen = Decimal("5000.00")
        market = Decimal("200000.00")
        total = cash + frozen + market

        asset = Asset(
            account_id="10000000001",
            account_type=AccountType.SECURITY,
            cash=cash,
            frozen_cash=frozen,
            market_value=market,
            total_asset=total,
            updated_at=datetime.now(UTC),
        )

        assert asset.total_asset == total


class TestTradeModel:
    """Tests for Trade model."""

    def test_trade_creation(self) -> None:
        """Test creating a Trade instance."""
        trade = Trade(
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
        )

        assert trade.account_id == "10000000001"
        assert trade.traded_id == "T20251222000001"
        assert trade.traded_price == Decimal("15.50")
        assert trade.traded_volume == 1000
        assert trade.traded_amount == Decimal("15500.00")
        assert trade.direction == 0
        assert trade.offset_flag == 48
        assert trade.id is None

    def test_trade_amount_calculation(self) -> None:
        """Test that traded_amount equals price * volume."""
        price = Decimal("15.50")
        volume = 1000
        amount = price * volume

        trade = Trade(
            account_id="10000000001",
            account_type=AccountType.SECURITY,
            traded_id="T20251222000001",
            stock_code="600000",
            traded_time=datetime.now(UTC),
            traded_price=price,
            traded_volume=volume,
            traded_amount=amount,
            strategy_name="策略A",
            direction=Direction.NA,
            offset_flag=OffsetFlag.OPEN,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )

        assert trade.traded_amount == amount

    def test_trade_optional_order_remark(self) -> None:
        """Test that order_remark is optional."""
        trade = Trade(
            account_id="10000000001",
            account_type=AccountType.SECURITY,
            traded_id="T20251222000001",
            stock_code="600000",
            traded_time=datetime.now(UTC),
            traded_price=Decimal("15.50"),
            traded_volume=1000,
            traded_amount=Decimal("15500.00"),
            strategy_name="策略A",
            direction=Direction.NA,
            offset_flag=OffsetFlag.OPEN,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )

        assert trade.order_remark is None
