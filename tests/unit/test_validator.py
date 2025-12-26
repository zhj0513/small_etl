"""Unit tests for validator service."""

from datetime import datetime

import polars as pl
import pytest

from small_etl.domain.enums import AccountType, Direction, OffsetFlag
from small_etl.services.validator import ValidatorService


class TestValidatorService:
    """Tests for ValidatorService."""

    @pytest.fixture
    def validator(self) -> ValidatorService:
        """Create validator instance."""
        return ValidatorService(tolerance=0.01)

    def test_validate_valid_assets(self, validator: ValidatorService, sample_asset_data: pl.DataFrame) -> None:
        """Test validation of valid asset data."""
        result = validator.validate_assets(sample_asset_data)

        assert result.is_valid is True
        assert result.error_message is None
        assert len(result.data) == 3

    def test_validate_invalid_assets(self, validator: ValidatorService, invalid_asset_data: pl.DataFrame) -> None:
        """Test validation catches invalid asset data."""
        result = validator.validate_assets(invalid_asset_data)

        assert result.is_valid is False
        assert result.error_message is not None
        assert len(result.data) == 0

    def test_validate_asset_total_mismatch(self, validator: ValidatorService) -> None:
        """Test validation catches total_asset calculation mismatch."""
        df = pl.DataFrame(
            {
                "id": [1],
                "account_id": ["10000000001"],
                "account_type": [AccountType.SECURITY],
                "cash": [100000.00],
                "frozen_cash": [5000.00],
                "market_value": [200000.00],
                "total_asset": [999999.00],  # Wrong total
                "updated_at": [datetime(2025, 12, 22, 14, 30, 0)],
            }
        )

        result = validator.validate_assets(df)

        assert result.is_valid is False
        assert result.error_message is not None

    def test_validate_asset_within_tolerance(self, validator: ValidatorService) -> None:
        """Test validation allows total_asset within tolerance."""
        df = pl.DataFrame(
            {
                "id": [1],
                "account_id": ["10000000001"],
                "account_type": [AccountType.SECURITY],
                "cash": [100000.00],
                "frozen_cash": [5000.00],
                "market_value": [200000.00],
                "total_asset": [305000.005],  # Within 0.01 tolerance
                "updated_at": [datetime(2025, 12, 22, 14, 30, 0)],
            }
        )

        result = validator.validate_assets(df)

        assert result.is_valid is True

    def test_validate_valid_trades(self, validator: ValidatorService, sample_trade_data: pl.DataFrame) -> None:
        """Test validation of valid trade data."""
        valid_account_ids = {"10000000001", "10000000002", "10000000003"}
        result = validator.validate_trades(sample_trade_data, valid_account_ids)

        assert result.is_valid is True
        assert len(result.data) == 3

    def test_validate_invalid_trades(self, validator: ValidatorService, invalid_trade_data: pl.DataFrame) -> None:
        """Test validation catches invalid trade data."""
        valid_account_ids = {"10000000001", "10000000002"}
        result = validator.validate_trades(invalid_trade_data, valid_account_ids)

        assert result.is_valid is False
        assert result.error_message is not None

    def test_validate_trade_foreign_key(self, validator: ValidatorService, sample_trade_data: pl.DataFrame) -> None:
        """Test validation catches missing account_id reference."""
        valid_account_ids = {"10000000001"}  # Missing 10000000002
        result = validator.validate_trades(sample_trade_data, valid_account_ids)

        assert result.is_valid is False
        assert "account_id" in result.error_message

    def test_validate_trade_amount_mismatch(self, validator: ValidatorService) -> None:
        """Test validation catches traded_amount calculation mismatch."""
        df = pl.DataFrame(
            {
                "id": [1],
                "account_id": ["10000000001"],
                "account_type": [AccountType.SECURITY],
                "traded_id": ["T20251222000001"],
                "stock_code": ["600000"],
                "traded_time": [datetime(2025, 12, 22, 10, 30, 0)],
                "traded_price": [15.50],
                "traded_volume": [1000],
                "traded_amount": [99999.00],  # Wrong amount
                "strategy_name": ["策略A"],
                "order_remark": ["正常交易"],
                "direction": [Direction.NA],
                "offset_flag": [OffsetFlag.OPEN],
                "created_at": [datetime(2025, 12, 22, 10, 30, 0)],
                "updated_at": [datetime(2025, 12, 22, 14, 30, 0)],
            }
        )

        valid_account_ids = {"10000000001"}
        result = validator.validate_trades(df, valid_account_ids)

        assert result.is_valid is False
        assert result.error_message is not None

    def test_validate_empty_dataframe(self, validator: ValidatorService) -> None:
        """Test validation of empty DataFrame."""
        empty_df = pl.DataFrame(
            {
                "id": [],
                "account_id": [],
                "account_type": [],
                "cash": [],
                "frozen_cash": [],
                "market_value": [],
                "total_asset": [],
                "updated_at": [],
            }
        ).cast(
            {
                "id": pl.Int64,
                "account_id": pl.Utf8,
                "account_type": pl.Int64,
                "cash": pl.Float64,
                "frozen_cash": pl.Float64,
                "market_value": pl.Float64,
                "total_asset": pl.Float64,
                "updated_at": pl.Datetime,
            }
        )

        result = validator.validate_assets(empty_df)

        assert result.is_valid is True
        assert len(result.data) == 0
