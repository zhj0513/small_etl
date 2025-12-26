"""Additional tests for validator to improve coverage."""

from datetime import datetime

import polars as pl

from small_etl.services.validator import ValidatorService


class TestValidatorServiceEdgeCases:
    """Tests for ValidatorService edge cases."""

    def test_validate_trades_with_fk_all_valid(self):
        """Test validate_trades when all foreign keys are valid."""
        service = ValidatorService()

        df = pl.DataFrame(
            {
                "id": [1],
                "account_id": ["10000000001"],
                "account_type": [1],
                "traded_id": ["T001"],
                "stock_code": ["600000"],
                "traded_time": [datetime(2025, 12, 22, 10, 30, 0)],
                "traded_price": [15.50],
                "traded_volume": [1000],
                "traded_amount": [15500.00],
                "strategy_name": ["策略A"],
                "order_remark": ["正常交易"],
                "direction": [0],
                "offset_flag": [48],
                "created_at": [datetime(2025, 12, 22, 10, 30, 0)],
                "updated_at": [datetime(2025, 12, 22, 14, 30, 0)],
            }
        )

        valid_account_ids = {"10000000001", "10000000002"}
        result = service.validate_trades(df, valid_account_ids)

        assert result.is_valid is True
        assert len(result.data) == 1

    def test_validate_trades_with_fk_invalid(self):
        """Test validate_trades when foreign key is invalid."""
        service = ValidatorService()

        df = pl.DataFrame(
            {
                "id": [1],
                "account_id": ["99999999999"],  # Not in valid_account_ids
                "account_type": [1],
                "traded_id": ["T001"],
                "stock_code": ["600000"],
                "traded_time": [datetime(2025, 12, 22, 10, 30, 0)],
                "traded_price": [15.50],
                "traded_volume": [1000],
                "traded_amount": [15500.00],
                "strategy_name": ["策略A"],
                "order_remark": ["正常交易"],
                "direction": [0],
                "offset_flag": [48],
                "created_at": [datetime(2025, 12, 22, 10, 30, 0)],
                "updated_at": [datetime(2025, 12, 22, 14, 30, 0)],
            }
        )

        valid_account_ids = {"10000000001", "10000000002"}
        result = service.validate_trades(df, valid_account_ids)

        assert result.is_valid is False
        assert "account_id" in result.error_message

    def test_validate_empty_dataframe(self):
        """Test validation of empty DataFrame."""
        service = ValidatorService()

        df = pl.DataFrame(
            schema={
                "id": pl.Int64,
                "account_id": pl.Utf8,
                "account_type": pl.Int32,
                "cash": pl.Float64,
                "frozen_cash": pl.Float64,
                "market_value": pl.Float64,
                "total_asset": pl.Float64,
                "updated_at": pl.Datetime,
            }
        )

        result = service.validate_assets(df)

        assert result.is_valid is True
        assert len(result.data) == 0

    def test_validate_trades_without_fk_validation(self):
        """Test validate_trades without foreign key set."""
        service = ValidatorService()

        df = pl.DataFrame(
            {
                "id": [1],
                "account_id": ["10000000001"],
                "account_type": [1],
                "traded_id": ["T001"],
                "stock_code": ["600000"],
                "traded_time": [datetime(2025, 12, 22, 10, 30, 0)],
                "traded_price": [15.50],
                "traded_volume": [1000],
                "traded_amount": [15500.00],
                "strategy_name": ["策略A"],
                "order_remark": ["正常交易"],
                "direction": [0],
                "offset_flag": [48],
                "created_at": [datetime(2025, 12, 22, 10, 30, 0)],
                "updated_at": [datetime(2025, 12, 22, 14, 30, 0)],
            }
        )

        # No FK validation when valid_account_ids is None
        result = service.validate_trades(df, None)

        assert result.is_valid is True
        assert len(result.data) == 1
