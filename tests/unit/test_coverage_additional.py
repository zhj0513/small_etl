"""Additional tests for s3_connector, analytics, and validator to improve coverage."""

from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from small_etl.data_access.s3_connector import S3Connector
from small_etl.services.analytics import AnalyticsService
from small_etl.services.validator import ValidatorService


class TestS3Connector:
    """Tests for S3Connector."""

    def test_download_csv_to_stream(self):
        """Test download_csv_to_stream returns BytesIO."""
        with patch("small_etl.data_access.s3_connector.Minio") as mock_minio_class:
            mock_client = MagicMock()
            mock_minio_class.return_value = mock_client

            mock_response = MagicMock()
            mock_response.read.return_value = b"col1,col2\nval1,val2"
            mock_client.get_object.return_value = mock_response

            connector = S3Connector("localhost:9000", "access", "secret")
            result = connector.download_csv_to_stream("bucket", "file.csv")

            assert result.read() == b"col1,col2\nval1,val2"

    def test_list_objects(self):
        """Test list_objects returns list of object names."""
        with patch("small_etl.data_access.s3_connector.Minio") as mock_minio_class:
            mock_client = MagicMock()
            mock_minio_class.return_value = mock_client

            mock_obj1 = MagicMock()
            mock_obj1.object_name = "file1.csv"
            mock_obj2 = MagicMock()
            mock_obj2.object_name = "file2.csv"
            mock_obj3 = MagicMock()
            mock_obj3.object_name = None  # Test filtering None

            mock_client.list_objects.return_value = [mock_obj1, mock_obj2, mock_obj3]

            connector = S3Connector("localhost:9000", "access", "secret")
            result = connector.list_objects("bucket", prefix="data/")

            assert result == ["file1.csv", "file2.csv"]
            mock_client.list_objects.assert_called_once_with("bucket", prefix="data/")

    def test_bucket_exists(self):
        """Test bucket_exists returns boolean."""
        with patch("small_etl.data_access.s3_connector.Minio") as mock_minio_class:
            mock_client = MagicMock()
            mock_minio_class.return_value = mock_client

            mock_client.bucket_exists.return_value = True

            connector = S3Connector("localhost:9000", "access", "secret")
            assert connector.bucket_exists("existing-bucket") is True

            mock_client.bucket_exists.return_value = False
            assert connector.bucket_exists("nonexistent") is False


class TestAnalyticsServiceDb:
    """Tests for AnalyticsService database methods."""

    def test_asset_statistics_from_db_no_client(self):
        """Test asset_statistics_from_db raises when no client configured."""
        service = AnalyticsService(repository=None, duckdb_client=None)

        with pytest.raises(ValueError, match="Neither DuckDB client nor repository"):
            service.asset_statistics_from_db()

    def test_trade_statistics_from_db_no_client(self):
        """Test trade_statistics_from_db raises when no client configured."""
        service = AnalyticsService(repository=None, duckdb_client=None)

        with pytest.raises(ValueError, match="Neither DuckDB client nor repository"):
            service.trade_statistics_from_db()

    def test_asset_statistics_from_db_with_repo(self):
        """Test asset_statistics_from_db with repository fallback."""
        mock_repo = MagicMock()
        mock_repo.get_all_assets.return_value = pl.DataFrame(
            {
                "account_id": ["001"],
                "account_type": [1],
                "cash": [100000.00],
                "frozen_cash": [5000.00],
                "market_value": [200000.00],
                "total_asset": [305000.00],
            }
        )

        service = AnalyticsService(repository=mock_repo, duckdb_client=None)
        result = service.asset_statistics_from_db()

        assert result.total_records == 1
        assert result.total_cash == Decimal("100000.0")

    def test_trade_statistics_from_db_with_repo(self):
        """Test trade_statistics_from_db with repository fallback."""
        mock_repo = MagicMock()
        mock_repo.get_all_trades.return_value = pl.DataFrame(
            {
                "account_id": ["001"],
                "account_type": [1],
                "traded_id": ["T001"],
                "traded_price": [15.50],
                "traded_volume": [1000],
                "traded_amount": [15500.00],
                "strategy_name": ["策略A"],
                "offset_flag": [48],
            }
        )

        service = AnalyticsService(repository=mock_repo, duckdb_client=None)
        result = service.trade_statistics_from_db()

        assert result.total_records == 1
        assert result.total_volume == 1000


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

        # All rows should be valid since account_id is in valid_account_ids
        assert result.valid_count == 1
        assert result.invalid_count == 0

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

        # Row should be invalid due to foreign key violation
        assert result.invalid_count == 1
        assert any("account_id not found" in e.message for e in result.errors)

    def test_validate_empty_dataframe(self):
        """Test validation of empty DataFrame."""
        service = ValidatorService()

        # Create empty assets DataFrame
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
        assert result.total_rows == 0
        assert result.valid_count == 0
        assert result.invalid_count == 0

    def test_validate_fk_empty_valid_rows(self):
        """Test _validate_foreign_keys with empty valid rows."""
        from small_etl.services.validator import ValidationResult

        service = ValidatorService()

        # Create empty result
        empty_df = pl.DataFrame(
            schema={
                "account_id": pl.Utf8,
                "traded_id": pl.Utf8,
            }
        )
        result = ValidationResult(
            is_valid=False,
            valid_rows=empty_df,
            invalid_rows=empty_df,
            errors=[],
            total_rows=0,
            valid_count=0,
            invalid_count=0,
        )

        # Should return unchanged result
        new_result = service._validate_foreign_keys(result, {"001", "002"})
        assert new_result == result
