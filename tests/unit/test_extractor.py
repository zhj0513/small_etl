"""Unit tests for ExtractorService."""

from datetime import datetime
from unittest.mock import MagicMock

import polars as pl
import pytest
from omegaconf import OmegaConf

from small_etl.services.extractor import ExtractorService


class TestExtractorService:
    """Tests for ExtractorService."""

    @pytest.fixture
    def mock_s3_connector(self):
        """Create a mock S3 connector."""
        return MagicMock()

    @pytest.fixture
    def mock_duckdb_client(self):
        """Create a mock DuckDB client."""
        return MagicMock()

    @pytest.fixture
    def extractor_no_config(self, mock_s3_connector, mock_duckdb_client):
        """Create ExtractorService without config."""
        return ExtractorService(mock_s3_connector, mock_duckdb_client, config=None)

    @pytest.fixture
    def assets_config(self):
        """Create sample assets config."""
        return OmegaConf.create(
            {
                "assets": {
                    "columns": [
                        {"csv_name": "id", "name": "id", "dtype": "Int64"},
                        {"csv_name": "account_id", "name": "account_id", "dtype": "Utf8"},
                        {"csv_name": "account_type", "name": "account_type", "dtype": "Int32"},
                        {"csv_name": "cash", "name": "cash", "dtype": "Float64"},
                        {"csv_name": "frozen_cash", "name": "frozen_cash", "dtype": "Float64"},
                        {"csv_name": "market_value", "name": "market_value", "dtype": "Float64"},
                        {"csv_name": "total_asset", "name": "total_asset", "dtype": "Float64"},
                        {
                            "csv_name": "updated_at",
                            "name": "updated_at",
                            "dtype": "Datetime",
                            "format": "%Y-%m-%d %H:%M:%S",
                        },
                    ]
                }
            }
        )

    @pytest.fixture
    def trades_config(self):
        """Create sample trades config."""
        return OmegaConf.create(
            {
                "trades": {
                    "columns": [
                        {"csv_name": "id", "name": "id", "dtype": "Int64"},
                        {"csv_name": "account_id", "name": "account_id", "dtype": "Utf8"},
                        {"csv_name": "account_type", "name": "account_type", "dtype": "Int32"},
                        {"csv_name": "traded_id", "name": "traded_id", "dtype": "Utf8"},
                        {"csv_name": "stock_code", "name": "stock_code", "dtype": "Utf8"},
                        {
                            "csv_name": "traded_time",
                            "name": "traded_time",
                            "dtype": "Datetime",
                            "format": "%Y-%m-%d %H:%M:%S",
                        },
                        {"csv_name": "traded_price", "name": "traded_price", "dtype": "Float64"},
                        {"csv_name": "traded_volume", "name": "traded_volume", "dtype": "Int64"},
                        {"csv_name": "traded_amount", "name": "traded_amount", "dtype": "Float64"},
                        {"csv_name": "strategy_name", "name": "strategy_name", "dtype": "Utf8"},
                        {"csv_name": "order_remark", "name": "order_remark", "dtype": "Utf8"},
                        {"csv_name": "direction", "name": "direction", "dtype": "Int32"},
                        {"csv_name": "offset_flag", "name": "offset_flag", "dtype": "Int32"},
                        {
                            "csv_name": "created_at",
                            "name": "created_at",
                            "dtype": "Datetime",
                            "format": "%Y-%m-%d %H:%M:%S",
                        },
                        {
                            "csv_name": "updated_at",
                            "name": "updated_at",
                            "dtype": "Datetime",
                            "format": "%Y-%m-%d %H:%M:%S",
                        },
                    ]
                }
            }
        )

    def test_init(self, mock_s3_connector, mock_duckdb_client):
        """Test ExtractorService initialization."""
        extractor = ExtractorService(mock_s3_connector, mock_duckdb_client)
        assert extractor._s3 == mock_s3_connector
        assert extractor._duckdb == mock_duckdb_client
        assert extractor._config is None

    def test_get_polars_dtype_known_types(self, extractor_no_config):
        """Test _get_polars_dtype with known types."""
        assert extractor_no_config._get_polars_dtype("Int32") == pl.Int32()
        assert extractor_no_config._get_polars_dtype("Int64") == pl.Int64()
        assert extractor_no_config._get_polars_dtype("Utf8") == pl.Utf8()
        assert extractor_no_config._get_polars_dtype("Float64") == pl.Float64()
        assert extractor_no_config._get_polars_dtype("Datetime") == pl.Datetime()

    def test_get_polars_dtype_unknown_type(self, extractor_no_config):
        """Test _get_polars_dtype with unknown type defaults to Utf8."""
        assert extractor_no_config._get_polars_dtype("UnknownType") == pl.Utf8()

    def test_extract_assets_with_duckdb_fallback(self, mock_s3_connector, mock_duckdb_client):
        """Test extract_assets using DuckDB fallback (no config)."""
        csv_content = b"id,account_id,account_type,cash,frozen_cash,market_value,total_asset,updated_at\n1,10000000001,1,100000.00,5000.00,200000.00,305000.00,2025-12-22 14:30:00"
        mock_s3_connector.download_csv.return_value = csv_content

        expected_df = pl.DataFrame(
            {
                "id": [1],
                "account_id": ["10000000001"],
                "account_type": [1],
                "cash": [100000.00],
                "frozen_cash": [5000.00],
                "market_value": [200000.00],
                "total_asset": [305000.00],
                "updated_at": [datetime(2025, 12, 22, 14, 30, 0)],
            }
        )
        mock_duckdb_client.query.return_value = expected_df

        extractor = ExtractorService(mock_s3_connector, mock_duckdb_client, config=None)
        result = extractor.extract_assets("test-bucket", "assets.csv")

        mock_s3_connector.download_csv.assert_called_once_with("test-bucket", "assets.csv")
        mock_duckdb_client.load_csv_bytes.assert_called_once()
        mock_duckdb_client.query.assert_called_once()
        assert len(result) == 1

    def test_extract_assets_with_config(self, mock_s3_connector, mock_duckdb_client, assets_config):
        """Test extract_assets using config-driven transformation."""
        csv_content = b"id,account_id,account_type,cash,frozen_cash,market_value,total_asset,updated_at\n1,10000000001,1,100000.00,5000.00,200000.00,305000.00,2025-12-22 14:30:00"
        mock_s3_connector.download_csv.return_value = csv_content

        extractor = ExtractorService(mock_s3_connector, mock_duckdb_client, config=assets_config)
        result = extractor.extract_assets("test-bucket", "assets.csv")

        mock_s3_connector.download_csv.assert_called_once_with("test-bucket", "assets.csv")
        # DuckDB should NOT be called when config is used
        mock_duckdb_client.load_csv_bytes.assert_not_called()
        assert len(result) == 1
        assert "account_id" in result.columns

    def test_extract_trades_with_duckdb_fallback(self, mock_s3_connector, mock_duckdb_client):
        """Test extract_trades using DuckDB fallback (no config)."""
        csv_content = "id,account_id,account_type,traded_id,stock_code,traded_time,traded_price,traded_volume,traded_amount,strategy_name,order_remark,direction,offset_flag,created_at,updated_at\n1,10000000001,1,T001,600000,2025-12-22 10:30:00,15.50,1000,15500.00,策略A,正常交易,0,48,2025-12-22 10:30:00,2025-12-22 14:30:00".encode("utf-8")
        mock_s3_connector.download_csv.return_value = csv_content

        expected_df = pl.DataFrame(
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
        mock_duckdb_client.query.return_value = expected_df

        extractor = ExtractorService(mock_s3_connector, mock_duckdb_client, config=None)
        result = extractor.extract_trades("test-bucket", "trades.csv")

        mock_s3_connector.download_csv.assert_called_once_with("test-bucket", "trades.csv")
        mock_duckdb_client.load_csv_bytes.assert_called_once()
        mock_duckdb_client.query.assert_called_once()
        assert len(result) == 1

    def test_extract_trades_with_config(self, mock_s3_connector, mock_duckdb_client, trades_config):
        """Test extract_trades using config-driven transformation."""
        csv_content = "id,account_id,account_type,traded_id,stock_code,traded_time,traded_price,traded_volume,traded_amount,strategy_name,order_remark,direction,offset_flag,created_at,updated_at\n1,10000000001,1,T001,600000,2025-12-22 10:30:00,15.50,1000,15500.00,策略A,正常交易,0,48,2025-12-22 10:30:00,2025-12-22 14:30:00".encode("utf-8")
        mock_s3_connector.download_csv.return_value = csv_content

        extractor = ExtractorService(mock_s3_connector, mock_duckdb_client, config=trades_config)
        result = extractor.extract_trades("test-bucket", "trades.csv")

        mock_s3_connector.download_csv.assert_called_once_with("test-bucket", "trades.csv")
        # DuckDB should NOT be called when config is used
        mock_duckdb_client.load_csv_bytes.assert_not_called()
        assert len(result) == 1
        assert "traded_id" in result.columns

    def test_transform_dataframe_with_missing_column(self, mock_s3_connector, mock_duckdb_client):
        """Test _transform_dataframe skips missing columns."""
        config = OmegaConf.create(
            {
                "assets": {
                    "columns": [
                        {"csv_name": "account_id", "name": "account_id", "dtype": "Utf8"},
                        {"csv_name": "nonexistent", "name": "nonexistent", "dtype": "Utf8"},  # Missing column
                    ]
                }
            }
        )

        extractor = ExtractorService(mock_s3_connector, mock_duckdb_client, config=config)

        df = pl.DataFrame({"account_id": ["10000000001"]})
        result = extractor._transform_dataframe(df, config.assets.columns)

        assert "account_id" in result.columns
        assert "nonexistent" not in result.columns

    def test_transform_dataframe_all_dtypes(self, mock_s3_connector, mock_duckdb_client):
        """Test _transform_dataframe with all supported dtypes."""
        config = OmegaConf.create(
            {
                "test": {
                    "columns": [
                        {"csv_name": "int32_col", "name": "int32_col", "dtype": "Int32"},
                        {"csv_name": "int64_col", "name": "int64_col", "dtype": "Int64"},
                        {"csv_name": "utf8_col", "name": "utf8_col", "dtype": "Utf8"},
                        {"csv_name": "float64_col", "name": "float64_col", "dtype": "Float64"},
                        {"csv_name": "datetime_col", "name": "datetime_col", "dtype": "Datetime", "format": "%Y-%m-%d %H:%M:%S"},
                        {"csv_name": "unknown_col", "name": "unknown_col", "dtype": "Unknown"},  # Unknown type
                    ]
                }
            }
        )

        extractor = ExtractorService(mock_s3_connector, mock_duckdb_client, config=config)

        df = pl.DataFrame(
            {
                "int32_col": ["1", "2"],
                "int64_col": ["100", "200"],
                "utf8_col": ["a", "b"],
                "float64_col": ["1.5", "2.5"],
                "datetime_col": ["2025-12-22 14:30:00", "2025-12-23 15:00:00"],
                "unknown_col": ["test", "test2"],
            }
        )
        result = extractor._transform_dataframe(df, config.test.columns)

        assert result["int32_col"].dtype == pl.Int32
        assert result["int64_col"].dtype == pl.Int64
        assert result["utf8_col"].dtype == pl.Utf8
        assert result["float64_col"].dtype == pl.Float64
        assert result["datetime_col"].dtype == pl.Datetime
        # Unknown type should use alias (pass through)
        assert "unknown_col" in result.columns
