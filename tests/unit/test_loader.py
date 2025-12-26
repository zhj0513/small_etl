"""Tests for loader service."""

from unittest.mock import MagicMock

import pytest

from small_etl.services.loader import LoaderService, LoadResult


class TestLoaderService:
    """Tests for LoaderService."""

    @pytest.fixture
    def mock_repository(self):
        """Create a mock repository."""
        return MagicMock()

    @pytest.fixture
    def mock_duckdb(self):
        """Create a mock DuckDB client."""
        mock = MagicMock()
        mock.upsert_from_table.return_value = 5
        return mock

    @pytest.fixture
    def loader(self, mock_repository, mock_duckdb):
        """Create a LoaderService with mock repository and DuckDB client."""
        return LoaderService(
            repository=mock_repository,
            duckdb_client=mock_duckdb,
            database_url="postgresql://test:test@localhost/test",
        )

    def test_load_assets_success(self, loader, mock_duckdb):
        """Test loading assets from DuckDB table."""
        mock_duckdb.upsert_from_table.return_value = 10

        result = loader.load("_transformed_asset", "asset")

        assert result.success is True
        assert result.total_rows == 10
        assert result.loaded_count == 10
        mock_duckdb.upsert_from_table.assert_called_once_with(
            source_table="_transformed_asset",
            pg_table="asset",
            conflict_column="account_id",
            columns=["account_id", "account_type", "cash", "frozen_cash", "market_value", "total_asset", "updated_at"],
        )

    def test_load_trades_success(self, loader, mock_duckdb):
        """Test loading trades from DuckDB table."""
        mock_duckdb.upsert_from_table.return_value = 20

        result = loader.load("_transformed_trade", "trade")

        assert result.success is True
        assert result.total_rows == 20
        assert result.loaded_count == 20
        mock_duckdb.upsert_from_table.assert_called_once()

    def test_load_assets_error(self, loader, mock_duckdb):
        """Test loading assets with error."""
        mock_duckdb.upsert_from_table.side_effect = Exception("Database error")

        result = loader.load("_transformed_asset", "asset")

        assert result.success is False
        assert result.error_message == "Database error"

    def test_load_trades_error(self, loader, mock_duckdb):
        """Test loading trades with error."""
        mock_duckdb.upsert_from_table.side_effect = Exception("Database error")

        result = loader.load("_transformed_trade", "trade")

        assert result.success is False
        assert result.error_message == "Database error"

    def test_load_without_duckdb_returns_error(self, mock_repository):
        """Test loading without DuckDB client returns error result."""
        loader = LoaderService(repository=mock_repository)

        result = loader.load("_transformed_asset", "asset")

        assert result.success is False
        assert "DuckDB client and database_url required" in result.error_message

    def test_load_without_database_url_returns_error(self, mock_repository, mock_duckdb):
        """Test loading without database_url returns error result."""
        loader = LoaderService(repository=mock_repository, duckdb_client=mock_duckdb)

        result = loader.load("_transformed_asset", "asset")

        assert result.success is False
        assert "DuckDB client and database_url required" in result.error_message


class TestLoadResult:
    """Tests for LoadResult dataclass."""

    def test_load_result_defaults(self):
        """Test LoadResult default values."""
        result = LoadResult(success=True, total_rows=100, loaded_count=100)

        assert result.failed_count == 0
        assert result.error_message is None

    def test_load_result_with_error(self):
        """Test LoadResult with error."""
        result = LoadResult(
            success=False,
            total_rows=100,
            loaded_count=50,
            failed_count=50,
            error_message="Database connection failed",
        )

        assert result.success is False
        assert result.failed_count == 50
        assert result.error_message == "Database connection failed"
