"""Integration tests for the full ETL pipeline.

Tests the complete flow: S3 CSV -> Extract -> Validate -> Load -> PostgreSQL

Prerequisites:
- PostgreSQL running at localhost:15432
- MinIO running at localhost:19000
- Test CSV files uploaded to MinIO bucket
"""

import os

import pytest
from omegaconf import OmegaConf

from small_etl.application.pipeline import ETLPipeline
from small_etl.data_access.postgres_repository import PostgresRepository

# Test configuration
TEST_DB_HOST = os.getenv("DB_HOST", "localhost")
TEST_DB_PORT = int(os.getenv("DB_PORT", "15432"))
TEST_DB_USER = os.getenv("DB_USER", "etl")
TEST_DB_PASSWORD = os.getenv("DB_PASSWORD", "etlpass")
TEST_DB_NAME = os.getenv("DB_NAME", "etl_test_db")

TEST_DATABASE_URL = (
    f"postgresql://{TEST_DB_USER}:{TEST_DB_PASSWORD}"
    f"@{TEST_DB_HOST}:{TEST_DB_PORT}/{TEST_DB_NAME}"
)

# S3/MinIO configuration
TEST_S3_ENDPOINT = os.getenv("S3_ENDPOINT", "localhost:19000")
TEST_S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
TEST_S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin123")
TEST_S3_BUCKET = os.getenv("S3_BUCKET", "fake-data-for-training")


@pytest.fixture(scope="module")
def integration_config():
    """Create integration test configuration."""
    return OmegaConf.create(
        {
            "db": {
                "url": TEST_DATABASE_URL,
                "echo": False,
            },
            "s3": {
                "endpoint": TEST_S3_ENDPOINT,
                "access_key": TEST_S3_ACCESS_KEY,
                "secret_key": TEST_S3_SECRET_KEY,
                "secure": False,
                "bucket": TEST_S3_BUCKET,
                "assets_file": "account_assets/account_assets.csv",
                "trades_file": "trades/trades.csv",
            },
            "etl": {
                "batch_size": 100,
                "validation": {"tolerance": 0.01},
            },
        }
    )


@pytest.fixture(scope="module")
def postgres_repository():
    """Create PostgreSQL repository for integration tests."""
    repo = PostgresRepository(TEST_DATABASE_URL, echo=False)
    repo.create_tables()
    yield repo
    repo.close()


@pytest.fixture
def clean_database(postgres_repository: PostgresRepository):
    """Clean database before and after each test."""
    postgres_repository.truncate_tables()
    yield postgres_repository
    postgres_repository.truncate_tables()


class TestFullPipelineIntegration:
    """Integration tests for the complete ETL pipeline.

    Tests the full flow: S3 CSV -> Extract -> Validate -> Load -> PostgreSQL
    """

    def test_full_pipeline_run(
        self, integration_config, clean_database: PostgresRepository
    ) -> None:
        """Test complete pipeline execution from S3 to PostgreSQL."""
        with ETLPipeline(integration_config) as pipeline:
            result = pipeline.run()

        assert result.success is True, f"Pipeline failed: {result.error_message}"
        assert result.error_message is None

        # Verify assets loaded
        assert result.assets_validation is not None
        assert result.assets_load is not None
        assert result.assets_load.success is True
        assert result.assets_load.loaded_count > 0

        # Verify trades loaded
        assert result.trades_validation is not None
        assert result.trades_load is not None
        assert result.trades_load.success is True

        # Verify statistics computed
        assert result.assets_stats is not None
        assert result.trades_stats is not None

        # Verify data in database
        assert clean_database.get_asset_count() > 0
        assert clean_database.get_trade_count() > 0

    def test_assets_only_pipeline(
        self, integration_config, clean_database: PostgresRepository
    ) -> None:
        """Test assets-only pipeline execution."""
        with ETLPipeline(integration_config) as pipeline:
            result = pipeline.run_assets_only()

        assert result.success is True, f"Pipeline failed: {result.error_message}"
        assert result.assets_load is not None
        assert result.assets_load.success is True
        assert result.trades_load is None

        # Verify only assets in database
        assert clean_database.get_asset_count() > 0

    def test_trades_only_pipeline_after_assets(
        self, integration_config, clean_database: PostgresRepository
    ) -> None:
        """Test trades-only pipeline after assets are loaded."""
        # First load assets
        with ETLPipeline(integration_config) as pipeline:
            assets_result = pipeline.run_assets_only()
            assert assets_result.success is True

        # Then load trades in new pipeline instance
        with ETLPipeline(integration_config) as pipeline:
            trades_result = pipeline.run_trades_only()

        assert trades_result.success is True, f"Pipeline failed: {trades_result.error_message}"
        assert trades_result.trades_load is not None
        assert trades_result.trades_load.success is True

    def test_trades_only_fails_without_assets(
        self, integration_config, clean_database: PostgresRepository
    ) -> None:
        """Test trades-only pipeline fails when no assets exist."""
        with ETLPipeline(integration_config) as pipeline:
            result = pipeline.run_trades_only()

        assert result.success is False
        assert result.error_message is not None
        assert "No accounts found" in result.error_message

    def test_pipeline_idempotency(
        self, integration_config, clean_database: PostgresRepository
    ) -> None:
        """Test running pipeline twice produces same result (upsert behavior)."""
        # First run
        with ETLPipeline(integration_config) as pipeline:
            result1 = pipeline.run()
        assert result1.success is True
        count_after_first = clean_database.get_asset_count()

        # Second run (should upsert, not duplicate)
        with ETLPipeline(integration_config) as pipeline:
            result2 = pipeline.run()
        assert result2.success is True
        count_after_second = clean_database.get_asset_count()

        # Should have same count (upsert, not insert)
        assert count_after_first == count_after_second
