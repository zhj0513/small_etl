"""Validator service for reading S3 CSV and validating data using Pandera schemas."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pandera.errors
import polars as pl

from small_etl.domain.registry import DataTypeRegistry

if TYPE_CHECKING:
    from omegaconf import DictConfig

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of data validation.

    Attributes:
        is_valid: True if validation passed.
        data: Validated DataFrame (empty if validation failed).
        error_message: Error description if validation failed.
    """

    is_valid: bool
    data: pl.DataFrame
    error_message: str | None = None


class ValidatorService:
    """Service for reading S3 CSV and validating data using Pandera schemas.

    Args:
        s3_config: S3 configuration for reading CSV files.
    """

    def __init__(self, s3_config: "DictConfig | None" = None) -> None:
        self._s3_config = s3_config
        self._storage_options: dict[str, Any] | None = None

        if s3_config is not None:
            secure = getattr(s3_config, "secure", False)
            protocol = "https" if secure else "http"
            self._storage_options = {
                "key": s3_config.access_key,
                "secret": s3_config.secret_key,
                "client_kwargs": {"endpoint_url": f"{protocol}://{s3_config.endpoint}"},
            }
            logger.info(f"ValidatorService: S3 configured with endpoint: {s3_config.endpoint}")

    def read_csv_from_s3(self, bucket: str, object_name: str, data_type: str) -> pl.DataFrame:
        """Read CSV file from S3/MinIO using Polars.

        Args:
            bucket: S3 bucket name.
            object_name: CSV file object name/path.
            data_type: Data type name for logging.

        Returns:
            Polars DataFrame from CSV.

        Raises:
            RuntimeError: If S3 is not configured.
        """
        if self._storage_options is None:
            raise RuntimeError("S3 not configured. Pass s3_config to ValidatorService constructor.")

        s3_path = f"s3://{bucket}/{object_name}"
        logger.info(f"Reading {data_type} CSV from {s3_path}")

        df = pl.read_csv(s3_path, storage_options=self._storage_options)
        logger.info(f"Read {len(df)} {data_type} records from S3")
        return df

    def fetch_and_validate(
        self,
        bucket: str,
        object_name: str,
        data_type: str,
        valid_foreign_keys: set[str] | None = None,
    ) -> ValidationResult:
        """Read CSV from S3 and validate in one step.

        Args:
            bucket: S3 bucket name.
            object_name: CSV file object name/path.
            data_type: Data type name (e.g., "asset", "trade").
            valid_foreign_keys: Optional set of valid foreign key values.

        Returns:
            ValidationResult with data if valid, error message if not.
        """
        raw_df = self.read_csv_from_s3(bucket, object_name, data_type)
        return self.validate(raw_df, data_type, valid_foreign_keys)

    def _format_error_message(self, schema_errors: pandera.errors.SchemaErrors) -> str:
        """Format Pandera SchemaErrors into a readable error message.

        Args:
            schema_errors: Pandera schema validation errors.

        Returns:
            Formatted error message string.
        """
        failure_cases = schema_errors.failure_cases
        if failure_cases is None or len(failure_cases) == 0:
            return "Validation failed with unknown error"

        errors = []
        for row in failure_cases.head(5).iter_rows(named=True):
            row_idx = row.get("index", "N/A")
            column = row.get("column", "unknown")
            check = row.get("check", "unknown")
            failure_case = row.get("failure_case", "N/A")
            errors.append(f"Row {row_idx}, column '{column}': {check} (value: {failure_case})")

        total_errors = len(failure_cases)
        if total_errors > 5:
            errors.append(f"... and {total_errors - 5} more errors")

        return "; ".join(errors)

    def _convert_to_decimal(self, df: pl.DataFrame, data_type: str) -> pl.DataFrame:
        """Convert monetary fields to Decimal(20, 2) type for precise validation.

        Args:
            df: DataFrame with float columns.
            data_type: Data type name (e.g., "asset", "trade").

        Returns:
            DataFrame with monetary fields converted to Decimal.
        """
        if data_type == "asset":
            return df.with_columns([
                pl.col("cash").cast(pl.Decimal(20, 2)),
                pl.col("frozen_cash").cast(pl.Decimal(20, 2)),
                pl.col("market_value").cast(pl.Decimal(20, 2)),
                pl.col("total_asset").cast(pl.Decimal(20, 2)),
            ])
        elif data_type == "trade":
            return df.with_columns([
                pl.col("traded_price").cast(pl.Decimal(20, 2)),
                pl.col("traded_amount").cast(pl.Decimal(20, 2)),
            ])
        return df

    def validate(
        self,
        df: pl.DataFrame,
        data_type: str,
        valid_foreign_keys: set[str] | None = None,
    ) -> ValidationResult:
        """Validate data using the registered schema for the data type.

        Args:
            df: DataFrame to validate.
            data_type: Data type name (e.g., "asset", "trade").
            valid_foreign_keys: Optional set of valid foreign key values.

        Returns:
            ValidationResult with data if valid, error message if not.
        """
        config = DataTypeRegistry.get(data_type)

        if config.schema_class is None:
            raise ValueError(f"No schema registered for data type: {data_type}")

        logger.info(f"Validating {len(df)} {data_type} records with Pandera")

        if len(df) == 0:
            return ValidationResult(is_valid=True, data=df)

        # Convert monetary fields to Decimal for precise validation
        df = self._convert_to_decimal(df, data_type)

        try:
            config.schema_class.validate(df, lazy=True)
        except pandera.errors.SchemaErrors as e:
            error_msg = self._format_error_message(e)
            logger.error(f"{data_type} validation failed: {error_msg}")
            return ValidationResult(is_valid=False, data=df.clear(), error_message=error_msg)

        # Foreign key validation
        if config.foreign_key_column and valid_foreign_keys:
            fk_column = config.foreign_key_column
            invalid_fks = df.filter(~pl.col(fk_column).cast(pl.Utf8).is_in(list(valid_foreign_keys)))
            if len(invalid_fks) > 0:
                invalid_values = invalid_fks[fk_column].unique().to_list()[:5]
                error_msg = f"Foreign key validation failed: {fk_column} values {invalid_values} not found in asset table"
                logger.error(f"{data_type} validation failed: {error_msg}")
                return ValidationResult(is_valid=False, data=df.clear(), error_message=error_msg)

        logger.info(f"{data_type} validation passed: {len(df)} records")
        return ValidationResult(is_valid=True, data=df)

    def validate_assets(self, df: pl.DataFrame) -> ValidationResult:
        """Validate asset data using Pandera AssetSchema."""
        return self.validate(df, "asset")

    def validate_trades(self, df: pl.DataFrame, valid_account_ids: set[str] | None = None) -> ValidationResult:
        """Validate trade data using Pandera TradeSchema."""
        return self.validate(df, "trade", valid_account_ids)
