"""Validator service for data validation."""

import logging
from dataclasses import dataclass, field
from decimal import Decimal

import polars as pl

from small_etl.domain.enums import VALID_ACCOUNT_TYPES, VALID_DIRECTIONS, VALID_OFFSET_FLAGS

logger = logging.getLogger(__name__)


@dataclass
class ValidationError:
    """Represents a single validation error.

    Attributes:
        row_index: Index of the row with error.
        field: Field name where error occurred.
        message: Error description.
        value: The invalid value.
    """

    row_index: int
    field: str
    message: str
    value: str | None = None


@dataclass
class ValidationResult:
    """Result of data validation.

    Attributes:
        is_valid: True if all rows passed validation.
        valid_rows: DataFrame containing valid rows.
        invalid_rows: DataFrame containing invalid rows.
        errors: List of validation errors.
        total_rows: Total number of rows validated.
        valid_count: Number of valid rows.
        invalid_count: Number of invalid rows.
    """

    is_valid: bool
    valid_rows: pl.DataFrame
    invalid_rows: pl.DataFrame
    errors: list[ValidationError] = field(default_factory=list)
    total_rows: int = 0
    valid_count: int = 0
    invalid_count: int = 0


class ValidatorService:
    """Service for validating asset and trade data.

    Args:
        tolerance: Tolerance for calculated field comparisons (default: 0.01).
    """

    def __init__(self, tolerance: float = 0.01) -> None:
        self._tolerance = Decimal(str(tolerance))

    def validate_assets(self, df: pl.DataFrame) -> ValidationResult:
        """Validate asset data.

        Validates:
            - Required fields presence
            - Numeric fields >= 0
            - Account type in valid range
            - total_asset = cash + frozen_cash + market_value (within tolerance)

        Args:
            df: DataFrame with asset data.

        Returns:
            ValidationResult with valid/invalid rows and errors.
        """
        logger.info(f"Validating {len(df)} asset records")
        errors: list[ValidationError] = []

        valid_mask = pl.lit(True)

        valid_mask = valid_mask & df["account_id"].is_not_null() & (df["account_id"].str.len_chars() > 0)

        valid_mask = valid_mask & df["account_type"].is_in(list(VALID_ACCOUNT_TYPES))

        valid_mask = valid_mask & (df["cash"] >= 0)
        valid_mask = valid_mask & (df["frozen_cash"] >= 0)
        valid_mask = valid_mask & (df["market_value"] >= 0)
        valid_mask = valid_mask & (df["total_asset"] >= 0)

        df_with_calc = df.with_columns((pl.col("cash") + pl.col("frozen_cash") + pl.col("market_value")).alias("calc_total"))
        diff = (df_with_calc["total_asset"] - df_with_calc["calc_total"]).abs()
        valid_mask = valid_mask & (diff <= float(self._tolerance))

        valid_mask = valid_mask & df["updated_at"].is_not_null()

        valid_rows = df.filter(valid_mask)
        invalid_rows = df.filter(~valid_mask)

        for idx, row in enumerate(invalid_rows.iter_rows(named=True)):
            if not row.get("account_id"):
                errors.append(ValidationError(idx, "account_id", "account_id is required"))
            if row.get("account_type") not in VALID_ACCOUNT_TYPES:
                errors.append(ValidationError(idx, "account_type", f"Invalid account_type: {row.get('account_type')}", str(row.get("account_type"))))
            if row.get("cash", 0) < 0:
                errors.append(ValidationError(idx, "cash", "cash must be >= 0", str(row.get("cash"))))
            if row.get("frozen_cash", 0) < 0:
                errors.append(ValidationError(idx, "frozen_cash", "frozen_cash must be >= 0", str(row.get("frozen_cash"))))
            if row.get("market_value", 0) < 0:
                errors.append(ValidationError(idx, "market_value", "market_value must be >= 0", str(row.get("market_value"))))

            cash = Decimal(str(row.get("cash", 0)))
            frozen = Decimal(str(row.get("frozen_cash", 0)))
            market = Decimal(str(row.get("market_value", 0)))
            total = Decimal(str(row.get("total_asset", 0)))
            calc_total = cash + frozen + market
            if abs(total - calc_total) > self._tolerance:
                errors.append(
                    ValidationError(
                        idx,
                        "total_asset",
                        f"total_asset mismatch: expected {calc_total}, got {total}",
                        str(total),
                    )
                )

        result = ValidationResult(
            is_valid=len(invalid_rows) == 0,
            valid_rows=valid_rows,
            invalid_rows=invalid_rows,
            errors=errors,
            total_rows=len(df),
            valid_count=len(valid_rows),
            invalid_count=len(invalid_rows),
        )

        logger.info(f"Validation complete: {result.valid_count} valid, {result.invalid_count} invalid")
        return result

    def validate_trades(self, df: pl.DataFrame, valid_account_ids: set[str] | None = None) -> ValidationResult:
        """Validate trade data.

        Validates:
            - Required fields presence
            - Numeric fields > 0 for price/amount/volume
            - Account type in valid range
            - Direction and offset_flag in valid range
            - traded_amount = traded_price * traded_volume (within tolerance)
            - account_id exists in valid_account_ids (if provided)

        Args:
            df: DataFrame with trade data.
            valid_account_ids: Optional set of valid account IDs for foreign key validation.

        Returns:
            ValidationResult with valid/invalid rows and errors.
        """
        logger.info(f"Validating {len(df)} trade records")
        errors: list[ValidationError] = []

        valid_mask = pl.lit(True)

        valid_mask = valid_mask & df["account_id"].is_not_null() & (df["account_id"].str.len_chars() > 0)
        valid_mask = valid_mask & df["traded_id"].is_not_null() & (df["traded_id"].str.len_chars() > 0)
        valid_mask = valid_mask & df["stock_code"].is_not_null() & (df["stock_code"].str.len_chars() > 0)
        valid_mask = valid_mask & df["strategy_name"].is_not_null() & (df["strategy_name"].str.len_chars() > 0)

        valid_mask = valid_mask & df["account_type"].is_in(list(VALID_ACCOUNT_TYPES))
        valid_mask = valid_mask & df["direction"].is_in(list(VALID_DIRECTIONS))
        valid_mask = valid_mask & df["offset_flag"].is_in(list(VALID_OFFSET_FLAGS))

        valid_mask = valid_mask & (df["traded_price"] > 0)
        valid_mask = valid_mask & (df["traded_volume"] > 0)
        valid_mask = valid_mask & (df["traded_amount"] > 0)

        df_with_calc = df.with_columns((pl.col("traded_price") * pl.col("traded_volume")).alias("calc_amount"))
        diff = (df_with_calc["traded_amount"] - df_with_calc["calc_amount"]).abs()
        valid_mask = valid_mask & (diff <= float(self._tolerance))

        valid_mask = valid_mask & df["traded_time"].is_not_null()
        valid_mask = valid_mask & df["created_at"].is_not_null()
        valid_mask = valid_mask & df["updated_at"].is_not_null()

        if valid_account_ids:
            valid_mask = valid_mask & df["account_id"].cast(pl.Utf8).is_in(list(valid_account_ids))

        valid_rows = df.filter(valid_mask)
        invalid_rows = df.filter(~valid_mask)

        for idx, row in enumerate(invalid_rows.iter_rows(named=True)):
            if not row.get("traded_id"):
                errors.append(ValidationError(idx, "traded_id", "traded_id is required"))
            if row.get("account_type") not in VALID_ACCOUNT_TYPES:
                errors.append(ValidationError(idx, "account_type", f"Invalid account_type: {row.get('account_type')}", str(row.get("account_type"))))
            if row.get("direction") not in VALID_DIRECTIONS:
                errors.append(ValidationError(idx, "direction", f"Invalid direction: {row.get('direction')}", str(row.get("direction"))))
            if row.get("offset_flag") not in VALID_OFFSET_FLAGS:
                errors.append(ValidationError(idx, "offset_flag", f"Invalid offset_flag: {row.get('offset_flag')}", str(row.get("offset_flag"))))
            if row.get("traded_price", 0) <= 0:
                errors.append(ValidationError(idx, "traded_price", "traded_price must be > 0", str(row.get("traded_price"))))
            if row.get("traded_volume", 0) <= 0:
                errors.append(ValidationError(idx, "traded_volume", "traded_volume must be > 0", str(row.get("traded_volume"))))

            price = Decimal(str(row.get("traded_price", 0)))
            volume = int(row.get("traded_volume", 0))
            amount = Decimal(str(row.get("traded_amount", 0)))
            calc_amount = price * volume
            if abs(amount - calc_amount) > self._tolerance:
                errors.append(
                    ValidationError(
                        idx,
                        "traded_amount",
                        f"traded_amount mismatch: expected {calc_amount}, got {amount}",
                        str(amount),
                    )
                )

            if valid_account_ids and str(row.get("account_id")) not in valid_account_ids:
                errors.append(
                    ValidationError(
                        idx,
                        "account_id",
                        f"account_id not found in asset table: {row.get('account_id')}",
                        str(row.get("account_id")),
                    )
                )

        result = ValidationResult(
            is_valid=len(invalid_rows) == 0,
            valid_rows=valid_rows,
            invalid_rows=invalid_rows,
            errors=errors,
            total_rows=len(df),
            valid_count=len(valid_rows),
            invalid_count=len(invalid_rows),
        )

        logger.info(f"Validation complete: {result.valid_count} valid, {result.invalid_count} invalid")
        return result
