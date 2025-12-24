"""Validator service for data validation using Pandera schemas."""

import logging
from dataclasses import dataclass, field

import pandera.errors
import pandera.polars as pa
import polars as pl

from small_etl.domain.schemas import AssetSchema, TradeSchema

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
    """Service for validating asset and trade data using Pandera schemas.

    Args:
        tolerance: Tolerance for calculated field comparisons (default: 0.01).
                   Note: Currently not used as schemas use DEFAULT_TOLERANCE.
    """

    def __init__(self, tolerance: float = 0.01) -> None:
        self._tolerance = tolerance

    def _parse_pandera_errors(
        self, schema_errors: pandera.errors.SchemaErrors
    ) -> tuple[list[ValidationError], set[int]]:
        """Parse Pandera SchemaErrors into ValidationError list.

        Args:
            schema_errors: Pandera schema validation errors.

        Returns:
            Tuple of (list of ValidationError, set of invalid row indices).
        """
        errors: list[ValidationError] = []
        invalid_indices: set[int] = set()

        # failure_cases is a DataFrame with columns: schema_context, column, check, check_number, failure_case, index
        failure_cases = schema_errors.failure_cases
        if failure_cases is None or len(failure_cases) == 0:
            return errors, invalid_indices

        for row in failure_cases.iter_rows(named=True):
            row_idx = row.get("index")
            column = row.get("column")
            check = row.get("check")
            failure_case = row.get("failure_case")
            schema_context = row.get("schema_context")

            # For dataframe-level checks, extract field name from check name
            # e.g., "total_asset_equals_sum" -> "total_asset"
            # e.g., "traded_amount_equals_product" -> "traded_amount"
            field = str(column) if column is not None else "unknown"
            if schema_context == "DataFrameSchema" and check:
                # Extract first part of check name as field name
                check_str = str(check)
                if "_equals_" in check_str:
                    field = check_str.split("_equals_")[0]
                elif "_" in check_str:
                    field = check_str.split("_")[0]

            if row_idx is not None:
                invalid_indices.add(int(row_idx))
                errors.append(
                    ValidationError(
                        row_index=int(row_idx),
                        field=field,
                        message=f"Failed check: {check}",
                        value=str(failure_case) if failure_case is not None else None,
                    )
                )
            else:
                # DataFrame-level check failure without row index
                errors.append(
                    ValidationError(
                        row_index=-1,
                        field=field,
                        message=f"Failed check: {check}",
                        value=str(failure_case) if failure_case is not None else None,
                    )
                )

        return errors, invalid_indices

    def _validate_with_schema(
        self,
        df: pl.DataFrame,
        schema: type[pa.DataFrameModel],
        data_type: str,
    ) -> ValidationResult:
        """Validate DataFrame using a Pandera schema.

        Args:
            df: DataFrame to validate.
            schema: Pandera DataFrameModel class.
            data_type: Type of data being validated (for logging).

        Returns:
            ValidationResult with valid/invalid rows and errors.
        """
        logger.info(f"Validating {len(df)} {data_type} records with Pandera")
        total_rows = len(df)

        if total_rows == 0:
            return ValidationResult(
                is_valid=True,
                valid_rows=df,
                invalid_rows=df.clear(),
                errors=[],
                total_rows=0,
                valid_count=0,
                invalid_count=0,
            )

        # Add original row index before validation
        df_with_index = df.with_row_index("_original_index")

        try:
            # lazy=True collects all errors instead of failing on first
            schema.validate(df_with_index, lazy=True)
            # All rows passed validation
            logger.info(f"Validation complete: {total_rows} valid, 0 invalid")
            return ValidationResult(
                is_valid=True,
                valid_rows=df,
                invalid_rows=df.clear(),
                errors=[],
                total_rows=total_rows,
                valid_count=total_rows,
                invalid_count=0,
            )
        except pandera.errors.SchemaErrors as e:
            # Parse errors and separate valid/invalid rows
            errors, invalid_indices = self._parse_pandera_errors(e)

            if invalid_indices:
                valid_rows = df_with_index.filter(~pl.col("_original_index").is_in(list(invalid_indices))).drop(
                    "_original_index"
                )
                invalid_rows = df_with_index.filter(pl.col("_original_index").is_in(list(invalid_indices))).drop(
                    "_original_index"
                )
            else:
                # DataFrame-level check failed but no row-level indices
                # Mark all rows as invalid for safety
                valid_rows = df.clear()
                invalid_rows = df

            valid_count = len(valid_rows)
            invalid_count = len(invalid_rows)

            logger.info(f"Validation complete: {valid_count} valid, {invalid_count} invalid")
            return ValidationResult(
                is_valid=False,
                valid_rows=valid_rows,
                invalid_rows=invalid_rows,
                errors=errors,
                total_rows=total_rows,
                valid_count=valid_count,
                invalid_count=invalid_count,
            )

    def validate_assets(self, df: pl.DataFrame) -> ValidationResult:
        """Validate asset data using Pandera AssetSchema.

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
        return self._validate_with_schema(df, AssetSchema, "asset")

    def validate_trades(self, df: pl.DataFrame, valid_account_ids: set[str] | None = None) -> ValidationResult:
        """Validate trade data using Pandera TradeSchema.

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
        # First validate with Pandera schema
        result = self._validate_with_schema(df, TradeSchema, "trade")

        # If valid_account_ids provided and there are valid rows, validate foreign keys
        if valid_account_ids and result.valid_count > 0:
            result = self._validate_foreign_keys(result, valid_account_ids)

        return result

    def _validate_foreign_keys(
        self, result: ValidationResult, valid_account_ids: set[str]
    ) -> ValidationResult:
        """Validate foreign key constraints on valid rows.

        Args:
            result: Current validation result.
            valid_account_ids: Set of valid account IDs.

        Returns:
            Updated ValidationResult with foreign key validation applied.
        """
        valid_rows = result.valid_rows
        if len(valid_rows) == 0:
            return result

        # Add index for tracking
        valid_rows_indexed = valid_rows.with_row_index("_fk_index")

        # Find rows with invalid account_ids
        fk_valid_mask = valid_rows_indexed["account_id"].cast(pl.Utf8).is_in(list(valid_account_ids))
        invalid_fk_rows = valid_rows_indexed.filter(~fk_valid_mask)

        if len(invalid_fk_rows) == 0:
            return result

        # Create errors for foreign key violations
        fk_errors: list[ValidationError] = []
        for row in invalid_fk_rows.iter_rows(named=True):
            fk_errors.append(
                ValidationError(
                    row_index=int(row["_fk_index"]),
                    field="account_id",
                    message=f"account_id not found in asset table: {row.get('account_id')}",
                    value=str(row.get("account_id")),
                )
            )

        # Update valid/invalid rows
        new_valid_rows = valid_rows_indexed.filter(fk_valid_mask).drop("_fk_index")
        new_invalid_rows = pl.concat([result.invalid_rows, invalid_fk_rows.drop("_fk_index")])

        return ValidationResult(
            is_valid=len(new_valid_rows) == result.total_rows,
            valid_rows=new_valid_rows,
            invalid_rows=new_invalid_rows,
            errors=result.errors + fk_errors,
            total_rows=result.total_rows,
            valid_count=len(new_valid_rows),
            invalid_count=len(new_invalid_rows),
        )
