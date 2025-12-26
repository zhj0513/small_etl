"""DuckDB client for in-memory data operations and PostgreSQL integration."""

import logging
from typing import Any

import duckdb
import polars as pl

logger = logging.getLogger(__name__)


class DuckDBClient:
    """DuckDB client for in-memory data operations and PostgreSQL access.

    Uses DuckDB's in-memory mode and postgres extension for fast data operations.
    """

    def __init__(self) -> None:
        self._conn = duckdb.connect(":memory:")
        self._pg_attached = False
        logger.info("DuckDBClient initialized with in-memory database")

    def attach_postgres(self, database_url: str) -> None:
        """Attach PostgreSQL database using DuckDB postgres extension.

        Args:
            database_url: PostgreSQL connection URL.
        """
        if self._pg_attached:
            return

        self._conn.execute("INSTALL postgres")
        self._conn.execute("LOAD postgres")
        self._conn.execute(f"ATTACH '{database_url}' AS pg (TYPE POSTGRES)")
        self._pg_attached = True
        logger.info("PostgreSQL database attached via DuckDB postgres extension")

    def upsert_to_postgres(self, df: pl.DataFrame, table_name: str, conflict_column: str) -> int:
        """Upsert DataFrame to PostgreSQL table using DuckDB postgres extension.

        Uses UPDATE + INSERT pattern to handle foreign key constraints properly.

        Args:
            df: Polars DataFrame to upsert.
            table_name: Target PostgreSQL table name.
            conflict_column: Column name for conflict detection (unique key).

        Returns:
            Number of rows upserted.
        """
        if df.is_empty():
            return 0

        if not self._pg_attached:
            raise RuntimeError("PostgreSQL not attached. Call attach_postgres() first.")

        temp_table = f"_temp_{table_name}"
        self._conn.register(temp_table, df.to_arrow())

        columns = df.columns
        update_columns = [c for c in columns if c != conflict_column]

        if update_columns:
            set_clause = ", ".join([f"{c} = t.\"{c}\"" for c in update_columns])
            update_sql = f"""
                UPDATE pg.{table_name} AS p
                SET {set_clause}
                FROM {temp_table} AS t
                WHERE p.{conflict_column} = t."{conflict_column}"
            """
            self._conn.execute(update_sql)

        columns_str = ", ".join(columns)
        values_str = ", ".join([f'"{c}"' for c in columns])
        insert_sql = f"""
            INSERT INTO pg.{table_name} ({columns_str})
            SELECT {values_str} FROM {temp_table} t
            WHERE NOT EXISTS (
                SELECT 1 FROM pg.{table_name} p
                WHERE p.{conflict_column} = t."{conflict_column}"
            )
        """
        self._conn.execute(insert_sql)
        self._conn.unregister(temp_table)

        row_count = len(df)
        logger.info(f"Upserted {row_count} rows to pg.{table_name}")
        return row_count

    def query(self, sql: str) -> pl.DataFrame:
        """Execute SQL query and return results as Polars DataFrame.

        Args:
            sql: SQL query string.

        Returns:
            Query results as Polars DataFrame.
        """
        result = self._conn.execute(sql)
        return result.pl()

    def query_column_values(self, table: str, column: str) -> set[str]:
        """Query distinct values of a column from PostgreSQL table.

        Args:
            table: Table name (without pg. prefix).
            column: Column name to query.

        Returns:
            Set of distinct values as strings.
        """
        if not self._pg_attached:
            raise RuntimeError("PostgreSQL not attached. Call attach_postgres() first.")

        sql = f"SELECT DISTINCT {column} FROM pg.{table}"
        result = self._conn.execute(sql).fetchall()
        return {str(row[0]) for row in result}

    def query_count(self, table: str) -> int:
        """Query row count of a PostgreSQL table.

        Args:
            table: Table name (without pg. prefix).

        Returns:
            Number of rows in the table.
        """
        if not self._pg_attached:
            raise RuntimeError("PostgreSQL not attached. Call attach_postgres() first.")

        sql = f"SELECT COUNT(*) FROM pg.{table}"
        result = self._conn.execute(sql).fetchone()
        return result[0] if result else 0

    def query_asset_statistics(self) -> dict[str, Any]:
        """Query asset statistics from PostgreSQL via DuckDB.

        Returns:
            Dictionary with aggregated asset statistics.
        """
        if not self._pg_attached:
            raise RuntimeError("PostgreSQL not attached. Call attach_postgres() first.")

        overall_sql = """
            SELECT
                COUNT(*) as total_records,
                COALESCE(SUM(cash), 0) as total_cash,
                COALESCE(SUM(frozen_cash), 0) as total_frozen_cash,
                COALESCE(SUM(market_value), 0) as total_market_value,
                COALESCE(SUM(total_asset), 0) as total_assets,
                COALESCE(AVG(cash), 0) as avg_cash,
                COALESCE(AVG(total_asset), 0) as avg_total_asset
            FROM pg.asset
        """
        overall = self._conn.execute(overall_sql).fetchone()
        if overall is None:
            return {
                "total_records": 0,
                "total_cash": 0,
                "total_frozen_cash": 0,
                "total_market_value": 0,
                "total_assets": 0,
                "avg_cash": 0,
                "avg_total_asset": 0,
                "by_account_type": {},
            }

        by_type_sql = """
            SELECT
                account_type,
                COUNT(*) as count,
                COALESCE(SUM(cash), 0) as sum_cash,
                COALESCE(SUM(total_asset), 0) as sum_total,
                COALESCE(AVG(total_asset), 0) as avg_total
            FROM pg.asset
            GROUP BY account_type
            ORDER BY account_type
        """
        by_type_rows = self._conn.execute(by_type_sql).fetchall()

        by_account_type = {}
        for row in by_type_rows:
            by_account_type[row[0]] = {
                "count": row[1],
                "sum_cash": row[2],
                "sum_total": row[3],
                "avg_total": row[4],
            }

        logger.info(f"Queried asset statistics via DuckDB: {overall[0]} records")

        return {
            "total_records": overall[0],
            "total_cash": overall[1],
            "total_frozen_cash": overall[2],
            "total_market_value": overall[3],
            "total_assets": overall[4],
            "avg_cash": overall[5],
            "avg_total_asset": overall[6],
            "by_account_type": by_account_type,
        }

    def query_trade_statistics(self) -> dict[str, Any]:
        """Query trade statistics from PostgreSQL via DuckDB.

        Returns:
            Dictionary with aggregated trade statistics.
        """
        if not self._pg_attached:
            raise RuntimeError("PostgreSQL not attached. Call attach_postgres() first.")

        overall_sql = """
            SELECT
                COUNT(*) as total_records,
                COALESCE(SUM(traded_volume), 0) as total_volume,
                COALESCE(SUM(traded_amount), 0) as total_amount,
                COALESCE(AVG(traded_price), 0) as avg_price,
                COALESCE(AVG(traded_volume), 0) as avg_volume
            FROM pg.trade
        """
        overall = self._conn.execute(overall_sql).fetchone()
        if overall is None:
            return {
                "total_records": 0,
                "total_volume": 0,
                "total_amount": 0,
                "avg_price": 0,
                "avg_volume": 0,
                "by_account_type": {},
                "by_offset_flag": {},
                "by_strategy": {},
            }

        by_type_sql = """
            SELECT
                account_type,
                COUNT(*) as count,
                COALESCE(SUM(traded_volume), 0) as sum_volume,
                COALESCE(SUM(traded_amount), 0) as sum_amount
            FROM pg.trade
            GROUP BY account_type
            ORDER BY account_type
        """
        by_type_rows = self._conn.execute(by_type_sql).fetchall()

        by_account_type = {}
        for row in by_type_rows:
            by_account_type[row[0]] = {
                "count": row[1],
                "sum_volume": row[2],
                "sum_amount": row[3],
            }

        by_flag_sql = """
            SELECT
                offset_flag,
                COUNT(*) as count,
                COALESCE(SUM(traded_volume), 0) as sum_volume,
                COALESCE(SUM(traded_amount), 0) as sum_amount
            FROM pg.trade
            GROUP BY offset_flag
            ORDER BY offset_flag
        """
        by_flag_rows = self._conn.execute(by_flag_sql).fetchall()

        by_offset_flag = {}
        for row in by_flag_rows:
            by_offset_flag[row[0]] = {
                "count": row[1],
                "sum_volume": row[2],
                "sum_amount": row[3],
            }

        by_strategy_sql = """
            SELECT
                strategy_name,
                COUNT(*) as count,
                COALESCE(SUM(traded_volume), 0) as sum_volume,
                COALESCE(SUM(traded_amount), 0) as sum_amount
            FROM pg.trade
            GROUP BY strategy_name
            ORDER BY strategy_name
        """
        by_strategy_rows = self._conn.execute(by_strategy_sql).fetchall()

        by_strategy = {}
        for row in by_strategy_rows:
            by_strategy[row[0]] = {
                "count": row[1],
                "sum_volume": row[2],
                "sum_amount": row[3],
            }

        logger.info(f"Queried trade statistics via DuckDB: {overall[0]} records")

        return {
            "total_records": overall[0],
            "total_volume": overall[1],
            "total_amount": overall[2],
            "avg_price": overall[3],
            "avg_volume": overall[4],
            "by_account_type": by_account_type,
            "by_offset_flag": by_offset_flag,
            "by_strategy": by_strategy,
        }

    def register_dataframe(self, df: pl.DataFrame, table_name: str) -> None:
        """Register Polars DataFrame as a DuckDB view.

        Args:
            df: Polars DataFrame to register.
            table_name: Name for the registered view.
        """
        self._conn.register(table_name, df.to_arrow())
        logger.debug(f"Registered DataFrame as view: {table_name}")

    def unregister(self, table_name: str) -> None:
        """Unregister a previously registered view.

        Args:
            table_name: Name of the view to unregister.
        """
        self._conn.unregister(table_name)
        logger.debug(f"Unregistered view: {table_name}")

    def create_transformed_table(
        self,
        source_table: str,
        target_table: str,
        columns_config: list[Any],
    ) -> int:
        """Create a transformed table using SQL type conversions.

        Args:
            source_table: Name of the source view/table.
            target_table: Name for the new transformed table.
            columns_config: List of column configurations with csv_name, name, dtype, etc.

        Returns:
            Number of rows in the transformed table.
        """
        # Get source column types
        source_cols_info = self._conn.execute(f"DESCRIBE {source_table}").fetchall()
        source_col_types = {row[0]: row[1] for row in source_cols_info}

        select_expressions = []
        for col_cfg in columns_config:
            csv_name = col_cfg.csv_name
            target_name = col_cfg.name
            dtype = col_cfg.dtype
            source_type = source_col_types.get(csv_name, "").upper()

            if dtype == "Datetime":
                # If already TIMESTAMP, just rename
                if "TIMESTAMP" in source_type or "DATETIME" in source_type:
                    expr = f'"{csv_name}" AS "{target_name}"'
                else:
                    fmt = getattr(col_cfg, "format", "%Y-%m-%dT%H:%M:%S")
                    # DuckDB strptime format: %f for microseconds
                    duckdb_fmt = fmt.replace("%.f", ".%g")
                    expr = f'strptime("{csv_name}", \'{duckdb_fmt}\') AS "{target_name}"'
            elif dtype == "Decimal":
                # If already DECIMAL, just rename
                if "DECIMAL" in source_type:
                    expr = f'"{csv_name}" AS "{target_name}"'
                else:
                    precision = getattr(col_cfg, "precision", 20)
                    scale = getattr(col_cfg, "scale", 2)
                    expr = f'CAST("{csv_name}" AS DECIMAL({precision},{scale})) AS "{target_name}"'
            elif dtype == "Float64":
                if "DOUBLE" in source_type or "FLOAT" in source_type:
                    expr = f'"{csv_name}" AS "{target_name}"'
                else:
                    expr = f'CAST("{csv_name}" AS DOUBLE) AS "{target_name}"'
            elif dtype == "Int32":
                if "INTEGER" in source_type and "BIGINT" not in source_type:
                    expr = f'"{csv_name}" AS "{target_name}"'
                else:
                    expr = f'CAST("{csv_name}" AS INTEGER) AS "{target_name}"'
            elif dtype == "Int64":
                if "BIGINT" in source_type:
                    expr = f'"{csv_name}" AS "{target_name}"'
                else:
                    expr = f'CAST("{csv_name}" AS BIGINT) AS "{target_name}"'
            elif dtype == "Utf8":
                if "VARCHAR" in source_type:
                    expr = f'"{csv_name}" AS "{target_name}"'
                else:
                    expr = f'CAST("{csv_name}" AS VARCHAR) AS "{target_name}"'
            else:
                expr = f'"{csv_name}" AS "{target_name}"'

            select_expressions.append(expr)

        select_clause = ", ".join(select_expressions)
        sql = f"CREATE TABLE {target_table} AS SELECT {select_clause} FROM {source_table}"
        self._conn.execute(sql)

        count_result = self._conn.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()
        row_count = count_result[0] if count_result else 0

        logger.info(f"Created transformed table {target_table} with {row_count} rows")
        return row_count

    def drop_table(self, table_name: str) -> None:
        """Drop a DuckDB local table.

        Args:
            table_name: Name of the table to drop.
        """
        self._conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        logger.debug(f"Dropped table: {table_name}")

    def upsert_from_table(
        self,
        source_table: str,
        pg_table: str,
        conflict_column: str,
        columns: list[str],
    ) -> int:
        """Upsert data from DuckDB local table to PostgreSQL.

        Args:
            source_table: Name of the source DuckDB table.
            pg_table: Target PostgreSQL table name.
            conflict_column: Column name for conflict detection (unique key).
            columns: List of columns to insert/update.

        Returns:
            Number of rows upserted.
        """
        if not self._pg_attached:
            raise RuntimeError("PostgreSQL not attached. Call attach_postgres() first.")

        # Get row count from source table
        count_result = self._conn.execute(f"SELECT COUNT(*) FROM {source_table}").fetchone()
        row_count = count_result[0] if count_result else 0

        if row_count == 0:
            return 0

        # Filter columns that exist in source table
        source_cols_result = self._conn.execute(f"DESCRIBE {source_table}").fetchall()
        source_cols = {row[0] for row in source_cols_result}
        valid_columns = [c for c in columns if c in source_cols]

        update_columns = [c for c in valid_columns if c != conflict_column]

        if update_columns:
            set_clause = ", ".join([f'{c} = t."{c}"' for c in update_columns])
            update_sql = f"""
                UPDATE pg.{pg_table} AS p
                SET {set_clause}
                FROM {source_table} AS t
                WHERE p.{conflict_column} = t."{conflict_column}"
            """
            self._conn.execute(update_sql)

        columns_str = ", ".join(valid_columns)
        values_str = ", ".join([f'"{c}"' for c in valid_columns])
        insert_sql = f"""
            INSERT INTO pg.{pg_table} ({columns_str})
            SELECT {values_str} FROM {source_table} t
            WHERE NOT EXISTS (
                SELECT 1 FROM pg.{pg_table} p
                WHERE p.{conflict_column} = t."{conflict_column}"
            )
        """
        self._conn.execute(insert_sql)

        logger.info(f"Upserted {row_count} rows from {source_table} to pg.{pg_table}")
        return row_count

    def close(self) -> None:
        """Close the DuckDB connection."""
        self._conn.close()
        logger.info("DuckDBClient connection closed")

    def __enter__(self) -> "DuckDBClient":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
