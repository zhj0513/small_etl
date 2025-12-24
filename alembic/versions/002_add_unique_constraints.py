"""Add explicit unique constraints for DuckDB postgres extension compatibility.

Revision ID: 002_add_unique_constraints
Revises: 96f12c403d72
Create Date: 2025-12-24

DuckDB's postgres extension requires explicit UNIQUE CONSTRAINTs (not just unique indexes)
for ON CONFLICT clauses to work properly.
"""

from typing import Sequence, Union

from alembic import op

revision: str = "002_add_unique_constraints"
down_revision: Union[str, None] = "96f12c403d72"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add explicit unique constraints."""
    # Drop the foreign key constraint first (it depends on the unique index)
    op.drop_constraint("trade_account_id_fkey", "trade", type_="foreignkey")

    # Drop the existing unique indexes
    op.drop_index("ix_asset_account_id", table_name="asset")
    op.drop_index("ix_trade_traded_id", table_name="trade")

    # Create explicit unique constraints (which also create indexes)
    op.create_unique_constraint("uq_asset_account_id", "asset", ["account_id"])
    op.create_unique_constraint("uq_trade_traded_id", "trade", ["traded_id"])

    # Recreate the foreign key constraint referencing the new unique constraint
    op.create_foreign_key(
        "trade_account_id_fkey",
        "trade",
        "asset",
        ["account_id"],
        ["account_id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    """Remove explicit unique constraints and restore unique indexes."""
    # Drop the foreign key constraint first
    op.drop_constraint("trade_account_id_fkey", "trade", type_="foreignkey")

    # Drop the unique constraints
    op.drop_constraint("uq_asset_account_id", "asset", type_="unique")
    op.drop_constraint("uq_trade_traded_id", "trade", type_="unique")

    # Recreate the original unique indexes
    op.create_index("ix_asset_account_id", "asset", ["account_id"], unique=True)
    op.create_index("ix_trade_traded_id", "trade", ["traded_id"], unique=True)

    # Recreate the foreign key constraint
    op.create_foreign_key(
        "trade_account_id_fkey",
        "trade",
        "asset",
        ["account_id"],
        ["account_id"],
        ondelete="CASCADE",
    )
