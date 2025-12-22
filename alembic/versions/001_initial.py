"""Initial migration - create asset and trade tables.

Revision ID: 001_initial
Revises:
Create Date: 2024-12-22
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create asset and trade tables."""
    op.create_table(
        "asset",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("account_id", sa.String(20), nullable=False, unique=True, index=True),
        sa.Column("account_type", sa.Integer(), nullable=False, index=True),
        sa.Column("cash", sa.Numeric(20, 2), nullable=False),
        sa.Column("frozen_cash", sa.Numeric(20, 2), nullable=False),
        sa.Column("market_value", sa.Numeric(20, 2), nullable=False),
        sa.Column("total_asset", sa.Numeric(20, 2), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "trade",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("account_id", sa.String(20), nullable=False, index=True),
        sa.Column("account_type", sa.Integer(), nullable=False, index=True),
        sa.Column("traded_id", sa.String(50), nullable=False, unique=True, index=True),
        sa.Column("stock_code", sa.String(10), nullable=False, index=True),
        sa.Column("traded_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("traded_price", sa.Numeric(20, 2), nullable=False),
        sa.Column("traded_volume", sa.Integer(), nullable=False),
        sa.Column("traded_amount", sa.Numeric(20, 2), nullable=False),
        sa.Column("strategy_name", sa.String(50), nullable=False),
        sa.Column("order_remark", sa.String(100), nullable=True),
        sa.Column("direction", sa.Integer(), nullable=False),
        sa.Column("offset_flag", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_foreign_key(
        "fk_trade_account_id",
        "trade",
        "asset",
        ["account_id"],
        ["account_id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    """Drop trade and asset tables."""
    op.drop_constraint("fk_trade_account_id", "trade", type_="foreignkey")
    op.drop_table("trade")
    op.drop_table("asset")
