"""Update fctns

Revision ID: 202511172306
Revises: 202511071733
Create Date: 2025-11-18 00:06:56.578903

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "202511172306"
down_revision = "202511071733"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "api_billing",
        "created_at",
        existing_type=postgresql.TIMESTAMP(),
        server_default=sa.text("now()"),
        existing_nullable=True,
    )
    op.alter_column(
        "api_billing",
        "updated_at",
        existing_type=postgresql.TIMESTAMP(),
        server_default=sa.text("now()"),
        existing_nullable=True,
    )
    op.alter_column(
        "api_call_log",
        "timestamp",
        existing_type=postgresql.TIMESTAMP(),
        server_default=sa.text("now()"),
        existing_nullable=False,
    )
    op.alter_column(
        "api_pricing_plan",
        "created_at",
        existing_type=postgresql.TIMESTAMP(),
        server_default=sa.text("now()"),
        existing_nullable=True,
    )
    op.alter_column(
        "api_pricing_plan",
        "updated_at",
        existing_type=postgresql.TIMESTAMP(),
        server_default=sa.text("now()"),
        existing_nullable=True,
    )
    op.alter_column(
        "api_user",
        "created_at",
        existing_type=postgresql.TIMESTAMP(),
        server_default=sa.text("now()"),
        existing_nullable=True,
    )
    op.alter_column(
        "api_user_pricing",
        "created_at",
        existing_type=postgresql.TIMESTAMP(),
        server_default=sa.text("now()"),
        existing_nullable=True,
    )
    op.alter_column(
        "api_user_pricing",
        "updated_at",
        existing_type=postgresql.TIMESTAMP(),
        server_default=sa.text("now()"),
        existing_nullable=True,
    )
    op.alter_column(
        "fleet_aggregate",
        "timestamp",
        existing_type=postgresql.TIMESTAMP(),
        server_default=sa.text("now()"),
        existing_nullable=True,
    )
    op.alter_column(
        "regional_aggregate",
        "timestamp",
        existing_type=postgresql.TIMESTAMP(),
        server_default=sa.text("now()"),
        existing_nullable=True,
    )
    op.alter_column(
        "vehicle_aggregate",
        "timestamp",
        existing_type=postgresql.TIMESTAMP(),
        server_default=sa.text("now()"),
        existing_nullable=True,
    )
    op.alter_column(
        "vehicle_data",
        "timestamp",
        existing_type=postgresql.TIMESTAMP(),
        server_default=sa.text("now()"),
        existing_nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "vehicle_data",
        "timestamp",
        existing_type=postgresql.TIMESTAMP(),
        server_default=None,
        existing_nullable=True,
    )
    op.alter_column(
        "vehicle_aggregate",
        "timestamp",
        existing_type=postgresql.TIMESTAMP(),
        server_default=None,
        existing_nullable=True,
    )
    op.alter_column(
        "regional_aggregate",
        "timestamp",
        existing_type=postgresql.TIMESTAMP(),
        server_default=None,
        existing_nullable=True,
    )
    op.alter_column(
        "fleet_aggregate",
        "timestamp",
        existing_type=postgresql.TIMESTAMP(),
        server_default=None,
        existing_nullable=True,
    )
    op.alter_column(
        "api_user_pricing",
        "updated_at",
        existing_type=postgresql.TIMESTAMP(),
        server_default=None,
        existing_nullable=True,
    )
    op.alter_column(
        "api_user_pricing",
        "created_at",
        existing_type=postgresql.TIMESTAMP(),
        server_default=None,
        existing_nullable=True,
    )
    op.alter_column(
        "api_user",
        "created_at",
        existing_type=postgresql.TIMESTAMP(),
        server_default=None,
        existing_nullable=True,
    )
    op.alter_column(
        "api_pricing_plan",
        "updated_at",
        existing_type=postgresql.TIMESTAMP(),
        server_default=None,
        existing_nullable=True,
    )
    op.alter_column(
        "api_pricing_plan",
        "created_at",
        existing_type=postgresql.TIMESTAMP(),
        server_default=None,
        existing_nullable=True,
    )
    op.alter_column(
        "api_call_log",
        "timestamp",
        existing_type=postgresql.TIMESTAMP(),
        server_default=None,
        existing_nullable=False,
    )
    op.alter_column(
        "api_billing",
        "updated_at",
        existing_type=postgresql.TIMESTAMP(),
        server_default=None,
        existing_nullable=True,
    )
    op.alter_column(
        "api_billing",
        "created_at",
        existing_type=postgresql.TIMESTAMP(),
        server_default=None,
        existing_nullable=True,
    )
