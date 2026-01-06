"""Cleanup DB types

Revision ID: 202601020944
Revises: 202512221539
Create Date: 2026-01-02 10:44:07.853254

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "202601020944"
down_revision = "202512221539"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("api_billing", "paid", existing_type=sa.BOOLEAN(), nullable=False)
    op.alter_column(
        "api_billing",
        "created_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=False,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "api_billing",
        "updated_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=False,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "api_pricing_plan",
        "created_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=False,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "api_pricing_plan",
        "updated_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=False,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column("api_user", "is_active", existing_type=sa.BOOLEAN(), nullable=False)
    op.alter_column(
        "api_user",
        "created_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=False,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "api_user_pricing",
        "created_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=False,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "api_user_pricing",
        "updated_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=False,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "fleet_tesla_authentication_code",
        "created_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=False,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "fleet_tesla_authentication_code",
        "updated_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=False,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "premium_report",
        "created_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=False,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "premium_report",
        "updated_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=False,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column("user", "is_active", existing_type=sa.BOOLEAN(), nullable=False)
    op.alter_column(
        "vehicle_data",
        "timestamp",
        existing_type=postgresql.TIMESTAMP(),
        nullable=False,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "vehicle_model",
        "trendline_bib",
        existing_type=sa.BOOLEAN(),
        nullable=False,
        existing_comment="If the trendline is based on SoH calculated by BIB",
    )
    op.alter_column(
        "vehicle_model", "odometer_data", existing_type=sa.BOOLEAN(), nullable=False
    )
    op.alter_column(
        "vehicle_model", "soh_data", existing_type=sa.BOOLEAN(), nullable=False
    )
    op.alter_column(
        "vehicle_model", "soh_oem_data", existing_type=sa.BOOLEAN(), nullable=False
    )
    op.alter_column(
        "vehicle_status",
        "created_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=False,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "vehicle_status",
        "updated_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=False,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "user_tokens",
        "callback_url",
        existing_type=sa.VARCHAR(),
        nullable=False,
        existing_comment="Callback URL for the user token, it must be the same one as the one used to generate the code",
        schema="tesla",
    )


def downgrade() -> None:
    op.alter_column(
        "user_tokens",
        "callback_url",
        existing_type=sa.VARCHAR(),
        nullable=True,
        existing_comment="Callback URL for the user token, it must be the same one as the one used to generate the code",
        schema="tesla",
    )
    op.alter_column(
        "vehicle_status",
        "updated_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=True,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "vehicle_status",
        "created_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=True,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "vehicle_model", "soh_oem_data", existing_type=sa.BOOLEAN(), nullable=True
    )
    op.alter_column(
        "vehicle_model", "soh_data", existing_type=sa.BOOLEAN(), nullable=True
    )
    op.alter_column(
        "vehicle_model", "odometer_data", existing_type=sa.BOOLEAN(), nullable=True
    )
    op.alter_column(
        "vehicle_model",
        "trendline_bib",
        existing_type=sa.BOOLEAN(),
        nullable=True,
        existing_comment="If the trendline is based on SoH calculated by BIB",
    )
    op.alter_column(
        "vehicle_data",
        "timestamp",
        existing_type=postgresql.TIMESTAMP(),
        nullable=True,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column("user", "is_active", existing_type=sa.BOOLEAN(), nullable=True)
    op.alter_column(
        "premium_report",
        "updated_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=True,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "premium_report",
        "created_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=True,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "fleet_tesla_authentication_code",
        "updated_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=True,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "fleet_tesla_authentication_code",
        "created_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=True,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "api_user_pricing",
        "updated_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=True,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "api_user_pricing",
        "created_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=True,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "api_user",
        "created_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=True,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column("api_user", "is_active", existing_type=sa.BOOLEAN(), nullable=True)
    op.alter_column(
        "api_pricing_plan",
        "updated_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=True,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "api_pricing_plan",
        "created_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=True,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "api_billing",
        "updated_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=True,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column(
        "api_billing",
        "created_at",
        existing_type=postgresql.TIMESTAMP(),
        nullable=True,
        existing_server_default=sa.text("now()"),
    )
    op.alter_column("api_billing", "paid", existing_type=sa.BOOLEAN(), nullable=True)
