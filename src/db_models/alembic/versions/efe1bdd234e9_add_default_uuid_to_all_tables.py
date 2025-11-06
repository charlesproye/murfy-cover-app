"""Add default UUID to all tables

Revision ID: efe1bdd234e9
Revises: 9e26b9f878be
Create Date: 2025-11-06 11:30:55.887017

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "efe1bdd234e9"
down_revision = "9e26b9f878be"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "api_billing",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.alter_column(
        "api_call_log",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.alter_column(
        "api_pricing_plan",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.create_table_comment("api_pricing_plan", "", existing_comment=None, schema=None)
    op.alter_column(
        "api_user",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.alter_column(
        "api_user_pricing",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.alter_column(
        "battery",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.create_table_comment("battery", "", existing_comment=None, schema=None)
    op.alter_column(
        "company",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.create_table_comment("company", "", existing_comment=None, schema=None)
    op.alter_column(
        "flash_report_combination",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.create_table_comment(
        "flash_report_combination", "", existing_comment=None, schema=None
    )
    op.alter_column(
        "fleet",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.create_table_comment("fleet", "", existing_comment=None, schema=None)
    op.alter_column(
        "fleet_aggregate",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.create_table_comment("fleet_aggregate", "", existing_comment=None, schema=None)
    op.alter_column(
        "make",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.create_table_comment("make", "", existing_comment=None, schema=None)
    op.alter_column(
        "oem",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.create_table_comment("oem", "", existing_comment=None, schema=None)
    op.alter_column(
        "region",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.create_table_comment("region", "", existing_comment=None, schema=None)
    op.alter_column(
        "regional_aggregate",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.create_table_comment(
        "regional_aggregate", "", existing_comment=None, schema=None
    )
    op.alter_column(
        "role",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.create_table_comment("role", "", existing_comment=None, schema=None)
    op.alter_column(
        "user",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.create_table_comment("user", "", existing_comment=None, schema=None)
    op.alter_column(
        "user_fleet",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.create_table_comment("user_fleet", "", existing_comment=None, schema=None)
    op.alter_column(
        "vehicle",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.alter_column(
        "vehicle_aggregate",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.create_table_comment("vehicle_aggregate", "", existing_comment=None, schema=None)
    op.alter_column(
        "vehicle_data",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.create_table_comment("vehicle_data", "", existing_comment=None, schema=None)
    op.add_column(
        "vehicle_model", sa.Column("odometer_data", sa.Boolean(), nullable=True)
    )
    op.add_column("vehicle_model", sa.Column("soh_data", sa.Boolean(), nullable=True))
    op.add_column(
        "vehicle_model", sa.Column("soh_oem_data", sa.Boolean(), nullable=True)
    )
    op.alter_column(
        "vehicle_model",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.create_table_comment("vehicle_model", "", existing_comment=None, schema=None)
    op.alter_column(
        "user",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
        schema="tesla",
    )
    op.alter_column(
        "user_tokens",
        "id",
        existing_type=sa.UUID(),
        server_default=sa.text("gen_random_uuid()"),
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
        schema="tesla",
    )


def downgrade() -> None:
    op.alter_column(
        "user_tokens",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
        schema="tesla",
    )
    op.alter_column(
        "user",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
        schema="tesla",
    )
    op.drop_table_comment("vehicle_model", existing_comment="", schema=None)
    op.alter_column(
        "vehicle_model",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.drop_column("vehicle_model", "soh_oem_data")
    op.drop_column("vehicle_model", "soh_data")
    op.drop_column("vehicle_model", "odometer_data")
    op.drop_table_comment("vehicle_data", existing_comment="", schema=None)
    op.alter_column(
        "vehicle_data",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.drop_table_comment("vehicle_aggregate", existing_comment="", schema=None)
    op.alter_column(
        "vehicle_aggregate",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.alter_column(
        "vehicle",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.drop_table_comment("user_fleet", existing_comment="", schema=None)
    op.alter_column(
        "user_fleet",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.drop_table_comment("user", existing_comment="", schema=None)
    op.alter_column(
        "user",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.drop_table_comment("role", existing_comment="", schema=None)
    op.alter_column(
        "role",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.drop_table_comment("regional_aggregate", existing_comment="", schema=None)
    op.alter_column(
        "regional_aggregate",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.drop_table_comment("region", existing_comment="", schema=None)
    op.alter_column(
        "region",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.drop_table_comment("oem", existing_comment="", schema=None)
    op.alter_column(
        "oem",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.drop_table_comment("make", existing_comment="", schema=None)
    op.alter_column(
        "make",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.drop_table_comment("fleet_aggregate", existing_comment="", schema=None)
    op.alter_column(
        "fleet_aggregate",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.drop_table_comment("fleet", existing_comment="", schema=None)
    op.alter_column(
        "fleet",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.drop_table_comment("flash_report_combination", existing_comment="", schema=None)
    op.alter_column(
        "flash_report_combination",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.drop_table_comment("company", existing_comment="", schema=None)
    op.alter_column(
        "company",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.drop_table_comment("battery", existing_comment="", schema=None)
    op.alter_column(
        "battery",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.alter_column(
        "api_user_pricing",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.alter_column(
        "api_user",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.drop_table_comment("api_pricing_plan", existing_comment="", schema=None)
    op.alter_column(
        "api_pricing_plan",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.alter_column(
        "api_call_log",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
    op.alter_column(
        "api_billing",
        "id",
        existing_type=sa.UUID(),
        server_default=None,
        existing_comment="Unique identifier of the row",
        existing_nullable=False,
    )
