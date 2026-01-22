"""Link Flash report Combination to Report

Revision ID: 202601211116
Revises: 202601151051
Create Date: 2026-01-21 12:16:16.467618

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "202601211116"
down_revision = "202601151051"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "report", sa.Column("flash_report_combination_id", sa.UUID(), nullable=True)
    )
    op.alter_column("report", "vehicle_id", existing_type=sa.UUID(), nullable=True)
    op.drop_index(
        op.f("ix_premium_report_vehicle_date_type_unique"), table_name="report"
    )
    op.create_index(
        "ix_premium_report_vehicle_date_type_unique",
        "report",
        [
            "vehicle_id",
            "flash_report_combination_id",
            sa.literal_column("date(created_at)"),
            "report_type",
        ],
        unique=True,
    )
    op.create_foreign_key(
        "report_flash_report_combination_id_fkey",
        "report",
        "flash_report_combination",
        ["flash_report_combination_id"],
        ["id"],
    )
    op.execute("ALTER TYPE reporttype ADD VALUE IF NOT EXISTS 'flash';")


def downgrade() -> None:
    op.drop_constraint(
        "report_flash_report_combination_id_fkey", "report", type_="foreignkey"
    )
    op.drop_index("ix_premium_report_vehicle_date_type_unique", table_name="report")
    op.create_index(
        op.f("ix_premium_report_vehicle_date_type_unique"),
        "report",
        ["vehicle_id", sa.literal_column("date(created_at)"), "report_type"],
        unique=True,
    )
    op.alter_column("report", "vehicle_id", existing_type=sa.UUID(), nullable=False)
    op.drop_column("report", "flash_report_combination_id")
