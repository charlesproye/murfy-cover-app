"""add_unique_constraint_vehicle_date_premium_report

Revision ID: 202601061021
Revises: 202601020944
Create Date: 2026-01-06 11:21:37.646777

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "202601061021"
down_revision = "202601020944"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_premium_report_vehicle_date_unique",
        "premium_report",
        ["vehicle_id", sa.literal_column("date(created_at)")],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_premium_report_vehicle_date_unique", table_name="premium_report")
