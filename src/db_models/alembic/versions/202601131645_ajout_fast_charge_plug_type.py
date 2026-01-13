"""Ajout fast charge plug type

Revision ID: 202601131645
Revises: 202601061543
Create Date: 2026-01-13 17:45:00.576744

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "202601131645"
down_revision = "202601061543"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "vehicle_model",
        sa.Column("fast_charge_plug_type", sa.String(length=100), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("vehicle_model", "fast_charge_plug_type")
