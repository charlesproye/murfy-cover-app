"""Ajout du statut readout et bib requested

Revision ID: 202601211145
Revises: 202601151051
Create Date: 2026-01-21 12:45:13.042419

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "202601211145"
down_revision = "202601211116"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "vehicle_model",
        "has_trendline_oem",
        existing_type=sa.BOOLEAN(),
        server_default=None,
        existing_comment="If the trendline is based on SoH calculated by Readout",
        existing_nullable=False,
    )

    op.alter_column(
        "vehicle_model",
        "has_trendline_bib",
        existing_type=sa.BOOLEAN(),
        server_default=None,
        existing_comment="If the trendline is based on SoH calculated by BIB",
        existing_nullable=False,
    )

    op.add_column(
        "vehicle",
        sa.Column("readout_report_requested_status", sa.BOOLEAN(), nullable=True),
    )
    op.add_column(
        "vehicle", sa.Column("bib_report_requested_status", sa.BOOLEAN(), nullable=True)
    )


def downgrade() -> None:
    op.alter_column(
        "vehicle_model",
        "has_trendline_oem",
        existing_type=sa.BOOLEAN(),
        server_default=sa.text("false"),
        existing_comment="If the trendline is based on SoH calculated by Readout",
        existing_nullable=False,
    )
    op.alter_column(
        "vehicle_model",
        "has_trendline_bib",
        existing_type=sa.BOOLEAN(),
        server_default=sa.text("false"),
        existing_comment="If the trendline is based on SoH calculated by BIB",
        existing_nullable=False,
    )

    op.drop_column("vehicle", "readout_report_requested_status")
    op.drop_column("vehicle", "bib_report_requested_status")
