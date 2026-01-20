"""Chgt type trendline ajout trendlines oem et soh_bib

Revision ID: 202601151051
Revises: 202601141000
Create Date: 2026-01-15 11:51:56.628995

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "202601151051"
down_revision = "202601141000"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "oem",
        "trendline",
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        type_=sa.String(length=2000),
        existing_nullable=True,
    )
    op.alter_column(
        "oem",
        "trendline_min",
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        type_=sa.String(length=2000),
        existing_nullable=True,
    )
    op.alter_column(
        "oem",
        "trendline_max",
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        type_=sa.String(length=2000),
        existing_nullable=True,
    )
    op.add_column(
        "vehicle_data",
        sa.Column("soh_bib", sa.Numeric(precision=5, scale=3), nullable=True),
    )
    op.execute("UPDATE vehicle_data SET soh_bib = soh")
    op.drop_column("vehicle_data", "soh")
    op.add_column(
        "vehicle_model",
        sa.Column("trendline_bib_min", sa.String(length=2000), nullable=True),
    )
    op.add_column(
        "vehicle_model",
        sa.Column("trendline_bib_max", sa.String(length=2000), nullable=True),
    )
    op.add_column(
        "vehicle_model",
        sa.Column("trendline_oem", sa.String(length=2000), nullable=True),
    )
    op.add_column(
        "vehicle_model",
        sa.Column("trendline_oem_min", sa.String(length=2000), nullable=True),
    )
    op.add_column(
        "vehicle_model",
        sa.Column("trendline_oem_max", sa.String(length=2000), nullable=True),
    )
    op.add_column(
        "vehicle_model",
        sa.Column(
            "has_trendline_bib",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
            comment="If the trendline is based on SoH calculated by BIB",
        ),
    )
    op.add_column(
        "vehicle_model",
        sa.Column(
            "has_trendline_oem",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
            comment="If the trendline is based on SoH calculated by Readout",
        ),
    )
    op.alter_column(
        "vehicle_model",
        "trendline_bib",
        existing_type=sa.BOOLEAN(),
        type_=sa.String(length=2000),
        nullable=True,
        comment=None,
        existing_comment="If the trendline is based on SoH calculated by BIB",
    )
    op.drop_column("vehicle_model", "trendline_max")
    op.drop_column("vehicle_model", "trendline_min")
    op.drop_column("vehicle_model", "trendline")


def downgrade() -> None:
    op.add_column(
        "vehicle_model",
        sa.Column(
            "trendline",
            postgresql.JSON(astext_type=sa.Text()),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "vehicle_model",
        sa.Column(
            "trendline_min",
            postgresql.JSON(astext_type=sa.Text()),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "vehicle_model",
        sa.Column(
            "trendline_max",
            postgresql.JSON(astext_type=sa.Text()),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.alter_column(
        "vehicle_model",
        "trendline_bib",
        existing_type=sa.String(length=2000),
        type_=sa.BOOLEAN(),
        nullable=False,
        comment="If the trendline is based on SoH calculated by BIB",
    )
    op.drop_column("vehicle_model", "has_trendline_oem")
    op.drop_column("vehicle_model", "has_trendline_bib")
    op.drop_column("vehicle_model", "trendline_oem_max")
    op.drop_column("vehicle_model", "trendline_oem_min")
    op.drop_column("vehicle_model", "trendline_oem")
    op.drop_column("vehicle_model", "trendline_bib_max")
    op.drop_column("vehicle_model", "trendline_bib_min")
    op.add_column(
        "vehicle_data",
        sa.Column(
            "soh", sa.NUMERIC(precision=5, scale=3), autoincrement=False, nullable=True
        ),
    )
    op.drop_column("vehicle_data", "soh_bib")
    op.alter_column(
        "oem",
        "trendline_max",
        existing_type=sa.String(length=2000),
        type_=postgresql.JSON(astext_type=sa.Text()),
        existing_nullable=True,
    )
    op.alter_column(
        "oem",
        "trendline_min",
        existing_type=sa.String(length=2000),
        type_=postgresql.JSON(astext_type=sa.Text()),
        existing_nullable=True,
    )
    op.alter_column(
        "oem",
        "trendline",
        existing_type=sa.String(length=2000),
        type_=postgresql.JSON(astext_type=sa.Text()),
        existing_nullable=True,
    )
