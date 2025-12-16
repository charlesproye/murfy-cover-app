"""Create make and soh_model tables

Revision ID: 202512111447
Revises: 202512111200
Create Date: 2025-12-11 15:47:29.469553

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "202512111447"
down_revision = "202512111200"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "make",
        sa.Column("make", sa.String(length=255), nullable=False),
        sa.PrimaryKeyConstraint("make"),
    )
    op.create_table(
        "soh_model",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("make", sa.String(length=255), nullable=False),
        sa.Column("model_name", sa.String(length=255), nullable=True),
        sa.Column("car_model_name", sa.String(length=255), nullable=True),
        sa.Column("model_uri", sa.String(length=2000), nullable=False),
        sa.Column("metrics", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.ForeignKeyConstraint(["make"], ["make.make"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("soh_model")
    op.drop_table("make")
