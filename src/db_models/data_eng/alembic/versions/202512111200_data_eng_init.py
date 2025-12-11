"""Initial schema for data-engineering database."""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "202512111200"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dim_activation_metric",
        sa.Column("oem", sa.String(length=255), nullable=False, primary_key=True),
        sa.Column("price_per_month", sa.Numeric(10, 2), nullable=True),
    )

    op.create_table(
        "fct_activation_metric",
        sa.Column("date", sa.String(length=255), nullable=False),
        sa.Column("nb_vehicles_activated", sa.Integer(), nullable=True),
        sa.Column("oem", sa.String(length=255), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("date", "oem"),
    )

    op.create_table(
        "fct_scraped_unusable_links",
        sa.Column("link", sa.String(length=255), nullable=True),
        sa.Column("source", sa.String(length=255), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.PrimaryKeyConstraint("link", "source"),
    )

    op.create_table(
        "data_catalog",
        sa.Column("column_name", sa.String(length=255), nullable=False),
        sa.Column("type", sa.String(length=100), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("oem_name", sa.String(length=255), nullable=False),
        sa.Column("step", sa.String(length=100), nullable=False),
        sa.PrimaryKeyConstraint("column_name", "step", "oem_name"),
    )


def downgrade() -> None:
    op.drop_table("data_catalog")
    op.drop_table("fct_scraped_unusable_links")
    op.drop_table("fct_activation_metric")
    op.drop_table("dim_activation_metric")
