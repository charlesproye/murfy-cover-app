"""Add useful constraints to Tesla tables

Revision ID: 4c1994ec12e8
Revises: efe1bdd234e9
Create Date: 2025-11-06 16:09:16.470239

"""

import sqlalchemy as sa
from alembic import op

from core.tesla.tesla_utils import TeslaRegions

# revision identifiers, used by Alembic.
revision = "4c1994ec12e8"
down_revision = "efe1bdd234e9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create the ENUM type first using values from TeslaRegions
    tesla_regions_enum = sa.Enum(
        TeslaRegions,
        name="tesla_regions",
        schema="tesla",
    )
    tesla_regions_enum.create(op.get_bind(), checkfirst=True)

    op.add_column(
        "user",
        sa.Column(
            "region",
            tesla_regions_enum,
            nullable=False,
            server_default=TeslaRegions.EUROPE,
        ),
        schema="tesla",
    )
    op.alter_column(
        "user",
        "email",
        existing_type=sa.VARCHAR(length=100),
        nullable=False,
        schema="tesla",
    )
    op.alter_column(
        "user",
        "vin",
        existing_type=sa.VARCHAR(length=50),
        type_=sa.String(length=17),
        nullable=False,
        schema="tesla",
    )
    op.create_unique_constraint(None, "user", ["email"], schema="tesla")
    op.create_unique_constraint(None, "user", ["vin"], schema="tesla")
    op.add_column(
        "user_tokens",
        sa.Column(
            "callback_url",
            sa.String(),
            nullable=True,
            comment="Callback URL for the user token, it must be the same one as the one used to generate the code",
        ),
        schema="tesla",
    )
    op.alter_column(
        "user_tokens",
        "code",
        existing_type=sa.VARCHAR(length=5000),
        type_=sa.String(length=63),
        nullable=False,
        schema="tesla",
    )
    op.alter_column(
        "user_tokens",
        "refresh_token",
        existing_type=sa.VARCHAR(length=5000),
        type_=sa.String(length=67),
        existing_nullable=True,
        schema="tesla",
    )
    op.create_unique_constraint(None, "user_tokens", ["code"], schema="tesla")


def downgrade() -> None:
    op.drop_constraint(None, "user_tokens", schema="tesla", type_="unique")
    op.alter_column(
        "user_tokens",
        "refresh_token",
        existing_type=sa.String(length=67),
        type_=sa.VARCHAR(length=5000),
        existing_nullable=True,
        schema="tesla",
    )
    op.alter_column(
        "user_tokens",
        "code",
        existing_type=sa.String(length=63),
        type_=sa.VARCHAR(length=5000),
        nullable=True,
        schema="tesla",
    )
    op.drop_column("user_tokens", "callback_url", schema="tesla")
    op.drop_constraint(None, "user", schema="tesla", type_="unique")
    op.drop_constraint(None, "user", schema="tesla", type_="unique")
    op.alter_column(
        "user",
        "vin",
        existing_type=sa.String(length=17),
        type_=sa.VARCHAR(length=50),
        nullable=True,
        schema="tesla",
    )
    op.alter_column(
        "user",
        "email",
        existing_type=sa.VARCHAR(length=100),
        nullable=True,
        schema="tesla",
    )
    op.drop_column("user", "region", schema="tesla")
