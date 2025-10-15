"""add tesla into prod

Revision ID: 0e3f4f860efb
Revises: 19a50bb96a7d
Create Date: 2025-02-05 14:38:49.249080

"""

import uuid

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

# revision identifiers, used by Alembic.
revision = "0e3f4f860efb"
down_revision = "19a50bb96a7d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create tesla schema if it doesn't exist
    op.execute("CREATE SCHEMA IF NOT EXISTS tesla")

    # Create user table in tesla schema
    op.create_table(
        "user",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
        sa.Column("user_id", UUID(as_uuid=True), nullable=False),
        sa.Column("full_name", sa.String(100)),
        sa.Column("email", sa.String(100)),
        sa.Column("vin", sa.String(50)),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("CURRENT_TIMESTAMP")
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("CURRENT_TIMESTAMP")
        ),
        schema="tesla",
    )

    # Create user_tokens table in tesla schema
    op.create_table(
        "user_tokens",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
        sa.Column("user_id", UUID(as_uuid=True), nullable=False),
        sa.Column("code", sa.String(5000)),
        sa.Column("access_token", sa.String(5000)),
        sa.Column("refresh_token", sa.String(5000)),
        sa.Column("expires_at", sa.DateTime()),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("CURRENT_TIMESTAMP")
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("CURRENT_TIMESTAMP")
        ),
        schema="tesla",
    )


def downgrade() -> None:
    # Drop tables in tesla schema
    op.drop_table("user_tokens", schema="tesla")
    op.drop_table("user", schema="tesla")

    # Drop tesla schema
    op.execute("DROP SCHEMA IF EXISTS tesla")

