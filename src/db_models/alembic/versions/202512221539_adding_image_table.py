"""adding image table

Revision ID: 202512221539
Revises: 202512191515
Create Date: 2025-12-22 16:39:14.810280

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import ENUM

from db_models.enums import AssetTypeEnum

# revision identifiers, used by Alembic.
revision = "202512221539"
down_revision = "202512191515"
branch_labels = None
depends_on = None


def upgrade() -> None:
    asset_type_enum = ENUM(AssetTypeEnum, name="asset_type_enum", create_type=False)
    asset_type_enum.create(op.get_bind(), checkfirst=True)
    op.create_table(
        "asset",
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("type", asset_type_enum, nullable=False),
        sa.Column("public_url", sa.String(), nullable=True),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("make") as batch_op:
        batch_op.add_column(sa.Column("image_id", sa.UUID(), nullable=True))
        batch_op.create_foreign_key(
            "fk_make_image_id_asset", "asset", ["image_id"], ["id"]
        )
    op.add_column("vehicle_model", sa.Column("image_id", sa.UUID(), nullable=True))
    op.create_foreign_key(None, "vehicle_model", "asset", ["image_id"], ["id"])
    op.drop_column("vehicle_model", "url_image")


def downgrade() -> None:
    op.add_column(
        "vehicle_model",
        sa.Column(
            "url_image", sa.VARCHAR(length=2000), autoincrement=False, nullable=True
        ),
    )
    op.drop_constraint(
        "vehicle_model_image_id_fkey", "vehicle_model", type_="foreignkey"
    )
    op.drop_column("vehicle_model", "image_id")
    op.drop_constraint("make_image_id_fkey", "make", type_="foreignkey")
    op.drop_column("make", "image_id")
    op.drop_table("asset")
    op.execute("DROP TYPE IF EXISTS asset_type_enum")
