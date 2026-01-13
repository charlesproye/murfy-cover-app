"""Rename premium_report to report

Revision ID: 202601131700
Revises: 202601131645
Create Date: 2026-01-13 16:15:50.688661

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "202601131700"
down_revision = "202601131645"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "report",
        sa.Column("vehicle_id", sa.UUID(), nullable=False),
        sa.Column("report_url", sa.String(length=2000), nullable=False),
        sa.Column(
            "report_type",
            sa.Enum("premium", "readout", name="reporttype"),
            nullable=False,
        ),
        sa.Column("task_id", sa.String(length=255), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.ForeignKeyConstraint(
            ["vehicle_id"],
            ["vehicle.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_premium_report_vehicle_date_type_unique",
        "report",
        ["vehicle_id", sa.literal_column("date(created_at)"), "report_type"],
        unique=True,
    )
    # Migrate data from premium_report to report
    op.execute(
        """
        INSERT INTO report (id, vehicle_id, report_url, report_type, task_id, created_at, updated_at)
        SELECT id, vehicle_id, report_url, 'premium'::reporttype, task_id, created_at, updated_at
        FROM premium_report
        """
    )
    op.drop_index(
        op.f("ix_premium_report_vehicle_date_unique"), table_name="premium_report"
    )
    op.drop_table("premium_report")


def downgrade() -> None:
    op.create_table(
        "premium_report",
        sa.Column("vehicle_id", sa.UUID(), autoincrement=False, nullable=False),
        sa.Column(
            "report_url", sa.VARCHAR(length=2000), autoincrement=False, nullable=False
        ),
        sa.Column(
            "task_id", sa.VARCHAR(length=255), autoincrement=False, nullable=True
        ),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(),
            server_default=sa.text("now()"),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(),
            server_default=sa.text("now()"),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            autoincrement=False,
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.ForeignKeyConstraint(
            ["vehicle_id"], ["vehicle.id"], name=op.f("premium_report_vehicle_id_fkey")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("premium_report_pkey")),
    )
    op.create_index(
        op.f("ix_premium_report_vehicle_date_unique"),
        "premium_report",
        ["vehicle_id", sa.literal_column("date(created_at)")],
        unique=True,
    )
    # Migrate data from report (premium type only) back to premium_report
    op.execute(
        """
        INSERT INTO premium_report (id, vehicle_id, report_url, task_id, created_at, updated_at)
        SELECT id, vehicle_id, report_url, task_id, created_at, updated_at
        FROM report
        WHERE report_type = 'premium'
        """
    )
    op.drop_index("ix_premium_report_vehicle_date_type_unique", table_name="report")
    op.drop_table("report")
    op.execute("DROP TYPE reporttype")
