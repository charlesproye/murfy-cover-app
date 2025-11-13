"""Init DB

Revision ID: 202511071733
Revises:
Create Date: 2025-11-07 18:33:34.472314

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import ENUM

from core.tesla.tesla_utils import TeslaRegions
from db_models.enums import LanguageEnum

# revision identifiers, used by Alembic.
revision = "202511071733"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS tesla")
    op.execute("CREATE SCHEMA IF NOT EXISTS public")

    op.create_table(
        "api_pricing_plan",
        sa.Column("name", sa.String(length=50), nullable=False),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column(
            "requests_limit",
            sa.Integer(),
            nullable=False,
            comment="Limite quotidienne de requêtes API",
        ),
        sa.Column(
            "max_distinct_vins",
            sa.Integer(),
            nullable=False,
            comment="Nombre maximal de VINs distincts autorisés par jour",
        ),
        sa.Column(
            "price_per_request",
            sa.Numeric(precision=10, scale=4),
            nullable=False,
            comment="Prix par requête en euros",
        ),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_table(
        "battery",
        sa.Column("battery_name", sa.String(length=100), nullable=True),
        sa.Column("source", sa.String(length=100), nullable=True),
        sa.Column("battery_chemistry", sa.String(length=100), nullable=True),
        sa.Column("battery_oem", sa.String(length=100), nullable=True),
        sa.Column("capacity", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("net_capacity", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of the last update to this row",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of creation of this row",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "company",
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of the last update to this row",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of creation of this row",
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    language_enum = ENUM(LanguageEnum, name="language_enum", create_type=False)
    language_enum.create(op.get_bind(), checkfirst=True)
    op.create_table(
        "flash_report_combination",
        sa.Column("vin", sa.String(), nullable=False),
        sa.Column("make", sa.String(), nullable=False),
        sa.Column("model", sa.String(), nullable=False),
        sa.Column("type", sa.String(), nullable=False),
        sa.Column("version", sa.String(), nullable=True),
        sa.Column("odometer", sa.Integer(), nullable=False),
        sa.Column("token", sa.String(), nullable=False),
        sa.Column(
            "language", language_enum, nullable=False, server_default=LanguageEnum.EN
        ),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of the last update to this row",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of creation of this row",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("token"),
    )

    op.create_table(
        "oem",
        sa.Column("oem_name", sa.String(length=100), nullable=False),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("trendline", sa.JSON(), nullable=True),
        sa.Column("trendline_min", sa.JSON(), nullable=True),
        sa.Column("trendline_max", sa.JSON(), nullable=True),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of the last update to this row",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of creation of this row",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "region",
        sa.Column("region_name", sa.String(length=100), nullable=False),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of the last update to this row",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of creation of this row",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "role",
        sa.Column("role_name", sa.String(length=50), nullable=False),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of the last update to this row",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of creation of this row",
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    tesla_regions_enum = ENUM(
        TeslaRegions, name="tesla_regions", schema="tesla", create_type=False
    )
    tesla_regions_enum.create(op.get_bind(), checkfirst=True)
    op.create_table(
        "user",
        sa.Column("full_name", sa.String(length=100), nullable=True),
        sa.Column("email", sa.String(length=100), nullable=False),
        sa.Column("vin", sa.String(length=17), nullable=False),
        sa.Column(
            "region",
            tesla_regions_enum,
            nullable=False,
            server_default=TeslaRegions.EUROPE,
        ),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of the last update to this row",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of creation of this row",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("email"),
        sa.UniqueConstraint("vin"),
        schema="tesla",
    )
    op.create_table(
        "fleet",
        sa.Column("fleet_name", sa.String(length=100), nullable=False),
        sa.Column("company_id", sa.UUID(), nullable=False),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of the last update to this row",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of creation of this row",
        ),
        sa.ForeignKeyConstraint(
            ["company_id"],
            ["company.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "make",
        sa.Column("make_name", sa.String(length=100), nullable=False),
        sa.Column("oem_id", sa.UUID(), nullable=True),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of the last update to this row",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of creation of this row",
        ),
        sa.ForeignKeyConstraint(
            ["oem_id"],
            ["oem.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "regional_aggregate",
        sa.Column("region_id", sa.UUID(), nullable=False),
        sa.Column("avg_soh", sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column("avg_soc", sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column("avg_temperature", sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column("avg_voltage", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column(
            "energy_consumption", sa.Numeric(precision=10, scale=2), nullable=True
        ),
        sa.Column("timestamp", sa.DateTime(), nullable=True),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of the last update to this row",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of creation of this row",
        ),
        sa.ForeignKeyConstraint(
            ["region_id"],
            ["region.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "user_tokens",
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("code", sa.String(length=63), nullable=False),
        sa.Column("access_token", sa.String(length=5000), nullable=True),
        sa.Column("refresh_token", sa.String(length=67), nullable=True),
        sa.Column("expires_at", sa.DateTime(), nullable=True),
        sa.Column(
            "callback_url",
            sa.String(),
            nullable=True,
            comment="Callback URL for the user token, it must be the same one as the one used to generate the code",
        ),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of the last update to this row",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of creation of this row",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["tesla.user.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("code"),
        schema="tesla",
    )
    op.create_table(
        "user",
        sa.Column("company_id", sa.UUID(), nullable=False),
        sa.Column("first_name", sa.String(length=100), nullable=False),
        sa.Column("last_name", sa.String(length=100), nullable=True),
        sa.Column("last_connection", sa.DateTime(), nullable=True),
        sa.Column("email", sa.String(length=100), nullable=False),
        sa.Column("password", sa.String(length=100), nullable=True),
        sa.Column("phone", sa.String(length=20), nullable=True),
        sa.Column("role_id", sa.UUID(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=True),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of the last update to this row",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of creation of this row",
        ),
        sa.ForeignKeyConstraint(
            ["company_id"],
            ["company.id"],
        ),
        sa.ForeignKeyConstraint(
            ["role_id"],
            ["role.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("email"),
    )
    op.create_table(
        "api_billing",
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("period_start", sa.Date(), nullable=False),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("total_requests", sa.Integer(), nullable=False),
        sa.Column("distinct_vins", sa.Integer(), nullable=False),
        sa.Column("total_amount", sa.Numeric(precision=10, scale=2), nullable=False),
        sa.Column("invoice_number", sa.String(length=50), nullable=True),
        sa.Column("paid", sa.Boolean(), nullable=True),
        sa.Column("payment_date", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["user.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_api_billing_paid", "api_billing", ["paid"], unique=False)
    op.create_index(
        "ix_api_billing_period",
        "api_billing",
        ["period_start", "period_end"],
        unique=False,
    )
    op.create_index("ix_api_billing_user_id", "api_billing", ["user_id"], unique=False)
    op.create_table(
        "api_call_log",
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("vin", sa.String(length=50), nullable=False),
        sa.Column(
            "endpoint",
            sa.String(length=100),
            nullable=False,
            comment="Point d'accès appelé (ex: /vehicle/static)",
        ),
        sa.Column("timestamp", sa.DateTime(), nullable=False),
        sa.Column(
            "response_time",
            sa.Float(),
            nullable=True,
            comment="Temps de réponse en millisecondes",
        ),
        sa.Column(
            "status_code",
            sa.Integer(),
            nullable=True,
            comment="Code de statut HTTP de la réponse",
        ),
        sa.Column("is_billed", sa.Boolean(), nullable=False),
        sa.Column("billed_at", sa.DateTime(), nullable=True),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of the last update to this row",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of creation of this row",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["user.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_api_call_log_is_billed", "api_call_log", ["is_billed"], unique=False
    )
    op.create_index(
        "ix_api_call_log_timestamp", "api_call_log", ["timestamp"], unique=False
    )
    op.create_index(
        "ix_api_call_log_user_id", "api_call_log", ["user_id"], unique=False
    )
    op.create_index("ix_api_call_log_vin", "api_call_log", ["vin"], unique=False)
    op.create_table(
        "api_user",
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("api_key", sa.String(length=100), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("last_access", sa.DateTime(), nullable=True),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of the last update to this row",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["user.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("api_key"),
    )
    op.create_index("ix_api_user_api_key", "api_user", ["api_key"], unique=False)
    op.create_index("ix_api_user_user_id", "api_user", ["user_id"], unique=False)
    op.create_table(
        "api_user_pricing",
        sa.Column("user_id", sa.UUID(), nullable=True),
        sa.Column("pricing_plan_id", sa.UUID(), nullable=False),
        sa.Column(
            "custom_requests_limit",
            sa.Integer(),
            nullable=True,
            comment="Limite personnalisée qui remplace celle du plan si définie",
        ),
        sa.Column(
            "custom_max_distinct_vins",
            sa.Integer(),
            nullable=True,
            comment="Limite de VINs distincts personnalisée qui remplace celle du plan si définie",
        ),
        sa.Column(
            "custom_price_per_request",
            sa.Numeric(precision=10, scale=4),
            nullable=True,
            comment="Prix personnalisé qui remplace celui du plan si défini",
        ),
        sa.Column(
            "effective_date",
            sa.Date(),
            nullable=False,
            comment="Date d'entrée en vigueur de ce plan pour cet utilisateur",
        ),
        sa.Column(
            "expiration_date",
            sa.Date(),
            nullable=True,
            comment="Date d'expiration si applicable",
        ),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.ForeignKeyConstraint(
            ["pricing_plan_id"],
            ["api_pricing_plan.id"],
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["user.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_user_pricing_pricing_plan_id",
        "api_user_pricing",
        ["pricing_plan_id"],
        unique=False,
    )
    op.create_index(
        "ix_user_pricing_user_id", "api_user_pricing", ["user_id"], unique=False
    )
    op.create_table(
        "fleet_aggregate",
        sa.Column("fleet_id", sa.UUID(), nullable=False),
        sa.Column("avg_soh", sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column("avg_value", sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column(
            "avg_energy_consumption", sa.Numeric(precision=10, scale=2), nullable=True
        ),
        sa.Column("timestamp", sa.DateTime(), nullable=True),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of the last update to this row",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of creation of this row",
        ),
        sa.ForeignKeyConstraint(
            ["fleet_id"],
            ["fleet.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "user_fleet",
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("fleet_id", sa.UUID(), nullable=False),
        sa.Column("role_id", sa.UUID(), nullable=False),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of the last update to this row",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of creation of this row",
        ),
        sa.ForeignKeyConstraint(
            ["fleet_id"],
            ["fleet.id"],
        ),
        sa.ForeignKeyConstraint(
            ["role_id"],
            ["role.id"],
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["user.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "vehicle_model",
        sa.Column("model_name", sa.String(length=100), nullable=False),
        sa.Column("type", sa.String(length=50), nullable=True),
        sa.Column("version", sa.String(length=50), nullable=True),
        sa.Column("oem_id", sa.UUID(), nullable=True),
        sa.Column("make_id", sa.UUID(), nullable=True),
        sa.Column("battery_id", sa.UUID(), nullable=True),
        sa.Column("autonomy", sa.Integer(), nullable=True),
        sa.Column("url_image", sa.String(length=2000), nullable=True),
        sa.Column("warranty_date", sa.Integer(), nullable=True),
        sa.Column("warranty_km", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("source", sa.String(length=100), nullable=True),
        sa.Column("trendline", sa.JSON(), nullable=True),
        sa.Column("trendline_min", sa.JSON(), nullable=True),
        sa.Column("trendline_max", sa.JSON(), nullable=True),
        sa.Column(
            "trendline_bib",
            sa.Boolean(),
            nullable=True,
            comment="If the trendline is based on SoH calculated by BIB",
        ),
        sa.Column("odometer_data", sa.Boolean(), nullable=True),
        sa.Column("soh_data", sa.Boolean(), nullable=True),
        sa.Column("soh_oem_data", sa.Boolean(), nullable=True),
        sa.Column(
            "commissioning_date",
            sa.DateTime(),
            nullable=True,
            comment="First time seen on the market",
        ),
        sa.Column(
            "end_of_life_date",
            sa.DateTime(),
            nullable=True,
            comment="Last time seen on the market",
        ),
        sa.Column("expected_consumption", sa.Integer(), nullable=True),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of the last update to this row",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of creation of this row",
        ),
        sa.ForeignKeyConstraint(
            ["battery_id"],
            ["battery.id"],
        ),
        sa.ForeignKeyConstraint(
            ["make_id"],
            ["make.id"],
        ),
        sa.ForeignKeyConstraint(
            ["oem_id"],
            ["oem.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "vehicle",
        sa.Column("fleet_id", sa.UUID(), nullable=False),
        sa.Column("region_id", sa.UUID(), nullable=False),
        sa.Column("vehicle_model_id", sa.UUID(), nullable=False),
        sa.Column("vin", sa.String(length=50), nullable=True),
        sa.Column("activation_status", sa.Boolean(), nullable=True),
        sa.Column("is_eligible", sa.Boolean(), nullable=True),
        sa.Column("is_pinned", sa.Boolean(), nullable=True),
        sa.Column("start_date", sa.Date(), nullable=True),
        sa.Column("licence_plate", sa.String(length=50), nullable=True),
        sa.Column("end_of_contract_date", sa.Date(), nullable=True),
        sa.Column("last_date_data", sa.Date(), nullable=True),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of the last update to this row",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of creation of this row",
        ),
        sa.ForeignKeyConstraint(
            ["fleet_id"],
            ["fleet.id"],
        ),
        sa.ForeignKeyConstraint(
            ["region_id"],
            ["region.id"],
        ),
        sa.ForeignKeyConstraint(
            ["vehicle_model_id"],
            ["vehicle_model.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_vehicle_fleet_id", "vehicle", ["fleet_id"], unique=False)
    op.create_index(
        "ix_vehicle_model_id", "vehicle", ["vehicle_model_id"], unique=False
    )
    op.create_index("ix_vehicle_region_id", "vehicle", ["region_id"], unique=False)
    op.create_index("ix_vehicle_vin", "vehicle", ["vin"], unique=False)
    op.create_table(
        "vehicle_aggregate",
        sa.Column("vehicle_model_id", sa.UUID(), nullable=False),
        sa.Column("avg_soh", sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column(
            "energy_consumption", sa.Numeric(precision=10, scale=2), nullable=True
        ),
        sa.Column("timestamp", sa.DateTime(), nullable=True),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of the last update to this row",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of creation of this row",
        ),
        sa.ForeignKeyConstraint(
            ["vehicle_model_id"],
            ["vehicle_model.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "vehicle_data",
        sa.Column("vehicle_id", sa.UUID(), nullable=False),
        sa.Column("odometer", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("region", sa.String(length=100), nullable=True),
        sa.Column("speed", sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column("location", sa.String(length=100), nullable=True),
        sa.Column("soh", sa.Numeric(precision=5, scale=3), nullable=True),
        sa.Column("cycles", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("consumption", sa.Numeric(precision=5, scale=3), nullable=True),
        sa.Column("soh_comparison", sa.Numeric(precision=6, scale=3), nullable=True),
        sa.Column("timestamp", sa.DateTime(), nullable=True),
        sa.Column(
            "level_1",
            sa.Numeric(precision=6, scale=2),
            nullable=True,
            comment="Level 1 of charging. Corresponds to charging in the range 1.4-1.9 kW, 120V, AC, 12-16 Ah",
        ),
        sa.Column(
            "level_2",
            sa.Numeric(precision=6, scale=2),
            nullable=True,
            comment="Level 2 of charging. Corresponds to charging in the range 1.9.3-19.2 kW, 208V, AC, 32-64 Ah",
        ),
        sa.Column(
            "level_3",
            sa.Numeric(precision=6, scale=2),
            nullable=True,
            comment="Level 3 of charging. Corresponds to charging in the range > 50kW, DC",
        ),
        sa.Column("soh_oem", sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
            comment="Unique identifier of the row",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of the last update to this row",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
            comment="Date of creation of this row",
        ),
        sa.ForeignKeyConstraint(
            ["vehicle_id"],
            ["vehicle.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("vehicle_data")
    op.drop_table("vehicle_aggregate")
    op.drop_index("ix_vehicle_vin", table_name="vehicle")
    op.drop_index("ix_vehicle_region_id", table_name="vehicle")
    op.drop_index("ix_vehicle_model_id", table_name="vehicle")
    op.drop_index("ix_vehicle_fleet_id", table_name="vehicle")
    op.drop_table("vehicle")
    op.drop_table("vehicle_model")
    op.drop_table("user_fleet")
    op.drop_table("fleet_aggregate")
    op.drop_index("ix_user_pricing_user_id", table_name="api_user_pricing")
    op.drop_index("ix_user_pricing_pricing_plan_id", table_name="api_user_pricing")
    op.drop_table("api_user_pricing")
    op.drop_index("ix_api_user_user_id", table_name="api_user")
    op.drop_index("ix_api_user_api_key", table_name="api_user")
    op.drop_table("api_user")
    op.drop_index("ix_api_call_log_vin", table_name="api_call_log")
    op.drop_index("ix_api_call_log_user_id", table_name="api_call_log")
    op.drop_index("ix_api_call_log_timestamp", table_name="api_call_log")
    op.drop_index("ix_api_call_log_is_billed", table_name="api_call_log")
    op.drop_table("api_call_log")
    op.drop_index("ix_api_billing_user_id", table_name="api_billing")
    op.drop_index("ix_api_billing_period", table_name="api_billing")
    op.drop_index("ix_api_billing_paid", table_name="api_billing")
    op.drop_table("api_billing")
    op.drop_table("user")
    op.drop_table("user_tokens", schema="tesla")
    op.drop_table("regional_aggregate")
    op.drop_table("make")
    op.drop_table("fleet")
    op.drop_table("user", schema="tesla")
    op.drop_table("role")
    op.drop_table("region")
    op.drop_table("oem")
    op.drop_table("flash_report_combination")
    op.drop_table("company")
    op.drop_table("battery")
    op.drop_table("api_pricing_plan")
