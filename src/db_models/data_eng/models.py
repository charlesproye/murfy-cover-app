"""SQLAlchemy models for the data-engineering database."""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import DateTime, ForeignKey, Integer, Numeric, String, Text, text
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import Mapped, mapped_column

from db_models.data_eng.base import DataEngBase


class DimActivationMetric(DataEngBase):
    __tablename__ = "dim_activation_metric"

    oem: Mapped[str] = mapped_column(String(255), primary_key=True)
    price_per_month: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))


class FctActivationMetric(DataEngBase):
    __tablename__ = "fct_activation_metric"

    date: Mapped[str] = mapped_column("date", String(255), primary_key=True)
    nb_vehicles_activated: Mapped[int | None] = mapped_column(Integer)
    oem: Mapped[str] = mapped_column(String(255), primary_key=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime)


class FctScrapedUnusableLinks(DataEngBase):
    __tablename__ = "fct_scraped_unusable_links"

    link: Mapped[str] = mapped_column(String(255), primary_key=True)
    source: Mapped[str] = mapped_column("source", String(255), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("now()"))


class DataCatalog(DataEngBase):
    __tablename__ = "data_catalog"

    column_name: Mapped[str] = mapped_column(String(255), primary_key=True)
    step: Mapped[str] = mapped_column(String(100), primary_key=True)
    oem_name: Mapped[str] = mapped_column(String(255), primary_key=True)
    type: Mapped[str] = mapped_column("type", String(100))
    description: Mapped[str | None] = mapped_column(Text)


class Make(DataEngBase):
    __tablename__ = "make"
    make: Mapped[str] = mapped_column(String(255), primary_key=True)


class SoHModel(DataEngBase):
    __tablename__ = "soh_model"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    make: Mapped[str] = mapped_column(
        String(255), ForeignKey("make.make", ondelete="CASCADE")
    )
    model_name: Mapped[str | None] = mapped_column(String(255))
    car_model_name: Mapped[str | None] = mapped_column(String(255))
    model_uri: Mapped[str] = mapped_column(String(2000))
    metrics: Mapped[dict | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=text("now()"))


__all__ = [
    "DataCatalog",
    "DimActivationMetric",
    "FctActivationMetric",
    "FctScrapedUnusableLinks",
    "Make",
    "SoHModel",
]
