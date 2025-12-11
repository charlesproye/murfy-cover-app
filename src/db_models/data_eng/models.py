"""SQLAlchemy models for the data-engineering database."""

from sqlalchemy import Column, DateTime, Integer, Numeric, String, Text, text

from db_models.data_eng.base import DataEngBase


class DimActivationMetric(DataEngBase):
    __tablename__ = "dim_activation_metric"

    oem = Column(String(255), primary_key=True, nullable=False)
    price_per_month = Column(Numeric(10, 2), nullable=True)


class FctActivationMetric(DataEngBase):
    __tablename__ = "fct_activation_metric"

    date = Column("date", String(255), primary_key=True, nullable=False)
    nb_vehicles_activated = Column(Integer, nullable=True)
    oem = Column(String(255), primary_key=True, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class FctScrapedUnusableLinks(DataEngBase):
    __tablename__ = "fct_scraped_unusable_links"

    link = Column(String(255), primary_key=True, nullable=True)
    source = Column("source", String(255), primary_key=True, nullable=True)
    created_at = Column(DateTime, nullable=False, server_default=text("now()"))


class DataCatalog(DataEngBase):
    __tablename__ = "data_catalog"

    column_name = Column(String(255), primary_key=True, nullable=False)
    step = Column(String(100), primary_key=True, nullable=False)
    oem_name = Column(String(255), primary_key=True, nullable=False)
    type = Column("type", String(100), nullable=False)
    description = Column(Text, nullable=True)


__all__ = [
    "DataCatalog",
    "DimActivationMetric",
    "FctActivationMetric",
    "FctScrapedUnusableLinks",
]
