"""Models for company-related tables"""

import uuid

from sqlalchemy import JSON, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from db_models.base_uuid_model import BaseUUIDCreatedAt, BaseUUIDModel


class Company(BaseUUIDCreatedAt):
    __tablename__ = "company"
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str | None] = mapped_column(String)


class Oem(BaseUUIDModel):
    __tablename__ = "oem"
    oem_name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str | None] = mapped_column(String)
    trendline: Mapped[dict | None] = mapped_column(JSON)
    trendline_min: Mapped[dict | None] = mapped_column(JSON)
    trendline_max: Mapped[dict | None] = mapped_column(JSON)


class Make(BaseUUIDCreatedAt):
    __tablename__ = "make"
    make_name: Mapped[str] = mapped_column(String(100), nullable=False)
    oem_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("oem.id"))
    description: Mapped[str | None] = mapped_column(String)
    image_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("asset.id"))
