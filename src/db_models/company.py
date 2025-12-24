"""Models for company-related tables"""

import uuid

from sqlalchemy import (
    JSON,
    Column,
    ForeignKey,
    String,
)
from sqlalchemy.orm import Mapped

from db_models.base_uuid_model import BaseUUIDCreatedAt, BaseUUIDModel


class Company(BaseUUIDCreatedAt):
    __tablename__ = "company"
    name: str = Column(String(100), nullable=False)
    description: str = Column(String, nullable=True)


class Oem(BaseUUIDModel):
    __tablename__ = "oem"
    oem_name: str = Column(String(100), nullable=False)
    description: str = Column(String)
    trendline = Column(JSON)
    trendline_min = Column(JSON)
    trendline_max = Column(JSON)


class Make(BaseUUIDCreatedAt):
    __tablename__ = "make"
    make_name: str = Column(String(100), nullable=False)
    oem_id: Mapped[uuid.UUID] = Column(ForeignKey("oem.id"))
    description: str = Column(String)
    image_id: Mapped[uuid.UUID] = Column(ForeignKey("asset.id"))
