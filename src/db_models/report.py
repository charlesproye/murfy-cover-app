"""Models for report-related tables"""

import uuid

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    func,
)
from sqlalchemy import Enum as SqlEnum
from sqlalchemy.orm import Mapped

from db_models.base_uuid_model import BaseUUIDModel
from db_models.enums import LanguageEnum


class FlashReportCombination(BaseUUIDModel):
    __tablename__ = "flash_report_combination"
    vin: str = Column(String, nullable=False)
    make: str = Column(String, nullable=False)
    model: str = Column(String, nullable=True)
    type: str = Column(String, nullable=True)
    version: str | None = Column(String, nullable=True)
    odometer: int = Column(Integer, nullable=True)
    token: str | None = Column(String, nullable=True)

    language: LanguageEnum = Column(
        SqlEnum(LanguageEnum, name="language_enum"),
        nullable=False,
        default=LanguageEnum.EN,
        server_default=LanguageEnum.EN,
    )


class PremiumReport(BaseUUIDModel):
    __tablename__ = "premium_report"
    vehicle_id: Mapped[uuid.UUID] = Column(ForeignKey("vehicle.id"), nullable=False)
    report_url: str = Column(String(2000), nullable=False)
    task_id: str | None = Column(String(255))
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
