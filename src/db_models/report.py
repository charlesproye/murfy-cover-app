"""Models for report-related tables"""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, func
from sqlalchemy import Enum as SqlEnum
from sqlalchemy.orm import Mapped, mapped_column

from db_models.base_uuid_model import BaseUUID, BaseUUIDCreatedAt, BaseUUIDModel
from db_models.enums import LanguageEnum


class FlashReportCombination(BaseUUIDCreatedAt):
    __tablename__ = "flash_report_combination"
    vin: Mapped[str] = mapped_column(String, nullable=False)
    make: Mapped[str] = mapped_column(String, nullable=False)
    model: Mapped[str | None] = mapped_column(String)
    type: Mapped[str | None] = mapped_column(String)
    version: Mapped[str | None] = mapped_column(String)
    odometer: Mapped[int | None] = mapped_column(Integer)
    token: Mapped[str | None] = mapped_column(String)

    language: Mapped[LanguageEnum] = mapped_column(
        SqlEnum(LanguageEnum, name="language_enum"),
        nullable=False,
        default=LanguageEnum.EN,
        server_default=LanguageEnum.EN,
    )


class PremiumReport(BaseUUIDModel):
    __tablename__ = "premium_report"
    vehicle_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("vehicle.id"), nullable=False
    )
    report_url: Mapped[str] = mapped_column(String(2000), nullable=False)
    task_id: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    # Unique constraint: one report per vehicle per day (UTC date)
    __table_args__ = (
        Index(
            "ix_premium_report_vehicle_date_unique",
            vehicle_id,
            func.date(created_at),
            unique=True,
        ),
    )
