"""Models for fleet-related tables"""

import uuid

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from db_models.base_uuid_model import BaseUUIDCreatedAt


class Fleet(BaseUUIDCreatedAt):
    __tablename__ = "fleet"
    fleet_name: Mapped[str] = mapped_column(String(100), nullable=False)
    company_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("company.id"), nullable=False
    )


class UserFleet(BaseUUIDCreatedAt):
    __tablename__ = "user_fleet"
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("user.id"), nullable=False)
    fleet_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("fleet.id"), nullable=False)
    role_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("role.id"), nullable=False)
