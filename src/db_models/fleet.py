"""Models for fleet-related tables"""

import uuid

from sqlalchemy import (
    Column,
    ForeignKey,
    String,
)
from sqlalchemy.orm import Mapped

from db_models.base_uuid_model import BaseUUIDCreatedAt


class Fleet(BaseUUIDCreatedAt):
    __tablename__ = "fleet"
    fleet_name: str = Column(String(100), nullable=False)
    company_id: Mapped[uuid.UUID] = Column(ForeignKey("company.id"), nullable=False)


class UserFleet(BaseUUIDCreatedAt):
    __tablename__ = "user_fleet"
    user_id: Mapped[uuid.UUID] = Column(ForeignKey("user.id"), nullable=False)
    fleet_id: Mapped[uuid.UUID] = Column(ForeignKey("fleet.id"), nullable=False)
    role_id: Mapped[uuid.UUID] = Column(ForeignKey("role.id"), nullable=False)
