"""Models for fleet-related tables"""

import uuid

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Numeric,
    String,
    func,
)
from sqlalchemy.orm import Mapped

from db_models.base_uuid_model import BaseUUIDModel


class Fleet(BaseUUIDModel):
    __tablename__ = "fleet"
    fleet_name: str = Column(String(100), nullable=False)
    company_id: Mapped[uuid.UUID] = Column(ForeignKey("company.id"), nullable=False)


class FleetAggregate(BaseUUIDModel):
    __tablename__ = "fleet_aggregate"
    fleet_id: Mapped[uuid.UUID] = Column(ForeignKey("fleet.id"), nullable=False)
    avg_soh = Column(Numeric(5, 2))
    avg_value = Column(Numeric(5, 2))
    avg_energy_consumption = Column(Numeric(10, 2))
    timestamp = Column(DateTime, server_default=func.now())


class UserFleet(BaseUUIDModel):
    __tablename__ = "user_fleet"
    user_id: Mapped[uuid.UUID] = Column(ForeignKey("user.id"), nullable=False)
    fleet_id: Mapped[uuid.UUID] = Column(ForeignKey("fleet.id"), nullable=False)
    role_id: Mapped[uuid.UUID] = Column(ForeignKey("role.id"), nullable=False)
