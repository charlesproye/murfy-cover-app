"""Models for user-related tables"""

import uuid

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    String,
)
from sqlalchemy.orm import Mapped

from db_models.base_uuid_model import BaseUUIDModel


class Role(BaseUUIDModel):
    __tablename__ = "role"
    role_name: str = Column(String(50), nullable=False)


class User(BaseUUIDModel):
    __tablename__ = "user"
    company_id: Mapped[uuid.UUID] = Column(ForeignKey("company.id"), nullable=False)
    first_name: str = Column(String(100), nullable=False)
    last_name: str = Column(String(100))
    last_connection = Column(DateTime)
    email: str = Column(String(100), nullable=False, unique=True)
    password: str = Column(String(100))
    phone: str = Column(String(20))
    role_id: Mapped[uuid.UUID] = Column(ForeignKey("role.id"))
    is_active: bool = Column(Boolean, default=True)
