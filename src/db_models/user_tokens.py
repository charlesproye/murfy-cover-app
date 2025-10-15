"""Models for vehicle-related tables"""

import uuid
from typing import ClassVar

from sqlalchemy import Column, DateTime, ForeignKey, String
from sqlalchemy.orm import Mapped

from db_models.base_uuid_model import BaseUUIDModel


class User(BaseUUIDModel):
    __tablename__ = "user"
    __table_args__: ClassVar = {"schema": "tesla"}

    full_name: str = Column(String(100))
    email: str = Column(String(100))
    vin: str = Column(String(50))


class UserToken(BaseUUIDModel):
    __tablename__ = "user_tokens"
    __table_args__: ClassVar = {"schema": "tesla"}

    user_id: Mapped[uuid.UUID] = Column(ForeignKey("tesla.user.id"), nullable=False)
    code: str = Column(String(5000))
    access_token: str = Column(String(5000))
    refresh_token: str = Column(String(5000))
    expires_at = Column(DateTime)

