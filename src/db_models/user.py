"""Models for user-related tables"""

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from db_models.base_uuid_model import BaseUUIDCreatedAt, BaseUUIDModel


class Role(BaseUUIDCreatedAt):
    __tablename__ = "role"
    role_name: Mapped[str] = mapped_column(String(50), nullable=False)


class User(BaseUUIDModel):
    __tablename__ = "user"
    company_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("company.id"), nullable=False
    )
    first_name: Mapped[str] = mapped_column(String(100), nullable=False)
    last_name: Mapped[str | None] = mapped_column(String(100))
    last_connection: Mapped[datetime | None] = mapped_column(DateTime)
    email: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    password: Mapped[str | None] = mapped_column(String(100))
    phone: Mapped[str | None] = mapped_column(String(20))
    role_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("role.id"))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
