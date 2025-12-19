"""Models for vehicle-related tables"""

import uuid
from datetime import datetime
from typing import ClassVar

from sqlalchemy import Column, DateTime, Enum, ForeignKey, String
from sqlalchemy.orm import Mapped

from core.tesla.tesla_utils import TeslaRegions
from db_models.base_uuid_model import BaseUUIDModel


class User(BaseUUIDModel):
    __tablename__ = "user"
    __table_args__: ClassVar = {"schema": "tesla"}

    full_name: str = Column(String(100))

    email: str = Column[str](String(100), nullable=False, unique=True)
    vin: str = Column[str](String(17), nullable=False, unique=True)

    region: TeslaRegions = Column(
        Enum(
            TeslaRegions,
            native_enum=True,
            create_constraint=True,
            name="tesla_regions",
            schema="tesla",
        ),
        nullable=False,
        default=TeslaRegions.EUROPE,
        server_default=TeslaRegions.EUROPE,
    )


class UserToken(BaseUUIDModel):
    __tablename__ = "user_tokens"
    __table_args__: ClassVar = {"schema": "tesla"}

    user_id: Mapped[uuid.UUID] = Column(ForeignKey("tesla.user.id"), nullable=False)
    code: Mapped[str] = Column(String, nullable=False, unique=True)
    access_token: str | None = Column[str](String(5000), nullable=True)
    refresh_token: str | None = Column[str](String(67), nullable=True)
    expires_at: datetime | None = Column[datetime](DateTime, nullable=True)
    callback_url: str = Column[str](
        String,
        comment="Callback URL for the user token, it must be the same one as the one used to generate the code",
    )
