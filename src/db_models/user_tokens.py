"""Models for vehicle-related tables"""

import uuid
from datetime import datetime
from typing import ClassVar

from sqlalchemy import DateTime, Enum, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from core.tesla.tesla_utils import TeslaRegions
from db_models.base_uuid_model import BaseUUIDModel


class User(BaseUUIDModel):
    __tablename__ = "user"
    __table_args__: ClassVar = {"schema": "tesla"}

    full_name: Mapped[str | None] = mapped_column(String(100))

    email: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    vin: Mapped[str] = mapped_column(String(17), nullable=False, unique=True)

    region: Mapped[TeslaRegions] = mapped_column(
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

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tesla.user.id"), nullable=False
    )
    code: Mapped[str] = mapped_column(String(2000), nullable=False, unique=True)
    access_token: Mapped[str | None] = mapped_column(String(5000))
    refresh_token: Mapped[str | None] = mapped_column(String(67))
    expires_at: Mapped[datetime | None] = mapped_column(DateTime)
    callback_url: Mapped[str] = mapped_column(
        String,
        comment="Callback URL for the user token, it must be the same one as the one used to generate the code",
    )
