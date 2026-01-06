"""Models for fleet-related tables"""

from sqlalchemy import Enum as SqlEnum
from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from db_models.base_uuid_model import BaseUUID
from db_models.enums import AssetTypeEnum


class Asset(BaseUUID):
    __tablename__ = "asset"
    name: Mapped[str] = mapped_column(String, nullable=False)
    public_url: Mapped[str | None] = mapped_column(String)
    type: Mapped[AssetTypeEnum] = mapped_column(
        SqlEnum(AssetTypeEnum, name="asset_type_enum"),
        nullable=False,
    )
