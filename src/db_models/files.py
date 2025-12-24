"""Models for fleet-related tables"""

from sqlalchemy import (
    Column,
    String,
)
from sqlalchemy import Enum as SqlEnum

from db_models.base_uuid_model import BaseUUID
from db_models.enums import AssetTypeEnum


class Asset(BaseUUID):
    __tablename__ = "asset"
    name: str = Column(String, nullable=False)
    public_url: str = Column(String, nullable=True)
    type: AssetTypeEnum = Column(
        SqlEnum(AssetTypeEnum, name="asset_type_enum"),
        nullable=False,
    )
