"""Factories for Tesla schema models (separate schema)."""

from datetime import datetime, timedelta
from uuid import uuid4

from polyfactory import Use

from core.tesla.tesla_utils import TeslaRegions
from db_models.user_tokens import User as UserTesla
from db_models.user_tokens import UserToken
from tests.factories.base import BaseAsyncFactory


class UserTeslaFactory(BaseAsyncFactory[UserTesla]):
    __model__ = UserTesla

    full_name = "Tesla Test User"
    email = Use(lambda: f"tesla-{uuid4().hex[:8]}@example.com")
    vin = Use(lambda: f"5YJ3E1EA1KF{uuid4().hex[:6].upper()}")
    region = TeslaRegions.EUROPE


class UserTokenFactory(BaseAsyncFactory[UserToken]):
    __model__ = UserToken

    # user_id must be provided
    code = Use(lambda: f"tesla-code-{uuid4().hex}")
    access_token = Use(lambda: f"tesla-access-{uuid4().hex}")
    refresh_token = Use(lambda: f"tesla-refresh-{uuid4().hex[:67]}")
    expires_at = Use(lambda: datetime.now() + timedelta(hours=8))
    callback_url = "http://localhost:3000/callback"


__all__ = [
    "UserTeslaFactory",
    "UserTokenFactory",
]
