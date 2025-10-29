from datetime import UTC, datetime, timedelta

import jwt
from pydantic import Field
from pydantic_settings import BaseSettings


class JwtSettings(BaseSettings):
    JWT_SECRET: str = Field(default=...)
    JWT_ALGORITHM: str = Field(default="HS256")


settings = JwtSettings()


def create_access_token(client_id: str) -> str:
    expire = datetime.now(UTC) + timedelta(minutes=60)
    payload = {"sub": client_id, "exp": expire}
    token = jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)
    return token


def verify_token(token: str) -> bool:
    try:
        jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM])
        return True
    except jwt.ExpiredSignatureError:
        return False
    except jwt.InvalidTokenError:
        return False

