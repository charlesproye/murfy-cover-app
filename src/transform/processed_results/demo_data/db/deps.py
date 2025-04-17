from typing import AsyncGenerator, List
from uuid import UUID

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import jwt
from pydantic import ValidationError
from redis.asyncio import Redis, from_url
from sqlmodel.ext.asyncio.session import AsyncSession

import os
from .session import data_session

async def get_db_by_schema(schema: str = None) -> AsyncGenerator[AsyncSession, None]:
    session = await data_session(schema)
    try:
        yield session
    finally:
        await session.close()
