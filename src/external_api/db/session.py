from __future__ import annotations

import logging
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel.ext.asyncio.session import AsyncSession

from external_api.core.config import settings

logger = logging.getLogger(__name__)

# Configuration des pools de connexion
POOL_SIZE = settings.POOL_SIZE
MAX_OVERFLOW = settings.MAX_OVERFLOW
POOL_TIMEOUT = settings.POOL_TIMEOUT
POOL_RECYCLE = settings.POOL_RECYCLE

# Instance d'engine pour la base de données
_engine: AsyncEngine | None = None
_session_maker = None


def get_engine() -> AsyncEngine:
    """Get or create the database engine."""
    global _engine
    if _engine is None:
        _engine = create_async_engine(
            settings.ASYNC_DB_DATA_EV_URI,
            echo=False,
            future=True,
            pool_size=POOL_SIZE,
            max_overflow=MAX_OVERFLOW,
            pool_timeout=30,
            pool_pre_ping=True,
            pool_recycle=1800,
            connect_args={"server_settings": {"search_path": "public"}},
        )
    return _engine


def get_session_maker():
    """Get or create the session maker."""
    global _session_maker
    if _session_maker is None:
        engine = get_engine()
        _session_maker = sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autocommit=False,
            autoflush=False,
        )
    return _session_maker


# @asynccontextmanager
async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Get a database session.
    To be used as a FastAPI dependency.
    """
    session_maker = get_session_maker()
    session = session_maker()
    try:
        yield session
    finally:
        await session.close()

