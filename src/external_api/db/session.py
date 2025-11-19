from __future__ import annotations

import logging
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.ext.asyncio.session import AsyncSession
from sqlalchemy.orm import sessionmaker

from db_models.core.config import db_settings

logger = logging.getLogger(__name__)


# Instance d'engine pour la base de données
_engine: AsyncEngine | None = None
_session_maker = None


def get_engine() -> AsyncEngine:
    """Get or create the database engine."""
    global _engine
    if _engine is None:
        _engine = create_async_engine(
            db_settings.ASYNC_DB_DATA_EV_URI,
            echo=False,
            future=True,
            pool_size=db_settings.POOL_SIZE,
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


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Get a database session.
    To be used as a FastAPI dependency.
    """
    session_maker = get_session_maker()
    session = session_maker()
    try:
        yield session
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()
