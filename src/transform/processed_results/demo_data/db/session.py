from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy.orm import sessionmaker
from sqlmodel.ext.asyncio.session import AsyncSession
from typing import Optional

from .config import settings

WEB_CONCURRENCY: int = 4
DB_POOL_SIZE: int = 20
POOL_SIZE: int = 5
MAX_OVERFLOW: int = 10

_engine: Optional[AsyncEngine] = None
_session_maker = None

def get_engine() -> AsyncEngine:
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

def get_session_maker() -> sessionmaker:
    global _session_maker
    if _session_maker is None:
        engine = get_engine()
        _session_maker = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
    return _session_maker

async def data_session() -> AsyncSession:
    session_maker = get_session_maker()
    return session_maker()
