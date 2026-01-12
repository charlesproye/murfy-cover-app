"""
Shared test fixtures for all tests.
Provides database sessions, test client, and helper utilities.
"""

from collections.abc import AsyncGenerator

import httpx
import pytest_asyncio
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from core.sql_utils import get_async_db
from db_models.core.config import db_settings
from external_api.app import app

# Test database configuration
# Uses the same DB as the app but ensures we're using test environment
_test_db_uri = db_settings.ASYNC_DB_DATA_EV_URI
assert _test_db_uri is not None, "ASYNC_DB_DATA_EV_URI must be set in environment"
TEST_DB_URI: str = _test_db_uri


@pytest_asyncio.fixture
async def engine() -> AsyncGenerator[AsyncEngine, None]:
    """
    Create a test database engine.
    Uses the real PostgreSQL database from Docker.
    """
    engine = create_async_engine(
        TEST_DB_URI,
        echo=False,  # Set to True for SQL debugging
        future=True,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
    )
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def db_session(engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    """
    Create a new database session for each test.
    Rolls back changes after each test to maintain isolation.
    """
    async_session = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )

    async with async_session() as session:
        # Start a transaction
        await session.begin()

        try:
            yield session
        finally:
            # Rollback the transaction to clean up test data
            await session.rollback()
            await session.close()


@pytest_asyncio.fixture
async def app_client(
    db_session: AsyncSession,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """
    Create an async HTTP client for testing FastAPI endpoints with database session override.
    """

    async def override_get_db() -> AsyncGenerator[AsyncSession, None]:
        yield db_session

    app.dependency_overrides[get_async_db] = override_get_db

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client

    app.dependency_overrides.clear()


# ============================================================================
# Helper Functions for Tests
# ============================================================================


def get_auth_headers(access_token: str) -> dict:
    """Helper function to create authentication headers."""
    return {"Authorization": f"Bearer {access_token}"}


def get_cookie_dict(response) -> dict:
    """Helper function to extract cookies from response."""
    cookies = {}
    for cookie in response.cookies:
        cookies[cookie.name] = cookie.value
    return cookies
