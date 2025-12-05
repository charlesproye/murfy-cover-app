"""
Shared test fixtures for all tests.
Provides database sessions, test client, and seed data.
"""

from collections.abc import AsyncGenerator

import bcrypt
import httpx
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from db_models import (
    Battery,
    Company,
    Fleet,
    Make,
    Oem,
    Role,
    UserFleet,
    Vehicle,
    VehicleModel,
)
from db_models.core.config import db_settings
from db_models.enums import LanguageEnum
from db_models.vehicle import FlashReportCombination, Region, User
from external_api.app import app
from external_api.db.session import get_db

# Test database configuration
# Uses the same DB as the app but ensures we're using test environment
TEST_DB_URI = db_settings.ASYNC_DB_DATA_EV_URI


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
    async_session = sessionmaker(
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

    app.dependency_overrides[get_db] = override_get_db

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client

    app.dependency_overrides.clear()


# ============================================================================
# Seed Data Fixtures - Minimal test data
# ============================================================================


@pytest_asyncio.fixture
async def foo_company(db_session: AsyncSession) -> Company:
    """Create a foo company."""
    new_company = Company(
        name="Foo Company",
        description="A test company for integration tests",
    )
    db_session.add(new_company)
    await db_session.flush()  # Flush to get ID without committing
    await db_session.refresh(new_company)
    return new_company


@pytest_asyncio.fixture
async def admin_role(db_session: AsyncSession) -> Role:
    """Create an admin role."""
    role = Role(
        role_name="admin",
    )
    db_session.add(role)
    await db_session.flush()
    await db_session.refresh(role)
    return role


@pytest_asyncio.fixture
async def foo_user(
    db_session: AsyncSession, foo_company: Company, admin_role: Role
) -> User:
    """
    Create a foo user with hashed password.
    Password: 'testpassword123'
    """
    plain_password = "testpassword123"
    hashed_password = bcrypt.hashpw(
        plain_password.encode("utf-8"), bcrypt.gensalt()
    ).decode("utf-8")

    user = User(
        company_id=foo_company.id,
        role_id=admin_role.id,
        email="foo-user@example.com",
        first_name="Foo",
        last_name="User",
        password=hashed_password,
        phone="+33123456789",
        is_active=True,
    )
    db_session.add(user)
    await db_session.flush()
    await db_session.refresh(user)

    # Store plain password for testing (add as attribute, not in DB)
    user.plain_password = plain_password

    return user


@pytest_asyncio.fixture
async def foo_fleet(db_session: AsyncSession, foo_company: Company) -> Fleet:
    """Create a foo fleet."""
    fleet = Fleet(
        fleet_name="Foo Fleet",
        company_id=foo_company.id,
    )
    db_session.add(fleet)
    await db_session.flush()
    await db_session.refresh(fleet)
    return fleet


@pytest_asyncio.fixture
async def foo_user_fleet(
    db_session: AsyncSession, foo_user: User, foo_fleet: Fleet, admin_role: Role
) -> UserFleet:
    """Create a user-fleet relationship."""
    user_fleet = UserFleet(
        user_id=foo_user.id,
        fleet_id=foo_fleet.id,
        role_id=admin_role.id,
    )
    db_session.add(user_fleet)
    await db_session.flush()
    await db_session.refresh(user_fleet)
    return user_fleet


@pytest_asyncio.fixture
async def tesla_oem(db_session: AsyncSession) -> Oem:
    """Create a Tesla OEM."""
    oem = Oem(
        oem_name="tesla",  # Lowercase for consistency with flash_report
        description="Tesla Motors",
    )
    db_session.add(oem)
    await db_session.flush()
    await db_session.refresh(oem)
    return oem


@pytest_asyncio.fixture
async def tesla_make(db_session: AsyncSession, tesla_oem: Oem) -> Make:
    """Create a test make."""
    make = Make(
        make_name="tesla",  # Lowercase for consistency with flash_report
        oem_id=tesla_oem.id,
        description="Tesla vehicles",
    )
    db_session.add(make)
    await db_session.flush()
    await db_session.refresh(make)
    return make


@pytest_asyncio.fixture
async def lfp_battery(db_session: AsyncSession) -> Battery:
    """Create a LFP battery."""
    battery = Battery(
        capacity=75.0,
        net_capacity=72.0,
    )
    db_session.add(battery)
    await db_session.flush()
    await db_session.refresh(battery)
    return battery


@pytest_asyncio.fixture
async def tesla_model_3_awd(
    db_session: AsyncSession, tesla_oem: Oem, tesla_make: Make, lfp_battery: Battery
) -> VehicleModel:
    """Create a Tesla Model 3 AWD vehicle model."""
    # Simple polynomial trendline: y = 100 - 0.0001 * x
    # This simulates battery degradation over mileage
    trendline_formula = "100 - 0.0001 * x"

    vehicle_model = VehicleModel(
        model_name="model 3",  # Lowercase for consistency with flash_report expectations
        type="long range awd",  # Lowercase for consistency
        version="2021",
        oem_id=tesla_oem.id,
        make_id=tesla_make.id,
        battery_id=lfp_battery.id,
        autonomy=580,
        source="test",
        trendline={"trendline": trendline_formula},
        trendline_min={"trendline": "95 - 0.0001 * x"},
        trendline_max={"trendline": "105 - 0.0001 * x"},
    )
    db_session.add(vehicle_model)
    await db_session.flush()
    await db_session.refresh(vehicle_model)
    return vehicle_model


@pytest_asyncio.fixture
async def france_region(db_session: AsyncSession) -> Region:
    """Create a France region."""
    region = Region(
        region_name="France",
    )
    db_session.add(region)
    await db_session.flush()
    await db_session.refresh(region)
    return region


@pytest_asyncio.fixture
async def tesla_vehicle(
    db_session: AsyncSession,
    foo_fleet: Fleet,
    france_region: Region,
    tesla_model_3_awd: VehicleModel,
) -> Vehicle:
    """Create a test vehicle."""
    vehicle = Vehicle(
        fleet_id=foo_fleet.id,
        region_id=france_region.id,
        vehicle_model_id=tesla_model_3_awd.id,
        vin="5YJ3E1EA1KF123456",
        activation_status=True,
        is_eligible=True,
        licence_plate="AB-123-CD",
    )
    db_session.add(vehicle)
    await db_session.flush()
    await db_session.refresh(vehicle)
    return vehicle


@pytest_asyncio.fixture
async def tesla_flash_report_combination(
    db_session: AsyncSession,
    tesla_model_3_awd: VehicleModel,  # Ensure vehicle model exists
) -> FlashReportCombination:
    """Create a Tesla Model 3 AWD flash report combination."""

    flash_report = FlashReportCombination(
        vin="5YJ3E1EA1KF123456",
        make="tesla",
        model="model 3",
        type="long range awd",
        version="2021",
        odometer=50000,
        token="test-flash-report-token-12345",
        language=LanguageEnum.EN,
    )
    db_session.add(flash_report)
    await db_session.flush()
    await db_session.refresh(flash_report)
    return flash_report


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
