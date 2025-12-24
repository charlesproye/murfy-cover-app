"""
Database model tests for vehicle-related tables.
Tests data integrity, relationships, constraints, and validations.
"""

import uuid

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from db_models import (
    ApiUser,
    Company,
    FlashReportCombination,
    Fleet,
    User,
    UserFleet,
    Vehicle,
)
from tests.factories import (
    BatteryFactory,
    CompanyFactory,
    FleetFactory,
    MakeFactory,
    OemFactory,
    RegionFactory,
    RoleFactory,
    UserFactory,
    VehicleModelFactory,
)


class TestCompanyModel:
    """Test suite for Company model."""

    @pytest.mark.asyncio
    async def test_create_company(self, db_session: AsyncSession):
        """Test creating a company."""
        company = await CompanyFactory.create_async(
            session=db_session,
            name="New Test Company",
            description="A new company",
        )

        assert company.id is not None
        assert company.name == "New Test Company"
        assert company.description == "A new company"
        assert company.created_at is not None

    @pytest.mark.asyncio
    async def test_company_name_required(self, db_session: AsyncSession):
        """Test that company name is required (NOT NULL constraint)."""
        company = Company(
            name=None,  # This should fail
            description="Test",
        )
        db_session.add(company)

        with pytest.raises(IntegrityError):
            await db_session.flush()


class TestUserModel:
    """Test suite for User model."""

    @pytest.mark.asyncio
    async def test_create_user(self, db_session: AsyncSession):
        """Test creating a user."""
        company = await CompanyFactory.create_async(session=db_session)
        role = await RoleFactory.create_async(session=db_session)

        user = await UserFactory.create_async(
            session=db_session,
            company_id=company.id,
            role_id=role.id,
            email="newuser@example.com",
            first_name="New",
            last_name="User",
            phone="+33987654321",
            is_active=True,
        )

        assert user.id is not None
        assert user.email == "newuser@example.com"
        assert user.first_name == "New"
        assert user.company_id == company.id

    @pytest.mark.asyncio
    async def test_user_email_unique(self, db_session: AsyncSession):
        """Test that user email must be unique."""
        company = await CompanyFactory.create_async(session=db_session)
        role = await RoleFactory.create_async(session=db_session)

        # Create first user
        await UserFactory.create_async(
            session=db_session,
            company_id=company.id,
            role_id=role.id,
            email="duplicate@example.com",
            first_name="User",
            last_name="One",
        )

        # Try to create second user with same email
        user2 = User(
            company_id=company.id,
            role_id=role.id,
            email="duplicate@example.com",  # Same email - should fail
            first_name="User",
            last_name="Two",
            password="password2",
        )
        db_session.add(user2)

        with pytest.raises(IntegrityError):
            await db_session.flush()

    @pytest.mark.asyncio
    async def test_user_company_foreign_key(self, db_session: AsyncSession):
        """Test that user must have a valid company_id (foreign key)."""
        role = await RoleFactory.create_async(session=db_session)
        invalid_company_id = uuid.uuid4()

        user = User(
            company_id=invalid_company_id,  # Non-existent company
            role_id=role.id,
            email="test@example.com",
            first_name="Test",
            password="password",
        )
        db_session.add(user)

        with pytest.raises(IntegrityError):
            await db_session.flush()


class TestVehicleModel:
    """Test suite for Vehicle model."""

    @pytest.mark.asyncio
    async def test_create_vehicle(self, db_session: AsyncSession):
        """Test creating a vehicle."""
        # Build dependency chain
        company = await CompanyFactory.create_async(session=db_session)
        fleet = await FleetFactory.create_async(
            session=db_session, fleet_name="Test Fleet", company_id=company.id
        )
        region = await RegionFactory.create_async(session=db_session)
        oem = await OemFactory.create_async(session=db_session)
        make = await MakeFactory.create_async(session=db_session, oem_id=oem.id)
        battery = await BatteryFactory.create_async(session=db_session)
        vehicle_model = await VehicleModelFactory.create_async(
            session=db_session,
            oem_id=oem.id,
            make_id=make.id,
            battery_id=battery.id,
        )

        vehicle = Vehicle(
            fleet_id=fleet.id,
            region_id=region.id,
            vehicle_model_id=vehicle_model.id,
            vin="NEWVIN123456789",
            licence_plate="XY-789-ZZ",
            activation_status=True,
            is_eligible=True,
        )
        db_session.add(vehicle)
        await db_session.flush()
        await db_session.refresh(vehicle)

        assert vehicle.id is not None
        assert vehicle.vin == "NEWVIN123456789"
        assert vehicle.fleet_id == fleet.id

    @pytest.mark.asyncio
    async def test_vehicle_foreign_keys(self, db_session: AsyncSession):
        """Test that vehicle requires valid foreign keys."""
        region = await RegionFactory.create_async(session=db_session)
        invalid_fleet_id = uuid.uuid4()
        invalid_model_id = uuid.uuid4()

        vehicle = Vehicle(
            fleet_id=invalid_fleet_id,  # Non-existent fleet
            region_id=region.id,
            vehicle_model_id=invalid_model_id,  # Non-existent model
            vin="TESTVIN",
        )
        db_session.add(vehicle)

        with pytest.raises(IntegrityError):
            await db_session.flush()


class TestVehicleModelTable:
    """Test suite for VehicleModel table."""

    @pytest.mark.asyncio
    async def test_create_vehicle_model(self, db_session: AsyncSession):
        """Test creating a vehicle model with all relationships."""
        oem = await OemFactory.create_async(session=db_session)
        make = await MakeFactory.create_async(session=db_session, oem_id=oem.id)
        battery = await BatteryFactory.create_async(session=db_session)

        vehicle_model = await VehicleModelFactory.create_async(
            session=db_session,
            model_name="Model Y",
            type="Performance AWD",
            version="2023",
            oem_id=oem.id,
            make_id=make.id,
            battery_id=battery.id,
            autonomy=525,
            source="test",
        )

        assert vehicle_model.id is not None
        assert vehicle_model.model_name == "Model Y"
        assert vehicle_model.oem_id == oem.id


class TestFlashReportCombination:
    """Test suite for FlashReportCombination model."""

    @pytest.mark.asyncio
    async def test_create_flash_report_combination(self, db_session: AsyncSession):
        """Test creating a flash report combination."""
        token = str(uuid.uuid4())
        flash_report = FlashReportCombination(
            vin="5YJ3E1EA1KF654321",
            make="tesla",
            model="model 3",
            type="long range awd",
            version="MT352",
            odometer=50000,
            token=token,
            language="EN",
        )
        db_session.add(flash_report)
        await db_session.flush()
        await db_session.refresh(flash_report)

        assert flash_report.id is not None
        assert flash_report.vin == "5YJ3E1EA1KF654321"
        assert flash_report.token == token


class TestApiUserModel:
    """Test suite for ApiUser model."""

    @pytest.mark.asyncio
    async def test_create_api_user(self, db_session: AsyncSession):
        """Test creating an API user."""
        company = await CompanyFactory.create_async(session=db_session)
        role = await RoleFactory.create_async(session=db_session)
        user = await UserFactory.create_async(
            session=db_session,
            company_id=company.id,
            role_id=role.id,
        )

        api_key = str(uuid.uuid4())
        api_user = ApiUser(
            user_id=user.id,
            api_key=api_key,
            is_active=True,
        )
        db_session.add(api_user)
        await db_session.flush()
        await db_session.refresh(api_user)

        assert api_user.id is not None
        assert api_user.api_key == api_key
        assert api_user.user_id == user.id

    @pytest.mark.asyncio
    async def test_api_user_key_unique(self, db_session: AsyncSession):
        """Test that API key must be unique."""
        company = await CompanyFactory.create_async(session=db_session)
        role = await RoleFactory.create_async(session=db_session)

        user1 = await UserFactory.create_async(
            session=db_session,
            company_id=company.id,
            role_id=role.id,
            email="apiuser1@example.com",
            first_name="API",
            last_name="User1",
        )

        user2 = await UserFactory.create_async(
            session=db_session,
            company_id=company.id,
            role_id=role.id,
            email="apiuser2@example.com",
            first_name="API",
            last_name="User2",
        )

        api_key = str(uuid.uuid4())

        # Create first API user
        api_user1 = ApiUser(
            user_id=user1.id,
            api_key=api_key,
            is_active=True,
        )
        db_session.add(api_user1)
        await db_session.flush()

        # Try to create second API user with same key
        api_user2 = ApiUser(
            user_id=user2.id,
            api_key=api_key,  # Same API key - should fail
            is_active=True,
        )
        db_session.add(api_user2)

        with pytest.raises(IntegrityError):
            await db_session.flush()


class TestRelationships:
    """Test suite for model relationships."""

    @pytest.mark.asyncio
    async def test_user_company_relationship(self, db_session: AsyncSession):
        """Test user-company relationship."""
        role = await RoleFactory.create_async(session=db_session)

        # Create company
        company = await CompanyFactory.create_async(
            session=db_session, name="Relationship Test Company"
        )

        # Create user with company
        user = await UserFactory.create_async(
            session=db_session,
            company_id=company.id,
            role_id=role.id,
            email="relationship@example.com",
            first_name="Test",
        )

        assert user.company_id == company.id

    @pytest.mark.asyncio
    async def test_user_fleet_relationship(self, db_session: AsyncSession):
        """Test user-fleet relationship through UserFleet."""
        company = await CompanyFactory.create_async(session=db_session)
        role = await RoleFactory.create_async(session=db_session)

        # Create user
        user = await UserFactory.create_async(
            session=db_session,
            company_id=company.id,
            role_id=role.id,
            email="fleetuser@example.com",
            first_name="Fleet",
        )

        # Create fleet
        fleet = Fleet(
            fleet_name="Test Fleet Relationship",
            company_id=company.id,
        )
        db_session.add(fleet)
        await db_session.flush()
        await db_session.refresh(fleet)

        # Create user-fleet relationship
        user_fleet = UserFleet(
            user_id=user.id,
            fleet_id=fleet.id,
            role_id=role.id,
        )
        db_session.add(user_fleet)
        await db_session.flush()
        await db_session.refresh(user_fleet)

        assert user_fleet.user_id == user.id
        assert user_fleet.fleet_id == fleet.id

    @pytest.mark.asyncio
    async def test_vehicle_model_relationships(self, db_session: AsyncSession):
        """Test vehicle model relationships with OEM, make, and battery."""
        oem = await OemFactory.create_async(session=db_session)
        make = await MakeFactory.create_async(session=db_session, oem_id=oem.id)
        battery = await BatteryFactory.create_async(session=db_session)

        vehicle_model = await VehicleModelFactory.create_async(
            session=db_session,
            model_name="Relationship Test Model",
            oem_id=oem.id,
            make_id=make.id,
            battery_id=battery.id,
            source="test",
        )

        assert vehicle_model.oem_id == oem.id
        assert vehicle_model.make_id == make.id
        assert vehicle_model.battery_id == battery.id
