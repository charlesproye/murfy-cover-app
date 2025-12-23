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
    Battery,
    Company,
    FlashReportCombination,
    Fleet,
    Make,
    Oem,
    Region,
    Role,
    User,
    UserFleet,
    Vehicle,
    VehicleModel,
)


class TestCompanyModel:
    """Test suite for Company model."""

    @pytest.mark.asyncio
    async def test_create_company(self, db_session: AsyncSession):
        """Test creating a company."""
        company = Company(
            name="New Test Company",
            description="A new company",
        )
        db_session.add(company)
        await db_session.flush()
        await db_session.refresh(company)

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
    async def test_create_user(
        self, db_session: AsyncSession, foo_company: Company, admin_role: Role
    ):
        """Test creating a user."""
        user = User(
            company_id=foo_company.id,
            role_id=admin_role.id,
            email="newuser@example.com",
            first_name="New",
            last_name="User",
            password="hashedpassword",
            phone="+33987654321",
            is_active=True,
        )
        db_session.add(user)
        await db_session.flush()
        await db_session.refresh(user)

        assert user.id is not None
        assert user.email == "newuser@example.com"
        assert user.first_name == "New"
        assert user.company_id == foo_company.id

    @pytest.mark.asyncio
    async def test_user_email_unique(
        self, db_session: AsyncSession, foo_company: Company, admin_role: Role
    ):
        """Test that user email must be unique."""
        # Create first user
        user1 = User(
            company_id=foo_company.id,
            role_id=admin_role.id,
            email="duplicate@example.com",
            first_name="User",
            last_name="One",
            password="password1",
        )
        db_session.add(user1)
        await db_session.flush()

        # Try to create second user with same email
        user2 = User(
            company_id=foo_company.id,
            role_id=admin_role.id,
            email="duplicate@example.com",  # Same email - should fail
            first_name="User",
            last_name="Two",
            password="password2",
        )
        db_session.add(user2)

        with pytest.raises(IntegrityError):
            await db_session.flush()

    @pytest.mark.asyncio
    async def test_user_company_foreign_key(
        self, db_session: AsyncSession, admin_role: Role
    ):
        """Test that user must have a valid company_id (foreign key)."""
        invalid_company_id = uuid.uuid4()

        user = User(
            company_id=invalid_company_id,  # Non-existent company
            role_id=admin_role.id,
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
    async def test_create_vehicle(
        self,
        db_session: AsyncSession,
        foo_fleet: Fleet,
        france_region: Region,
        tesla_model_3_awd: VehicleModel,
    ):
        """Test creating a vehicle."""
        vehicle = Vehicle(
            fleet_id=foo_fleet.id,
            region_id=france_region.id,
            vehicle_model_id=tesla_model_3_awd.id,
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
        assert vehicle.fleet_id == foo_fleet.id

    @pytest.mark.asyncio
    async def test_vehicle_foreign_keys(
        self, db_session: AsyncSession, france_region: Region
    ):
        """Test that vehicle requires valid foreign keys."""
        invalid_fleet_id = uuid.uuid4()
        invalid_model_id = uuid.uuid4()

        vehicle = Vehicle(
            fleet_id=invalid_fleet_id,  # Non-existent fleet
            region_id=france_region.id,
            vehicle_model_id=invalid_model_id,  # Non-existent model
            vin="TESTVIN",
        )
        db_session.add(vehicle)

        with pytest.raises(IntegrityError):
            await db_session.flush()


class TestVehicleModelTable:
    """Test suite for VehicleModel table."""

    @pytest.mark.asyncio
    async def test_create_vehicle_model(
        self,
        db_session: AsyncSession,
        tesla_oem: Oem,
        tesla_make: Make,
        lfp_battery: Battery,
    ):
        """Test creating a vehicle model with all relationships."""
        vehicle_model = VehicleModel(
            model_name="Model Y",
            type="Performance AWD",
            version="2023",
            oem_id=tesla_oem.id,
            make_id=tesla_make.id,
            battery_id=lfp_battery.id,
            autonomy=525,
            source="test",
        )
        db_session.add(vehicle_model)
        await db_session.flush()
        await db_session.refresh(vehicle_model)

        assert vehicle_model.id is not None
        assert vehicle_model.model_name == "Model Y"
        assert vehicle_model.oem_id == tesla_oem.id


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
    async def test_create_api_user(self, db_session: AsyncSession, foo_user: User):
        """Test creating an API user."""
        api_key = str(uuid.uuid4())
        api_user = ApiUser(
            user_id=foo_user.id,
            api_key=api_key,
            is_active=True,
        )
        db_session.add(api_user)
        await db_session.flush()
        await db_session.refresh(api_user)

        assert api_user.id is not None
        assert api_user.api_key == api_key
        assert api_user.user_id == foo_user.id

    @pytest.mark.asyncio
    async def test_api_user_key_unique(
        self, db_session: AsyncSession, foo_company: Company
    ):
        """Test that API key must be unique."""
        # Need to create users with different emails
        role = Role(role_name="test_role")
        db_session.add(role)
        await db_session.flush()
        await db_session.refresh(role)

        user1 = User(
            company_id=foo_company.id,
            role_id=role.id,
            email="apiuser1@example.com",
            first_name="API",
            last_name="User1",
            password="password",
        )
        db_session.add(user1)
        await db_session.flush()
        await db_session.refresh(user1)

        user2 = User(
            company_id=foo_company.id,
            role_id=role.id,
            email="apiuser2@example.com",
            first_name="API",
            last_name="User2",
            password="password",
        )
        db_session.add(user2)
        await db_session.flush()
        await db_session.refresh(user2)

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
    async def test_user_company_relationship(
        self, db_session: AsyncSession, admin_role: Role
    ):
        """Test user-company relationship."""
        # Create company
        company = Company(name="Relationship Test Company")
        db_session.add(company)
        await db_session.flush()
        await db_session.refresh(company)

        # Create user with company
        user = User(
            company_id=company.id,
            role_id=admin_role.id,
            email="relationship@example.com",
            first_name="Test",
            password="password",
        )
        db_session.add(user)
        await db_session.flush()
        await db_session.refresh(user)

        assert user.company_id == company.id

    @pytest.mark.asyncio
    async def test_user_fleet_relationship(
        self,
        db_session: AsyncSession,
        foo_company: Company,
        admin_role: Role,
    ):
        """Test user-fleet relationship through UserFleet."""
        # Create user
        user = User(
            company_id=foo_company.id,
            role_id=admin_role.id,
            email="fleetuser@example.com",
            first_name="Fleet",
            password="password",
        )
        db_session.add(user)
        await db_session.flush()
        await db_session.refresh(user)

        # Create fleet
        fleet = Fleet(
            fleet_name="Test Fleet Relationship",
            company_id=foo_company.id,
        )
        db_session.add(fleet)
        await db_session.flush()
        await db_session.refresh(fleet)

        # Create user-fleet relationship
        user_fleet = UserFleet(
            user_id=user.id,
            fleet_id=fleet.id,
            role_id=admin_role.id,
        )
        db_session.add(user_fleet)
        await db_session.flush()
        await db_session.refresh(user_fleet)

        assert user_fleet.user_id == user.id
        assert user_fleet.fleet_id == fleet.id

    @pytest.mark.asyncio
    async def test_vehicle_model_relationships(
        self,
        db_session: AsyncSession,
        tesla_oem: Oem,
        tesla_make: Make,
        lfp_battery: Battery,
    ):
        """Test vehicle model relationships with OEM, make, and battery."""
        vehicle_model = VehicleModel(
            model_name="Relationship Test Model",
            oem_id=tesla_oem.id,
            make_id=tesla_make.id,
            battery_id=lfp_battery.id,
            source="test",
        )
        db_session.add(vehicle_model)
        await db_session.flush()
        await db_session.refresh(vehicle_model)

        assert vehicle_model.oem_id == tesla_oem.id
        assert vehicle_model.make_id == tesla_make.id
        assert vehicle_model.battery_id == lfp_battery.id
