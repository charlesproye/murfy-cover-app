import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from db_models.enums import LanguageEnum
from external_api.schemas.flash_report import VehicleSpecs, VehicleSpecsType
from external_api.services.flash_report.flash_report import (
    get_db_names,
    get_flash_report_data,
    insert_combination,
    send_email,
    send_vehicle_specs,
)
from tests.factories import (
    BatteryFactory,
    FlashReportCombinationFactory,
    MakeFactory,
    OemFactory,
    VehicleModelFactory,
)


@pytest.mark.asyncio
async def test_get_db_names(db_session: AsyncSession):
    oem = await OemFactory.create_async(session=db_session, oem_name="tesla")
    make = await MakeFactory.create_async(
        session=db_session, oem_id=oem.id, make_name="tesla"
    )
    battery = await BatteryFactory.create_async(session=db_session)
    vehicle_model = await VehicleModelFactory.create_async(
        session=db_session,
        oem_id=oem.id,
        make_id=make.id,
        battery_id=battery.id,
        model_name="model 3",
    )

    oem_name, model_name = await get_db_names(
        make=make.make_name,
        model=vehicle_model.model_name,
        db=db_session,
    )

    assert oem_name == "tesla"
    assert model_name == "model 3"


@pytest.mark.asyncio
async def test_insert_combination(db_session: AsyncSession):
    """Test inserting a flash report combination."""
    token = await insert_combination(
        make="tesla",
        model="model 3",
        vehicle_type="long range awd",
        version="MT352",
        odometer=100000,
        vin="LRW3E7EKXRC152510",
        language=LanguageEnum.EN,
        db=db_session,
    )
    assert token is not None
    assert len(token) == 36  # UUID format


@pytest.mark.asyncio
async def test_get_flash_report_data(db_session: AsyncSession):
    """Test getting flash report data with test fixture."""
    # Create vehicle model that matches the flash report data
    oem = await OemFactory.create_async(session=db_session, oem_name="tesla")
    make = await MakeFactory.create_async(
        session=db_session, oem_id=oem.id, make_name="tesla"
    )
    battery = await BatteryFactory.create_async(session=db_session)
    vehicle_model = await VehicleModelFactory.create_async(
        session=db_session,
        oem_id=oem.id,
        make_id=make.id,
        battery_id=battery.id,
        model_name="model 3",
        type="long range awd",
        version="MT352",
    )

    # Create flash report combination using factory
    flash_report = await FlashReportCombinationFactory.create_async(
        session=db_session,
        vin="5YJ3E1EA1KF654321",
        make=make.make_name,
        model=vehicle_model.model_name,
        type=vehicle_model.type,
        version=vehicle_model.version,
        odometer=50000,
        language=LanguageEnum.EN,
    )

    result = await get_flash_report_data(flash_report, db=db_session)
    assert result is not None


# ============================================================================
# Integration Tests (require external APIs)
# ============================================================================


@pytest.mark.integration
@pytest.mark.asyncio
async def test_send_vehicle_specs_tesla_vin_decoder(db_session: AsyncSession):
    """Test VIN decoder for Tesla (requires external VIN decoder API)."""
    # Create test data using factories
    oem = await OemFactory.create_async(session=db_session, oem_name="tesla")
    make = await MakeFactory.create_async(
        session=db_session, oem_id=oem.id, make_name="tesla"
    )
    battery = await BatteryFactory.create_async(session=db_session)
    await VehicleModelFactory.create_async(
        session=db_session,
        oem_id=oem.id,
        make_id=make.id,
        battery_id=battery.id,
        model_name="model 3",
    )

    vin = "5YJ3E7EB8KF100000"

    result = await send_vehicle_specs(vin, db=db_session)

    assert result == VehicleSpecs(
        has_trendline=True,
        make="tesla",
        model="model 3",
        type_version_list=[
            VehicleSpecsType(type="long range", version="MT303"),
            VehicleSpecsType(type="long range performance", version="MT304"),
        ],
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_send_vehicle_specs_vin_decoder(db_session: AsyncSession):
    """Test VIN decoder for Renault (requires external VIN decoder API and Renault data)."""
    vin = "VF1AG000765232122"

    result = await send_vehicle_specs(vin, db=db_session)

    assert result == VehicleSpecs(
        has_trendline=True,
        make="renault",
        model="zoe",
        type_version_list=None,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_send_email():
    """Test email sending (requires SMTP server configuration)."""
    await send_email(is_french=True, email="test@example.com", token="test-token-12345")
