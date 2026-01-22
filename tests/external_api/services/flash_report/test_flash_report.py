import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from db_models.enums import LanguageEnum
from external_api.schemas.flash_report import VehicleSpecs, VehicleSpecsType
from external_api.services.flash_report.flash_report import (
    get_db_names,
    insert_combination,
    send_vehicle_specs,
)
from tests.factories import (
    BatteryFactory,
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
        has_trendline_bib=True,
        has_trendline_oem=True,
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
        has_trendline_bib=True,
        has_trendline_oem=True,
        type_version_list=None,
    )
