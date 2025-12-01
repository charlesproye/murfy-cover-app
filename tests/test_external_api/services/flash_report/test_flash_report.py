import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from db_models.enums import LanguageEnum
from db_models.vehicle import FlashReportCombination, Make, VehicleModel
from external_api.schemas.flash_report import VehicleSpecs, VehicleSpecsType
from external_api.services.flash_report.flash_report import (
    get_db_names,
    get_flash_report_data,
    insert_combination,
    send_email,
    send_vehicle_specs,
)


@pytest.mark.asyncio
async def test_get_db_names(
    db_session: AsyncSession,
    tesla_model_3_awd: VehicleModel,
    tesla_make: Make,
):
    """Test get_db_names with test fixtures."""
    oem, model_name = await get_db_names(
        make=tesla_make.make_name,
        model=tesla_model_3_awd.model_name,
        db=db_session,
    )

    assert oem == "tesla"
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
async def test_get_flash_report_data(
    db_session: AsyncSession,
    tesla_flash_report_combination: FlashReportCombination,
):
    """Test getting flash report data with test fixture."""
    result = await get_flash_report_data(tesla_flash_report_combination, db=db_session)
    assert result is not None


# ============================================================================
# Integration Tests (require external APIs)
# ============================================================================


@pytest.mark.integration
@pytest.mark.asyncio
async def test_send_vehicle_specs_tesla_vin_decoder(
    db_session: AsyncSession,
    tesla_model_3_awd: VehicleModel,
):
    """Test VIN decoder for Tesla (requires external VIN decoder API)."""
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
