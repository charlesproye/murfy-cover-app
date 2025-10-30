import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select

from db_models.vehicle import FlashReportCombination
from external_api.core.config import Settings
from external_api.schemas.flash_report import VehicleSpecs
from external_api.services.flash_report.flash_report import (
    get_db_names,
    get_flash_report_data,
    insert_combination,
    send_email,
    send_vehicle_specs,
)

settings = Settings()


@pytest.mark.asyncio
async def test_get_db_names():
    engine = create_async_engine(settings.ASYNC_DB_DATA_EV_URI, echo=True)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        oem, model_name = await get_db_names(
            make="Tesla",
            model="Model 3",
            db=session,
        )

    assert oem == "tesla"
    assert model_name == "model 3"


@pytest.mark.asyncio
async def test_send_vehicle_specs_tesla_vin_decoder():
    vin = "LRW3E7EKXRC152510"

    engine = create_async_engine(settings.ASYNC_DB_DATA_EV_URI, echo=True)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        result = await send_vehicle_specs(vin, db=session)

        assert result == VehicleSpecs(
            has_trendline=True,
            make="tesla",
            model="model 3",
            type="long range awd",
            version="MT352",
        )


@pytest.mark.asyncio
async def test_insert_combination():
    engine = create_async_engine(settings.ASYNC_DB_DATA_EV_URI, echo=True)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        await insert_combination(
            make="tesla",
            model="model 3",
            vehicle_type="long range awd",
            version="MT352",
            odometer=100000,
            vin="LRW3E7EKXRC152510",
            db=session,
        )


@pytest.mark.asyncio
async def test_send_vehicle_specs_vin_decoder():
    vin = "VF1AG000765232122"

    engine = create_async_engine(settings.ASYNC_DB_DATA_EV_URI, echo=True)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        result = await send_vehicle_specs(vin, db=session)

        assert result == VehicleSpecs(
            has_trendline=True,
            make="renault",
            model="zoe",
            type=None,
            version=None,
        )


@pytest.mark.asyncio
async def test_send_email():
    await send_email("charles.proye@gmail.com")


@pytest.mark.asyncio
async def test_get_flash_report_data():
    token = "0853e467-93ab-4eaa-8500-7c26550f3f55"

    engine = create_async_engine(settings.ASYNC_DB_DATA_EV_URI, echo=True)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        stmt = select(FlashReportCombination).where(
            FlashReportCombination.token == token
        )
        result = await session.execute(stmt)
        flash_report_combination = result.scalar_one_or_none()
        result = await get_flash_report_data(flash_report_combination, db=session)

        assert result is not None

