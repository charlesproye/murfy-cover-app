import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from external_api.core.config import Settings
from external_api.services.flash_report.flash_report import (
    get_db_names,
    insert_combination,
    send_email,
    send_flash_report_data,
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

        assert result == {
            "has_trendline": True,
            "make": "tesla",
            "model": "model 3",
            "type": "long range awd",
            "version": "MT352",
        }


@pytest.mark.asyncio
async def test_insert_combination():
    engine = create_async_engine(settings.ASYNC_DB_DATA_EV_URI, echo=True)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        await insert_combination(
            make="tesla",
            model="model 3",
            type="long range awd",
            version="MT352",
            odometer=100000,
            db=session,
        )


@pytest.mark.asyncio
async def test_send_vehicle_specs_vin_decoder():
    vin = "VF1AG000765232122"

    engine = create_async_engine(settings.ASYNC_DB_DATA_EV_URI, echo=True)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        result = await send_vehicle_specs(vin, db=session)

        assert result == {
            "has_trendline": True,
            "make": "renault",
            "model": "zoe",
            "type": None,
            "version": None,
        }


@pytest.mark.asyncio
async def test_send_email():
    await send_email("charles.proye@gmail.com")


@pytest.mark.asyncio
async def test_send_flash_report_data_tesla():
    input = {
        "vin": "LRW3E7EKXRC152510",
        "make": "tesla",
        "model": "model 3",
        "type": "long range awd",
        "odometer": 100000,
        "version": "MT352",
    }

    engine = create_async_engine(settings.ASYNC_DB_DATA_EV_URI, echo=True)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        result = await send_flash_report_data(
            vin=input["vin"],
            make=input["make"],
            model=input["model"],
            type=input["type"],
            odometer=input["odometer"],
            version=input["version"],
            db=session,
        )

    assert result == {
        "has_trendline": True,
        "vehicle_info": {
            "vin": "LRW3E7EKXRC152510",
            "brand": "tesla",
            "model": "model 3",
            "version": "MT352",
            "type": "long range awd",
            "mileage": 100000,
            "image": "https://static-assets.tesla.com/configurator/compositor?context=design_studio_2?&bkba_opt=1&view=STUD_3QTR&size=600&model=my&options=$APBS,$DV2W,$INPW0,$PPSW,$PRMY1,$SC04,$MDLY,$WY19B,$MTY13,$STY5S,$CPF0&crop=1400,850,300,130&",
            "warranty_date": 8,
            "warranty_km": 192000,
        },
        "battery_info": {
            "chemistry": "NMC",
            "net_capacity": 79,
            "capacity": 79,
            "range": 629,
            "trendline": "0.9917820676682839 + -0.0960774606679407 * np.log1p(x/114205.00431470481)",
            "trendline_min": "1.0018286790372979 + -0.08315376993623276 * np.log1p(x/90161.15645145321)",
            "trendline_max": "0.98173545629927 + -0.12026362010532386 * np.log1p(x/157844.67563869906)",
            "soh": 0.9313552629267816,
        },
    }


@pytest.mark.asyncio
async def test_send_flash_report_data_other():
    input = {
        "vin": "KNMJH6AD0JU000000",
        "make": "kia",
        "model": "e-niro",
        "type": "64 kwh",
        "odometer": 100000,
        "version": None,
    }

    engine = create_async_engine(settings.ASYNC_DB_DATA_EV_URI, echo=True)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        result = await send_flash_report_data(
            vin=input["vin"],
            make=input["make"],
            model=input["model"],
            type=input["type"],
            odometer=input["odometer"],
            version=input["version"],
            db=session,
        )

    assert not result["has_trendline"]

