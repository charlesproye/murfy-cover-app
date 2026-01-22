import uuid

import pandas as pd
from fastapi import HTTPException, status
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from activation.config.mappings import mapping_vehicle_type
from db_models import Battery, FlashReportCombination, Make, VehicleModel
from db_models.enums import LanguageEnum
from external_api.schemas.flash_report import (
    VehicleSpecs,
    VehicleSpecsType,
)
from external_api.services.flash_report.vin_decoder.tesla_vin_decoder import (
    TeslaVinDecoder,
)
from external_api.services.flash_report.vin_decoder.vin_decoder import VinDecoder


async def log_vin_decoded(
    db: AsyncSession,
    vin: str,
    make: str,
    model: str,
    type: str | None = None,
    version: str | None = None,
    odometer: int | None = None,
    language: LanguageEnum = LanguageEnum.EN,
    token: str | None = None,
) -> None:
    db.add(
        FlashReportCombination(
            vin=vin,
            make=make,
            model=model,
            type=type,
            version=version,
            odometer=odometer,
            language=language,
            token=token,
        )
    )
    await db.flush()
    await db.commit()


async def get_make_has_trendline(make_name: str, db: AsyncSession) -> bool:
    """Check if make has any trendline (BIB or readout)."""
    query_has_trendline = """
        SELECT count(*)
        FROM vehicle_model vm
        left join make m on m.id = vm.make_id
        WHERE (vm.has_trendline_bib = true OR vm.has_trendline_oem = true)
        AND m.make_name = :make_name
        AND (vm.trendline_bib IS NOT NULL OR vm.trendline_oem IS NOT NULL)
    """
    result = await db.execute(text(query_has_trendline), {"make_name": make_name})
    return result.fetchone()[0] > 0


async def get_make_has_trendline_bib(make_name: str, db: AsyncSession) -> bool:
    """Check if make has BIB trendline."""
    query_has_trendline_bib = """
        SELECT count(*)
        FROM vehicle_model vm
        left join make m on m.id = vm.make_id
        WHERE vm.has_trendline_bib = true AND m.make_name = :make_name
    """
    result = await db.execute(text(query_has_trendline_bib), {"make_name": make_name})
    return result.fetchone()[0] > 0


async def get_make_has_trendline_oem(make_name: str, db: AsyncSession) -> bool:
    """Check if make has readout trendline."""
    query_has_trendline_oem = """
        SELECT count(*)
        FROM vehicle_model vm
        left join make m on m.id = vm.make_id
        WHERE vm.has_trendline_oem = true AND m.make_name = :make_name
    """
    result = await db.execute(text(query_has_trendline_oem), {"make_name": make_name})
    return result.fetchone()[0] > 0


async def get_db_names(
    make: str,
    model: str,
    type: str | None = None,
    db: AsyncSession = None,
) -> tuple[str | None, str | None]:
    query_all_models = (
        select(
            VehicleModel.model_name,
            VehicleModel.id,
            VehicleModel.type,
            VehicleModel.commissioning_date,
            VehicleModel.end_of_life_date,
            Make.make_name,
            Battery.capacity,
        )
        .select_from(VehicleModel)
        .join(Make, VehicleModel.make_id == Make.id, isouter=True)
        .join(Battery, VehicleModel.battery_id == Battery.id, isouter=True)
    )

    result = await db.execute(query_all_models)
    rows = result.fetchall()
    columns = result.keys()
    model_existing = pd.DataFrame(rows, columns=columns)

    vehicle_model_id = mapping_vehicle_type(
        type or "", make, model, model_existing, None
    )

    if vehicle_model_id is None:
        return None, None

    query_model = """
        SELECT m.make_name, vm.model_name
        FROM vehicle_model vm
        LEFT JOIN make m ON vm.make_id = m.id
        LEFT JOIN battery b ON b.id = vm.battery_id
        WHERE vm.id = :vehicle_model_id
    """
    result = await db.execute(text(query_model), {"vehicle_model_id": vehicle_model_id})
    record = result.mappings().first()

    if not record:
        return None, None

    return record["make_name"], record["model_name"]


async def send_vehicle_specs(vin: str, db: AsyncSession) -> VehicleSpecs:
    if len(vin) != 17:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="VIN must be 17 characters long",
        )

    tesla_vin_decoder = TeslaVinDecoder()
    result = tesla_vin_decoder.decode(vin)

    if result and result.get("Country"):
        make_db_name, model_db_name = await get_db_names(
            "tesla",
            result.get("Model"),
            result["Type"][0]
            if isinstance(result.get("Type"), list)
            else result.get("Type"),
            db=db,
        )

        if not make_db_name:
            return VehicleSpecs(
                has_trendline=False,
                has_trendline_bib=False,
                has_trendline_oem=False,
                make="tesla",
                model=None,
                type_version_list=None,
            )

        has_trendline = await get_make_has_trendline(make_db_name, db)
        has_trendline_bib = await get_make_has_trendline_bib(make_db_name, db)
        has_trendline_oem = await get_make_has_trendline_oem(make_db_name, db)

        type_version = {}

        if isinstance(result.get("Version"), list):
            versions = result.get("Version")
            for version in versions:
                # Match version with right type when multiples types
                query = "select type from vehicle_model where version = :version"
                result = await db.execute(text(query), {"version": version})
                type_tesla = result.fetchone()[0] if result.fetchone() else None
                if type_tesla and type_tesla not in type_version:
                    type_version[type_tesla] = version
        else:
            type_version[result.get("Type")] = result.get("Version")

        return VehicleSpecs(
            has_trendline=has_trendline,
            has_trendline_bib=has_trendline_bib,
            has_trendline_oem=has_trendline_oem,
            make="tesla",
            model=model_db_name,
            type_version_list=[
                VehicleSpecsType(type=type, version=version)
                for type, version in type_version.items()
            ],
        )
    else:
        vin_decoder = VinDecoder()
        make, model = vin_decoder.decode(vin)

        if make:
            if model:
                make_db_name, model_db_name = await get_db_names(
                    make, model, type=None, db=db
                )

                if not make_db_name:
                    return VehicleSpecs(
                        has_trendline=False,
                        has_trendline_bib=False,
                        has_trendline_oem=False,
                        make=make.lower().replace("ë", "e"),
                        model=None,
                        type_version_list=None,
                    )

                has_trendline = await get_make_has_trendline(make_db_name, db)
                has_trendline_bib = await get_make_has_trendline_bib(make_db_name, db)
                has_trendline_oem = await get_make_has_trendline_oem(make_db_name, db)

                return VehicleSpecs(
                    has_trendline=has_trendline,
                    has_trendline_bib=has_trendline_bib,
                    has_trendline_oem=has_trendline_oem,
                    make=make_db_name,
                    model=model_db_name,
                    type_version_list=None,
                )

            else:  # Model not found, just make
                return VehicleSpecs(
                    has_trendline=False,
                    has_trendline_bib=False,
                    has_trendline_oem=False,
                    make=make.lower().replace("ë", "e"),
                    model=None,
                    type_version_list=None,
                )
        else:  # Make not found
            return VehicleSpecs(
                has_trendline=False,
                has_trendline_bib=False,
                has_trendline_oem=False,
                make=None,
                model=None,
                type_version_list=None,
            )


async def insert_combination(
    vin: str,
    make: str,
    model: str,
    vehicle_type: str,
    version: str | None,
    odometer: int,
    language: LanguageEnum,
    db: AsyncSession,
) -> str:
    token = str(uuid.uuid4())
    db.add(
        FlashReportCombination(
            vin=vin,
            make=make,
            model=model,
            type=vehicle_type,
            version=version,
            odometer=odometer,
            token=token,
            language=language,
        )
    )
    await db.flush()
    await db.commit()

    return token
