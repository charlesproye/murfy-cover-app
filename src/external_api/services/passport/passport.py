import logging

from fastapi import HTTPException
from sqlalchemy import Numeric, cast, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from db_models import Oem, Vehicle, VehicleData, VehicleModel
from external_api.schemas.graph import DataGraphResponse, DataPoint
from external_api.services.s3 import get_make_image_url, get_model_image_url

logger = logging.getLogger(__name__)


async def get_fleet_id_of_vin(vin: str, db: AsyncSession):
    query = (
        select(Vehicle.id, Vehicle.fleet_id)
        .select_from(Vehicle)
        .where(Vehicle.vin == vin)
    )
    result = await db.execute(query)
    vehicle = result.mappings().first()
    if not vehicle or "fleet_id" not in vehicle:
        return False

    return vehicle.fleet_id


async def get_graph_data(vin: str, db: AsyncSession = None):
    # Get trendlines
    trendlines_list_query = (
        select(
            Oem.trendline.label("oem_trendline"),
            Oem.trendline_min.label("oem_trendline_min"),
            Oem.trendline_max.label("oem_trendline_max"),
            VehicleModel.trendline.label("vehicle_model_trendline"),
            VehicleModel.trendline_min.label("vehicle_model_trendline_min"),
            VehicleModel.trendline_max.label("vehicle_model_trendline_max"),
        )
        .select_from(Vehicle)
        .join(VehicleModel, Vehicle.vehicle_model_id == VehicleModel.id)
        .join(Oem, VehicleModel.oem_id == Oem.id)
        .where(Vehicle.vin == vin)
    )
    trendlines_list_result = await db.execute(trendlines_list_query)
    trendlines_list = trendlines_list_result.mappings().first()

    if not trendlines_list:
        raise HTTPException(
            status_code=404, detail="Vehicle not found or no data available"
        )

    trendlines_final = {
        "trendline": trendlines_list["vehicle_model_trendline"]
        if trendlines_list["vehicle_model_trendline"]
        else trendlines_list["oem_trendline"]
        if trendlines_list["oem_trendline"]
        else None,
        "trendline_min": trendlines_list["vehicle_model_trendline_min"]
        if trendlines_list["vehicle_model_trendline_min"]
        else trendlines_list["oem_trendline_min"]
        if trendlines_list["oem_trendline_min"]
        else None,
        "trendline_max": trendlines_list["vehicle_model_trendline_max"]
        if trendlines_list["vehicle_model_trendline_max"]
        else trendlines_list["oem_trendline_max"]
        if trendlines_list["oem_trendline_max"]
        else None,
    }

    # Get data points
    soh_expr = (func.round(cast(VehicleData.soh, Numeric), 3) * 100).label("soh")
    odometer_expr = func.round(cast(VehicleData.odometer, Numeric), 0).label("odometer")

    query = (
        select(soh_expr, odometer_expr)
        .select_from(Vehicle)
        .join(VehicleData, Vehicle.id == VehicleData.vehicle_id)
        .where(
            Vehicle.vin == vin,
            VehicleData.soh.isnot(None),
            VehicleData.odometer.isnot(None),
        )
        .distinct()
        .order_by(odometer_expr.asc())
    )
    result = await db.execute(query)
    data = result.mappings().all()

    return DataGraphResponse(
        initial_point=DataPoint(soh=100, odometer=0),
        data_points=[
            DataPoint(soh=row["soh"], odometer=row["odometer"]) for row in data
        ]
        if data
        else [],
        trendline=trendlines_final["trendline"].get("trendline")
        if trendlines_final["trendline"]
        else None,
        trendline_min=trendlines_final["trendline_min"].get("trendline")
        if trendlines_final["trendline_min"]
        else None,
        trendline_max=trendlines_final["trendline_max"].get("trendline")
        if trendlines_final["trendline_max"]
        else None,
    )


async def get_infos(vin: str, db: AsyncSession):
    query = text("""
        WITH last_week AS (
            SELECT
                date_trunc('week', CURRENT_DATE) - INTERVAL '1 week' as start_date,
                date_trunc('week', CURRENT_DATE) - INTERVAL '1 second' as end_date
        ),
        latest_data_odometer AS (
            SELECT
                vehicle_id,
                timestamp as last_data_date,
                odometer,
                soh,
                soh_comparison,
                consumption,
                COALESCE(cycles, 0) as cycles
            FROM vehicle_data
            WHERE odometer IS NOT NULL
            AND vehicle_id = (SELECT id FROM vehicle WHERE vin = :vin)
            ORDER BY timestamp DESC
            LIMIT 1
        ),
        latest_data_soh AS (
            SELECT
                vehicle_id,
                timestamp as last_data_date,
                odometer,
                soh,
                soh_comparison,
                consumption,
                COALESCE(cycles, 0) as cycles
            FROM vehicle_data
            WHERE soh IS NOT NULL
            AND vehicle_id = (SELECT id FROM vehicle WHERE vin = :vin)
            ORDER BY timestamp DESC
            LIMIT 1
        ),
        weekly_data AS (
            SELECT
                vehicle_id,
                timestamp as last_data_date,
                odometer,
                soh,
                soh_comparison,
                consumption,
                COALESCE(cycles, 0) as cycles
            FROM vehicle_data
            WHERE (soh IS NOT NULL OR odometer IS NOT NULL)
            AND vehicle_id = (SELECT id FROM vehicle WHERE vin = :vin)
            AND timestamp BETWEEN (SELECT start_date FROM last_week) AND (SELECT end_date FROM last_week)
            ORDER BY timestamp DESC
            LIMIT 1
        ),
        battery_info AS (
            SELECT
                b.battery_chemistry as chemistry,
                b.capacity,
                b.battery_oem as oem,
                COALESCE(vm.autonomy, 0) as range
            FROM battery b
            JOIN vehicle_model vm ON b.id = vm.battery_id
            JOIN vehicle v ON vm.id = v.vehicle_model_id
            WHERE v.vin = :vin
        ),
        images_list AS (
            SELECT
                model_image.public_url as model_image_public_url,
                model_image.name as model_image_name,
                make_image.public_url as make_image_public_url,
                make_image.name as make_image_name
            FROM vehicle_model vm
            JOIN vehicle v ON vm.id = v.vehicle_model_id
            LEFT JOIN asset model_image ON vm.image_id = model_image.id
            LEFT JOIN make ON vm.make_id = make.id
            LEFT JOIN asset make_image ON make.image_id = make_image.id
            WHERE v.vin = :vin
        )
        SELECT
            v.vin,
            v.start_date,
            v.licence_plate,
            v.activation_status,
            v.end_of_contract_date,
            CONCAT(INITCAP(vm.model_name),
                CASE
                    WHEN vm.type is null THEN ''
                    ELSE CONCAT(' ', vm.type)
                END
            ) as model_name,
            INITCAP(oem.oem_name) as oem_name,
            vm.warranty_date,
            vm.warranty_km,
            vm.trendline->>'trendline' as trendline,
            bi.chemistry,
            bi.capacity,
            bi.range,
            il.model_image_public_url,
            il.model_image_name,
            il.make_image_public_url,
            il.make_image_name,
            bi.oem,
            COALESCE(
                CASE
                    WHEN ld_odometer.last_data_date >= (SELECT start_date FROM last_week) THEN wd.cycles
                    ELSE ld_odometer.cycles
                END,
                wd.cycles,
                ld_odometer.cycles,
                0
            ) as cycles,
            COALESCE(
                CASE
                    WHEN ld_soh.last_data_date >= (SELECT start_date FROM last_week) THEN wd.last_data_date
                    ELSE ld_soh.last_data_date
                END,
                wd.last_data_date,
                ld_odometer.last_data_date
            ) as last_data_date,
            COALESCE(
                CASE
                    WHEN ld_odometer.last_data_date >= (SELECT start_date FROM last_week) THEN wd.odometer
                    ELSE ld_odometer.odometer
                END,
                wd.odometer,
                ld_odometer.odometer
            ) as odometer,
            COALESCE(
                CASE
                    WHEN ld_soh.last_data_date >= (SELECT start_date FROM last_week) THEN wd.soh
                    ELSE ld_soh.soh
                END,
                wd.soh,
                ld_soh.soh
            ) as soh,
            COALESCE(
                CASE
                    WHEN ld_odometer.last_data_date >= (SELECT start_date FROM last_week) THEN wd.consumption
                    ELSE ld_odometer.consumption
                END,
                wd.consumption,
                ld_odometer.consumption
            ) as consumption,
            v.bib_score as score
        FROM vehicle v
        JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
        JOIN oem ON vm.oem_id = oem.id
        JOIN battery_info bi ON TRUE
        LEFT JOIN images_list il ON TRUE
        LEFT JOIN latest_data_odometer ld_odometer ON ld_odometer.vehicle_id = v.id
        LEFT JOIN latest_data_soh ld_soh ON ld_soh.vehicle_id = v.id
        LEFT JOIN weekly_data wd ON wd.vehicle_id = v.id
        WHERE v.vin = :vin
    """)
    result = await db.execute(query, {"vin": vin})
    data = result.mappings().first()

    if not data:
        logger.error("Missing data or vehicle not available for VIN lookup")
        raise HTTPException(
            status_code=404, detail="Vehicle not found or not available"
        )

    image_url = None
    if data["model_image_public_url"]:
        image_url = data["model_image_public_url"]
    elif data["model_image_name"]:
        image_url = get_model_image_url(data["model_image_name"])
    elif data["make_image_public_url"]:
        image_url = data["make_image_public_url"]
    elif data["make_image_name"]:
        image_url = get_make_image_url(data["make_image_name"])

    formatted_data = {
        "vehicle_info": {
            "vin": data["vin"],
            "brand": data["oem_name"],
            "model": data["model_name"],
            "odometer": data["odometer"],
            "score": data["score"],
            "start_date": data["start_date"].isoformat()
            if data["start_date"]
            else None,
            "image_url": image_url,
            "licence_plate": data["licence_plate"],
            "warranty_date": data["warranty_date"],
            "warranty_km": data["warranty_km"],
            "cycles": data["cycles"],
        },
        "battery_info": {
            "oem": data["oem"],
            "chemistry": data["chemistry"],
            "capacity": data["capacity"],
            "range": data["range"] if data["range"] is not None else 0,
            "consumption": data["consumption"],
            "soh": data["soh"] * 100 if data["soh"] else None,
            "trendline": data["trendline"],
        },
        "end_of_contract_date": data["end_of_contract_date"].isoformat()
        if data["end_of_contract_date"]
        else None,
        "last_data_date": data["last_data_date"].isoformat()
        if data["last_data_date"]
        else None,
        "activation_status": data["activation_status"],
    }

    return formatted_data


async def get_estimated_range(vin: str, db: AsyncSession):
    query = text("""
        WITH vehicle_info AS (
            SELECT
                v.id,
                vm.autonomy,
                v.start_date,
                vm.warranty_date,
                v.end_of_contract_date
            FROM vehicle v
            JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
            WHERE v.vin = :vin
        ),
        current_data AS (
            SELECT
                ROUND(soh::numeric, 2)*100 as current_soh,
                ROUND(odometer::numeric, 0) as current_odometer,
                timestamp as current_timestamp
            FROM vehicle_data vd
            JOIN vehicle_info vi ON vd.vehicle_id = vi.id
            WHERE soh IS NOT NULL AND odometer IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 1
        ),
        soh_degradation AS (
            SELECT
                CASE
                    WHEN cd.current_odometer > 0 THEN
                        (100 - cd.current_soh) / NULLIF(cd.current_odometer, 0)
                    ELSE NULL
                END as degradation_per_km,
                -- Calcul du taux de dégradation par jour
                CASE
                    WHEN EXTRACT(EPOCH FROM (cd.current_timestamp - vi.start_date)) > 0 THEN
                        (100 - cd.current_soh) / NULLIF(EXTRACT(EPOCH FROM (cd.current_timestamp - vi.start_date))/86400, 0)
                    ELSE NULL
                END as degradation_per_day
            FROM current_data cd
            CROSS JOIN vehicle_info vi
        ),
        predictions AS (
            SELECT
                vi.autonomy as initial_autonomy,
                cd.current_soh,
                cd.current_odometer,
                -- Calculating the SoH at the end of the warranty
                cd.current_soh - (
                    GREATEST(0, EXTRACT(EPOCH FROM ((vi.start_date + (vi.warranty_date || ' years')::interval) - cd.current_timestamp))/86400)
                    * sd.degradation_per_day
                ) as warranty_end_soh,
                -- Calculating the SoH at the end of the contract
                cd.current_soh - (
                    GREATEST(0, EXTRACT(EPOCH FROM (vi.end_of_contract_date - cd.current_timestamp))/86400)
                    * sd.degradation_per_day
                ) as contract_end_soh,
                -- Estimating the odometer at the end of the warranty
                cd.current_odometer + (
                    EXTRACT(EPOCH FROM ((vi.start_date + (vi.warranty_date || ' years')::interval) - cd.current_timestamp))/86400
                    * (cd.current_odometer / EXTRACT(EPOCH FROM (cd.current_timestamp - vi.start_date))/86400)
                ) as warranty_end_odometer,
                -- Estimating the odometer at the end of the contract
                cd.current_odometer + (
                    EXTRACT(EPOCH FROM (vi.end_of_contract_date - cd.current_timestamp))/86400
                    * (cd.current_odometer / EXTRACT(EPOCH FROM (cd.current_timestamp - vi.start_date))/86400)
                ) as contract_end_odometer,
                -- Calculating the SoH for the extension of 30 000km
                cd.current_soh - (30000 * CASE
                    WHEN cd.current_odometer > 0 THEN
                        (100 - cd.current_soh) / NULLIF(cd.current_odometer, 0)
                    ELSE NULL
                END) as extended_soh
            FROM vehicle_info vi
            CROSS JOIN current_data cd
            CROSS JOIN soh_degradation sd
        )
        SELECT *
        FROM predictions
    """)

    result = await db.execute(query, {"vin": vin})
    data = result.mappings().first()

    if not data or data["initial_autonomy"] is None:
        return None

    initial_range = {
        "soh": 100,
        "range_min": data["initial_autonomy"],
        "range_max": data["initial_autonomy"],
        "odometer": 0,
    }

    if data["current_soh"] is None:
        return None

    current_range = {
        "soh": round(data["current_soh"]),
        "range_min": round((data["initial_autonomy"] * data["current_soh"] / 100) - 10),
        "range_max": round((data["initial_autonomy"] * data["current_soh"] / 100) + 10),
        "odometer": round(data["current_odometer"]),
    }

    future_predictions = []

    # Adding the extended prediction of 30 000km
    if data["extended_soh"] is not None:
        extended_soh = max(round(data["extended_soh"]), 0)
        extended_soh = min(extended_soh, round(data["current_soh"]))
        if extended_soh > 0 and extended_soh != round(data["current_soh"]):
            future_predictions.append(
                {
                    "soh": extended_soh,
                    "range_min": max(
                        round((data["initial_autonomy"] * extended_soh / 100) - 10), 0
                    ),
                    "range_max": max(
                        round((data["initial_autonomy"] * extended_soh / 100) + 10), 0
                    ),
                    "odometer": round(data["current_odometer"] + 30000),
                }
            )

    if data["contract_end_soh"] is not None:
        contract_soh = max(round(data["contract_end_soh"]), 0)
        contract_soh = min(contract_soh, round(data["current_soh"]))
        if contract_soh > 0 and contract_soh != round(data["current_soh"]):
            future_predictions.append(
                {
                    "soh": contract_soh,
                    "range_min": max(
                        round((data["initial_autonomy"] * contract_soh / 100) - 10), 0
                    ),
                    "range_max": max(
                        round((data["initial_autonomy"] * contract_soh / 100) + 10), 0
                    ),
                    "odometer": max(
                        round(data["contract_end_odometer"])
                        if data.get("contract_end_odometer") is not None
                        else None,
                        data["current_odometer"],
                    ),
                }
            )

    if data["warranty_end_soh"] is not None:
        warranty_soh = max(round(data["warranty_end_soh"]), 0)
        warranty_soh = min(warranty_soh, round(data["current_soh"]))
        if warranty_soh > 0 and warranty_soh != round(data["current_soh"]):
            future_predictions.append(
                {
                    "soh": warranty_soh,
                    "range_min": max(
                        round((data["initial_autonomy"] * warranty_soh / 100) - 10), 0
                    ),
                    "range_max": max(
                        round((data["initial_autonomy"] * warranty_soh / 100) + 10), 0
                    ),
                    "odometer": max(
                        round(data["warranty_end_odometer"]), data["current_odometer"]
                    ),
                }
            )

    return {
        "initial": initial_range,
        "current": current_range,
        "predictions": future_predictions,
    }


async def get_charging_cycles(vin: str, db: AsyncSession):
    query = text("""
        SELECT
            COALESCE(SUM(level_1), 0) as level_1_count,
            COALESCE(SUM(level_2), 0) as level_2_count,
            COALESCE(SUM(level_3), 0) as level_3_count,
            COALESCE(SUM(level_1), 0) + COALESCE(SUM(level_2), 0) + COALESCE(SUM(level_3), 0) as total_count
        FROM vehicle_data
        WHERE vehicle_id = (SELECT id FROM vehicle WHERE vin = :vin)
    """)
    result = await db.execute(query, {"vin": vin})
    return result.mappings().first()


async def get_kpis_additional(vin: str, db: AsyncSession):
    query = text("""
        WITH vehicle_info AS (
            SELECT
                v.id,
                vm.autonomy as vehicle_range
            FROM vehicle v
            JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
            WHERE v.vin = :vin
        )
        SELECT
            ROUND(odometer::numeric, 0) as odometer,
            consumption,
            cycles
        FROM vehicle_data vd
        JOIN vehicle_info vi ON vd.vehicle_id = vi.id
        WHERE odometer IS NOT NULL
        ORDER BY timestamp DESC
    """)

    result = await db.execute(query, {"vin": vin})
    raw_data = result.mappings().all()
    if len(raw_data) == 0:
        raise HTTPException(status_code=404, detail="No additional kpis data found")

    # Finding the maximum value of 'cycles' in raw_data, ignoring None values
    max_cycles = max(
        (row["cycles"] if row["cycles"] is not None else 0 for row in raw_data),
        default=0,
    )

    # Getting global average of average consumption for each row
    consumption_average = None
    data = [row for row in raw_data if row["consumption"] is not None]
    if len(data) > 1:
        sorted_data = sorted(data, key=lambda x: x["odometer"])
        consumption_average = 0
        total_distance = 0
        odometer_previous = sorted_data[0]["odometer"]
        for row in sorted_data[1:]:
            distance = row["odometer"] - odometer_previous
            total_distance += distance
            consumption_average += distance * row["consumption"]
            odometer_previous = row["odometer"]
        consumption_average = consumption_average / total_distance
    elif len(data) == 1:
        consumption_average = data[0]["consumption"]

    return {"consumption": consumption_average, "cycles": max_cycles}


async def post_pin_vehicle(vin: str, is_pinned: bool, db: AsyncSession):
    query = text("""
        UPDATE vehicle
        SET is_pinned = :is_pinned
        WHERE vin = :vin
    """)
    await db.execute(query, {"vin": vin, "is_pinned": is_pinned})
    await db.commit()
    return {"message": "Vehicle info updated successfully"}


async def get_pinned_vehicle(vin: str, db: AsyncSession):
    query = text("""
        SELECT is_pinned
        FROM vehicle
        WHERE vin = :vin
    """)
    result = await db.execute(query, {"vin": vin})
    return result.mappings().first()
