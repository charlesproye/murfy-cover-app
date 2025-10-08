from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def get_all_pinned_vehicles(
    fleet_id: str, page: int, limit: int, db: AsyncSession
):
    offset = (page - 1) * limit
    query = text("""
        SELECT DISTINCT ON (v.id)
            v.vin,
            v.start_date,
            v.is_pinned,
            ROUND((vd.soh * 100)::numeric, 1) as soh,
            vd.odometer,
            ROUND(((1 - vd.soh) * 100) / NULLIF(vd.odometer / 10000, 0)::numeric, 2) as soh_per_10000km,
            vd.timestamp
        FROM vehicle v
        LEFT JOIN vehicle_data vd ON v.id = vd.vehicle_id
        WHERE v.fleet_id = :fleet_id
        AND v.is_pinned = true
        ORDER BY v.id, vd.timestamp DESC NULLS LAST
        LIMIT :limit OFFSET :offset
    """)

    result = await db.execute(
        query, {"fleet_id": fleet_id, "limit": limit, "offset": offset}
    )
    rows = result.mappings().all()

    if not rows:
        return []

    return [
        {
            "vin": row["vin"],
            "startDate": row["start_date"],
            "isPinned": row["is_pinned"],
            "soh": row["soh"],
            "odometer": row["odometer"],
            "timestamp": row["timestamp"],
            "medianSohLost10000km": row["soh_per_10000km"],
        }
        for row in rows
    ]


async def get_all_fast_charge(fleet_id: str, page: int, limit: int, db: AsyncSession):
    offset = (page - 1) * limit
    query = text("""
        SELECT DISTINCT ON (v.id)
            v.vin,
            v.start_date,
            vd.timestamp,
            ROUND((vd.soh * 100)::numeric, 1) as soh,
            vd.odometer,
            vd.level_1,
            vd.level_3,
            ROUND(
                CASE
                    WHEN (vd.level_1 + vd.level_3) > 0
                    THEN vd.level_3::numeric / (vd.level_1 + vd.level_2 + vd.level_3)
                    ELSE NULL
                END, 3
            ) AS fast_charge_ratio
        FROM vehicle v
        LEFT JOIN vehicle_data vd ON v.id = vd.vehicle_id
        WHERE v.fleet_id = :fleet_id
        ORDER BY v.id, vd.timestamp DESC NULLS LAST
        LIMIT :limit OFFSET :offset
    """)
    result = await db.execute(
        query, {"fleet_id": fleet_id, "limit": limit, "offset": offset}
    )
    rows = result.mappings().all()
    return [
        {
            "vin": row["vin"],
            "startDate": row["start_date"],
            "timestamp": row["timestamp"],
            "soh": row["soh"],
            "odometer": row["odometer"],
            "totalNormalCharge": row["level_1"],
            "totalFastCharge": row["level_3"],
            "fastChargeRatio": row["fast_charge_ratio"],
        }
        for row in rows
    ]


async def get_all_consumption(fleet_id: str, page: int, limit: int, db: AsyncSession):
    offset = (page - 1) * limit
    query = text("""
        SELECT DISTINCT ON (v.id)
            v.vin,
            v.start_date,
            ROUND((vd.soh * 100)::numeric, 1) as soh,
            vd.consumption,
            vd.timestamp,
            vd.odometer
        FROM vehicle v
        LEFT JOIN vehicle_data vd ON v.id = vd.vehicle_id
        WHERE v.fleet_id = :fleet_id
        ORDER BY v.id, vd.timestamp DESC NULLS LAST
        LIMIT :limit OFFSET :offset
    """)

    result = await db.execute(
        query, {"fleet_id": fleet_id, "limit": limit, "offset": offset}
    )
    rows = result.mappings().all()
    return [
        {
            "vin": row["vin"],
            "startDate": row["start_date"],
            "soh": row["soh"],
            "consumption": row["consumption"],
            "odometer": row["odometer"],
            "timestamp": row["timestamp"],
        }
        for row in rows
    ]

