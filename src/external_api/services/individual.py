from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def get_all_pinned_vehicles(
    fleet_ids: list[str], page: int, limit: int, db: AsyncSession
):
    offset = (page - 1) * limit
    query = text("""
        SELECT DISTINCT ON (v.id)
            v.vin,
            ma.make_name,
            vd.odometer,
            ROUND((vd.soh_bib * 100)::numeric, 1) as soh,
            ROUND((vd.soh_oem * 100)::numeric, 1) as soh_oem,
            ROUND(((1 - vd.soh_bib) * 100) / NULLIF(vd.odometer / 10000, 0)::numeric, 2) as soh_per_10000km,
            v.start_date
        FROM vehicle v
        LEFT JOIN vehicle_data vd ON v.id = vd.vehicle_id
        LEFT JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
        LEFT JOIN make ma ON vm.make_id = ma.id
        WHERE v.fleet_id = ANY(:fleet_ids)
        AND v.is_pinned = true
        ORDER BY v.id, vd.timestamp DESC NULLS LAST
        LIMIT :limit OFFSET :offset
    """)

    result = await db.execute(
        query, {"fleet_ids": fleet_ids, "limit": limit, "offset": offset}
    )
    rows = result.mappings().all()

    if not rows:
        return []

    return [
        {
            "vin": row["vin"],
            "startDate": row["start_date"],
            "makeName": row["make_name"],
            "soh": row["soh"],
            "sohOem": row["soh_oem"],
            "odometer": row["odometer"],
            "sohPer10000km": row["soh_per_10000km"],
        }
        for row in rows
    ]
