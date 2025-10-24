from datetime import datetime, timedelta
from uuid import UUID

from pydantic import UUID4
from sqlalchemy import ARRAY, bindparam, text
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.ext.asyncio import AsyncSession

from external_api.schemas.user import FleetInfo


async def get_kpis(
    fleets: list[FleetInfo],
    brands: list[UUID4] | None = None,
    regions: list[UUID4] | None = None,
    pinned_vehicles: bool = False,
    db: AsyncSession = None,
) -> list[dict]:
    """
    Get KPIs for the dashboard

    Args:
        fleets: List of fleet IDs
        brands: List of brand IDs
        regions: List of region IDs
        pinned_vehicles: Whether to only show pinned vehicles
        db: Database session

    Returns:
        List of KPIs
    """
    # Extract fleet IDs from FleetInfo objects
    fleet_id_list = [fleet.get("id") for fleet in fleets] if fleets else []

    query = text("""
        WITH latest_vehicle_data AS (
            SELECT DISTINCT ON (v.id)
                v.id,
                vd.odometer,
                vd.soh * 100 as soh,
                vd.timestamp
            FROM vehicle v
            JOIN vehicle_data vd ON v.id = vd.vehicle_id
            JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
            WHERE v.fleet_id = ANY(:fleet_ids)
            AND v.is_displayed = true
            AND (:brands IS NULL OR vm.oem_id = ANY(:brands))
            AND (:regions IS NULL OR v.region_id = ANY(:regions))
            AND (:pinned_vehicles = false OR v.is_pinned = true)
            ORDER BY v.id, vd.timestamp DESC
        )
        SELECT
            CURRENT_DATE as week,
            COALESCE(ROUND(AVG(odometer)), 0) as avg_odometer,
            COALESCE(ROUND(AVG(soh), 1), 0) as avg_soh,
            COUNT(DISTINCT id) as vehicle_count
        FROM latest_vehicle_data
    """).bindparams(
        bindparam("fleet_ids", type_=ARRAY(PGUUID)),
        bindparam("brands", type_=ARRAY(PGUUID)),
        bindparam("regions", type_=ARRAY(PGUUID)),
    )

    result = await db.execute(
        query,
        {
            "fleet_ids": fleet_id_list,
            "brands": brands,
            "regions": regions,
            "pinned_vehicles": pinned_vehicles,
        },
    )

    rows = result.mappings().all()
    return [dict(row) for row in rows]


async def get_scatter_plot_brands(
    fleets: list[str],
    brands: list[str] | None,
    fleets_input_list: list[str],
    pinned_vehicles: bool = False,
    db: AsyncSession = None,
):
    brands_list = brands if brands else None
    fleet_list = (
        fleets
        if not fleets_input_list or fleets_input_list[0].lower() == "all"
        else fleets_input_list
    )

    query = text("""
        WITH latest_vehicle_data AS (
            SELECT DISTINCT ON (vd.vehicle_id)
                v.vin,
                vd.odometer,
                ROUND((vd.soh * 100), 1) as soh,
                vm.oem_id,
                o.oem_name,
                v.fleet_id,
                f.fleet_name
            FROM vehicle_data vd
            JOIN vehicle v ON vd.vehicle_id = v.id
            JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
            JOIN oem o ON vm.oem_id = o.id
            JOIN fleet f ON v.fleet_id = f.id
            WHERE v.fleet_id = ANY(:fleet_list)
            AND v.is_displayed = true
            AND (CAST(:brands_list AS uuid[]) IS NULL OR vm.oem_id = ANY(CAST(:brands_list AS uuid[])))
            AND (:pinned_vehicles = false OR v.is_pinned = true)
            ORDER BY vd.vehicle_id, vd.timestamp DESC
        )
        SELECT
            vin,
            odometer,
            ROUND(soh, 2) as soh,
            oem_id,
            oem_name,
            fleet_id,
            fleet_name
        FROM latest_vehicle_data
        ORDER BY odometer DESC, fleet_name, oem_name, vin
    """)

    result = await db.execute(
        query,
        {
            "fleet_list": fleet_list,
            "brands_list": brands_list,
            "pinned_vehicles": pinned_vehicles,
        },
    )

    rows = result.mappings().all()

    # Organiser les données par fleet puis par marque
    fleet_data = {}
    for row in rows:
        fleet_key = row["fleet_name"]
        brand_key = row["oem_name"]

        # Initialiser le dictionnaire de la fleet si nécessaire
        if fleet_key not in fleet_data:
            fleet_data[fleet_key] = {}

        # Initialiser la liste de la marque si nécessaire
        if brand_key not in fleet_data[fleet_key]:
            fleet_data[fleet_key][brand_key] = []

        # Ajouter les données du véhicule
        fleet_data[fleet_key][brand_key].append(
            {"vin": row["vin"], "odometer": row["odometer"], "soh": row["soh"]}
        )

    return fleet_data


async def get_scatter_plot_regions(
    fleets: list[str],
    regions: list[str] | None,
    fleets_input_list: list[str],
    pinned_vehicles: bool = False,
    db: AsyncSession = None,
):
    regions_list = regions if regions else None
    fleet_list = (
        fleets
        if not fleets_input_list or fleets_input_list[0].lower() == "all"
        else fleets_input_list
    )

    query = text("""
        WITH latest_vehicle_data AS (
            SELECT DISTINCT ON (vd.vehicle_id)
                v.vin,
                vd.odometer,
                ROUND((vd.soh * 100), 1) as soh,
                v.region_id,
                r.region_name,
                v.fleet_id,
                f.fleet_name
            FROM vehicle_data vd
            JOIN vehicle v ON vd.vehicle_id = v.id
            JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
            JOIN region r ON v.region_id = r.id
            JOIN fleet f ON v.fleet_id = f.id
            WHERE v.fleet_id = ANY(:fleet_list)
            AND v.is_displayed = true
            AND (CAST(:regions_list AS uuid[]) IS NULL OR v.region_id = ANY(CAST(:regions_list AS uuid[])))
            AND (:pinned_vehicles = false OR v.is_pinned = true)
            ORDER BY vd.vehicle_id, vd.timestamp DESC
        )
        SELECT
            vin,
            odometer,
            ROUND(soh, 2) as soh,
            region_id,
            region_name,
            fleet_id,
            fleet_name
        FROM latest_vehicle_data
        ORDER BY fleet_name, region_name, vin
    """)

    result = await db.execute(
        query,
        {
            "fleet_list": fleet_list,
            "regions_list": regions_list,
            "pinned_vehicles": pinned_vehicles,
        },
    )

    rows = result.mappings().all()

    # Organiser les données par fleet puis par région
    fleet_data = {}
    for row in rows:
        fleet_key = row["fleet_name"]
        region_key = row["region_name"]

        # Initialiser le dictionnaire de la fleet si nécessaire
        if fleet_key not in fleet_data:
            fleet_data[fleet_key] = {}

        # Initialiser la liste de la région si nécessaire
        if region_key not in fleet_data[fleet_key]:
            fleet_data[fleet_key][region_key] = []

        # Ajouter les données du véhicule
        fleet_data[fleet_key][region_key].append(
            {"vin": row["vin"], "odometer": row["odometer"], "soh": row["soh"]}
        )

    return fleet_data


async def get_global_table(
    fleet_ids: list[FleetInfo],
    brands: list[UUID4] | None = None,
    regions: list[UUID4] | None = None,
    pinned_vehicles: bool = False,
    db: AsyncSession = None,
) -> list[dict]:
    fleet_id_list = [fleet.get("id") for fleet in fleet_ids] if fleet_ids else []

    query = text("""
        WITH latest_vehicle_data AS (
            SELECT DISTINCT ON (vd.vehicle_id)
                vd.vehicle_id,
                v.fleet_id,
                f.fleet_name,
                vm.oem_id,
                o.oem_name,
                v.region_id,
                r.region_name,
                vd.odometer as last_odometer,
                ROUND((vd.soh * 100), 1) as last_soh
            FROM vehicle_data vd
            JOIN vehicle v ON vd.vehicle_id = v.id
            JOIN fleet f ON v.fleet_id = f.id
            JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
            JOIN oem o ON vm.oem_id = o.id
            JOIN region r ON v.region_id = r.id
            WHERE v.fleet_id = ANY(:fleet_ids)
            AND v.is_displayed = true
            AND (:brands IS NULL OR vm.oem_id = ANY(:brands))
            AND (:regions IS NULL OR v.region_id = ANY(:regions))
            AND (:pinned_vehicles = false OR v.is_pinned = true)
            ORDER BY vd.vehicle_id, vd.timestamp DESC
        ),
        aggregated_data AS (
            SELECT
                fleet_id,
                fleet_name,
                oem_id,
                oem_name,
                region_id,
                region_name,
                ROUND(AVG(last_odometer)) as avg_odometer,
                ROUND(AVG(last_soh), 1) as avg_soh,
                COUNT(DISTINCT vehicle_id) as vehicle_count,
                ROUND(AVG(last_odometer) / NULLIF((100 - AVG(last_soh)), 0)) as km_per_soh_loss
            FROM latest_vehicle_data
            GROUP BY fleet_id, fleet_name, oem_id, oem_name, region_id, region_name
        )
        SELECT
            fleet_id,
            fleet_name,
            oem_id,
            oem_name,
            region_id,
            region_name,
            avg_odometer,
            avg_soh,
            vehicle_count
        FROM aggregated_data
        ORDER BY
            km_per_soh_loss DESC NULLS LAST,
            oem_name,
            region_name
    """).bindparams(
        bindparam("fleet_ids", type_=ARRAY(PGUUID)),
        bindparam("brands", type_=ARRAY(PGUUID)),
        bindparam("regions", type_=ARRAY(PGUUID)),
    )

    result = await db.execute(
        query,
        {
            "fleet_ids": fleet_id_list,
            "brands": brands,
            "regions": regions,
            "pinned_vehicles": pinned_vehicles,
        },
    )

    return [
        {
            "fleet_id": str(row[0]),
            "fleet_name": row[1],
            "oem_id": str(row[2]),
            "oem_name": row[3],
            "region_id": str(row[4]),
            "region_name": row[5],
            "avg_odometer": float(row[6]) if row[6] is not None else None,
            "avg_soh": float(row[7]) if row[7] is not None else None,
            "vehicle_count": row[8],
        }
        for row in result.all()
    ]


async def get_last_timestamp_with_data(fleet_id: str, db: AsyncSession):
    query = text("""
        SELECT MAX(vd.timestamp) as last_update
        FROM vehicle_data vd
        JOIN vehicle v ON vd.vehicle_id = v.id
        WHERE v.fleet_id = :fleet_id
        AND vd.soh IS NOT NULL
        AND vd.odometer IS NOT NULL
    """)
    result = await db.execute(query, {"fleet_id": fleet_id})
    last_update_data = result.mappings().all()
    last_update = (
        last_update_data[0]["last_update"] if len(last_update_data) > 0 else None
    )
    return last_update


async def get_individual_kpis(fleet_id: str, db: AsyncSession):
    latest_timestamp = await get_last_timestamp_with_data(fleet_id, db)
    if not latest_timestamp:
        return [
            {
                "avg_odometer": None,
                "avg_soh": None,
                "vehicle_count": 0,
                "pinned_count": 0,
            },
        ]

    # Convert to datetime if it's not already
    if isinstance(latest_timestamp, str):
        latest_timestamp = datetime.fromisoformat(
            latest_timestamp.replace("Z", "+00:00")
        )

    # Calculate 1 month and 2 months ago using timedelta
    # This is approximate but works well for most use cases
    one_month_since_last_data = latest_timestamp - timedelta(days=30)
    two_months_since_last_data = latest_timestamp - timedelta(days=60)

    query = text("""
        WITH vehicles_data AS (
            SELECT DISTINCT ON (vd.vehicle_id)
                vd.vehicle_id,
                vd.timestamp,
                vd.odometer,
                ROUND((vd.soh * 100), 1) as soh,
                v.is_pinned
            FROM vehicle_data vd
            JOIN vehicle v ON vd.vehicle_id = v.id
            WHERE v.fleet_id = :fleet_id
            AND vd.timestamp > :one_month_since_last_data
            ORDER BY vd.vehicle_id, vd.timestamp DESC
        )
        SELECT
            ROUND(AVG(odometer)) as avg_odometer,
            ROUND(AVG(soh), 1) as avg_soh,
            COUNT(DISTINCT vehicle_id) as vehicle_count,
            COUNT(*) FILTER (WHERE is_pinned = true) as pinned_count
        FROM vehicles_data
    """)
    result = await db.execute(
        query,
        {"fleet_id": fleet_id, "one_month_since_last_data": one_month_since_last_data},
    )
    latest_data = result.mappings().all()

    query = text("""
        WITH previous_vehicle_data AS (
            SELECT DISTINCT ON (vd.vehicle_id)
                vd.vehicle_id,
                vd.timestamp,
                vd.odometer,
                ROUND((vd.soh * 100), 1) as soh,
                v.is_pinned
            FROM vehicle_data vd
            JOIN vehicle v ON vd.vehicle_id = v.id
            WHERE v.fleet_id = :fleet_id
            AND vd.timestamp < :one_month_since_last_data
            AND vd.timestamp > :two_months_since_last_data
            ORDER BY vd.vehicle_id, vd.timestamp DESC
        )
        SELECT
            ROUND(AVG(odometer)) as avg_odometer,
            ROUND(AVG(soh), 1) as avg_soh,
            COUNT(DISTINCT vehicle_id) as vehicle_count,
            COUNT(*) FILTER (WHERE is_pinned = true) as pinned_count
        FROM previous_vehicle_data
    """)
    result_previous = await db.execute(
        query,
        {
            "fleet_id": fleet_id,
            "one_month_since_last_data": one_month_since_last_data,
            "two_months_since_last_data": two_months_since_last_data,
        },
    )
    previous_data = result_previous.mappings().all()

    return [
        {
            "avg_odometer": latest_data[0]["avg_odometer"],
            "avg_soh": latest_data[0]["avg_soh"],
            "vehicle_count": latest_data[0]["vehicle_count"],
            "pinned_count": latest_data[0]["pinned_count"],
        },
        # Only include previous data if avg_odometer and avg_soh are not None
        *(
            [
                {
                    "avg_odometer": previous_data[0]["avg_odometer"],
                    "avg_soh": previous_data[0]["avg_soh"],
                    "vehicle_count": previous_data[0]["vehicle_count"],
                    "pinned_count": previous_data[0]["pinned_count"],
                }
            ]
            if previous_data
            and previous_data[0]["avg_odometer"] is not None
            and previous_data[0]["avg_soh"] is not None
            else []
        ),
    ]


async def get_filter(
    base_fleet: list[FleetInfo],
    fleet_id: UUID4 | None = None,
    db: AsyncSession = None,
) -> list[dict]:
    """
    Get filter data for the dashboard

    Args:
        base_fleet: List of fleet IDs
        fleet_id: Fleet ID
        db: Database session

    Returns:
        List of filter data
    """
    # Extract fleet IDs from FleetInfo objects
    fleet_id_list = [fleet.get("id") for fleet in base_fleet] if base_fleet else []

    query = text("""
        WITH brands AS (
            SELECT DISTINCT ON (o.id)
                o.id as oem_id,
                INITCAP(o.oem_name) as oem_name
            FROM vehicle v
            JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
            JOIN oem o ON vm.oem_id = o.id
            WHERE v.fleet_id = ANY(:fleet_ids)
            AND v.is_displayed = true
        ),
        fleets AS (
            SELECT
                id,
                fleet_name as name
            FROM fleet
            WHERE id = ANY(:fleet_ids)
        )
        SELECT
            'brands' as type,
            json_build_object(
                'id', oem_id,
                'name', oem_name
            ) as data
        FROM brands
        UNION ALL
        SELECT
            'fleets' as type,
            json_build_object(
                'id', id,
                'name', name
            ) as data
        FROM fleets
    """)

    result = await db.execute(query, {"fleet_ids": fleet_id_list})

    rows = result.mappings().all()
    return [dict(row) for row in rows]


async def get_range_soh(fleet_id: str, type: str, db: AsyncSession):
    if type == "soh":
        query = text("""
            WITH latest_vehicle_data AS (
                SELECT DISTINCT ON (vd.vehicle_id)
                    vd.vehicle_id,
                    vd.soh * 100 as value
                FROM vehicle_data vd
                JOIN vehicle v ON vd.vehicle_id = v.id
                WHERE v.fleet_id = :fleet_id
                AND v.is_displayed = true
                ORDER BY vd.vehicle_id, vd.timestamp DESC
            ),
            ranges AS (
                SELECT unnest(ARRAY[70, 80, 85, 90, 95, 100]) as upper_bound,
                       unnest(ARRAY[0, 70, 80, 85, 90, 95]) as lower_bound
            )
            SELECT
                r.lower_bound,
                r.upper_bound,
                COUNT(lvd.vehicle_id) as vehicle_count
            FROM ranges r
            LEFT JOIN latest_vehicle_data lvd
            ON lvd.value > r.lower_bound AND lvd.value <= r.upper_bound
            GROUP BY r.lower_bound, r.upper_bound
            ORDER BY r.lower_bound DESC
        """)
    else:
        query = text("""
            WITH latest_vehicle_data AS (
                SELECT DISTINCT ON (vd.vehicle_id)
                    vd.vehicle_id,
                    vd.odometer as value
                FROM vehicle_data vd
                JOIN vehicle v ON vd.vehicle_id = v.id
                WHERE v.fleet_id = :fleet_id
                AND v.is_displayed = true
                ORDER BY vd.vehicle_id, vd.timestamp DESC
            ),
            value_bounds AS (
                SELECT
                    MIN(value) as min_value,
                    MAX(value) as max_value
                FROM latest_vehicle_data
            ),
            value_intervals AS (
                SELECT
                    ROUND(min_value + (i * (max_value - min_value) / 5)) as lower_bound,
                    ROUND(min_value + ((i + 1) * (max_value - min_value) / 5)) as upper_bound
                FROM value_bounds, generate_series(0, 4) as i
                WHERE max_value IS NOT NULL
            )
            SELECT
                vi.lower_bound,
                vi.upper_bound,
                COUNT(lvd.vehicle_id) as vehicle_count
            FROM value_intervals vi
            LEFT JOIN latest_vehicle_data lvd
            ON lvd.value >= vi.lower_bound
            AND (
                CASE
                    WHEN vi.upper_bound = (SELECT ROUND(max_value) FROM value_bounds)
                    THEN lvd.value <= vi.upper_bound
                    ELSE lvd.value < vi.upper_bound
                END
            )
            GROUP BY vi.lower_bound, vi.upper_bound
            ORDER BY vi.lower_bound
        """)

    result = await db.execute(query, {"fleet_id": fleet_id})
    rows = result.mappings().all()

    return [
        {
            "lower_bound": int(row["lower_bound"]),
            "upper_bound": int(row["upper_bound"]),
            "vehicle_count": row["vehicle_count"],
        }
        for row in rows
    ]


async def get_new_vehicles(fleet_id: str, period: str, db: AsyncSession):
    if period not in ["monthly", "annually"]:
        period = "monthly"

    # Déterminer l'intervalle et la période en fonction de la période
    interval = "4 months" if period == "monthly" else "4 years"
    period_trunc = "month" if period == "monthly" else "year"

    query = text(f"""
        SELECT
            DATE_TRUNC(:period_trunc, start_date) AS period,
            COUNT(*) AS vehicle_count
        FROM vehicle
        WHERE fleet_id = :fleet_id
        AND is_displayed = true
        AND start_date >= CURRENT_DATE - INTERVAL '{interval}'
        GROUP BY period
        ORDER BY period DESC
        LIMIT 4
    """)

    result = await db.execute(
        query, {"fleet_id": fleet_id, "period_trunc": period_trunc}
    )
    return result.mappings().all()


async def search_vin(vin: str, fleets: list[UUID], db: AsyncSession):
    if not fleets:
        return []

    query = text("""
        SELECT id, vin FROM vehicle
        WHERE vin ILIKE '%' || :vin || '%'
        AND fleet_id = ANY(:fleets)
        AND is_displayed = true
        ORDER BY vin
        LIMIT 10
    """)

    result = await db.execute(query, {"vin": vin.upper(), "fleets": fleets})
    return result.mappings().all()


def query_region_table():
    query = text("""
        WITH latest_vehicle_data AS (
            SELECT DISTINCT ON (vd.vehicle_id)
                vd.vehicle_id,
                vd.odometer,
                ROUND((vd.soh * 100), 1) as soh,
                v.region_id,
                r.region_name
            FROM vehicle v
            JOIN region r ON v.region_id = r.id
            JOIN vehicle_data vd ON v.id = vd.vehicle_id
            WHERE v.fleet_id = :fleet_id
            AND v.is_displayed = true
            ORDER BY vd.vehicle_id, vd.timestamp DESC
        )
        SELECT
            region_id,
            region_name,
            ROUND(AVG(odometer)) as avg_odometer,
            ROUND(AVG(soh), 1) as avg_soh,
            COUNT(vehicle_id) as nb_vehicle
        FROM latest_vehicle_data
        GROUP BY region_id, region_name
        ORDER BY region_name
    """)
    return query


def query_brand_table():
    query = text("""
        WITH latest_vehicle_data AS (
            SELECT DISTINCT ON (vd.vehicle_id)
                vd.vehicle_id,
                vd.odometer,
                ROUND((vd.soh * 100), 1) as soh,
                vm.oem_id,
                o.oem_name
            FROM vehicle v
            JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
            JOIN oem o ON vm.oem_id = o.id
            JOIN vehicle_data vd ON v.id = vd.vehicle_id
            WHERE v.fleet_id = :fleet_id
            AND v.is_displayed = true
            ORDER BY vd.vehicle_id, vd.timestamp DESC
        )
        SELECT
            oem_id,
            oem_name,
            ROUND(AVG(odometer)) as avg_odometer,
            ROUND(AVG(soh), 1) as avg_soh,
            COUNT(vehicle_id) as nb_vehicle
        FROM latest_vehicle_data
        GROUP BY oem_id, oem_name
        ORDER BY oem_name
    """)
    return query


def query_all_table():
    query = text("""
        WITH latest_vehicle_data AS (
            SELECT DISTINCT ON (vd.vehicle_id)
                vd.vehicle_id,
                vd.odometer,
                ROUND((vd.soh * 100), 1) as soh,
                vm.oem_id,
                o.oem_name,
                v.region_id,
                r.region_name
            FROM vehicle v
            JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
            JOIN oem o ON vm.oem_id = o.id
            JOIN region r ON v.region_id = r.id
            JOIN vehicle_data vd ON v.id = vd.vehicle_id
            WHERE v.fleet_id = :fleet_id
            AND v.activation_status = true
            ORDER BY vd.vehicle_id, vd.timestamp DESC
        )
        SELECT
            oem_id,
            oem_name,
            region_id,
            region_name,
            ROUND(AVG(odometer)) as avg_odometer,
            ROUND(AVG(soh), 1) as avg_soh,
            COUNT(vehicle_id) as nb_vehicle
        FROM latest_vehicle_data
        GROUP BY oem_id, oem_name, region_id, region_name
        ORDER BY oem_name, region_name
    """)
    return query


async def get_table_brand(fleet_id: str, filter: str, db: AsyncSession):
    if filter.lower() == "region":
        query = query_region_table()
    elif filter.lower() == "make":
        query = query_brand_table()
    else:
        query = query_all_table()

    result = await db.execute(query, {"fleet_id": fleet_id})
    return result.mappings().all()


async def get_trendline_brand(
    fleet_id: str, db: AsyncSession, brand: str | None = None
):
    query = text("""
        WITH first_brand AS (
            SELECT DISTINCT ON (vm.oem_id)
                vm.oem_id,
                o.oem_name,
                o.trendline::text as trendline,
                o.trendline_max::text as trendline_max,
                o.trendline_min::text as trendline_min
            FROM vehicle v
            JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
            JOIN oem o ON vm.oem_id = o.id
            WHERE v.fleet_id = :fleet_id
            AND v.is_displayed = true
            ORDER BY vm.oem_id
            LIMIT 1
        ),
        brands_with_data AS (
            SELECT DISTINCT
                vm.oem_id,
                o.oem_name,
                o.trendline::text as trendline,
                o.trendline_max::text as trendline_max,
                o.trendline_min::text as trendline_min,
                MIN(vd.timestamp) as first_data
            FROM vehicle v
            JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
            JOIN oem o ON vm.oem_id = o.id
            LEFT JOIN vehicle_data vd ON v.id = vd.vehicle_id
            WHERE v.fleet_id = :fleet_id
            AND v.is_displayed = true
            GROUP BY vm.oem_id, o.oem_name, o.trendline::text, o.trendline_max::text, o.trendline_min::text
        ),
        latest_vehicle_data AS (
            SELECT DISTINCT ON (v.id)
                v.id,
                v.vin,
                ROUND((vd.soh * 100), 1) as soh,
                vd.odometer,
                vd.soh_comparison,
                v.fleet_id = :fleet_id as in_fleet,
                CASE
                    WHEN vd.soh_comparison >= 3.50 THEN 'A'
                    WHEN vd.soh_comparison >= 2.00 THEN 'B'
                    WHEN vd.soh_comparison >= 1.45 THEN 'C'
                    WHEN vd.soh_comparison > -0.74 THEN 'D'
                    WHEN vd.soh_comparison > -4.00 THEN 'E'
                    ELSE 'F'
                END as score
            FROM vehicle v
            JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
            JOIN vehicle_data vd ON v.id = vd.vehicle_id
            WHERE vm.oem_id = COALESCE(:brand, (SELECT oem_id FROM first_brand))
            AND v.is_displayed = true
            ORDER BY v.id, vd.timestamp DESC
        )
        SELECT DISTINCT
            b.oem_id,
            b.oem_name,
            b.trendline,
            b.trendline_max,
            b.trendline_min,
            b.first_data,
            lvd.soh,
            lvd.odometer,
            lvd.vin,
            lvd.in_fleet,
            lvd.score,
            CASE WHEN b.oem_id = COALESCE(:brand, (SELECT oem_id FROM first_brand)) THEN 0 ELSE 1 END as sort_order
        FROM brands_with_data b
        LEFT JOIN latest_vehicle_data lvd ON b.oem_id = COALESCE(:brand, (SELECT oem_id FROM first_brand))
        ORDER BY
            sort_order,
            b.first_data ASC NULLS LAST,
            b.oem_name
    """)

    result = await db.execute(
        query,
        {
            "fleet_id": fleet_id,
            "brand": brand
            if brand and brand != "undefined" and brand != "null" and brand != "All"
            else None,
        },
    )
    rows = result.mappings().all()

    # Créer une liste unique de marques avec leur trendline
    seen_brands = set()
    brands_list = []
    for row in rows:
        brand_key = (row["oem_id"], row["oem_name"])
        if brand_key not in seen_brands:
            seen_brands.add(brand_key)
            brands_list.append(
                {
                    "oem_id": row["oem_id"],
                    "oem_name": row["oem_name"],
                    "trendline": row["trendline"],
                    "trendline_max": row["trendline_max"],
                    "trendline_min": row["trendline_min"],
                }
            )

    # Créer la liste de trendline
    trendline = []
    for row in rows:
        if row["soh"] is not None:
            trendline.append(
                {
                    "soh": row["soh"],
                    "odometer": row["odometer"],
                    "vin": row["vin"],
                    "in_fleet": row["in_fleet"],
                    "score": row["score"],
                }
            )

    return {"brands": brands_list, "trendline": trendline}


async def get_brands(fleet_id: str, db: AsyncSession):
    query = text("""
        SELECT DISTINCT
            vm.oem_id,
            o.oem_name,
            CASE
                WHEN ROW_NUMBER() OVER (ORDER BY o.oem_name) = 1 THEN true
                ELSE false
            END as is_default
        FROM vehicle v
        JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
        JOIN oem o ON vm.oem_id = o.id
        WHERE v.fleet_id = :fleet_id
        ORDER BY o.oem_name
    """)

    result = await db.execute(query, {"fleet_id": fleet_id})
    return result.mappings().all()


async def get_soh_by_groups(fleet_id: str, group: str, page: int, db: AsyncSession):
    def convert_k_to_number(k_value: str) -> int:
        return int(k_value.lower().replace("k", "")) * 1000

    # Check if we're dealing with SoH percentage or odometer ranges
    if "%" in group:
        # Parse the range values, handling the special case for "< 80%"
        if group.startswith("<"):
            max_val = float(group.strip("<%"))
            min_val = 0
        else:
            # For ranges like "95-90%", we need to swap because the higher value comes first
            val1, val2 = (float(x.strip("%")) for x in group.split("-"))
            max_val = max(val1, val2)
            min_val = min(val1, val2)
        filter_column = "soh"
    else:
        min_val, max_val = map(convert_k_to_number, group.split("-"))
        filter_column = "odometer"

    items_per_page = 20
    offset = (page - 1) * items_per_page

    query = text(f"""
        WITH latest_vehicle_data AS (
            SELECT DISTINCT ON (v.id)
                v.vin,
                vd.soh * 100 as soh,
                vd.odometer,
                o.oem_name
            FROM vehicle v
            JOIN vehicle_data vd ON v.id = vd.vehicle_id
            JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
            JOIN oem o ON vm.oem_id = o.id
            WHERE v.fleet_id = :fleet_id
            AND v.is_displayed = true
            ORDER BY v.id, vd.timestamp DESC
        ),
        filtered_data AS (
            SELECT
                vin,
                ROUND(soh, 1) as soh,
                odometer,
                oem_name
            FROM latest_vehicle_data
            WHERE {filter_column} > :min_val AND {filter_column} <= :max_val
        ),
        count_data AS (
            SELECT COUNT(*) as total_count
            FROM filtered_data
        )
        SELECT
            fd.*,
            cd.total_count,
            CEIL(CAST(cd.total_count AS FLOAT) / :items_per_page) as total_pages
        FROM filtered_data fd
        CROSS JOIN count_data cd
        ORDER BY {filter_column} DESC, vin
        LIMIT :items_per_page
        OFFSET :offset
    """)

    result = await db.execute(
        query,
        {
            "fleet_id": fleet_id,
            "min_val": min_val,
            "max_val": max_val,
            "items_per_page": items_per_page,
            "offset": offset,
        },
    )

    rows = result.mappings().all()

    if not rows:
        return {
            "data": [],
            "pagination": {"current_page": page, "total_pages": 0, "total_items": 0},
        }

    return {
        "data": [
            {
                "vin": row["vin"],
                "soh": row["soh"],
                "odometer": row["odometer"],
                "oem_name": row["oem_name"],
            }
            for row in rows
        ],
        "pagination": {
            "current_page": page,
            "total_pages": int(rows[0]["total_pages"]),
            "total_items": rows[0]["total_count"],
        },
    }


async def get_extremum_soh(
    fleet_id: str,
    brand: str | None,
    page: int | None,
    page_size: int | None,
    extremum: str | None,
    sorting_column: str | None,
    sorting_order: str | None,
    db: AsyncSession = None,
):
    # Handle pagination parameters
    use_pagination = page is not None and page_size is not None

    if use_pagination:
        # Validate pagination parameters
        if page < 1:
            page = 1
        if page_size < 1 or page_size > 100:  # Set reasonable limits
            page_size = 10
        # Calculate offset
        offset = (page - 1) * page_size
    else:
        # No pagination - set default values for query parameters
        page = 1
        page_size = 1  # Will be overridden in query
        offset = 0

    # Build the limit clause based on pagination
    limit_clause = "LIMIT :page_size OFFSET :offset" if use_pagination else ""

    # Build the sorting clause based on the sorting column/order and extremum/quality
    sorting_clause = ""
    if sorting_column is not None and sorting_column.strip() != "":
        sorting_clause = f"ORDER BY {sorting_column} {sorting_order}"
    elif extremum == "Worst":
        sorting_clause = "ORDER BY soh_comparison ASC"
    else:
        sorting_clause = "ORDER BY soh_comparison DESC"

    query = text(f"""
        WITH brands_list AS (
            SELECT DISTINCT
                vm.oem_id,
                o.oem_name
            FROM vehicle v
            JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
            JOIN oem o ON vm.oem_id = o.id
            WHERE v.fleet_id = :fleet_id
            AND v.is_displayed = true
            ORDER BY o.oem_name
        ),
        latest_vehicle_data AS (
            SELECT DISTINCT ON (v.id)
                v.id,
                v.vin,
                ROUND((vd.soh * 100), 1) as soh,
                vd.odometer,
                vm.oem_id,
                o.oem_name,
                v.start_date,
                vm.warranty_date,
                (v.start_date + (vm.warranty_date || ' years')::interval) as warranty_end_date,
                EXTRACT(year FROM age((v.start_date + (vm.warranty_date || ' years')::interval), CURRENT_DATE)) as years_remaining,
                vd.soh_comparison
            FROM vehicle v
            JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
            JOIN oem o ON vm.oem_id = o.id
            JOIN vehicle_data vd ON v.id = vd.vehicle_id
            WHERE v.fleet_id = :fleet_id
            AND v.is_displayed = true
            AND (:brand='' OR vm.oem_id = CAST(:brand AS uuid))
            AND vd.soh IS NOT NULL
            AND vd.soh_comparison IS NOT NULL
            ORDER BY v.id, vd.timestamp DESC
        ),
        scored_vehicles AS (
            SELECT
                vin,
                soh,
                odometer,
                oem_name,
                years_remaining,
                soh_comparison,
                CASE
                    WHEN soh_comparison >= 3.50 THEN 'A'
                    WHEN soh_comparison >= 2.00 THEN 'B'
                    WHEN soh_comparison >= 1.45 THEN 'C'
                    WHEN soh_comparison > -0.74 THEN 'D'
                    WHEN soh_comparison > -4.00 THEN 'E'
                    ELSE 'F'
                END as score
            FROM latest_vehicle_data
        ),
        total_count AS (
            SELECT COUNT(*) as total
            FROM scored_vehicles
        ),
        ordered_vehicles AS (
            SELECT
                vin,
                ROUND(soh::numeric, 1) as soh,
                odometer,
                oem_name,
                years_remaining,
                score,
                soh_comparison
            FROM scored_vehicles
            {sorting_clause}
            {limit_clause}
        )
        SELECT json_build_object(
            'vehicles', (
                SELECT json_agg(to_json(v))
                FROM ordered_vehicles v
            ),
            'brands', (
                SELECT json_agg(
                    json_build_object(
                        'oem_id', oem_id,
                        'oem_name', oem_name
                    )
                )
                FROM (
                    SELECT oem_id, oem_name FROM brands_list
                    ORDER BY oem_name
                ) all_brands
            ),
            'pagination', (
                SELECT json_build_object(
                    'total_items', (SELECT total FROM total_count),
                    'total_pages', CASE WHEN :use_pagination THEN CEIL((SELECT total FROM total_count) / :page_size) ELSE 1 END,
                    'has_next', CASE WHEN :use_pagination THEN (:offset + :page_size) < (SELECT total FROM total_count) ELSE false END,
                    'has_previous', CASE WHEN :use_pagination THEN :page > 1 ELSE false END
                )
            )
        ) as result
    """)

    result = await db.execute(
        query,
        {
            "fleet_id": fleet_id,
            "offset": offset,
            "brand": brand if brand and brand != "undefined" and brand != "All" else "",
            "page": page,
            "page_size": page_size,
            "use_pagination": use_pagination,
        },
    )

    data = result.mappings().first()
    if data:
        result_data = data["result"]
        # Add page and page_size to pagination object because of a weird bug in sql
        if "pagination" in result_data:
            result_data["pagination"]["page"] = page if use_pagination else None
            result_data["pagination"]["page_size"] = (
                page_size if use_pagination else None
            )
        return result_data
    else:
        return {
            "vehicles": [],
            "brands": [],
            "pagination": {
                "page": page if use_pagination else None,
                "page_size": page_size if use_pagination else None,
                "total_items": 0,
                "total_pages": 0,
                "has_next": False,
                "has_previous": False,
            },
        }

