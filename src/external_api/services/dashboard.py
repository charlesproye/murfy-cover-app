from datetime import datetime, timedelta

from pydantic import UUID4
from sqlalchemy import and_, case, distinct, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from db_models import Vehicle, VehicleData


async def get_last_timestamp_with_data(fleet_ids: list[str], db: AsyncSession):
    query = (
        select(func.max(VehicleData.timestamp).label("last_update"))
        .join(Vehicle, VehicleData.vehicle_id == Vehicle.id)
        .where(
            and_(
                Vehicle.fleet_id.in_(fleet_ids),
                VehicleData.soh.isnot(None),
                VehicleData.odometer.isnot(None),
            )
        )
    )
    result = await db.execute(query, {"fleet_ids": fleet_ids})
    last_update_data = result.mappings().all()
    last_update = (
        last_update_data[0]["last_update"] if len(last_update_data) > 0 else None
    )
    return last_update


async def get_individual_kpis(fleet_ids: list[str], db: AsyncSession):
    latest_timestamp = await get_last_timestamp_with_data(fleet_ids, db)
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
    one_month_since_last_data = latest_timestamp - timedelta(days=30)
    two_months_since_last_data = latest_timestamp - timedelta(days=60)

    # Getting fleet data in a one-month window from the last data
    # Not used for fleet size cause we want the full number
    subquery = (
        select(
            Vehicle.id,
            VehicleData.timestamp,
            VehicleData.odometer,
            func.round(VehicleData.soh * 100, 1).label("soh"),
            Vehicle.is_pinned,
        )
        .join(Vehicle, VehicleData.vehicle_id == Vehicle.id)
        .where(
            Vehicle.fleet_id.in_(fleet_ids),
            VehicleData.timestamp > one_month_since_last_data,
        )
        .order_by(Vehicle.id, VehicleData.timestamp.desc())
        .distinct(Vehicle.id)
        .subquery()
    )

    query = select(
        func.round(func.avg(subquery.c.odometer)).label("avg_odometer"),
        func.round(func.avg(subquery.c.soh), 1).label("avg_soh"),
        select(func.count(distinct(Vehicle.id)))
        .where(
            Vehicle.fleet_id.in_(fleet_ids),
        )
        .scalar_subquery()
        .label("vehicle_count"),
        func.count(case((subquery.c.is_pinned, 1))).label("pinned_count"),
    )
    result = await db.execute(
        query,
        {
            "fleet_ids": fleet_ids,
            "one_month_since_last_data": one_month_since_last_data,
        },
    )
    latest_data = result.mappings().all()

    # Getting fleet data in a one-month window from the last data, the month before the one just above
    prev_data_row = (
        select(
            Vehicle.id,
            VehicleData.timestamp,
            VehicleData.odometer,
            func.round(VehicleData.soh * 100, 1).label("soh"),
            Vehicle.is_pinned,
        )
        .join(Vehicle, VehicleData.vehicle_id == Vehicle.id)
        .where(
            Vehicle.fleet_id.in_(fleet_ids),
            VehicleData.timestamp < one_month_since_last_data,
            VehicleData.timestamp > two_months_since_last_data,
        )
        .order_by(Vehicle.id, VehicleData.timestamp.desc())
        .distinct(Vehicle.id)
        .subquery()
    )

    query = select(
        func.round(func.avg(prev_data_row.c.odometer)).label("avg_odometer"),
        func.round(func.avg(prev_data_row.c.soh), 1).label("avg_soh"),
        select(func.count(distinct(Vehicle.id)))
        .select_from(VehicleData)
        .join(Vehicle, VehicleData.vehicle_id == Vehicle.id)
        .where(
            Vehicle.fleet_id.in_(fleet_ids),
            VehicleData.timestamp < one_month_since_last_data,
        )
        .scalar_subquery()
        .label("vehicle_count"),
        func.count(case((prev_data_row.c.is_pinned, 1))).label("pinned_count"),
    )
    result_previous = await db.execute(
        query,
        {
            "fleet_ids": fleet_ids,
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


async def get_range_soh(fleet_ids: list[str], type: str, db: AsyncSession):
    if type == "soh":
        query = text("""
            WITH latest_vehicle_data AS (
                SELECT DISTINCT ON (vd.vehicle_id)
                    vd.vehicle_id,
                    vd.soh * 100 as value
                FROM vehicle_data vd
                JOIN vehicle v ON vd.vehicle_id = v.id
                WHERE v.fleet_id = ANY(:fleet_ids)
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
                WHERE v.fleet_id = ANY(:fleet_ids)
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

    result = await db.execute(query, {"fleet_ids": fleet_ids})
    rows = result.mappings().all()

    return [
        {
            "lower_bound": int(row["lower_bound"]),
            "upper_bound": int(row["upper_bound"]),
            "vehicle_count": row["vehicle_count"],
        }
        for row in rows
    ]


async def get_new_vehicles(fleet_ids: list[str], period: str, db: AsyncSession):
    if period not in ["monthly", "annually"]:
        period = "monthly"

    interval = "4 months" if period == "monthly" else "4 years"
    period_trunc = "month" if period == "monthly" else "year"

    query = text(f"""
        WITH periods AS (
            SELECT DATE_TRUNC(:period_trunc, generate_series(
                DATE_TRUNC(:period_trunc, CURRENT_DATE - INTERVAL '{interval}'),
                DATE_TRUNC(:period_trunc, CURRENT_DATE),
                INTERVAL '1 {period_trunc}'
            )) AS period
        ),
        vehicle_counts AS (
            SELECT
                DATE_TRUNC(:period_trunc, created_at) AS period,
                COUNT(*) AS vehicle_count
            FROM vehicle
            WHERE fleet_id = ANY(:fleet_ids)
            AND created_at >= CURRENT_DATE - INTERVAL '{interval}'
            GROUP BY period
        )
        SELECT
            p.period,
            COALESCE(vc.vehicle_count, 0) AS vehicle_count
        FROM periods p
        LEFT JOIN vehicle_counts vc ON p.period = vc.period
        ORDER BY p.period DESC
        LIMIT 4
    """)

    result = await db.execute(
        query, {"fleet_ids": fleet_ids, "period_trunc": period_trunc}
    )
    return result.mappings().all()


async def get_search_vin(vin: str, fleets_ids: list[UUID4], db: AsyncSession):
    query = text("""
        SELECT id, vin FROM vehicle
        WHERE vin ILIKE '%' || :vin || '%'
        AND fleet_id = ANY(:fleets_ids)
        ORDER BY vin
        LIMIT 10
    """)

    result = await db.execute(query, {"vin": vin.upper(), "fleets_ids": fleets_ids})
    return result.mappings().all()


def query_region_table():
    query = text("""
        WITH latest_vehicle_data AS (
            SELECT DISTINCT ON (vd.vehicle_id)
                v.id as vehicle_id,
                vd.odometer,
                ROUND((vd.soh * 100), 1) as soh,
                v.region_id,
                r.region_name
            FROM vehicle v
            JOIN region r ON v.region_id = r.id
            JOIN vehicle_data vd ON v.id = vd.vehicle_id
            WHERE v.fleet_id = ANY(:fleet_ids)
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
                v.id as vehicle_id,
                vd.odometer,
                ROUND((vd.soh * 100), 1) as soh,
                vm.oem_id,
                o.oem_name
            FROM vehicle v
            JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
            JOIN oem o ON vm.oem_id = o.id
            JOIN vehicle_data vd ON v.id = vd.vehicle_id
            WHERE v.fleet_id = ANY(:fleet_ids)
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
                v.id as vehicle_id,
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
            WHERE v.fleet_id = ANY(:fleet_ids)
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


async def get_table_brand(fleet_ids: list[str], filter: str, db: AsyncSession):
    if filter.lower() == "region":
        query = query_region_table()
    elif filter.lower() == "make":
        query = query_brand_table()
    else:
        query = query_all_table()

    result = await db.execute(query, {"fleet_ids": fleet_ids})
    return result.mappings().all()


async def get_trendline_brand(
    fleet_ids: list[str], db: AsyncSession, brand: str | None = None
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
            WHERE v.fleet_id = ANY(:fleet_ids)
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
            WHERE v.fleet_id = ANY(:fleet_ids)
            GROUP BY vm.oem_id, o.oem_name, o.trendline::text, o.trendline_max::text, o.trendline_min::text
        ),
        latest_vehicle_data AS (
            SELECT DISTINCT ON (v.id)
                v.id,
                v.vin,
                ROUND((vd.soh * 100), 1) as soh,
                vd.odometer,
                vd.soh_comparison,
                v.fleet_id = ANY(:fleet_ids) as in_fleet,
                v.bib_score as score
            FROM vehicle v
            JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
            JOIN vehicle_data vd ON v.id = vd.vehicle_id
            WHERE vm.oem_id = COALESCE(:brand, (SELECT oem_id FROM first_brand))
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
            "fleet_ids": fleet_ids,
            "brand": brand
            if brand and brand != "undefined" and brand != "null" and brand != "All"
            else None,
        },
    )
    rows = result.mappings().all()

    # Creating a list of unique brands with their trendlines
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

    # Creating the trendline list
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

    brands_list_sorted = sorted(brands_list, key=lambda x: x["oem_name"].lower())
    return {"brands": brands_list_sorted, "trendline": trendline}


async def get_soh_by_groups(
    fleet_ids: list[str], group: str, page: int, db: AsyncSession
):
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
            WHERE v.fleet_id = ANY(:fleet_ids)
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
            "fleet_ids": fleet_ids,
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


async def get_extremum_vehicles(
    fleet_ids: list[str],
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
            WHERE v.fleet_id = ANY(:fleet_ids)
            ORDER BY o.oem_name
        ),
        latest_vehicle_data AS (
            SELECT DISTINCT ON (v.id)
                v.id,
                v.vin,
                v.bib_score as score,
                ROUND((vd.soh * 100), 1) as soh,
                vd.odometer,
                vm.oem_id,
                o.oem_name,
                v.start_date,
                vm.warranty_date,
                (v.start_date + (vm.warranty_date || ' years')::interval) as warranty_end_date,
                EXTRACT(year FROM age((v.start_date + (vm.warranty_date || ' years')::interval), CURRENT_DATE)) as years_remaining,
                vd.soh_comparison,
                ROUND(
                    CASE
                        WHEN (vd.level_1 + vd.level_3) > 0
                        THEN vd.level_3::numeric / (vd.level_1 + vd.level_2 + vd.level_3)
                        ELSE NULL
                    END, 3
                ) AS fast_charge_ratio,
                vd.consumption
            FROM vehicle v
            JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
            JOIN oem o ON vm.oem_id = o.id
            JOIN vehicle_data vd ON v.id = vd.vehicle_id
            WHERE v.fleet_id = ANY(:fleet_ids)
            AND (:brand='' OR vm.oem_id = CAST(:brand AS uuid))
            AND vd.soh IS NOT NULL
            AND vd.soh_comparison IS NOT NULL
            ORDER BY v.id, vd.timestamp DESC
        ),
        total_count AS (
            SELECT COUNT(*) as total
            FROM latest_vehicle_data
        ),
        ordered_vehicles AS (
            SELECT
                vin,
                ROUND(soh::numeric, 1) as soh,
                odometer,
                oem_name,
                years_remaining,
                score,
                fast_charge_ratio,
                consumption
            FROM latest_vehicle_data
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
            "fleet_ids": fleet_ids,
            "offset": offset,
            "brand": brand if brand and brand != "undefined" and brand != "all" else "",
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
