import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import select, text

from activation.config.config import MAKES_WITH_SOH_BIB
from core.sql_utils import get_connection, get_sqlalchemy_engine
from db_models import Oem, Vehicle, VehicleModel


async def write_metrics_to_db(logger: logging.Logger):
    with get_connection() as conn_rdb, conn_rdb.cursor() as cursor:
        cursor.execute("""
        SELECT oem_name, COUNT(*)
        FROM vehicle v
        LEFT JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
        LEFT JOIN oem ON vm.oem_id = oem.id
        WHERE activation_status = true
        GROUP BY 1
        """)
        active_vehicles = cursor.fetchall()
        logger.info(f"Active vehicles: {active_vehicles}")

    with (
        get_connection(db_name="data-engineering") as conn_data_engineering,
        conn_data_engineering.cursor() as cursor,
    ):
        for oem_name, count in active_vehicles:
            cursor.execute(
                """
                    INSERT INTO fct_activation_metric (date, nb_vehicles_activated, oem, updated_at)
                    VALUES (%(date)s, %(nb_vehicles_activated)s, %(oem)s, %(updated_at)s)
                    ON CONFLICT (date, oem) DO NOTHING
                    """,
                {
                    "date": datetime.now().strftime("%Y-%m-%d"),
                    "nb_vehicles_activated": int(count),
                    "updated_at": datetime.now(),
                    "oem": oem_name,
                },
            )
            conn_data_engineering.commit()
        logger.info("Metrics written to fct_activation_metric")


async def compare_active_vehicles(
    fleet_info: pd.DataFrame,
) -> tuple[set[str], set[str]]:
    active = fleet_info[fleet_info["real_activation"]]

    engine = get_sqlalchemy_engine()

    with engine.connect() as con:
        result = con.execute(
            text("SELECT vin FROM vehicle WHERE activation_status = True")
        )
        vehicle_db = {row[0] for row in result.fetchall()}

    vehicle_gsheet = set(active["vin"].dropna().unique())

    db_not_in_gsheet = vehicle_db - vehicle_gsheet
    gsheet_not_in_db = vehicle_gsheet - vehicle_db

    return db_not_in_gsheet, gsheet_not_in_db


async def check_vehicles_without_type_postgre(
    vehicles_without_type_vin: list[str],
) -> tuple[int, list[str]]:
    engine = get_sqlalchemy_engine()

    query = (
        select(Vehicle.vin)
        .select_from(Vehicle)
        .join(VehicleModel, VehicleModel.id == Vehicle.vehicle_model_id)
        .join(Oem, Oem.id == VehicleModel.oem_id)
        .where(
            VehicleModel.type.is_(None),
            Vehicle.activation_status.is_(True),
            Oem.oem_name.in_(MAKES_WITH_SOH_BIB),
        )
    )

    with engine.connect() as con:
        result = con.execute(query)
        vehicles = result.fetchall()

    set_vehicles = {row[0] for row in vehicles} - set(vehicles_without_type_vin)
    return len(set_vehicles), list(set_vehicles)
