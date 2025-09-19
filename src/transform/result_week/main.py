import logging
import sys
from turtle import up
from core.console_utils import main_decorator
from core.s3.settings import S3Settings
from core.spark_utils import create_spark_session
from transform.result_week.result_phase_to_result_week import ResultPhaseToResultWeek
from core.pandas_utils import concat
from core.sql_utils import left_merge_rdb_table, insert_df_and_deduplicate

ORCHESTRATED_MAKES = {
    "bmw": (True, False, False),
    "mercedes-benz": (True, False, False),
    "renault": (True, True, True),
    "volvo-cars": (True, True, False),
    "stellantis": (True, False, False),
    "kia": (True, False, False),
    "ford": (True, True, False),
    "tesla-fleet-telemetry": (True, True, True),
    "volkswagen": (True, False, False),
    "tesla": (False, True, True)
}

VEHICLE_DATA_RDB_TABLE_SRC_DEST_COLS = {
    "SOH": "soh",
    "SOH_OEM": "soh_oem",
    "ODOMETER": "odometer",
    "LEVEL_1": "level_1",
    "LEVEL_2": "level_2",
    "LEVEL_3": "level_3",
    "vehicle_id": "vehicle_id",
    "DATE":"timestamp",
    "CONSUMPTION": "consumption",
    "ESTIMATED_CYCLES": "cycles"
}


@main_decorator
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )

    logger = logging.getLogger("ResultPhaseToResultWeek")
    settings = S3Settings()
    spark = create_spark_session(settings.S3_KEY, settings.S3_SECRET)

    result_week_list = []

    for make, (is_orchestrated, has_soh, has_levels) in ORCHESTRATED_MAKES.items():
        if is_orchestrated:
            result_week = ResultPhaseToResultWeek(make=make, spark=spark, logger=logger, has_soh=has_soh, has_levels=has_levels).run()
            result_week_list.append(result_week)
        else:
            pass
    results_week = concat(result_week_list)

    df_global = left_merge_rdb_table(results_week, "vehicle", "VIN", "vin", {"id": "vehicle_id"})
    insert_df_and_deduplicate(df_global, "vehicle_data", ["vehicle_id", "timestamp"], VEHICLE_DATA_RDB_TABLE_SRC_DEST_COLS, logger=logger, uuid_cols=['vehicle_id'])


if __name__ == "__main__":
    main()
