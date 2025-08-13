import logging
import sys
from core.console_utils import main_decorator
from core.s3.settings import S3Settings
from core.spark_utils import create_spark_session
from transform.result_week.result_phase_to_result_week import ResultPhaseToResultWeek
from core.pandas_utils import concat
# from core.sql_utils import left_merge_rdb_table, truncate_rdb_table_and_insert_df

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
}

VEHICLE_DATA_RDB_TABLE_SRC_DEST_COLS = {
    "SOH": "soh",
    "ODOMETER": "odometer",
    "LEVEL_1": "level_1",
    "LEVEL_2": "level_2",
    "level_3": "level_3",
    "vehicle_id": "vehicle_id",
    "DATE":"timestamp"
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
            print(result_week)
            print(result_week.columns)
            result_week_list.append(result_week)
        else:
            pass
    results_week = concat(result_week_list)

    print(results_week)
    print(results_week.columns)

    #df_global = left_merge_rdb_table(results_week, "vehicle", "VIN", "vin", {"id": "vehicle_id"})
    # truncate_rdb_table_and_insert_df(df_global, "vehicle_data", VEHICLE_DATA_RDB_TABLE_SRC_DEST_COLS, logger=logger)


if __name__ == "__main__":
    main()
