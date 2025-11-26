import logging

import pandas as pd

from core.pandas_utils import concat
from core.sql_utils import insert_df_and_deduplicate, left_merge_rdb_table
from transform.result_week.result_phase_to_result_week import ResultPhaseToResultWeek

ORCHESTRATED_MAKES = {
    "bmw": (True, False, False),
    "mercedes-benz": (True, False, False),
    "renault": (True, True, True),
    "volvo-cars": (True, True, False),
    "stellantis": (True, False, False),
    "kia": (True, True, False),
    "ford": (True, True, False),
    "tesla-fleet-telemetry": (True, True, True),
    "volkswagen": (True, False, False),
}

VEHICLE_DATA_RDB_TABLE_SRC_DEST_COLS = {
    "SOH": "soh",
    "SOH_OEM": "soh_oem",
    "ODOMETER": "odometer",
    "LEVEL_1": "level_1",
    "LEVEL_2": "level_2",
    "LEVEL_3": "level_3",
    "vehicle_id": "vehicle_id",
    "DATE": "timestamp",
    "CONSUMPTION": "consumption",
    "ESTIMATED_CYCLES": "cycles",
}

LOGGER = logging.getLogger(__name__)


def main(logger: logging.Logger = LOGGER) -> pd.DataFrame:
    result_week_list = []

    for make, (is_orchestrated, has_soh, has_levels) in ORCHESTRATED_MAKES.items():
        if is_orchestrated:
            result_week = ResultPhaseToResultWeek(
                make=make,
                has_soh=has_soh,
                has_levels=has_levels,
                logger=logger,
            ).run()
            result_week_list.append(result_week)
        else:
            pass
    results_week = concat(result_week_list)

    df_global = left_merge_rdb_table(
        results_week, "vehicle", "VIN", "vin", {"id": "vehicle_id"}
    )
    insert_df_and_deduplicate(
        df_global,
        "vehicle_data",
        ["vehicle_id", "timestamp"],
        VEHICLE_DATA_RDB_TABLE_SRC_DEST_COLS,
        uuid_cols=["vehicle_id"],
    )

    return df_global


if __name__ == "__main__":
    main()
