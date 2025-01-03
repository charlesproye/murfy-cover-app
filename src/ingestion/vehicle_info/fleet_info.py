import os
import pandas as pd
from logging import getLogger
import asyncio

from core.pandas_utils import *
from core.s3_utils import S3_Bucket
from core.singleton_s3_bucket import bucket
from core.config import *
from config import *

logger = getLogger("ingestion.vehicle_info")

async def read_fleet_info(ownership_filter: str = None) -> pd.DataFrame:
    fleet_info_with_contract_start_date = (
        bucket
        .read_csv_df(
            AYVENS_FLEET_WITH_CAR_REGISTRATION_KEY,
            parse_dates=["Car registration date", "Contract start date"],
            date_format="%d-%m-%Y",
        )
    )
    fleet_info_with_only_start_date = (
        bucket
        .read_csv_df(
            AYVENS_FLEET_WITH_ONLY_CONTRACT_START_DATE_KEY,
            parse_dates=["Contract start datum"],
            date_format="%d/%m/%Y",
        )
    )
    return (
        bucket.read_csv_df(AYVENS_FLEET_INFO_CSV_KEY)
        .rename(columns=lambda x: str.lower(x.replace(" ", "_")))
        .rename(columns=FLEET_INFO_COLS_NAME_MAPPING)
        .pipe(map_col_to_dict, "version", MODEL_VERSION_NAME_MAPPING)
        .pipe(map_col_to_dict, "make", MAKE_NAME_MAPPING)
        .pipe(map_col_to_dict, "country", COUNTRY_NAME_MAPPING)
        .drop_duplicates(subset="vin")
        .pipe(safe_astype, AYVENS_COL_DTYPES)
        .drop(columns=["autonomie", "capacity"])
        .pipe(set_all_str_cols_to_lower, but=["vin"])
        .pipe(left_merge, fleet_info_with_only_start_date, left_on="vin", right_on="VIN", src_dest_cols=COLS_TO_MERGE_ON_AYVENS)
        .pipe(left_merge, fleet_info_with_contract_start_date, left_on="vin", right_on="VIN", src_dest_cols=COLS_TO_MERGE_ON_AYVENS)
        .pipe(safe_astype, AYVENS_COL_DTYPES)
        .eval("activation_status = activation_status.str.lower().eq('activated').fillna(False).astype('bool')")
        .eval("version = version.mask(model == 'e-transit' & (version == 'x' | version.isna()), '2022')")
    )

if __name__ == "__main__":
    fleet_info = asyncio.run(read_fleet_info())
    print(fleet_info)
