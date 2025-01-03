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
    return (
        bucket.read_csv_df(FLEET_INFO_KEY, parse_dates=["End of Contract", "Start Date"], date_format="%d/%m/%Y")
        .rename(columns=lambda x: str.lower(x.replace(" ", "_")))
        .rename(columns=FLEET_INFO_COLS_NAME_MAPPING)
        .pipe(map_col_to_dict, "version", MODEL_VERSION_NAME_MAPPING)
        .pipe(map_col_to_dict, "make", MAKE_NAME_MAPPING)
        .pipe(map_col_to_dict, "country", COUNTRY_NAME_MAPPING)
        .drop_duplicates(subset="vin")
        .pipe(safe_astype, COL_DTYPES)
    )

if __name__ == "__main__":
    fleet_info = asyncio.run(read_fleet_info())
    print(fleet_info)
    
