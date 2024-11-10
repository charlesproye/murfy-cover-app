from core.pandas_utils import *
from core.config import *
from core.s3_utils import S3_Bucket
from core.singleton_s3_bucket import bucket
from core.console_utils import single_dataframe_script_main
from transform.fleet_info.config import *

def get_fleet_info(bucket=S3_Bucket()) -> DF:
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
        .pipe(set_all_str_cols_to_lower)
        .assign(vin=lambda df: df["vin"].str.upper())
        .pipe(left_merge, fleet_info_with_only_start_date, left_on="vin", right_on="VIN", src_dest_cols=COLS_TO_MERGE_ON_AYVENS)
        .pipe(left_merge, fleet_info_with_contract_start_date, left_on="vin", right_on="VIN", src_dest_cols=COLS_TO_MERGE_ON_AYVENS)
        .assign(fleet=lambda df: "ayvens_fleet_" + df["fleet"].astype("string"))
    )

if __name__ == "__main__":
    single_dataframe_script_main(get_fleet_info)
    
fleet_info = get_fleet_info()
