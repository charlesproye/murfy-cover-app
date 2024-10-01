import pandas as pd
from pandas import DataFrame as DF

from core.s3_utils import S3_Bucket
from core.console_utils import single_dataframe_script_main

def parse_all_responses_as_raw_tss() -> DF:
    bucket = S3_Bucket()
    keys = bucket.list_responses_keys_of_brand("BMW")
    return pd.concat([parse_response_as_raw_ts(key, bucket) for _, key in keys.iterrows()])

def parse_response_as_raw_ts(key:str, bucket:S3_Bucket) -> DF:
    api_response = bucket.read_json_file(key["key"])                            # The json response contains a "data" key who's values are a list of dicts.
    raw_ts = DF.from_dict(api_response["data"])                                 # The dicts have a "key" and "value" items.
    unit_not_none = raw_ts["unit"].notna()                                      # They also have a "unit" item but not all of them are not null.
    raw_ts.loc[unit_not_none, "key"] += "_" + raw_ts.loc[unit_not_none, "unit"] # So we append that "unit" to the key only if the "unit" is not none.
    raw_ts = (
        raw_ts
        .pipe(
            pd.pivot_table,                                                     
            columns="key",
            values="value",
            index="date_of_value",
            aggfunc="first"
        )
        .assign(vin=key["vin"])
        .reset_index(drop=False)
    )
    
    return raw_ts

    
if __name__ == "__main__":
    single_dataframe_script_main(parse_all_responses_as_raw_tss)

