from core.pandas_utils import *
from core.console_utils import single_dataframe_script_main
from core.s3_utils import S3_Bucket
from core.singleton_s3_bucket import bucket
from transform.fleet_info.config import *

def get_fleet_info(bucket: S3_Bucket=bucket) -> DF:
    response = bucket.read_json_file(S3_JSON_FLEET_INFO_RESPONSE_KEY)
    return (                                                            # The response is a list of dicts.
        pd.json_normalize(response, record_path=["codes"], meta=["vin"])# The response is a list of dicts is unstructured json, so we need to normalize it. 
        # After normalizing, we end up with a DF with columns vin, code, displayName, isActive.
        # We pivor the table and end up with a one hot encoded DF.
        .pivot_table(index="vin", columns="displayName", values="isActive", aggfunc=pd.Series.mode, fill_value=False)
        .filter(like="Model", axis=1)                                   # The only columns we are interested in are the model column so we filter out the rest.
        .idxmax(axis=1)                                                 # We take the idxmax of the row, ie:the name of the column, ie: the name of the model, to get the model of the vehicle.                
        .astype("string")                                               # We convert the dtype to string.
        .str                                                            # We use the str accessor to extract the model and version from the string.
        .extract(r'^(?P<model>Model \w+) (?P<version>.+)$')             # We extract the model and version from the string.
    )

if __name__ == "__main__":
    single_dataframe_script_main(get_fleet_info)

fleet_info = get_fleet_info()
