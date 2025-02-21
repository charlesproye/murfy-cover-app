"""
We recieved a report of Aviloo SoH estimations from Ayvens on some Teslas.  
This module facilitates the obtention of these results.  
"""
from os.path import join, dirname

from core.singleton_s3_bucket import bucket
from core.pandas_utils import *

READOUTS_DF_PATH =  "miscellaneous/aviloo_soh_readouts.csv"

def get_aviloo_soh_readouts() -> DF:
    return (
        bucket.read_csv_df(
            READOUTS_DF_PATH,
            dtype={
                "Score Aviloo": "int64",
                "SoH Readout": "float64",
                "VIN": "string",
                "BIB SOH": "float64",
                "Brand (FlashTest)": "string",
                "Model Group (FlashTest)": "string",
                "Mileage": "float64",
            }
        )
        .rename(columns={"VIN": "vin", "SoH Readout": "soh_readout"})
        .eval("soh_readout = soh_readout / 100.0")
    )

if __name__ == "__main__":
    print(get_aviloo_soh_readouts())
