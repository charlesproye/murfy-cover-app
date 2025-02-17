"""
We recieved a report of Aviloo SoH estimations from Ayvens on some Teslas.  
This module facilitates the obtention of these results.  
"""
from os.path import join, dirname

from core.pandas_utils import *

READOUTS_DF_PATH = join(dirname(__file__), "data_cache/aviloo_soh_readouts.csv")

def get_aviloo_soh_readouts() -> DF:
    return (
        pd.read_csv(
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
    )

if __name__ == "__main__":
    print(get_aviloo_soh_readouts())
