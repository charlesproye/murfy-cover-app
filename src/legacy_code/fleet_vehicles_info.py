from typing import Dict, Any, Generator
import os
import time
from contextlib import contextmanager
from urllib.parse import urlencode, urlparse, parse_qs
import requests

import traceback
from logging import raiseExceptions
from matplotlib.pyplot import xticks
import pandas as pd
import json 
import csv
import seaborn as sn
import numpy as np
from datetime import datetime, timedelta
import time
from rich import print
from rich.traceback import install as install_rich_traceback
# from rich.progress import track
from rich.progress import Progress
from rich.traceback import Traceback

from linkbycar_utils import headers
from constants_variables import *


def main():
    install_rich_traceback(extra_lines=0, width=130)
    vehicles_info_df = compute_vehicles_info_df()
    print(vehicles_info_df)
    print()
    ev_lst = vehicles_info_df.query('is_electric')["vin"].to_list()
    print("electric vehicles:", *ev_lst, sep="\n")

    fleet_info_df = pd.read_parquet("data_cache/vehicles_info/vehicles_info.parquet")

def iterate_over_raw_time_series(meta_data:str="row", query:str="is_electric & status == 'ENABLED'") -> Generator[tuple[str|pd.Series, pd.DataFrame], None, None]:
    from download_raw_api_time_time_series import path_to_raw_df_of
    """
    ### Description:
    Iterates over every cached time series of the fleet and yields their dataframe as well as some meta data.
    ### Parameters:
    metad_data: {"row" or "vin"}
    """

    fleet_vehicle_df = get_fleet_vehicles_info_df()
     
    if query:
        fleet_vehicle_df = fleet_vehicle_df.query(query)

    for index, row in fleet_vehicle_df.iterrows():
        vehicle_df = pd.read_parquet(path_to_raw_df_of(row["vin"]))
        if meta_data == "row":
            yield row, vehicle_df
        else:
            yield row["vin"], vehicle_df


def compute_vehicles_info_df() -> pd.DataFrame:
    from download_raw_api_time_time_series import path_to_raw_df_of, df_is_of_eletectric_vehicle

    vehicles_info_df = get_fleet_vehicles_info_df()
    vehicles_info_df = vehicles_info_df.set_index("vin", drop=False)
    
    vehicles_info_df["is_electric"] = False
    vehicles_info_df["is_cached"] = False
    vehicles_info_df["length"] = 0
    for vin in vehicles_info_df.query("status == 'ENABLED'")["vin"]:
        path_to_vehicle_df_cache = path_to_raw_df_of(vin)
        is_cached = os.path.exists(path_to_vehicle_df_cache)
        vehicles_info_df.at[vin, "is_cached"] = is_cached
        if is_cached:
            vehicle_df = pd.read_parquet(path_to_vehicle_df_cache)
            vehicles_info_df.at[vin, "is_electric"] = df_is_of_eletectric_vehicle(vehicle_df)
            vehicles_info_df.at[vin, "length"] = len(vehicle_df)

            
    vehicles_info_df.to_parquet(PATH_TO_VEHICLES_INFO_DF)
    vehicles_info_df.to_csv(os.path.join("data_cache", "vehicles_info", "vehicles_info.csv"), index=False)

    return vehicles_info_df
    

def get_linkbycar_vehicles_endpoint_df() -> pd.DataFrame:
    response = requests.get("https://api.linkbycar.com/v1/vehicles", headers = headers)
    json_response = response.json()
    endpoint_vehicles_df = pd.DataFrame.from_dict(json_response["vehicles"])

    return endpoint_vehicles_df

def get_fleet_vehicles_info_df() -> pd.DataFrame:
    vehicles_info_df = pd.read_parquet(PATH_TO_VEHICLES_INFO_DF)

    return  vehicles_info_df

if __name__ == "__main__":
    main()

