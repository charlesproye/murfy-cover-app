import os
from urllib.parse import urlencode
import requests
from sys import argv

import pandas as pd
import json 
from datetime import timedelta
from datetime import datetime as DT
from rich import print
from rich.traceback import install as install_rich_traceback
from rich.progress import Progress
from rich.traceback import Traceback
from rich.progress import track

from constants_variables import *
from linkbycar_utils import headers
from fleet_vehicles_info import get_fleet_vehicles_info_df

time_dict = {}

def main():
    install_rich_traceback(extra_lines=0, width=130)
    args = argv[1:]
    # Permet d'obtenir le numéro des véhicules que l'on identifie
    vin_iterator = args if len(args) != 0 else get_fleet_vehicles_info_df().query("status == 'ENABLED'")['vin']
        
    for vin in vin_iterator:
        print(vin, ":")
        try:
            print(raw_time_series_of(vin))
        except KeyboardInterrupt as _:
            print('[blue]exiting...')
            break
        except Exception as e:
            print(f"[red]{type(e)} for {vin}:")
            print(Traceback(extra_lines=0, width=130))

DEFAULT_PAGE_SIZE = 500

def raw_time_series_of(vin: str, download_from_api:bool=True):
    path_to_cache = path_to_raw_df_of(vin)
    if os.path.exists(path_to_cache):
        print("reading from cache...")
        vehicle_df = pd.read_parquet(path_to_cache)
        print(f"download_from_api:{download_from_api}")
        if not download_from_api:
            return vehicle_df
        print(vehicle_df)
        if vehicle_df.empty:
            vehicle_df = download_time_series_from_date_range(vin)
        print("df_is_of_eletectric_vehicle:", df_is_of_eletectric_vehicle(vehicle_df))
        if not df_is_of_eletectric_vehicle(vehicle_df):
            vehicle_df.to_parquet(path_to_raw_df_of(vin))
            return vehicle_df
        last_date = vehicle_df.index.max()
        print(f"last_date: {last_date}, too_early_date: {DT.now() - RAW_VEHICLE_DF_DATE_MARGIN}")
        last_date_too_early = last_date <= DT.now() - RAW_VEHICLE_DF_DATE_MARGIN
        print("last_date_too_early:", last_date_too_early)
        if last_date_too_early: # If the cached df is out dateted
            print(f"Donwloading latest data from the API[{last_date}, {NOW}]...")
            vehicle_df = pd.concat((vehicle_df, download_time_series_from_date_range(vin, last_date, NOW)), axis="index")
        first_date = vehicle_df.index.min()
        first_date_too_late = first_date > DEFAULT_START_DATE + RAW_VEHICLE_DF_DATE_MARGIN
        print("first_date_too_late:", first_date_too_late)
        if first_date_too_late: # If the df does not start from 2021-1-1
            print(f"Donwloading earlier data from the API [{DEFAULT_START_DATE}, {first_date}]...")
            vehicle_df = pd.concat((download_time_series_from_date_range(vin, DEFAULT_START_DATE, first_date), vehicle_df), axis="index")
    elif download_from_api:
        print("df not cached trying to download it from API...")
        vehicle_df = download_time_series_from_date_range(vin)

    vehicle_df = vehicle_df.sort_index()
    vehicle_df.to_parquet(path_to_raw_df_of(vin))

    return vehicle_df


def download_time_series_from_date_range(vin: str, start_date: DT=DEFAULT_START_DATE, end_date: DT=NOW) -> pd.DataFrame:
    # Send first request to get the total count of data points
    url = create_data_points_request_url(vin, start_date, end_date)
    vehicle_df, json_response = download_vehicle_df_from_url(url)
    print(vehicle_df)
    print(vehicle_df.columns)
    if vehicle_df.empty:
        print("[red]Reiceved empty df, exiting download_time_series_from_date_range")
        return vehicle_df
    # If the vin is not of an electric vehicle, don't waist time downloading the data from the API
    if not df_is_of_eletectric_vehicle(vehicle_df):
        print("[yellow]Df is not of an electric vehicle, skipping the downloading.")
        return vehicle_df
    data_points_total_count = json_response["total_count"]
    with Progress() as progress:
        # Debugging
        task1 = progress.add_task("[blue]Downloading...", total=data_points_total_count)
        page_idx = 2
        nb_data_points_so_far = len(json_response["dataPoints"])
        while track(True):
            try:
                # Data handling
                next_page_url = create_data_points_request_url(vin, start_date, total_count=False, page_idx=page_idx)
                df_to_concat, json_response = download_vehicle_df_from_url(next_page_url)
                if df_to_concat.empty or not json_response:
                    print(f"breaking out of loop: {df_to_concat.empty}, json_response:\n{json.dumps(json_response, indent=1)}")
                    break
                vehicle_df = pd.concat((vehicle_df, df_to_concat), axis="index")
                vehicle_df = vehicle_df.sort_index()
                # Iteration variables
                page_idx += 1
                nb_datapoints_in_response = len(json_response["dataPoints"])
                nb_data_points_so_far += nb_datapoints_in_response
                # Debugging
                progress.update(task1, advance=nb_datapoints_in_response)
                print(f"page index: {page_idx}, df_to_concat start index: {df_to_concat.index.min()}, df_to_concat end index: {df_to_concat.index.max()}, total nb rows: {len(vehicle_df)}, df_to_concat.empty:", df_to_concat.empty)
            except KeyboardInterrupt:
                print("returning downloaded df so far...")
                return vehicle_df.sort_index()
            
    vehicle_df = vehicle_df.sort_index()
    return vehicle_df


def download_vehicle_df_from_url(url: str) -> tuple[pd.DataFrame, dict]:
    # Send request
    response = requests.get(url, headers=headers)
    if response.status_code < 200 or response.status_code > 299:
        print("[red]status:", response.status_code)
        print(response.text)
        print(requests.HTTPError.errno)
        return pd.DataFrame(), None
    # Parse response into dict
    json_response: dict = response.json()
    # Extract the data from the dict that we are interested about into a list of dicts to pass to pd.DataFrame.from_dict
    single_dict_data_points = {}
    for data_point in json_response["dataPoints"]:
        single_dict_data_points[data_point["timestamp"]] = {**data_point["value"], **single_dict_data_points.get(data_point["timestamp"], {})}
    # Convert list of dict into dataframe
    vehicle_df = pd.DataFrame.from_dict(single_dict_data_points,  orient="index")
    df_dtype_dict = {col: dtype for col, dtype in DF_TYPE_DICT.items() if col in vehicle_df.columns}
    vehicle_df = vehicle_df.astype(df_dtype_dict, errors="ignore")
    vehicle_df.index = linkbycar_str_to_datetime(vehicle_df.index.to_series().astype(str))

    return vehicle_df, json_response

def linkbycar_str_to_datetime(series: pd.Series) -> pd.Series:
    before_dot = series.str.split(".").str[0]
    datetime_series = pd.to_datetime(before_dot)

    return datetime_series

def df_is_of_eletectric_vehicle(vehicle_df: pd.DataFrame) -> bool:
    """
    ### Description:
    Checks the columns of the vehicle_df to see if it represents an electric vehicle.
    """
    is_electric = "batteryLevel" in vehicle_df.columns

    return is_electric

def create_data_points_request_url(vin: str, start: DT|timedelta=timedelta(days=365 * 3), end: DT=DT.now(), total_count=True, page_idx=1) -> pd.DataFrame:
    if type(start) == timedelta:
        start_date_str = (DT.now() - start).strftime("%Y-%m-%d")
    else:
        start_date_str = start.strftime("%Y-%m-%d")
    end_date_str = end.strftime("%Y-%m-%d")
    url_path = f"https://api.linkbycar.com/v1/data_points/vin/{vin}?"
    query_params = {
        "fields": FIELDS, 
        "values": VALUES_TO_USE,
        # The doc advises to set this to False to decrease response latency.
        # This is true but what the doc forgets to mention is that the API does not return the "next_page" element 
        # if "compute_total_count" is set to False -_-.
        "compute_total_count": total_count,
        "from": start_date_str,
        "to": end_date_str,
        # Moreover, the API does not seem to return next page if this is set to false.
        "page_size": DEFAULT_PAGE_SIZE,
        "page": page_idx,
    }
    url = url_path + urlencode(query_params, doseq=True)

    return url

def path_to_raw_df_of(vin: str) -> str:
    path = os.path.join("data_cache", "vehicles_raw_time_series", f"{vin}.parquet")

    return path
    
if __name__=='__main__':
    main()

