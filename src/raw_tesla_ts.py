"""
This module provides the function `raw_time_series_of` to provide a raw time series from Tesla's personal API.
Can also be used as a script to obtain the entirety of the tesla fleet data.
"""
from glob import glob
from os.path import exists

import pandas as pd
from pandas import DataFrame as DF

from tesla_constants import *

def main():
    split_raw_csv_into_raw_parquets_ts()
    add_extra_raw_time_series("data_cache/tesla/extra_raw_time_series/LRWYGCFS6PC552861.csv", "LRWYGCFS6PC552861")
    # Count number of points per vehicle per day

    
def raw_ts_of(vin:str) -> DF:
    return pd.read_parquet(PATH_TO_RAW_TESLA_TS.format(vin=vin))

def split_raw_csv_into_raw_parquets_ts():
    """
    ### Description:
    Reads all the csv flies in data_cache/tesla_data_cache.
    Then splits them up by vin into seperate parquet time series.
    """
    # Create a list of all the files that we want to read using regex
    csv_files = glob('data_cache/tesla/raw_grouped_time_series/*.csv')
    # Read them all into a list
    df_list = [pd.read_csv(file, parse_dates=["readable_date"]) for file in csv_files]
    (
        pd.concat(df_list, ignore_index=True, axis="index")                         # concat them into a single df
        .rename(columns={"readable_date": "date"})                                  # Rename readable_date to date
        .astype(DATA_TYPE_RAW_DF_DICT)                                              # convert types
        .groupby("vin")                                                             # for each vin...
                                                                                    # save its data to a separate parquet file
        .apply(lambda df: df.to_parquet(PATH_TO_RAW_TESLA_TS.format(vin=df.name)), include_groups=False)  
    )

def add_extra_raw_time_series(path_to_raw_series: list[str]|str, vin:str):
    """
    ### Description:
    Function to add to the `raw_time_series` folder time series that are not present in the grouped files.
    """
    # collect extra files
    paths_to_raw_series = path_to_raw_series if isinstance(path_to_raw_series, list) else [path_to_raw_series]
    extra_raw_time_series_lst = [pd.read_csv(path_to_raw_serie, parse_dates=["readable_date"]) for path_to_raw_serie in paths_to_raw_series]
    extra_time_series: DF = (
        pd.concat(extra_raw_time_series_lst, axis="index", ignore_index=True)
        .rename(columns={"readable_date": "date"})                                  # Rename readable_date to date
    )
    # collect already cached time series 
    if exists(PATH_TO_RAW_TESLA_TS.format(vin=vin)):
        already_cached_df = pd.read_parquet(PATH_TO_RAW_TESLA_TS.format(vin=vin))
        extra_time_series = pd.concat((extra_time_series.loc[:, already_cached_df.columns], already_cached_df), axis="index", ignore_index=True)
    # write new raw time series on top of already existing one
    extra_time_series.to_parquet(PATH_TO_RAW_TESLA_TS.format(vin=vin))
    
# def count_nb_points_per_vehicle_per_day():
#     DF({vin: points_per_day(processed_time_series_of(vin)) for vin in iterate_over_vins()}).fillna(0).to_csv('data_points_per_day.csv')

# def points_per_day(vehicle_df: DF) -> Series:
#     return vehicle_df.groupby(vehicle_df['date'].dt.floor("1d")).size()


if __name__ == "__main__":
    main()
