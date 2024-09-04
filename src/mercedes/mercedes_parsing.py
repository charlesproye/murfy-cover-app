"""
This module implements a `parse_json_obj` function.  
It converts a HighMobility response dict into a df.  
When called as a script, it takes in two mandatory path arguments: `json_source` and `csv_dest`.
"""
from typing import Any
import json
from datetime import datetime as DT

import pandas as pd
from pandas import DataFrame as DF
from rich import print
import os

from core.argparse_utils import parse_kwargs
from utils.files import ABCFileManager
from s3fs import S3FileSystem


import os

def main(fs: S3FileSystem, fm1: ABCFileManager, fm2: ABCFileManager, json_source, csv_dest) :
    
    try:
        json_data = fm1.load(json_source)
        df = parse_response_as_df(json_data)
        print("Saving to CSV...")
        # df.to_csv(csv_dest)
        fm2.save(fs, df, csv_dest)
        print("CSV saved successfully")
        # df.to_csv(csv_dest)
        print("CSV saved successfully")
    except FileNotFoundError:
        print(f"Error: File not found - {json_source}")
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in file - {json_source}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        raise

def parse_response_as_df(src) -> DF:
    flatten_dict = flatten_json_obj(src, {})
    df = pd.concat(flatten_dict, axis="columns", names=['vaiable', 'property']).pipe(set_date)
    print("second into parse_response_as_df")
    return df

def set_date(df: DF) -> DF:
    if df.empty:
        return df
    
    # Check if there's already a DatetimeIndex
    if isinstance(df.index, pd.DatetimeIndex):
        return df
    
    # List of possible date column names
    date_column_names = ['timestamp', 'date', 'time']
    
    # Try to find a date column
    date_column = None
    for col in date_column_names:
        if col in df.columns.get_level_values(1):
            try:
                date_series = df.loc[:, df.columns.get_level_values(1) == col].iloc[:, 0]
                df['date'] = pd.to_datetime(date_series, utc=True)
                date_column = col
                break
            except ValueError:
                continue
    
    if date_column:
        df = df.set_index('date')
        df.index.name = 'date'
    else:
        df['date'] = pd.date_range(start=pd.Timestamp.now(), periods=len(df), freq='s')
        df = df.set_index('date')
        df.index.name = 'date'    
    return df

def flatten_json_obj(src:dict, dst:dict, timestamp=None, path:list[str]=[]) -> dict[Any,dict[str,Any]]:
    """
    ### Description:
    Recursively search for and set store values.  
    The values are stored in a dict that uses the timestamps as key to avoid having duplicate timestamps.   
    ### Parameters:
    - src: response dictionnary
    - dst:
        Must be empty when called for the first time. 
        IDK why but a default value would not reset which led to a bug where raw dataframes would retain the value of previous responses.  
    Don't fill timestamp and path either.
    ### Returns:
    dict of the values indexed by timestamp.
    """
    if "timestamp" in src:
        timestamp = DT.strptime(src["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
    for key, value in src.items():
        if isinstance(value, dict):
            print("value is a dict")
            dst = flatten_json_obj(value, dst, timestamp, path + ([key] if key != "data" else []))
        if isinstance(value, list):
            print("value is a list")
            str_path = ".".join(path + ([key] if key != "data" else []))
            # print(value)
            df = DF.from_records(value)
            if "data" in df.columns:
                print("inside df", df.columns)
                parsed_data = pd.json_normalize(df["data"])
                if "value" in parsed_data.columns and "unit" in parsed_data.columns:
                    unique_units = parsed_data["unit"].unique()
                    if len(unique_units) == 1:
                        parsed_data = parsed_data.drop(columns=["unit"])
                        str_path += "." + unique_units[0]
                        if len(parsed_data.columns) == 2:
                            parsed_data = parsed_data["value"]
                df = pd.concat((df.drop(columns=["data"]), parsed_data), axis="columns")
            if not df.empty:
                df = df.set_index("timestamp")
            dst[str_path] = df
        elif isinstance(value, pd.Series):
            str_path = ".".join(path + [key])
            dst[str_path] = value.to_frame()
        else: 
            print(f"value is not a dict, list, or Series, type: {type(value)}")
            str_path = ".".join(path + [key])
            dst[str_path] = pd.Series(value, name=key)
    return dst

# def flatten_list_of_dicts()

class mercedes_parsing():
    def __init__(self, fs: S3FileSystem, fm1: ABCFileManager, fm2: ABCFileManager, source : str, dest : str):
        self.source = source
        self.dest = dest
        self.fs = fs
        self.fm1=fm1
        self.fm2=fm2

    async def run(self):
        main(self.fs, self.fm1, self.fm2, self.source, self.dest)


