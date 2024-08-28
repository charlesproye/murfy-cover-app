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

from core.argparse_utils import parse_kwargs
from utils.files import ABCFileManager


def main(fm1: ABCFileManager, fm2: ABCFileManager, json_source, csv_dest):
    print("json", type(json_source))
    try:
        print("Attempting to open file...")
        json_data = fm1.load(json_source)
        print("File opened successfully")
        # json_data = json_data.read()
        # print("File content read successfully")
        
        # print("Attempting to parse JSON...")
        # parsed_data = json.loads(json_data)
        # print("JSON parsed successfully")
        print("Calling parse_response_as_df...")
        df = parse_response_as_df(json_data)
        print("DataFrame created successfully")
        
        print("Saving to CSV...")
        df.to_csv(csv_dest)
        print("CSV saved successfully")
    except FileNotFoundError:
        print(f"Error: File not found - {json_source}")
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in file - {json_source}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        raise
    # with open(json_source, 'r') as f:
    #     print("into main")
    #     df = parse_response_as_df(json.load(f))
    # df.to_csv(csv_dest)

def parse_response_as_df(src) -> DF:
    print("into parse_response_as_df")
    flatten_dict = flatten_json_obj(src, {})
    print("flatten_dict", flatten_dict)
    df = pd.concat(flatten_dict, axis="columns", names=['vaiable', 'property']).pipe(set_date)
    print("into parse_response_as_df")
    return df

def set_date(df:DF) -> DF:
    if df.empty:
        return df
    df.index = pd.to_datetime(df.index, utc=True)
    date = df.index.to_series().dt.as_unit("s")
    df["date"] = date
    df.index = date
    df.index.name = "date"

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
            dst = flatten_json_obj(value, dst, timestamp, path + ([key] if key != "data" else []))
        if isinstance(value, list):
            str_path = ".".join(path + ([key] if key != "data" else []))
            # print(value)
            df = DF.from_records(value)
            if "data" in df.columns:
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

    return dst

# def flatten_list_of_dicts()

class mercedes_parsing():
    def __init__(self, fm1: ABCFileManager, fm2: ABCFileManager, source, dest):
        self.source = source
        self.dest = dest,
        self.fm1=fm1
        self.fm2=fm2

    async def run(self):
        main(self.fm1, self.fm2, self.source, self.dest)


