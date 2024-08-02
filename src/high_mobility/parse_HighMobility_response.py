"""
This module implements a the `parse_json_obj` function.  
It converts a HighMobility response dict into a df.  
When called as a script, it takes in two mandatory path arguments: `json_source` and `csv_dest`.
"""
from typing import Any
import json
from datetime import datetime as DT

from pandas import DataFrame as DF
from rich import print

from core.argparse_utils import parse_kwargs


def main():
    kwargs = parse_kwargs(["json_source", "csv_dest"])
    with open(kwargs["json_source"]) as f:
        df = parse_response_as_df(json.load(f))
    df.to_csv(kwargs["csv_dest"])

def parse_response_as_df(src) -> DF:
    flatten_dict = flatten_json_obj(src, {})
    df = DF.from_dict(flatten_dict, orient="index").pipe(set_date)

    return df

def set_date(df:DF) -> DF:
    if df.empty:
        return df
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

    dst = try_add_to_dst(src, dst, timestamp, path, "data")
    dst = try_add_to_dst(src, dst, timestamp, path, "value")
    for key, value in src.items():
        if isinstance(value, dict):
            dst = flatten_json_obj(value, dst, timestamp, path + ([key] if key != "data" else []))

    return dst

def try_add_to_dst(src:dict, dst:dict[Any,dict[str,Any]], timestamp, path:list, key:str):
    if key in src and not isinstance(src[key], dict) and not timestamp is None:
        dst[timestamp] = {**dst.get(timestamp, {}), ".".join(path):src[key]}
    return dst

if __name__ == "__main__":
    main()
