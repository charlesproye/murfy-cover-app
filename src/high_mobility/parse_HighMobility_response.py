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
    df = parse_json_as_df(kwargs["json_source"])
    df.to_csv(read_json(kwargs["csv_dest"]))

def parse_json_as_df(src) -> DF:
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

def flatten_json_obj(src:dict, dst, timestamp=None, path:list[str]=[]) -> dict[Any,dict[str,Any]]:
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

def read_json(path: str) -> dict|list:
    with open(path) as f:
        return json.load(f)

if __name__ == "__main__":
    main()
