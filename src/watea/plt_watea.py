import dotenv
from typing import Generator

from pandas import Series
from pandas import DataFrame as DF
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
import numpy as np
from rich import print

import watea.watea_constants as constants
from watea.processed_watea_ts import iterate_over_processed_ts, processed_ts_of
from core.plt_utils import plt_single_vehicle, plt_fleet
from watea.watea_perfs import compute_perfs
from watea.watea_fleet_info import fleet_info_df
from core.argparse_utils import parse_kwargs

def main(): 
    kwargs = parse_kwargs(["plt_layout"], {"plt_id":"all"})
    plt_layout = getattr(constants, kwargs["plt_layout"])
    
    if kwargs["plt_id"] == "first":
        plt_single_vheicle(fleet_info_df["id"].iat[0], plt_layout)
    elif kwargs["plt_id"] == "all":
        for id in fleet_info_df["id"]:
            plt_single_vehicle(id, plt_layout)
    elif kwargs["plt_id"] == "fleet":
        plt_fleet(iterate_over_fleet, plt_layout, "odometer", f"{plt_layout} over odometer")
    elif kwargs["plt_id"]:
        plt_single_vheicle(id, plt_layout)
    
def plt_single_vheicle(id:str, plt_layout):
    vehicle_df = processed_ts_of(id)
    perfs_dict = compute_perfs(vehicle_df)
    plt_single_vehicle(vehicle_df, perfs_dict, plt_layout, title=id)

def iterate_over_fleet():
    for id, vehicle_df in iterate_over_processed_ts():
        perfs_dict = compute_perfs(vehicle_df)
        yield id, vehicle_df, perfs_dict

if __name__ == "__main__":
    main()
