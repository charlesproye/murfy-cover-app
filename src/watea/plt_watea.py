from rich.traceback import install as install_rich_traceback
from pandas import DataFrame as DF

from core.argparse_utils import parse_kwargs
from core import plt_utils
import watea.watea_constants as constants
from watea.processed_watea_ts import processed_ts_iterator, processed_ts_of
from watea.watea_perfs import energy_soh_perfs_of
from watea.watea_fleet_info import fleet_info_df
from watea.watea_perfs import fleet_wise_perfs_of_watea, default_dist_shape

def main():
    install_rich_traceback(extra_lines=0, width=130)
    kwargs = parse_kwargs(["plt_layout"], {"plt_id":"all", "x_col":"date", "vehilce_query":None, "fleet_query":None, "ts_query":None})
    plt_layout = getattr(constants, kwargs["plt_layout"])
    if kwargs["plt_id"] == "first":
        plt_fleet_info_df = fleet_info_df.query(kwargs["fleet_query"]) if kwargs["fleet_query"] else fleet_info_df
        my_plt_single_vehicle(plt_fleet_info_df["id"].iat[0], plt_layout, kwargs["x_col"])
    elif kwargs["plt_id"] == "all":
        for id, vehicle_df, perfs_dict in iterate_over_fleet(kwargs["fleet_query"]):
            plt_utils.plt_single_vehicle(vehicle_df, perfs_dict, plt_layout, default_dist_shape, title=id, x_col=kwargs["x_col"])
    elif kwargs["plt_id"] == "fleet":
        plt_utils.plt_fleet(
            lambda : iterate_over_fleet(kwargs["vehilce_query"]),
            plt_layout,
            fleet_wise_perfs_of_watea(),
            kwargs["x_col"],
            title=f"{kwargs['plt_layout']} over {kwargs['x_col']}"
        )
    else:
        print("plotting", kwargs["plt_id"])
        my_plt_single_vehicle(kwargs["plt_id"], plt_layout, kwargs["x_col"])

def my_plt_single_vehicle(id:str, plt_layout, x_col:str):
    vehicle_df = processed_ts_of(id)
    perfs_dict = energy_soh_perfs_of(vehicle_df, id)
    plt_utils.plt_single_vehicle(vehicle_df, perfs_dict, plt_layout, default_dist_shape, title=id, x_col=x_col)

def iterate_over_fleet(fleet_query=None, ts_query=None):
    for id, vehicle_df in processed_ts_iterator(fleet_query, ts_query):
        perfs_dict = energy_soh_perfs_of(vehicle_df, id)
        yield id, vehicle_df, perfs_dict

if __name__ == "__main__":
    main()
