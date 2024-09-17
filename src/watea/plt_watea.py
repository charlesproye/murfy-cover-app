from rich import print
from rich.traceback import install as install_rich_traceback
from pandas import DataFrame as DF

from core.console_utils import parse_kwargs
from core import plt_utils
import watea.watea_constants as constants
from watea.processed_watea_ts import processed_ts_it, processed_ts_of
from watea.watea_fleet_info import fleet_info_df

def main():
    install_rich_traceback(extra_lines=0, width=130)
    try:
        kwargs = parse_kwargs(["plt_layout"], {"plt_id":"all", "x_col":"date", "fleet_query":None, "ts_query":None})
        plt_layout = getattr(constants, kwargs["plt_layout"])
        if kwargs["plt_id"] == "first":
            plt_fleet_info_df = fleet_info_df.query(kwargs["fleet_query"]) if kwargs["fleet_query"] else fleet_info_df
            my_plt_single_vehicle(plt_fleet_info_df["id"].iat[0], plt_layout, kwargs["x_col"])
        elif kwargs["plt_id"] == "all":
            for id, vehicle_df, perfs_dict in iterate_over_fleet(kwargs["fleet_query"]):
                plt_utils.plt_single_vehicle(vehicle_df, perfs_dict, plt_layout, default_100_soh_dist, title=id, x_col=kwargs["x_col"])
        elif kwargs["plt_id"] == "fleet":
            plt_utils.plt_fleet(
                lambda : iterate_over_fleet(kwargs["fleet_query"]),
                plt_layout,
                kwargs["x_col"],
                title=f"{kwargs['plt_layout']} over {kwargs['x_col']}"
            )
        else:
            print("plotting", kwargs["plt_id"])
            my_plt_single_vehicle(kwargs["plt_id"], plt_layout, kwargs["x_col"])
    except KeyboardInterrupt:
        print("[blue]exiting...")

def my_plt_single_vehicle(id:str, plt_layout, x_col:str):
    vehicle_df = processed_ts_of(id)
    plt_utils.plt_single_vehicle(vehicle_df, {}, plt_layout, default_100_soh_dist, title=id, x_col=x_col)

def iterate_over_fleet(fleet_query=None, ts_query=None):
    for id, vehicle_df in processed_ts_it(fleet_query, ts_query, track_kwargs={}):
        yield id, vehicle_df, perfs_dict

if __name__ == "__main__":
    main()
