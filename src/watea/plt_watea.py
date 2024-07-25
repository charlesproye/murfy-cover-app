from rich.traceback import install as install_rich_traceback
from pandas import DataFrame as DF

from core.argparse_utils import parse_kwargs
from core import plt_utils
import watea.watea_constants as constants
from watea.processed_watea_ts import iterate_over_processed_ts, processed_ts_of
from watea.watea_perfs import compute_perfs
from watea.watea_fleet_info import fleet_info_df
from watea.fleet_watea_wise_perfs import fleet_charge_energy_points_df, default_charge_energy_dist

def main():
    install_rich_traceback(extra_lines=0, width=130)
    kwargs = parse_kwargs(["plt_layout"], {"plt_id":"all", "x_col":"date", "vehilce_query":None, "fleet_query":None})
    plt_layout = getattr(constants, kwargs["plt_layout"])
    if kwargs["plt_id"] == "first":
        plt_fleet_info_df = fleet_info_df.query(kwargs["vehilce_query"]) if kwargs["vehilce_query"] else fleet_info_df
        plt_single_vehicle(plt_fleet_info_df["id"].iat[0], plt_layout, kwargs["x_col"], kwargs["fleet_query"])
    elif kwargs["plt_id"] == "all":
        for id, vehicle_df, perfs_dict in iterate_over_fleet(kwargs["vehilce_query"]):
            plt_single_vehicle(id, plt_layout, kwargs["x_col"], kwargs["fleet_query"])
    elif kwargs["plt_id"] == "fleet":
        plt_utils.plt_fleet(
            lambda : iterate_over_fleet(kwargs["vehilce_query"]),
            plt_layout,
            {"charge_energy_points": fleet_charge_energy_points_df, "charge_energy_dist": default_charge_energy_dist},
            kwargs["x_col"],
            title=f"{kwargs['plt_layout']} over {kwargs['x_col']}"
        )
    else:
        print("plotting", kwargs["plt_id"])
        plt_single_vehicle(kwargs["plt_id"], plt_layout, kwargs["x_col"])

def plt_single_vehicle(id:str, plt_layout, x_col:str, fleet_query=None):
    vehicle_df = processed_ts_of(id).pipe(queried_ts, fleet_query)
    perfs_dict = compute_perfs(vehicle_df, id)
    plt_utils.plt_single_vehicle(vehicle_df, perfs_dict, plt_layout, title=id, x_col=x_col)

def queried_ts(vehicle_df: DF, fleet_query=None) -> DF:
    if fleet_query is None:
        return vehicle_df
    return vehicle_df.query(fleet_query)

def iterate_over_fleet(query=None):
    for id, vehicle_df in iterate_over_processed_ts(query):
        perfs_dict = compute_perfs(vehicle_df, id)
        yield id, vehicle_df, perfs_dict

if __name__ == "__main__":
    main()
