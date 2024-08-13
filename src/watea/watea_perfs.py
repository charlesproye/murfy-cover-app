import pandas as pd
from pandas import DataFrame as DF
from rich.traceback import install as install_rich_traceback
from itertools import starmap

from watea.processed_watea_ts import processed_ts_it
from watea.energy_distribution import *
from watea.watea_constants import *
from core.argparse_utils import parse_kwargs
from core.caching_utils import data_caching_wrapper, save_cache_to
from core.pandas_utils import *

# def main():
#     install_rich_traceback(extra_lines=0, width=130)
#     kwargs = parse_kwargs(optional_args={"task":"compute_all", "force_update":False})
#     # fleet_perfs = fleet_wise_perfs_of_watea()
#     # save_cache_to(fleet_perfs["default_dist_shape"], PATH_TO_DEFAULT_DIST_SHAPE)
#     # save_cache_to(fleet_perfs["default_100_soh_intercepts"], PATH_TO_DEFAULT_100_INTERCEPTS_SHAPE)
#     for id, vehicle_df in processed_ts_iterator(fleet_query=kwargs.get("query_str", None)):
#         energy_soh_perfs_of(vehicle_df, id, True)

def energy_soh_perfs_of(vehicle_df: DF, id:str, force_update=False) -> dict[str, DF|Series]:
    """
    ### Description:
    Gets all the data generated to compute the soh of a vehicle, except the fleet wise data.
    ### Returns:
    Dict of the dataframes used.
    """
    perfs = {"charging_points": charge_energy_points_of(id, vehicle_df, force_update)}
    perfs["medians"] = compute_charge_energy_median(perfs["charging_points"])
    perfs["intercepts"] = all_diffs_from_points_to_dist(perfs["medians"], default_dist_shape)
    perfs["energy_soh"] = soh_from_intercepts(perfs["intercepts"], default_100_soh_intercepts)
    perfs["charge_energy_dist"] = dists_from_default_dist_and_intercepts(perfs["intercepts"], default_dist_shape)

    return perfs

def fleet_wise_perfs_of_watea(force_update=False) -> dict[str, DF|Series]:
    energy_points = pd.concat(list(starmap(lambda id, ts: charge_energy_points_of(id, ts, force_update), processed_ts_it("has_power_during_charge"))))
    perfs: dict[str, DF] = {"charging_points":energy_points, "medians": compute_charge_energy_median(energy_points)}
    perfs["default_dist_shape"] = fit_poly_lr_to_charge_dist_xs(perfs["medians"].xs(DIST_TO_FIT_IDX))
    perfs["default_dist_shape"] = perfs["default_dist_shape"].sub(perfs["default_dist_shape"].min())
    perfs["intercepts"] = all_diffs_from_points_to_dist(perfs["medians"], perfs["default_dist_shape"])
    perfs["default_100_soh_intercepts"] = perfs["intercepts"].xs(0)
    perfs["charge_energy_dist"] = dists_from_default_dist_and_intercepts(perfs["intercepts"], perfs["default_dist_shape"])
    perfs["default_100_soh_dist"] = perfs["charge_energy_dist"].xs(0)

    return perfs

def charge_energy_points_of(id:str, vehicle_df: DF, force_update=False) -> DF:
    return data_caching_wrapper(
        id,
        PATH_TO_CHARGING_PERF.format(id=id),
        lambda _: extract_charging_points_of_ts(vehicle_df, id),
        force_update,
    )

if __name__ == '__main__':
    install_rich_traceback(extra_lines=0, width=130)

# TODO: find a better way implement data caching
fleet_perfs = fleet_wise_perfs_of_watea(force_update=__name__ == '__main__')
default_100_soh_intercepts = fleet_perfs["default_100_soh_intercepts"]
default_dist_shape = fleet_perfs["default_dist_shape"]
fleet_charge_energy_points_df = fleet_perfs["charging_points"]
default_100_soh_dist = fleet_perfs["default_100_soh_dist"]

