import dotenv

from pandas import Series
from pandas import DataFrame as DF
import matplotlib.pyplot as plt
from matplotlib.axes import Axes

from watea_constants import *
from processed_watea_ts import iterate_over_processed_ts, processed_ts_of
from core.plt_utils import fill_axs_by_sohs, plt_single_vehicle_sohs
from watea_perfs import compute_perfs
from watea_fleet_info import fleet_info_df
from core.argparse_utils import parse_kwargs

def main(): 
    kwargs = parse_kwargs()
    id = kwargs.get("plt_id", None)
    id = fleet_info_df.index[0] if id == "first" else id
    if kwargs.get("sohs", False):
        if id:
            vehicle_df = processed_ts_of(id, **kwargs)
            print(vehicle_df.dtypes)
            perfs_dict = compute_perfs(vehicle_df)
            plt_single_vehicle_sohs(vehicle_df, perfs_dict, x_col="date", x_col_periods="mean_date")


if __name__ == "__main__":
    main()
