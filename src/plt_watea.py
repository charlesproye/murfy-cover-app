import dotenv

from pandas import Series
from pandas import DataFrame as DF
import matplotlib.pyplot as plt
from matplotlib.axes import Axes

from tesla_constants import *
from processed_tesla_ts import iterate_overs_processed_ts, processed_time_series_of
from core.plt_utils import fill_axs_by_sohs, plt_single_vehicle_sohs
from tesla_perfs import compute_all_perfs
from tesla_fleet_info import fleet_info_df
from core.argparse_utils import parse_kwargs

def main(): 
    kwargs = parse_kwargs()
    if kwargs.get():

if __name__ == "__main__":
    main()
