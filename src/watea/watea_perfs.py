from typing import Generator

from pandas import DataFrame as DF
from rich import print

import core.time_series_processing as ts
from watea_constants import *
from core.caching_utils import data_caching_wrapper
from watea_fleet_info import iterate_over_ids
from core.argparse_utils import parse_kwargs
from raw_watea_ts import raw_ts_of
from core.plt_utils import plt_time_series
from processed_watea_ts import iterate_over_processed_ts

def main():
    kwargs = parse_kwargs()
    for id, vehicle_df in iterate_over_processed_ts(force_update=True, **kwargs):
        
        
if __name__ == '__main__':
    main()
