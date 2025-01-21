from logging import getLogger

import plotly.express as px

from core.caching_utils import cache_result
from core.pandas_utils import *
from core.stats_utils import *
from transform.results.config import *
from transform.results.ford_results import get_results as get_results_raw

@cache_result("data_cache/ford_res.parquet", on="local_storage")
def get_results() -> DF:
    return (
        get_results_raw()
    )

logger = getLogger("eda.results.monotonically_decreasing_soh")
results = get_results()

def make_soh_presentable(df:DF) -> DF:
    outliser_mask = mask_out_outliers_by_interquartile_range(df["soh"])
    assert outliser_mask.sum() > 0, "There seems to be only outliers???"
    df = df[outliser_mask].copy()
    df["soh"] = force_monotonic_decrease(df["soh"])
    return df

# Application du traitement sur le DataFrame original
#a@cache_result("data_cache/tesla_res_processed.parquet", on="local_storage")
def get_processed_results() -> DF:
    return (
        results
        .assign(date=lambda df: df["date"].dt.floor(UPDATE_FREQUENCY))
        .groupby(["vin", "date"])
        .agg({
            "odometer": "last",
            "soh": "median",
            "model": "first",
        })
        .reset_index()
        .groupby('vin')
        .apply(make_soh_presentable, include_groups=False)
        .reset_index(drop=False)  # Supprime l'index 'vin' créé par le groupby
        .assign(date=lambda df: df["date"].dt.floor(UPDATE_FREQUENCY))
        .groupby(["vin", "date"])
        .agg({
            "odometer": "last",    
            "soh": "median",
            "model": "first",
        })
        .pipe(filter_results_by_lines_bounds, VALID_SOH_POINTS_LINE_BOUNDS, logger=logger)
        .reset_index()
        .sort_values(["vin", "odometer"])
    )

px.line(
    get_processed_results().dropna(subset=["soh", "odometer"]), 
    x="odometer", 
    y="soh", 
    color="vin", 
    title="Processed SoH results"
).show()

