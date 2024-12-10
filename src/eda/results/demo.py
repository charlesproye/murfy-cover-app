from logging import getLogger

import plotly.express as px

from core.caching_utils import cache_result
from core.pandas_utils import *
from core.stats_utils import *
from transform.results.config import *
from transform.results.tesla_results import get_results as get_results_raw

@cache_result("data_cache/tesla_res.parquet", on="local_storage")
def get_results() -> DF:
    return (
        get_results_raw()
        .astype({
            "fast_charger_type": "string",
            "model": "string",
            "version": "string",
            "model_version": "string",
        })
    )

logger = getLogger("eda.results.monotonically_decreasing_soh")
results = get_results()

def make_soh_presentable(df:DF) -> DF:
    outliser_mask = mask_out_outliers_by_interquartile_range(df["soh"])
    df = df[outliser_mask].copy()
    if len(df) > 0:
        df["soh"] = force_monotonic_decrease(df["soh"])
    return df

# Application du traitement sur le DataFrame original
#a@cache_result("data_cache/tesla_res_processed.parquet", on="local_storage")
def get_processed_results() -> DF:
    return (
        results
        # 3. Appliquer le traitement de SoH par véhicule
        .groupby('vin')
        .apply(make_soh_presentable, include_groups=False)
        .reset_index(drop=False)  # Supprime l'index 'vin' créé par le groupby
        # 1. D'abord, arrondir les dates à la semaine 
        .assign(date=lambda df: df["date"].dt.floor(UPDATE_FREQUENCY))
        # 2. Grouper par vin et date pour avoir une valeur hebdomadaire
        .groupby(["vin", "date"])
        .agg({
            "odometer": "last",    
            "soh": "median",
            "model": "first",
        })
        .pipe(filter_results_by_lines_bounds, VALID_SOH_POINTS, logger=logger)
        .reset_index()
        # 4. Reset final de l'index
        .reset_index(drop=True)
        .sort_values(["vin", "odometer"])
    )

px.line(
    get_processed_results().dropna(subset=["soh", "odometer"]), 
    x="odometer", 
    y="soh", 
    color="vin", 
    title="Processed SoH results"
).show()

