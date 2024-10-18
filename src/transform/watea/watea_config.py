from datetime import timedelta as TD
from os.path import join, dirname
import pandas as pd

from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures, FunctionTransformer
from sklearn.pipeline import Pipeline

# Paths:
# Here we call the time series that watea sent us "watea_responses" in analogy to the responses we get from EV data APIs. 
WATEA_RESPONSES_REGEX = join(dirname(__file__), "data_cache/bib_export/*/*.parquet")
RAW_TS_PATH = join(dirname(__file__), "data_cache/raw_tss.parquet")
PROCESSED_TS_PATH = join(dirname(__file__), "data_cache/processed_tss.parquet")
FLEET_INFO_DF_PATH = join(dirname(__file__), "data_cache/fleet_info/fleet_info_df.{extension}")
RAW_FLEET_CHARGING_POINTS_PATH = join(dirname(__file__), "data_cache/soh_estimation/raw_fleet_charging_points.parquet")
PREPROCESSED_FLEET_CHARGING_POINTS_PATH = join(dirname(__file__), "data_cache/soh_estimation/preprocessed_fleet_charging_points.parquet")
PROCESSED_CLUSTER_PATH = join(dirname(__file__), "data_cache/soh_estimation/processed_cluster.parquet")

# Time series processing:
COLS_TO_DROP = [
    "autonomy_km",
    "speed_gps",
]
DTYPES = {
    # "date_translated ": pd.SparseDtype("datetime64[ns]"),
    "distance_totalizer": "float32",
    "battery_hv_soc": "float32",
    "battery_hv_temp": "float32",
    "battery_hv_voltage": "float32",
    "battery_hv_current": "float32",
}
RENAMING_MAP = {
    "distance_totalizer": "odometer",
    "battery_hv_soc": "soc",
    "date_translated": "date",
    "battery_hv_temp": "temp",
    "battery_hv_voltage": "voltage",
    "battery_hv_current": "current",
    "autonomy_km": "battery_range_km"
}
PERF_MAX_TIME_DIFF = TD(minutes=10)

# SOH estimation:
MAIN_CHARGING_REGIME_CLUSTER_IDX = 8
UMAP_N_COMPONENTS = 3
UMAP_INPUT_FEATURE_COLS = [
    "current",
    "voltage",
    "regime_seperation_feature",
    "temperature",
    "soc",
]
UMAP_RANDOM_STATE = 32
CHARGING_POINTS_AGG_OVER_CHARGES_DICT = {
    "odometer":"median",
    "energy_added":"median",
    "voltage":"median",
    "current":"median",
    "temperature":"median",
    "sec_duration":"median",
    "date":"median",
    "soc":"median",
    "min_voltage":"median",
    "soc_voltage_feature":"median",
    "default_100_soh_energy_added":"median",
    "soh":"median",
    "estimated_range": "mean",
    "estimated_range_diff": "mean",
    #Debugging
    "id":pd.Series.mode,
    "charge_idx":pd.Series.mode,
    "charge_id":pd.Series.mode,
}
SOH_ESTIMATION_FEATURES = [
    "current",
    "voltage",
    "energy_added",
    "regime_seperation_feature",
]
POLYNOMIAL_LINEAR_REGRESSION_PIPELINE = Pipeline([
    ('reshape', FunctionTransformer(lambda x: x.reshape(-1, 1))),
    ('poly_features', PolynomialFeatures(degree=6)),
    ('regressor', LinearRegression())
])
CHARGING_POINTS_GRP_BY_SOC_QUANTIZATION = 0.5

