from datetime import timedelta as TD
from os.path import join, dirname
import pandas as pd

import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures, FunctionTransformer
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer

# Default vehicle values
FORD_ETRANSIT_DEFAULT_KWH_CAPACITY = 89
FORD_ETRANSIT_DEFAULT_KWH_PER_SOC = FORD_ETRANSIT_DEFAULT_KWH_CAPACITY / 100 
FORD_ETRANSIT_DEFAULT_RANGE = 317
FORD_ETRANSIT_DEFAULT_KM_PER_SOC = FORD_ETRANSIT_DEFAULT_RANGE / 100

# paths
PATH_TO_RAW_TS_FOLDER = join(dirname(__file__), "data_cache/raw_time_series")
PATH_TO_RAW_TS = join(dirname(__file__), PATH_TO_RAW_TS_FOLDER, "{id}.snappy.parquet")
PATH_TO_PROCESSED_TS = join(dirname(__file__), "data_cache/processed_time_series/{id}.parquet")
PATH_TO_FLEET_INFO_DF = join(dirname(__file__), "data_cache/fleet_info/fleet_info_df.{extension}")
PATH_TO_RAW_FLEET_CHARGING_POINTS = join(dirname(__file__), "data_cache/soh_estimation/raw_fleet_charging_points.parquet")
PATH_TO_PREPROCESSED_FLEET_CHARGING_POINTS = join(dirname(__file__), "data_cache/soh_estimation/preprocessed_fleet_charging_points.parquet")
PATH_TO_PROCESSED_CLUSTER = join(dirname(__file__), "data_cache/soh_estimation/processed_cluster.parquet")
# recording dependant constants
PERF_MAX_TIME_DIFF = TD(minutes=10)

# soh estimation
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

