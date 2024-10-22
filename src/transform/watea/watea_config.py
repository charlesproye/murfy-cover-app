from datetime import timedelta as TD
from os.path import join, dirname
import pandas as pd

from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures, FunctionTransformer
from sklearn.pipeline import Pipeline

# Paths:
# Here we call the time series that watea sent us "watea_responses" in analogy to the responses we get from EV data APIs. 
BASE_DIR = join(dirname(__file__), "data_cache")
WATEA_RESPONSES_REGEX = join(BASE_DIR, "/bib_export/*/*.parquet")
RAW_TS_PATH = join(BASE_DIR, "raw_tss.parquet")
PROCESSED_TS_PATH = join(BASE_DIR, "processed_tss.parquet")
FLEET_INFO_DF_PATH = join(BASE_DIR, "fleet_info_df.parquet")
RAW_FLEET_CHARGING_POINTS_PATH = join(BASE_DIR, "raw_fleet_charging_points.parquet")
PREPROCESSED_FLEET_CHARGING_POINTS_PATH = join(BASE_DIR, "preprocessed_fleet_charging_points.parquet")
PROCESSED_CLUSTER_PATH = join(BASE_DIR, "processed_cluster.parquet")
SOH_PER_CHARGES_PATH = join(BASE_DIR, "soh_per_charges.parquet")

# Time series processing:
COLS_TO_DROP = [
    "autonomy_km",
    "speed_gps",
]
DTYPES = {
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
    "battery_hv_temp": "temperature",
    "battery_hv_voltage": "voltage",
    "battery_hv_current": "current",
    "autonomy_km": "battery_range_km"
}
PERF_MAX_TIME_DIFF = TD(minutes=10)

# Fleet info:
COLS_TO_DESCRIBE_IN_FLEET_INFO = [
    "power",
    "current",
    "voltage",
    "temperature"
]

# SOH estimation:
MIN_ODO_TO_BECONSIDERED_DEFAULT_SOH = 3000.0
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
SOH_ESTIMATION_FEATURES = ["voltage", "temperature", "current"]
POLYNOMIAL_LINEAR_REGRESSION_PIPELINE = Pipeline([
    ('reshape', FunctionTransformer(lambda x: x.reshape(-1, 1))),
    ('poly_features', PolynomialFeatures(degree=6)),
    ('regressor', LinearRegression())
])
CHARGING_POINTS_GRP_BY_SOC_QUANTIZATION = 0.5

