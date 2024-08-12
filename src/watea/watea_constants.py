from datetime import timedelta as TD
from os.path import join

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
PATH_TO_RAW_TS_FOLDER = "data_cache/raw_time_series"
PATH_TO_RAW_TS = join(PATH_TO_RAW_TS_FOLDER, "{id}.snappy.parquet")
PATH_TO_PROCESSED_TS = "data_cache/processed_time_series/{id}.parquet"
PATH_TO_FLEET_INFO_DF = "data_cache/fleet_info/fleet_info_df.{extension}"
PATH_TO_CHARGING_PERF = "data_cache/perfs/charging_perf_per_soc/{id}.parquet"
PATH_TO_FLEET_WISE_DISTRIBUTION = "data_cache/perfs/fleet_wise_perfs/charging_energy_distribution.parquet"
PATH_TO_FLEET_3D_DISTRIBUTION = "data_cache/plots/fleet_wise_perfs/charging_energy_distribution.html"
PATH_TO_DEFAULT_DIST_SHAPE = "perfs/default/dist_shape.parquet"
PATH_TO_DEFAULT_100_INTERCEPTS_SHAPE = "perfs/default/default_100_soh_intercepts.parquet"
PATH_TO_FLEET_CHARGING_POINTS = "perfs/default/fleet_charging_points.parquet"
# recording dependant constants
PERF_MAX_TIME_DIFF = TD(minutes=10)

# charge energy distribution
# energy points group by constants:
ENERGY_POINTS_GRP_BY_ODOMETER_QUANTIZATION = 3000
ENERGY_POINTS_GRP_BY_TEMPERATURE_QUANTIZATION = 5
ENERGY_POINTS_GRP_BY_VOLTAGE_QUANTIZATION = 0.5
ENERGY_POINTS_GRP_BY_SOC_QUANTIZATION = 0.5
# Data to extract from charge periods:
ENERGY_POINTS_AGGREGATION_DICT = {
    "cum_energy": "energy_added",
    "battery_range_km": "range_gained",
    "power": "power_diff",
    "cum_charge": "charge",
    "voltage": "voltage_diff",
    "current": "current_diff",
    "temp": "temp_diff",
}
# Unecessary columns
COLS_TO_DROP_FOR_ENERGY_POINTS = [
    "start_odometer",
    "end_odometer",
    "mean_odometer",
    "distance",
    "start_date",
    "end_date",
    "mean_date",
    "start_soc",
    "end_soc",
    # "mean_soc",
    "soc_diff",
    "end_cum_energy",
    "start_cum_energy",
    "mean_cum_energy",
    "mean_odo",
]
# MEDIAN COMPUTATION
MOST_COMMON_CHARGE_REGIME_QUERY = "energy_added > 300 & sec_duration < 900 & temp < 35 & mean_voltage < 400 & mean_current > -40"
# DIST FIT
SOC_RANGE = np.arange(0, 100 + ENERGY_POINTS_GRP_BY_SOC_QUANTIZATION, ENERGY_POINTS_GRP_BY_SOC_QUANTIZATION, dtype=float)
VOLTAGE_RANGE = np.arange(330, 400 + ENERGY_POINTS_GRP_BY_VOLTAGE_QUANTIZATION, ENERGY_POINTS_GRP_BY_VOLTAGE_QUANTIZATION, dtype=float)

# Define the pipeline
CHARGE_ENERGY_POINTS_TO_DIST_MODEL = Pipeline([
    ('reshape', FunctionTransformer(lambda x: x.reshape(-1, 1))),
    ('poly_features', PolynomialFeatures(degree=6)),
    ('regressor', LinearRegression())
])
DIST_TO_FIT_IDX = (0, 25.0)

# ========================================================plt constants========================================================
ODO_RANGE_FORMAT_STR = lambda odo_val: f"[{(odo_val/1000):.0f}k, {((odo_val+ENERGY_POINTS_GRP_BY_ODOMETER_QUANTIZATION)/1000):.0f}k]"
# plt layouts:
JUST_ENERGY_SOH = {
    "perfs_dict": {
        "energy_soh": ["soh"],
    },
}
ENERGY_SOH = {
    "perfs_dict": {
        "energy_soh": ["soh"],
    },
    "plt_energy_dist": True,
}

VOLTAGE_AND_SOC = {
    "vehicle_df": [
        {"y":"voltage", "linestyle":"", "marker":"."},
        {"y":"soc", "linestyle":"", "marker":"."},
    ]
}

IN_CHARGE_AND_POWER = {
    "vehicle_df": [
        ["soc", "in_charge_perf_mask"],
        [
            "power",
            "twinx",
            {"y":"temp", "color":"red", "linestyle":"--"}
        ],
        [
            "current",
            {"y":"window_current_mean", "color":"red", "linestyle":"--", "marker":"."}
        ],
            "voltage",
    ],
}

DEBUG_CHARGE_MASK = {
    "vehicle_df": [
        [
            "soc",
            "in_charge_perf_mask",
            {
                "y":"in_discharge_perf_mask",
                "color":"red",
                "alpha":0.6
            },
        ],
        [
            {
                "y":"in_charge_perf_idx",
                "linestyle":"--",
                "marker":"x",
                "color":"red",
                "alpha":0.4
            },
            {
                "y":"in_charge_idx",
                "linestyle":"--",
                "marker":"x",
                "color":"yellow",
                "alpha":0.4
            },
            "in_charge_perf_mask",
            "twinx",
            {
                "y":"smoothed_soc_dir",
                "marker":"x",
                "color":"green",
                "alpha":0.4
            },
        ],
    ]
}

POWER_AND_CHARGE = {
    "vehicle_df": [
        {"y":"current", "color":"green"},
        {"y":"voltage", "color":"red"},
        [
            "soc",
            "in_charge",
        ],
        [
            {"y":"cum_energy", "color":"green"},
            "twinx",
            {"y":"cum_charge", "color":"red"},
        ],
    ]
}
