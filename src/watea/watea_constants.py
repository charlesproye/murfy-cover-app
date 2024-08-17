from datetime import timedelta as TD
from os.path import join
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
PATH_TO_RAW_TS_FOLDER = "data_cache/raw_time_series"
PATH_TO_RAW_TS = join(PATH_TO_RAW_TS_FOLDER, "{id}.snappy.parquet")
PATH_TO_PROCESSED_TS = "data_cache/processed_time_series/{id}.parquet"
PATH_TO_FLEET_INFO_DF = "data_cache/fleet_info/fleet_info_df.{extension}"
PATH_TO_RAW_FLEET_CHARGING_POINTS = "data_cache/soh_estimation/raw_fleet_charging_points.parquet"
# recording dependant constants
PERF_MAX_TIME_DIFF = TD(minutes=10)

# soh estimation
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

# ========================================================plt constants========================================================
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
