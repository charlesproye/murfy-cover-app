from datetime import timedelta as TD
from os.path import join

import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures, FunctionTransformer
from sklearn.pipeline import Pipeline

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
PATH_TO_CHARGING_PERF_PER_SOC = "data_cache/perfs/charging_perf_per_soc/{id}.parquet"
PATH_TO_FLEET_WISE_DISTRIBUTION = "data_cache/perfs/fleet_wise_perfs/charging_energy_distribution.parquet"
PATH_TO_FLEET_3D_DISTRIBUTION = "data_cache/plots/fleet_wise_perfs/charging_energy_distribution.html"
PATH_TO_DEFAULT_DIST_SHAPE = "perfs/default/dist_shape.parquet"
PATH_TO_DEFAULT_100_INTERCEPTS_SHAPE = "perfs/default/default_100_soh_intercepts.parquet"
PATH_TO_FLEET_CHARGING_POINTS = "perfs/default/fleet_charging_points.parquet"
# recording dependant constants
PERF_MAX_TIME_DIFF = TD(minutes=10)

# charge energy distribution
ODOMETER_FLOOR_RANGE_FOR_ENERGY_DIST = 3000
ODO_RANGE_FORMAT_STR = lambda odo_val: f"[{(odo_val/1000):.0f}k, {((odo_val+ODOMETER_FLOOR_RANGE_FOR_ENERGY_DIST)/1000):.0f}k]"
TEMP_FLOOR_RANGE_FOR_ENERGY_DIST = 5
power_FLOOR_RANGE_FOR_ENERGY_DIST = 5
COLS_TO_DROP_FOR_ENERGY_DISTRIBUTION = [
    "start_odometer",
    "end_odometer",
    "mean_odometer",
    "distance",
    "start_date",
    "end_date",
    "mean_date",
    "start_soc",
    "end_soc",
    "mean_soc",
    "soc_diff",
    "end_cum_energy",
    "start_cum_energy",
    "mean_cum_energy",
    "mean_odo",
]
MOST_COMMON_CHARGE_REGIME_QUERY = "energy_added > 300 & energy_added < 500 & sec_duration < 900 & temp < 35 & power < 4 & power > 1.5"
SOC_RANGE = np.arange(0, 100.5, 0.5, dtype=float)
CHARGE_ENERGY_POINTS_TO_DIST_MODEL = Pipeline([
    ('reshape', FunctionTransformer(lambda x: x.reshape(-1, 1))),
    ('poly_features', PolynomialFeatures(degree=4)),
    ('regressor', LinearRegression())
])
DIST_TO_FIT_IDX = (0, 25.0)

# ========================================================plt constants========================================================
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
            {
                "kind":"hlines",
                "y":0.,
                "linestyle":"--",
                "color":"blue",
                "alpha":0.7,
            },
        ],
    ]
}
