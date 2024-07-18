from datetime import timedelta as TD
from os.path import join

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
    # "sec_per_soc",
    "end_cum_energy",
    "start_cum_energy",
    "mean_cum_energy",
    "mean_odo",
]

# ========================================================plt constants========================================================
VOLTAGE_AND_SOC = {
    "vehicle_df": [
        {"y":"voltage", "linestyle":"", "marker":"."},
        {"y":"soc", "linestyle":"", "marker":"."},
    ]
}

ENERGY_SOH = {
    
    "perfs_dict": {
        "energy_soh": ["soh"]
    }
}

IN_CHARGE_AND_POWER = {
    "vehicle_df": [
        ["soc", "in_charge_perf_mask"],
        [
            "power",
            # {"y":"window_power_median", "color":"yellow", "linestyle":"--", "marker":"x"},
            # {"y":"window_power_mean", "color":"green", "linestyle":"--", "marker":"x"},
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
DISCHARGE_PERF_COMPUTE_PLT_LAYOUT = {
    "vehicle_df": [
        ["odometer"],
        ["soc", "in_discharge_perf_mask"],
        "power",
        "cum_energy",
    ],
    "perfs_dict": {
        "discharge": ["discharge_soh"]
    }
}

CHARGE_PERF_COMPUTE_PLT_LAYOUT = {
    "perfs_dict": {
        "energy_soh": ["energy_soh"],
        "charge": [
            {"y":"energy_soh", "linestyle":"", "marker":".", "alpha":0.7},
            {"y":"sec_per_soc", "linestyle":"", "marker":".", "alpha":0.7},
        ],
        "charge_above_80": [
            {"y":"energy_soh", "linestyle":"", "marker":".", "alpha":0.7},
            {"y":"sec_per_soc", "linestyle":"", "marker":".", "alpha":0.7},
        ],
        "charge_bellow_80": [
            {"y":"energy_soh", "linestyle":"", "marker":".", "alpha":0.7},
            {"y":"sec_per_soc", "linestyle":"", "marker":".", "alpha":0.7},
        ],
    }
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

PERFS_COMPARAISON = {
    "perfs_dict": {
        "energy_soh": ["soh"],
        "charge_above_80": [
            {"y":"energy_soh", "linestyle":"", "marker":".", "alpha":0.7},
            {"y":"sec_per_soc", "linestyle":"", "marker":".", "alpha":0.7},
        ],
        "charge_bellow_80": [
            {"y":"energy_soh", "linestyle":"", "marker":".", "alpha":0.7},
            {"y":"sec_per_soc", "linestyle":"", "marker":".", "alpha":0.7},
        ],
    }
}

CHARGE_PERFS = {
    "perfs_dict": {
        "charge": [
            {"y":"energy_soh", "linestyle":"", "marker":".", "alpha":0.7},
            {"y":"sec_per_soc", "linestyle":"", "marker":".", "alpha":0.7},
        ],
        "charge_above_80": [
            {"y":"energy_soh", "linestyle":"", "marker":".", "alpha":0.7},
            {"y":"sec_per_soc", "linestyle":"", "marker":".", "alpha":0.7},
        ],
        "charge_bellow_80": [
            {"y":"energy_soh", "linestyle":"", "marker":".", "alpha":0.7},
            {"y":"sec_per_soc", "linestyle":"", "marker":".", "alpha":0.7},
        ],
    }
}

RANGE_SOH_COMPUTE_PLT_LAYOUT = {
    "vehicle_df": [
        ["odometer"],
        ["soc", "in_discharge", "twinx", {"y":"smoothed_soc_dir", "linestyle":"--", "marker":"x", "color":"red", "alpha":0.4}],
        "range_soh"
    ]
}
ELEC_VARS_PLT_LAYOUT = {
    "vehicle_df": [
        ["voltage"], "soc"
    ]
}
FLEET_RANGE_SOH_COMPUTE_PLT_LAYOUT = {
    "vehicle_df": [
        {"y":"range_soh", "linestyle":"", "marker":".", "alpha":0.7}
    ],
    "perfs_dict": {
        "charge": [
            {"y":"energy_soh", "linestyle":"", "marker":".", "alpha":0.7},
        ]
    }
}

CHARGE_PERF_PER_SOC = {
    # bar plot of distribution of energy per soc
    "perfs_dict": {
        "charge_eprf_per_soc": [
            
        ]
    }
}
