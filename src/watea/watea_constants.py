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

# plt constants
DISCHARGE_PERF_COMPUTE_PLT_LAYOUT = {
    "vehicle_df": [
        ["odometer"],
        ["soc", "in_discharge", "twinx", {"y":"smoothed_soc_dir", "linestyle":"--", "marker":"x", "color":"red", "alpha":0.4}],
        "power",
        "cum_energy",
    ],
    "perfs_dict": {
        "discharge_perfs": ["discharge_soh"]
    }
}

CHARGE_PERF_COMPUTE_PLT_LAYOUT = {
    "vehicle_df": [
        ["soc", "in_charge", "twinx", {"y":"smoothed_soc_dir", "linestyle":"--", "marker":"x", "color":"red", "alpha":0.4}],
        "power",
        "cum_energy",
    ],
    "perfs_dict": {
        "charge_perfs": ["energy_soh"]
    }
}

PERF_COMPARAISON_PLT_LAYOUT = {
    "perfs_dict": {
        "charge_perfs": [
            {"y":"energy_soh", "linestyle":"", "marker":".", "alpha":0.7},
            {"y":"sec_per_soc", "linestyle":"", "marker":".", "alpha":0.7},
        ],
        "discharge_perfs": [
            {"y":"discharge_soh", "linestyle":"", "marker":".", "alpha":0.7},
            {"y":"km_per_soc", "linestyle":"", "marker":".", "alpha":0.7},
        ],
    }
}

