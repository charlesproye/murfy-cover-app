from datetime import timedelta as TD

KJ_TO_KWH = 0.00027777778
MILES_TO_KM = 1.60934
KJ_TO_KWH = 0.00027777777


# period perfs
DEFAULT_DIFF_VARS = {
    "soc": "soc_diff",
    "date": "duration",
    "odometer": "distance",
}

# plotting
PERF_VARS_DICT = {
    "charging_perfs": ["sec_per_soc", "energy_soh", "soh_cum_charger_energy", "battery_range_added_soh"],
    "motion_perfs": ["km_per_soc", "range_soh"],
    "self_discharge_perfs": ["secs_per_soc", "range_soh"]
}
X_TIME_SERIES_COL_TO_X_PERIOD_COL = {
    "odometer": "mean_odo",
    "date": "mean_date",
}
DEFAULT_LINE_PLOT_KWARGS = {
    "marker":".",
}
DEFAULT_CHARGING_POINTS_PLT_KWARGS = {
    "alpha": 0.45,
}

SOH_LOST_PER_KM_DUMMY_RATIO = 0.000028

# Ayvens related *should* be removed.
KEY_LIST_COLUMN_NAMES = [ "key", "dtype_folder", 'brand', "vin", "file"]
EMTPY_S3_KEYS_WARNING_MSG = """
No responses found in {keys_prefix}.
"""
