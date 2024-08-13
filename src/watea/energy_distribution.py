# Currently this is in watea but it's destined to be moved to core when we will use it for tesla
from os.path import exists

from pandas import DataFrame as DF
from pandas import Series
import pandas as pd
import numpy as np
from sklearn.neighbors import KNeighborsRegressor

import core.perf_agg_processing as perfs
from core.pandas_utils import floor_to, series_start_end_diff, log_data_and_return_same_data
from core.caching_utils import save_cache_to
from watea.watea_constants import *
from watea.watea_fleet_info import fleet_info_df
from watea.processed_watea_ts import processed_ts_it

# soh estimation evaluation

# soh estimation
def estimate_soh(charging_points:DF) -> DF:
    return (
        charging_points
        .eval("soh = energy_added / default_100_soh_energy_added") # All of that just for a division...
    )

def estimate_default_100_soh_energy_added(charging_points:DF, feature_cols=DEFAULT_100_SOH_FEATURES, n_neighbors: int = 4, p=2) -> DF:
    fit_samples = charging_points.query("is_default_100_soh")
    regressor = (
        KNeighborsRegressor(n_neighbors=n_neighbors, weights="distance", p=p)
        .fit(fit_samples[feature_cols].values, fit_samples["energy_added"].values)
    )
    return (
        charging_points
        .assign(default_100_soh_energy_added=regressor.predict(charging_points[feature_cols]))
    )

# Preprocessing
def compute_regime_seperation_feature(fleet_charging_points:DF) -> DF:
    expected_voltage_from_soc = (
        fleet_charging_points
        .loc[:, ["voltage", "soc"]]
        .drop_duplicates()
        .sort_values("soc")
        .rolling(80, center=True, on="soc")
        .min()
        .rolling(80, center=True)
        .min()
        .dropna()
    )
    expected_voltage = (
        CHARGE_ENERGY_POINTS_TO_DIST_MODEL
        .fit(expected_voltage_from_soc["soc"].values, expected_voltage_from_soc["voltage"].values)
        .predict(fleet_charging_points["soc"].values)
        .squeeze()
    )
    fleet_charging_points["regime_seperation_feature"] = fleet_charging_points["voltage"] - expected_voltage
    
    return fleet_charging_points

# Extraction:
def agg_charging_points_over_charges(charging_points:DF, agg_dict:dict=CHARGING_POINTS_AGG_OVER_CHARGES_DICT) -> DF:
    agg_dict = {key: value for key, value in agg_dict.items() if key in charging_points.columns}
    return (
        charging_points
        .groupby("charge_id")
        .agg(agg_dict)
    )

def clean_charging_points(charging_points:DF) -> DF:
    return (
        charging_points
        .dropna(how="any")
        .query("energy_added < 502 & energy_added > 100")
    )

def get_raw_fleet_charging_points(force_update=False) -> DF:
    if exists(PATH_TO_RAW_FLEET_CHARGING_POINTS) and not force_update:
        return pd.read_parquet(PATH_TO_RAW_FLEET_CHARGING_POINTS)    
    else:
        charging_poitns = extract_raw_fleet_charging_points()
        save_cache_to(charging_poitns, PATH_TO_RAW_FLEET_CHARGING_POINTS)
        return charging_poitns
        
def extract_raw_fleet_charging_points() -> DF:
    return (
        pd.concat(
            [extract_charging_points_of_ts(ts, id) for id, ts in processed_ts_it("has_power_during_charge")],
            ignore_index=True
        )
        .eval("is_default_100_soh = odometer <= 3000")
    )

def extract_charging_points_of_ts(ts:DF, id:str) -> DF:
    # Use the index/location of the id of the ts in the fleet_info_df to track back the origin of energy points during debugging
    # TODO: use hashing or another method that is only dependent on the id instead of the location in the fleet info order
    id_idx = fleet_info_df.index.get_loc(id) 
    return (
        ts
        .assign(ffilled_odometer=ts["odometer"].ffill())
        .assign(floored_soc=floor_to(ts["soc"].ffill(), CHARGING_POINTS_GRP_BY_SOC_QUANTIZATION))
        .query("in_charge_perf_mask")
        .groupby(["in_charge_perf_idx","floored_soc",], sort=True)
        # TODO: Use pd.NamedAgg & agg kwargs instead of agg & rename
        .agg(
            odometer=pd.NamedAgg("ffilled_odometer", "mean"),
            energy_added=pd.NamedAgg("cum_energy", series_start_end_diff),
            voltage=pd.NamedAgg("voltage", "median"),
            current=pd.NamedAgg("current", "median"),
            temperature=pd.NamedAgg("temp", "median"),
            sec_duration=pd.NamedAgg("date", lambda s: series_start_end_diff(s).total_seconds()),
            date=pd.NamedAgg("date", lambda s: s.iat[0]),
            soc=pd.NamedAgg("floored_soc", "mean"),
        )
        .assign(energy_added=lambda df: df["energy_added"].replace(0, np.nan))
        # Deubgging
        .assign(id=id)
        .assign(charge_idx=lambda df: df.index.get_level_values(0))
        .assign(id_idx=id_idx)
        .assign(charge_id= lambda df: id + "_"  + df["charge_idx"].astype("string"))
        .eval("energy_added = energy_added * -1")
        .eval("current = current * -1")
    )

