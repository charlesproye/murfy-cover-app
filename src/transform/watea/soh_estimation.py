# Currently this is in watea but it's destined to be moved to core when we will use it for other data pipelines.
from pandas import DataFrame as DF
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures, FunctionTransformer
from sklearn.pipeline import Pipeline

from core.pandas_utils import floor_to, series_start_end_diff
from core.caching_utils import cache_result
from core.console_utils import single_dataframe_script_main
from transform.watea.watea_config import *
from transform.watea.watea_fleet_info import fleet_info
from transform.watea.watea_processed_tss import get_processed_tss

@cache_result(SOH_PER_CHARGES_PATH, on="local_storage")
def get_soh_per_charges() -> DF:
    cluster = get_processed_cluster()
    agg_dict = {key: value for key, value in CHARGING_POINTS_AGG_OVER_CHARGES_DICT.items() if key in cluster.columns}
    return (
        cluster
        .groupby("charge_id")
        .agg(agg_dict)
    )

# soh estimation
@cache_result(PROCESSED_CLUSTER_PATH, on="local_storage")
def get_processed_cluster() -> DF:
    return (
        get_preprocessed_charging_points()
        .query("to_use_for_soh_estimation")
        .pipe(estimate_soh)
    )

def estimate_soh(cluster:DF) -> DF:
    x = cluster[SOH_ESTIMATION_FEATURES].values
    y = cluster["energy_added"].values
    soh_estimator = (
        Pipeline([
            ('poly_features', PolynomialFeatures(degree=6)),
            ('regressor', LinearRegression())
        ])
        .fit(X=x, y=y)
    )
    cluster["general_energy_added"] = (
        soh_estimator
        .predict(X=x)
        .squeeze()
    )
    default_100_soh_cluster = cluster.query("is_default_100_soh")
    y2_pred = soh_estimator.predict(default_100_soh_cluster[['voltage', 'temperature', 'current']])
    residuals = default_100_soh_cluster['energy_added'] - y2_pred
    initial_intercept = soh_estimator.named_steps['regressor'].intercept_
    adjusted_intercept = initial_intercept + residuals.mean()
    soh_estimator.named_steps['regressor'].intercept_ = adjusted_intercept

    cluster:DF = (
        cluster
        .assign(default_100_energy_added=soh_estimator.predict(cluster[['voltage', 'temperature', 'current']]))
        .eval("soh = 100 * energy_added / default_100_energy_added")
    )

    return cluster

# Preprocessing
@cache_result(PREPROCESSED_FLEET_CHARGING_POINTS_PATH, on="local_storage")
def get_preprocessed_charging_points(force_update_extraction=True) -> DF:
    return (
        get_raw_charging_points(force_update=force_update_extraction)
        .pipe(clean_charging_points)
        .pipe(compute_regime_seperation_feature)
        .eval("to_use_for_soh_estimation = regime_seperation_feature <= 30 & current <= 98 & energy_added >= 300")
    )

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
        POLYNOMIAL_LINEAR_REGRESSION_PIPELINE
        .fit(expected_voltage_from_soc["soc"].values, expected_voltage_from_soc["voltage"].values)
        .predict(fleet_charging_points["soc"].values)
        .squeeze()
    )
    fleet_charging_points["regime_seperation_feature"] = fleet_charging_points["voltage"] - expected_voltage
    
    return fleet_charging_points

def clean_charging_points(charging_points:DF) -> DF:
    return (
        charging_points
        .dropna(how="any")
        .query("energy_added < 502 & energy_added > 100")
        .reset_index(drop=True)
    )

# Extraction:
@cache_result(RAW_FLEET_CHARGING_POINTS_PATH, on="local_storage")
def get_raw_charging_points() -> DF:
    tss = get_processed_tss()
    tss["odometer"] = tss.groupby("id")["odometer"].ffill()
    tss["soc"] = floor_to(tss.groupby("id")["soc"].ffill(), CHARGING_POINTS_GRP_BY_SOC_QUANTIZATION)
    tss = tss.merge(fleet_info[["id", "has_power_during_charge"]], on="id", how="left")
    tss = tss.query("in_charge_perf_mask & has_power_during_charge")
    charging_points = tss.groupby(["id", "in_charge_perf_idx","soc",])
    charging_points = charging_points.agg(
        odometer=pd.NamedAgg("odometer", "mean"),
        energy_added=pd.NamedAgg("cum_energy", series_start_end_diff),
        voltage=pd.NamedAgg("voltage", "median"),
        current=pd.NamedAgg("current", "median"),
        temperature=pd.NamedAgg("temperature", "median"),
        sec_duration=pd.NamedAgg("date", lambda s: series_start_end_diff(s).total_seconds()),
        date=pd.NamedAgg("date", lambda s: s.iat[0]),
    )
    charging_points = charging_points.reset_index()
    charging_points["energy_added"] = charging_points["energy_added"].replace(0, np.nan)
    charging_points["charge_id"] =  charging_points["id"] + "_"  + charging_points["in_charge_perf_idx"].astype("string")
    charging_points["energy_added"] *= -1 
    charging_points["current"] *= -1 
    charging_points["is_default_100_soh"] = charging_points["odometer"].lt(MIN_ODO_TO_BECONSIDERED_DEFAULT_SOH, fill_value=False)

    return charging_points

if __name__ == "__main__":
    single_dataframe_script_main(get_raw_charging_points, force_update=True)
    single_dataframe_script_main(get_preprocessed_charging_points, force_update=True)
    single_dataframe_script_main(get_processed_cluster, force_update=True)

