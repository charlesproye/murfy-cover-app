# Currently this is in watea but it's destined to be moved to core when we will use it for tesla
from pandas import DataFrame as DF
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN

from core.pandas_utils import floor_to, series_start_end_diff
from core.caching_utils import cache_result
from core.console_utils import single_dataframe_script_main
from transform.watea.watea_config import *
from transform.watea.watea_fleet_info import fleet_info
from transform.watea.watea_processed_tss import get_processed_tss

# soh estimation
@cache_result(PROCESSED_CLUSTER_PATH, on="local_storage")
def get_processed_cluster() -> DF:
    return (
        get_preprocessed_charging_points()
        .query("to_use_for_soh_estimation")
        .pipe(estimate_soh)
    )


def estimate_soh(cluster:DF) -> DF:
    x = cluster[["voltage", "temperature", "current"]].values
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
        # .pipe(compute_umap_features)
        # .pipe(segment_charing_regimes)
        .eval("to_use_for_soh_estimation = regime_seperation_feature <= 30 & current <= 98 & energy_added >= 300")
    )

# def segment_charing_regimes(charging_points: DF) -> DF:
#     dbscan = DBSCAN(eps=0.5, min_samples=5, metric='euclidean', n_jobs=-1)
#     umap_feature_cols = charging_points.filter(regex='umap_feature_').columns
#     charging_points['cluster_idx'] = dbscan.fit_predict(charging_points[umap_feature_cols])

#     return charging_points

# def compute_umap_features(df:DF, n_components=UMAP_N_COMPONENTS, features=UMAP_INPUT_FEATURE_COLS, target_feature="energy_added", n_neighbours=120) -> DF:
#     import umap # Import umap inside the function because import is slow (because of tensor flow)
#     umap_feature_cols = [f"umap_feature_{i}" for i in range(n_components)]
#     umap_feature_cols_to_drop = [col for col in umap_feature_cols if col in df.columns] # Drop umap feature columns if they are already in the df
#     df = df.drop(columns=umap_feature_cols_to_drop)
#     return (
#         Pipeline([
#             ('standar_scalar', StandardScaler()),
#             ('reducer', umap.UMAP(n_components=n_components, verbose=True, n_neighbors=n_neighbours)), #, random_state=UMAP_RANDOM_STATE)),
#             ('to_df', FunctionTransformer(lambda X: DF(X, columns=umap_feature_cols))),
#             ('concat_with_og_df', FunctionTransformer(lambda X: pd.concat((X, df.reset_index(drop=True)), axis="columns"))),
#         ])
#         .fit_transform(
#             X=df[features].values,
#             y=df[target_feature],
#         )
#     )

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
def agg_charging_points_over_charges(charging_points:DF, agg_dict:dict=CHARGING_POINTS_AGG_OVER_CHARGES_DICT) -> DF:
    agg_dict = {key: value for key, value in agg_dict.items() if key in charging_points.columns}
    return (
        charging_points
        .groupby("charge_id")
        .agg(agg_dict)
    )

@cache_result(RAW_FLEET_CHARGING_POINTS_PATH, on="local_storage")
def get_raw_charging_points() -> DF:
    tss = get_processed_tss()
    tss["odometer"] = tss["odometer"].ffill()
    tss["soc"] = floor_to(tss["soc"].ffill(), CHARGING_POINTS_GRP_BY_SOC_QUANTIZATION)
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
    # single_dataframe_script_main(get_raw_charging_points, force_update=True)
    single_dataframe_script_main(get_preprocessed_charging_points, force_update=True)
    single_dataframe_script_main(get_processed_cluster, force_update=True)

