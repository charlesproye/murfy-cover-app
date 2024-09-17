# Currently this is in watea but it's destined to be moved to core when we will use it for tesla
from os.path import exists

from pandas import DataFrame as DF
from pandas import Series
import pandas as pd
import numpy as np
from sklearn.neighbors import KNeighborsRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN

from core.pandas_utils import floor_to, series_start_end_diff
from core.caching_utils import singleton_data_caching
from watea.watea_constants import *
from watea.watea_fleet_info import fleet_info_df
from watea.processed_watea_ts import processed_ts_it

# soh estimation evaluation

# soh estimation

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
@singleton_data_caching(PATH_TO_PREPROCESSED_FLEET_CHARGING_POINTS)
def get_preprocessed_charging_points(force_update_extraction=True) -> DF:
    print(force_update_extraction)
    return (
        extract_raw_fleet_charging_points(force_update=force_update_extraction)
        .pipe(clean_charging_points)
        .pipe(compute_regime_seperation_feature)
        .pipe(compute_umap_features)
        .pipe(segment_charing_regimes)
    )
    
def segment_charing_regimes(charging_points: DF) -> DF:
    dbscan = DBSCAN(eps=0.5, min_samples=5, metric='euclidean', n_jobs=-1)
    umap_feature_cols = charging_points.filter(regex='umap_feature_').columns
    charging_points['cluster_idx'] = dbscan.fit_predict(charging_points[umap_feature_cols])

    return charging_points

def compute_umap_features(df:DF, n_components=UMAP_N_COMPONENTS, features=UMAP_INPUT_FEATURE_COLS, n_neighbours=120) -> DF:
    import umap # Import umap inside the function because import is slow (because of tensor flow)
    umap_feature_cols = [f"umap_feature_{i}" for i in range(n_components)]
    umap_feature_cols_to_drop = [col for col in umap_feature_cols if col in df.columns] # Drop umap feature columns if they are already in the df
    df = df.drop(columns=umap_feature_cols_to_drop)
    return (
        Pipeline([
            ('standar_scalar', StandardScaler()),
            ('reducer', umap.UMAP(n_components=n_components, verbose=True, n_neighbors=n_neighbours, random_state=UMAP_RANDOM_STATE)),
            ('to_df', FunctionTransformer(lambda X: DF(X, columns=umap_feature_cols))),
            ('concat_with_og_df', FunctionTransformer(lambda X: pd.concat((X, df.reset_index(drop=True)), axis="columns"))),
        ])
        .fit_transform(
            X=df[features].values,
            y=df["energy_added"],
        )
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
def agg_charging_points_over_charges(charging_points:DF, agg_dict:dict=CHARGING_POINTS_AGG_OVER_CHARGES_DICT) -> DF:
    agg_dict = {key: value for key, value in agg_dict.items() if key in charging_points.columns}
    return (
        charging_points
        .groupby("charge_id")
        .agg(agg_dict)
    )

@singleton_data_caching(PATH_TO_RAW_FLEET_CHARGING_POINTS)
def extract_raw_fleet_charging_points() -> DF:
    print("extracting fleet wise charging points.")
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
        .assign(ffilled_estimated_range=ts["battery_range_km"].ffill())
        .assign(floored_soc=floor_to(ts["soc"].ffill(), CHARGING_POINTS_GRP_BY_SOC_QUANTIZATION))
        .query("in_charge_perf_mask")
        .groupby(["in_charge_perf_idx","floored_soc",], sort=True)
        # For this aggregation, we use pd.NamedAgg instead of a simple dict and then a rename
        .agg(
            odometer=pd.NamedAgg("ffilled_odometer", "mean"),
            energy_added=pd.NamedAgg("cum_energy", series_start_end_diff),
            voltage=pd.NamedAgg("voltage", "median"),
            current=pd.NamedAgg("current", "median"),
            temperature=pd.NamedAgg("temp", "median"),
            sec_duration=pd.NamedAgg("date", lambda s: series_start_end_diff(s).total_seconds()),
            date=pd.NamedAgg("date", lambda s: s.iat[0]),
            soc=pd.NamedAgg("floored_soc", "mean"),
            estimated_range=pd.NamedAgg("ffilled_estimated_range", "median"),
            estimated_range_diff=pd.NamedAgg("ffilled_estimated_range", series_start_end_diff),
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

