from datetime import datetime as DT
import numpy as np
from rich.progress import track
import pandas as pd
from pandas import Series
from pandas import DataFrame as DF
import plotly.express as px

from core.config import *
from core.ev_models_info import models_info
from transform.ayvens.ayvens_fleet_info import fleet_info
from transform.ayvens.ayvens_get_raw_tss import get_ayvens_raw_tss

raw_tss = get_ayvens_raw_tss()
tss_dict = {}

COLS_TO_CPY_FROM_FLEET_INFO = [
    "make",
    "model",
    "version",
    "dummy_soh_maker_offset",
    "dummy_soh_model_offset",
    "dummy_soh_model_slope",
    "dummy_soh_vehicle_offset",
    "capacity",
]

RENAME_COLS_DICT = {
    "date_of_value": "date",
    "diagnostics.odometer": "odometer",
    "odometer.value": "odometer",
    "diagnostics.odometer": "odometer",
    "mileage_km": "odometer",
    "mileage": "odometer",
    "charging.battery_energy": "battery_energy",
    "charging.estimated_range": "estimated_range",
    "charging.battery_level": "soc",
    "soc_hv_header": "soc",
}

COLS_TO_KEEP = [
    "date",
    "soc",
    "odometer",
    "estimated_range",
    "battery_energy",
    "soc",
    "vin",
]

COL_DTYPES = {
    "soc": "float",
    "odometer": "float",
    "estimated_range": "float",
    "battery_energy": "float",
    "soc": "float",
    "dummy_soh_maker_offset": "float",
    "dummy_soh_model_offset": "float",
    "dummy_soh_model_slope": "float",
    "dummy_soh_vehicle_offset": "float",
    "dummy_soh_offset": "float",
    "vin": "string",
    "capacity": "float",
}

for brand, brand_raw_tss in track(raw_tss.items()):
    # Add model and model version columns
    brand_raw_tss = brand_raw_tss.rename(columns=RENAME_COLS_DICT)
    cols_to_drop = brand_raw_tss.columns[~brand_raw_tss.columns.isin(COLS_TO_KEEP)]
    brand_raw_tss = brand_raw_tss.drop(columns=cols_to_drop)
    brand_raw_tss[COLS_TO_CPY_FROM_FLEET_INFO] = fleet_info.loc[brand_raw_tss["vin"], COLS_TO_CPY_FROM_FLEET_INFO].values
    tss_dict[brand] = brand_raw_tss.eval("dummy_soh_offset = dummy_soh_maker_offset + dummy_soh_model_offset + dummy_soh_vehicle_offset")


tss_dict["renault"]["capacity"] = (
    models_info
    .query("model == 'zoe'")
    .set_index("version")
    .loc[tss_dict["renault"]["version"], "kwh_capacity"].values
)

tss_dict["mercedes-benz"]["range"] = (
    models_info
    .set_index("model")
    .loc[tss_dict["mercedes-benz"]["model"], "default_km_range"].values
)

tss = (
    pd.concat(tss_dict, ignore_index=True)
    .astype(COL_DTYPES)
    .sort_values(by=["make", "vin", "date"])
)

tss["date"] = pd.to_datetime(tss["date"], format="mixed").dt.tz_localize(None)
tss["registration_date"] = pd.to_datetime(fleet_info.loc[tss["vin"], "registration_date"].values, format="mixed")
tss["age_in_days"] = tss.eval("date - registration_date").dt.days
tss["age_in_years"] = tss.eval("date - registration_date").dt.days.div(365)

ages = tss.groupby("vin").agg({
    "age_in_years": "last", 
    "make": "first", 
    "model": "first", 
    "version": "first", 
})

fig = px.histogram(
    ages, 
    x="age_in_years",
    color="make",
    facet_col="make",
    facet_col_wrap=3,
    height=1000,
)
fig.write_html("data_cache/age_in_years_distribuyion.html")

odometers = (
    tss
    .groupby("vin")
    .agg({
        "odometer": "max",
        "make": "first",
    })
)

odometers.to_csv("data_cache/odometers.csv")

fig = px.histogram(
    odometers,
    x="odometer",
    nbins=15,
    color="make",
    facet_col="make",
    title="Distribution of vehicles over odometer"
)
fig.write_html("data_cache/odometer_distribution.html")

tss["soh"] = (
    tss
    .eval("soh = 100 + dummy_soh_offset - odometer * dummy_soh_model_slope")
    .groupby("vin")["soh"]
    .transform(lambda soh: soh + np.random.normal(0, 0.02, len(soh)))
    .clip(0, 100)
)
tss["soh_method"] = "general"

px.scatter(
    tss.groupby("vin").agg({"odometer": "last", "soh": "mean", "make": "first"}),
    x="odometer",
    y="soh",
    trendline="ols",
    color="make",
)

px.scatter(
    tss.groupby("vin").agg({"age_in_years": "last", "soh": "mean", "make": "first"}).dropna(how="any"),
    x="age_in_years",
    y="soh",
    # trendline="ols",
    color="make",
)

def get_sohs_of_brand(tss:DF, brand:str=None) -> DF:
    tss:DF = tss.query(f"make == '{brand}'") if brand else tss

    return (
        tss
        .groupby("vin")
        .agg({
            "odometer": "max",
            "soh": "median",
            "age_in_years": "last",
            "make": "first",
            "model": "first",
            "version": "first",
            "age_in_days": "last",
            "registration_date": "last",
        })
        .reset_index(drop=False)
        .sort_values(by=["vin", "odometer"])
    )

# Renault soh
# Note soc of renault is between 0 and 1, not 0 and 100.
renault_soh_mask:Series = tss.eval("make == 'renault'")
tss.loc[renault_soh_mask] = (
    tss[renault_soh_mask]
    .eval("expected_battery_energy = capacity * soc")
    .eval("soh = 100 * battery_energy / expected_battery_energy")
)
tss.loc[renault_soh_mask, "soh_method"] = "renault"
renault_soh = get_sohs_of_brand(tss, "renault")
renault_soh.to_csv("data_cache/renault_soh.csv")

fig = px.scatter(
    renault_soh,
    x="odometer",
    y="soh",
    trendline="ols",
)
fig.write_html("data_cache/renault_soh_over_odometer.html")

fig = px.scatter(
    renault_soh.dropna(subset=["age_in_years", "soh"], how="any"),
    x="age_in_years",
    y="soh",
    trendline="ols",
    color="version",
)
fig.write_html("data_cache/renault_soh_.html")
# Note: The soh for Vitos and Sprinters had very low values when using the official range estimations.  
# Their default range has been modified to 170 in the models_info.csv  to get a soh value that is coherent.

mercedes_soh_mask = tss["make"] == "mercedes-benz"
tss.loc[mercedes_soh_mask, "soh"] = (
    tss.loc[mercedes_soh_mask]
    .eval("estimated_range / soc / range * 100")
)
tss.loc[mercedes_soh_mask, "soh_method"] = "mercedes-benz"
mercedes_soh = get_sohs_of_brand(tss, "mercedes-benz")
mercedes_soh["soh"] = mercedes_soh["soh"] #.clip(70, 99.5)
mercedes_soh.to_csv("data_cache/mercedes_soh.csv")

# ##### Plot soh by odometer

fig = px.scatter(
    mercedes_soh.query("model != 'Vito' & model != 'Sprinter'"),
    x="odometer",
    y="soh",
    trendline="ols",
    trendline_scope="overall",
    color="model",
)
fig.write_html("data_cache/mercedes_soh_by_odometer.html")
# vitos and sprinters
fig = px.scatter(
    mercedes_soh.query("model == 'Vito' | model == 'Sprinter'"),
    x="odometer",
    y="soh",
    trendline="ols",
    trendline_scope="overall",
    color="model",
)
#fig.show()
fig.write_html("data_cache/mercedes_soh_by_odometer.html")

# ##### Plot by age

fig = px.scatter(
    mercedes_soh.dropna(subset=["age_in_years", "soh"], how="any"),
    x="age_in_years",
    y="soh",
    # trendline="ols",
    color="model",
)
fig.write_html("data_cache/mercedes_soh_over_age_in_years.html")

ford_tss:DF = (
    tss
    .query("make == 'ford'")
    .query("soc > 0.5")
    .eval("soh = battery_energy / soc / capacity * 100")
)
tss.loc[tss.eval("make == 'ford'"), "soh_method"] = "ford"
ford_soh = get_sohs_of_brand(tss, "ford")

fig = px.scatter(
    ford_soh,
    x="odometer",
    y="soh",
    color="model",
    height=600,
    title="Average State-of-Health (SoH) vs Mileage",
    trendline="ols",
    # trendline_scope="overall",
    hover_data=["vin"]
)
fig.update_layout(
    xaxis_title="Latest mileage (km)",
    yaxis_title="SoH (%)",
    legend_title="Model",
)
fig.update_traces(line=dict(color='black', dash='dash'))

fig.write_html("data_cache/ford_soh_over_odometer.html")

fig = px.scatter(
    ford_soh,
    x="age_in_years",
    y="soh",
    color="model",
    height=600,
    title="Average State-of-Health (SoH) vs Mileage",
    trendline="ols",
    # trendline_scope="overall",
    hover_data=["vin"]
)
fig.update_layout(
    xaxis_title="Latest mileage (km)",
    yaxis_title="SoH (%)",
    legend_title="Model",
)
fig.update_traces(line=dict(color='black', dash='dash'))

fig.write_html("data_cache/ford_soh_over_age_in_years.html")


all_sohs = pd.concat({
    "renault": renault_soh,
    "ford": ford_soh,
    "mercedes": mercedes_soh,
    "dummy": (
        tss
        .query("soh_method == 'general'")
        .groupby("vin")
        .agg({
            "soh": "mean",
            "odometer": "last",
            "make": "first",
            "version": "first",
        })
        .reset_index(drop=False)
    )
})

px.scatter(
    all_sohs,
    x="odometer",
    y="soh",
    color="make"
).show()
fig.write_html("data_cache/all_sohs_over_odometer.html")

px.scatter(
    all_sohs,
    x="age_in_years",
    y="soh",
    color="make"
).show()
fig.write_html("data_cache/all_sohs_over_age_in_years.html")

px.scatter(
    all_sohs.loc[["renault", "ford", "mercedes"]],
    x="odometer",
    y="soh",
    color="make"
).show()
fig.write_html("data_cache/relialbe_soh_over_odometer.html")

px.scatter(
    all_sohs.loc[["renault", "ford", "mercedes"]],
    x="age_in_years",
    y="soh",
    color="make"
).show()
fig.write_html("data_cache/relialbe_soh_over_age_in_years.html")

# #### Evolutoin of soh over a month
# Evolution of soh since last presentation.

old_tss = tss[tss["date"] <= pd.Timestamp("2024-09-17")]
new_tss = tss #[tss["date"] > pd.Timestamp("2024-09-17")]
old_renault_soh = get_sohs_of_brand(old_tss, "renault")
new_renault_soh = get_sohs_of_brand(new_tss, "renault")

yes: dict[str, list] = {}
for col in old_renault_soh.columns:
    yes[col] = []
    for vin in old_renault_soh["vin"].unique():
        yes[col].append(old_renault_soh.set_index("vin", drop=False).loc[vin, col])
        yes[col].append(new_renault_soh.set_index("vin", drop=False).loc[vin, col])
        yes[col].append(None)
yes = DF(yes)

import plotly.graph_objects as go

MARKER_SIZE = 8

fig = go.Figure(
    data=[
        go.Scatter(
            x=yes["odometer"],
            y=yes["soh"],
            mode="markers+lines",
            marker=dict(
                symbol="arrow",
                color="royalblue",
                size=MARKER_SIZE,
                angleref="previous",
                standoff=MARKER_SIZE / 2,
            ),
        ),
        go.Scatter(
            x=yes["odometer"],
            y=yes["soh"],
            # text=yes["years"],
            mode="markers",
            marker=dict(
                # color=yes["colors"],
                size=MARKER_SIZE,
            ),
        ),
    ]
)
fig.write_html("data_cache/renault_soh_evolution.html")



