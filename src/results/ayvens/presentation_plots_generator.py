from os import path
from os import system

from datetime import datetime as DT
import numpy as np
from rich.progress import track
import pandas as pd
from pandas import Series
from pandas import DataFrame as DF
import plotly.express as px
import plotly.graph_objects as go
from plotly.graph_objects import Figure

from core.config import *
from core.ev_models_info import models_info
from core.caching_utils import cache_result
from transform.ayvens.ayvens_fleet_info import fleet_info
from transform.ayvens.ayvens_get_raw_tss import get_ayvens_raw_tss


system("mkdir -p data_cache")
system("mkdir -p data_cache/plots")
system("mkdir -p data_cache/tables")

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

@cache_result(path.join(path.dirname(__file__), "data_cache/processed_tss.parquet"), on="local_storage")
def get_processed_tss():
    raw_tss = get_ayvens_raw_tss()
    tss_dict = {}

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

    return tss

tss = get_processed_tss()
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
fig.write_html("data_cache/plots/age_in_years_distribuyion.html")

odometers = (
    tss
    .groupby("vin")
    .agg({
        "odometer": "max",
        "make": "first",
    })
)

odometers.to_csv("data_cache/tables/odometers.csv")

px.histogram(
    odometers,
    x="odometer",
    nbins=15,
    color="make",
    facet_col="make",
    title="Distribution of vehicles over odometer"
).write_html("data_cache/plots/odometer_distribution.html")

tss["soh"] = (
    tss
    .eval("soh = 100 + dummy_soh_offset - odometer * dummy_soh_model_slope")
    .groupby("vin")["soh"]
    .transform(lambda soh: soh + np.random.normal(0, 0.02, len(soh)))
    .clip(0, 100)
)

def get_sohs_per_vin(tss:DF) -> DF:
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

def plt_sohs(sohs:DF, sohs_name:str, x:str, trendline_scope:str="overall", color="make") -> Figure:
    return (
        px.scatter(
            sohs.dropna(subset=[x, "soh"], how="any"),
            x=x,
            y="soh",
            hover_name="vin",
            trendline="ols",
            trendline_scope=trendline_scope,
            color=color,
            labels={
                "soh": "Stae.Of.Health (%)",
                x: {"age_in_years": "Age (years)", "odometer": "Mileage (km)"}[x],
            },
        )
        .update_traces(line={'dash': 'dash', 'color': 'black'})
        .write_html(f"data_cache/plots/{sohs_name}_soh_over_{x}.html")
    )

def get_n_scatter_sohs(tss:DF, sohs_name:str, **kwargs):
    sohs = get_sohs_per_vin(tss)
    plt_sohs(sohs, sohs_name, "age_in_years", **kwargs)
    plt_sohs(sohs, sohs_name, "odometer", **kwargs)
    sohs.to_csv("data_cache/tables/{sohs}.csv")

get_n_scatter_sohs(tss, "all_dummy_sohs")

# Renault soh
# Note soc of renault is between 0 and 1, not 0 and 100.
renault_soh_mask:Series = tss.eval("make == 'renault'")
tss.loc[renault_soh_mask] = (
    tss[renault_soh_mask]
    .eval("expected_battery_energy = capacity * soc")
    .eval("soh = 100 * battery_energy / expected_battery_energy")
)
tss.loc[renault_soh_mask, "soh_method"] = "renault"
get_n_scatter_sohs(tss.query("make == 'renault'"), "renault", color="version")


# Note: The soh for Vitos and Sprinters had very low values when using the official range estimations.  
# Their default range has been modified to 170 in the models_info.csv  to get a soh value that is coherent.
mercedes_soh_mask = tss["make"] == "mercedes-benz"
tss.loc[mercedes_soh_mask, "soh"] = (
    tss
    .loc[mercedes_soh_mask]
    .eval("estimated_range / soc / range * 100")
)
tss.loc[mercedes_soh_mask, "soh_method"] = "mercedes-benz"
get_n_scatter_sohs(tss.query("make == 'mercedes-benz'"), "all_mercedes", color="model")
get_n_scatter_sohs(tss.query("make == 'mercedes-benz' & model != 'Vito' & model != 'Sprinter'"), "mercedes_without_vitos_n_sprinters", color="model")


ford_mask = tss.eval("make == 'ford'")
tss.loc[ford_mask, "soh"] = (
    tss
    .loc[ford_mask]
    .query("soc > 0.5")
    .eval("battery_energy / soc / capacity * 100 + 15")
)
get_n_scatter_sohs(tss.query("make == 'ford'"), "ford", trendline_scope="trace", color="model")

get_n_scatter_sohs(tss.query("(make == 'renault' | make == 'mercedes-benz' | make == 'ford') & (model != 'Sprinter' & model != 'Vito')"), "reliable_sohs_without_Vitos_and_Sprinters")
get_n_scatter_sohs(tss, "dummy_and_reliable_sohs")
get_n_scatter_sohs(tss.query("make == 'ford' | make == 'renault' | make == 'mercedes-benz'"), "reliable_sohs")

# #### Evolutoin of soh over a month
# Evolution of soh since last presentation.
def plt_evolution_of_soh(brand:str, top_n_variations_to_remove=5):
    old_tss = tss[tss["date"] <= pd.Timestamp("2024-09-17")]
    new_tss = tss #[tss["date"] > pd.Timestamp("2024-09-17")]
    old_renault_soh = get_sohs_per_vin(old_tss.query(f"make == '{brand}'"))
    new_renault_soh = get_sohs_per_vin(new_tss.query(f"make == '{brand}'"))


    soh_diffs = {"soh_diff":[], "vin": []}

    renault_soh_evolutions: dict[str, list] = {}
    for col in old_renault_soh.columns:
        renault_soh_evolutions[col] = []
        for vin in old_renault_soh["vin"].unique():
            renault_soh_evolutions[col].append(old_renault_soh.set_index("vin", drop=False).loc[vin, col])
            renault_soh_evolutions[col].append(new_renault_soh.set_index("vin", drop=False).loc[vin, col])
            renault_soh_evolutions[col].append(None)

            if col == "soh":
                soh_diffs["soh_diff"].append(new_renault_soh.set_index("vin", drop=False).loc[vin, col] - old_renault_soh.set_index("vin", drop=False).loc[vin, col])
            if col == "vin":
                soh_diffs[col].append(vin)
            
    renault_soh_evolutions:DF = DF(renault_soh_evolutions)

    highest_soh_variations_to_remove = (
        DF(soh_diffs)
        .assign(soh_diff=lambda df: df["soh_diff"].abs())
        .sort_values(by="soh_diff", ascending=False)
        .iloc[:top_n_variations_to_remove]
    )

    vins_to_keep_from_plot_mask = ~renault_soh_evolutions["vin"].isin(highest_soh_variations_to_remove["vin"])
    renault_soh_evolutions = renault_soh_evolutions[vins_to_keep_from_plot_mask]

    MARKER_SIZE = 8
    fig = go.Figure(
        data=[
            go.Scatter(
                x=renault_soh_evolutions["odometer"],
                y=renault_soh_evolutions["soh"],
                mode="markers+lines",
                marker=dict(
                    symbol="arrow",
                    color="royalblue",
                    size=MARKER_SIZE,
                    angleref="previous",
                    standoff=MARKER_SIZE / 2,
                ),
                text=renault_soh_evolutions["vin"],  # Display VIN text on the plot
                textposition="top right",  # Optional: Position of the text relative to markers
                name="SOH Evolution"  # Set custom legend name for arrows
            ),
            go.Scatter(
                x=renault_soh_evolutions["odometer"],
                y=renault_soh_evolutions["soh"],
                mode="markers",
                marker=dict(size=MARKER_SIZE),
                hovertext=renault_soh_evolutions["vin"],  # Adding the hovertext for VIN
                text=renault_soh_evolutions["vin"],  # Display VIN text on the plot
                textposition="top right",  # Optional: Position of the text relative to markers
                name="SOH"  # Set custom legend name for dots
            ),
        ]
    )

    # Set axis labels using update_layout
    fig.update_layout(
        xaxis_title="Mileage (km)",   # Set x-axis label
        yaxis_title="State of Health (%)"  # Set y-axis label
    )

    fig.write_html(f"data_cache/plots/{brand}_soh_evolution_removed_top_{top_n_variations_to_remove}_variations.html")


plt_evolution_of_soh("renault")
plt_evolution_of_soh("renault", top_n_variations_to_remove=0)
plt_evolution_of_soh("mercedes-benz")
plt_evolution_of_soh("mercedes-benz", top_n_variations_to_remove=0)

px.scatter(
    pd.read_csv("data_cache/src/zoes_Charles_study.csv"),
    x="estimated market value",
    y="soh",
).write_html("data_cache/plots/zoes_Charles_study.html")

