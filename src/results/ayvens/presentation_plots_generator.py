from os import system
from logging import getLogger

import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.graph_objects import Figure
from rich.progress import track

from core.pandas_utils import *
from core.logging_utils import set_level_of_loggers_with_prefix
from core.config import *
from transform.fleet_info.main import fleet_info
from transform.processed_tss.main import get_processed_tss, GET_PROCESSED_TSS_FUNCTIONS


logger = getLogger("results.ayvens.presentation_plots_generator")
set_level_of_loggers_with_prefix("DEBUG", "results.ayvens.presentation_plots_generator")

system("mkdir -p data_cache")
system("mkdir -p data_cache/plots")
system("mkdir -p data_cache/tables")

logger.info("Getting all processed tss.")

logger.info("Plotting odometer distribution.")

# def get_odometers_per_vin(brand:str) -> DF:
#     print(brand)
#     try:
#         tss = get_processed_tss(brand)
#         print("received tss")
#         tss = tss.reset_index(drop=True)
#         return tss.groupby("vin")["odometer"].max()
#     except Exception as e:
#         logger.error(f"Error getting odometers for {brand}: {e}")
#         return DF()

# odo_dicts = {brand: get_odometers_per_vin(brand) for brand in track(GET_PROCESSED_TSS_FUNCTIONS.keys())}
# odometers = pd.concat(odo_dicts, keys=odo_dicts.keys(), names=["make"])

# odometers.to_csv("data_cache/tables/odometers.csv")

# px.histogram(
#     odometers,
#     x="odometer",
#     nbins=15,
#     color="make",
#     facet_col="make",
#     title="Distribution of vehicles over odometer"
# ).write_html("data_cache/plots/odometer_distribution.html")

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
            "capacity": "first",
        })
        .reset_index(drop=False)
        .sort_values(by=["vin", "odometer"])
    )

def plt_sohs(sohs:DF, plot_name:str, x:str, trendline_scope:str="overall", color="vin") -> Figure:
    logger.debug(f"Plotting soh over for {plot_name}.")
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
        .write_html(f"data_cache/plots/{plot_name}_soh_over_{x}.html")
    )

def get_n_scatter_sohs(tss:DF, plot_name:str="", **kwargs):
    logger.debug(f"Getting sohs for {plot_name}.")
    sohs = get_sohs_per_vin(tss)
    plt_sohs(sohs, plot_name, "age_in_years", **kwargs)
    plt_sohs(sohs, plot_name, "odometer", **kwargs)

    sohs.dropna(subset="soh").to_csv(f"{dirname(__file__)}/data_cache/tables/{plot_name}.csv", float_format="%.0f")

    return sohs

#==========================================Renault soh==========================================
logger.debug('computing soh for renault')
tss = get_processed_tss("renault").reset_index(drop=True).pipe(left_merge, fleet_info, left_on="vin", right_on="vin", src_dest_cols=["model", "version", "capacity", "range"])
tss = tss.eval("expected_battery_energy = capacity * soc")
tss = tss.eval("soh = 100 * battery_energy / expected_battery_energy")
sohs = (
    tss
    .groupby("vin")
    .agg(
        soh=pd.NamedAgg("soh", "mean"),
        odometer=pd.NamedAgg("odometer", "last"),
        model=pd.NamedAgg("model", "first"),
        version=pd.NamedAgg("version", "first"),
    )
    .reset_index(drop=False)
)
plt_sohs(sohs, "renault", "odometer")

logger.debug("Done.")

#==========================================Tesla================================================
from transform.processed_tss.tesla_processed_tss import get_processed_tss as tesla_get_processed_tss
tss = tesla_get_processed_tss()
print(tss["vin"].nunique())

energy_added_in_charge = (
    tss
    .query("in_charge_perf_mask")
    .groupby(["vin", "in_charge_perf_idx"])
    .agg(
        energy_added=pd.NamedAgg("charge_energy_added", series_start_end_diff),
        soc_diff=pd.NamedAgg("soc", series_start_end_diff),
        soc_start=pd.NamedAgg("soc", "first"),
        soc_end=pd.NamedAgg("soc", "last"),
        temp=pd.NamedAgg("inside_temp", "mean"),
        capacity=pd.NamedAgg("capacity", "first"),
        odometer=pd.NamedAgg("odometer", "first"),
        fast_charger_type=pd.NamedAgg("fast_charger_type", Series.mode),
        size=pd.NamedAgg("soc", "size"),
        model=pd.NamedAgg("model", "first"),
        version=pd.NamedAgg("version", "first"),
    )
    .reset_index(drop=False)
    .eval("soh = energy_added / (soc_diff / 100 * capacity)")
    .eval("model_version = model + version")
)
soh_energy_added_per_vehicle = (
    energy_added_in_charge
    .groupby("vin")
    .agg(
        soh=pd.NamedAgg("soh", "mean"),
        odometer=pd.NamedAgg("odometer", "last"),
        model=pd.NamedAgg("model", "first"),
        version=pd.NamedAgg("version", "first"),
    )
    .reset_index(drop=False)
    .eval("model_version = model + version")
    .query("soh < 1.15 & soh > 0.8")
    .eval("soh = soh.clip(lower=0.8, upper=1) * 100")
)
print(soh_energy_added_per_vehicle["vin"].count())
plt_sohs(soh_energy_added_per_vehicle, "tesla", "odometer")
plt_sohs(soh_energy_added_per_vehicle, "tesla", "odometer", color="model")

# ==========================================Mercedes============================================
# # Note: The soh for Vitos and Sprinters had very low values when using the official range estimations.  
# # Their default range has been modified to 170 in the models_info.csv  to get a soh value that is coherent.
# all_tss['mercdes-benz']
# all_tss['mercdes-benz'].loc[:, "soh_method"] = "mercedes-benz"
# get_n_scatter_sohs(tss.query("make == 'mercedes-benz'"), "all_mercedes", color="model")
# get_n_scatter_sohs(tss.query("make == 'mercedes-benz' & model != 'Vito' & model != 'Sprinter'"), "mercedes_without_vitos_n_sprinters", color="model")

# ford_mask = tss.eval("make == 'ford'")
# tss.loc[ford_mask, "soh"] = (
#     tss
#     .loc[ford_mask]
#     .query("soc > 0.5")
#     .eval("battery_energy / soc / capacity * 100")
# )
# tss.loc[tss.eval("model == 'Mustang Mach-E'"), "soh"] += 10
# ford_sohs = get_n_scatter_sohs(tss.query("make == 'ford'"), "ford", trendline_scope="overall", color="model")

# get_n_scatter_sohs(tss.query("(make == 'renault' | make == 'mercedes-benz' | make == 'ford') & (model != 'Sprinter' & model != 'Vito')"), "reliable_sohs_without_Vitos_and_Sprinters")
# get_n_scatter_sohs(tss, "dummy_and_reliable_sohs")
# get_n_scatter_sohs(tss.query("make == 'ford' | make == 'renault' | make == 'mercedes-benz'"), "reliable_sohs")

# #### Evolutoin of soh over a month
# Evolution of soh since last presentation.
def plt_evolution_of_soh(tss:DF, prev_date:pd.Timestamp, top_n_variations_to_remove=5):
    old_tss = tss[tss["date"] <= prev_date] #pd.Timestamp("2024-09-17")
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

