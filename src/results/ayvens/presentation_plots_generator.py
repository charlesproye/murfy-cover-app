from os import system
from logging import getLogger

import plotly.express as px
from plotly.graph_objects import Figure

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

#==========================================Renault soh==========================================
logger.debug('computing soh for renault')
tss = get_processed_tss("renault") #.reset_index(drop=True).pipe(left_merge, fleet_info, left_on="vin", right_on="vin", src_dest_cols=["model", "version", "capacity", "range"])
print(tss.columns)
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

