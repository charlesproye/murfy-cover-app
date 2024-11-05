# %% [markdown]
# # Watea presentation
# In this notebook we will present the results of our EDA.

# %% [markdown]
# ## Setup

# %% [markdown]
# ### Imports

# %%
from IPython import get_ipython
from os import system

import plotly.express as px
import plotly.graph_objects as go
from scipy.stats import linregress
from pandas import Series
from pandas import DataFrame as DF
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures, FunctionTransformer
from sklearn.pipeline import Pipeline


from core.caching_utils import cache_result
from core.pandas_utils import floor_to
from core.plt_utils import plt_3d_df
from transform.watea.soh_estimation import get_processed_cluster, get_soh_per_charges
from transform.watea.watea_processed_tss import get_processed_tss
from transform.watea.watea_fleet_info import fleet_info
from transform.watea.watea_config import POLYNOMIAL_LINEAR_REGRESSION_PIPELINE
from transform.watea.soh_estimation import get_preprocessed_charging_points

# Monkey patch plotly Figure.show to only show in notebooks
def notebook_only_show(self):
    try:
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            print("Showing figure")
            self.show()
        else:
            return self
    except NameError:
        return self
go.Figure.notebook_only_show = notebook_only_show


# %%
system("mkdir -p data_cache")
system("mkdir -p data_cache/tables")
system("mkdir -p data_cache/plots")

# %% [markdown]
# ### Data extraction

# %%
processed_cluster = get_processed_cluster()
charges = get_soh_per_charges()

# %%
soh_per_vehicle = charges.groupby('id').agg(
    soh=pd.NamedAgg(column="soh", aggfunc="mean"),
    soh_min=pd.NamedAgg(column="soh", aggfunc="min"),
    soh_max=pd.NamedAgg(column="soh", aggfunc="max"),
    odometer=pd.NamedAgg(column="odometer", aggfunc="max")
).reset_index(drop=False)

# %%
@cache_result("data_cache/{id}.parquet", on="local_storage", path_params=["id"])
def get_ts(id:str ) -> DF:
    return get_processed_tss().query(f"id == '{id}'")

# %%
def get_longest_of(fleet_info:DF) -> DF:
    tss = get_processed_tss()
    id_mask = tss["id"].isin(fleet_info["id"])
    tss = tss[id_mask]
    longest_id = tss["id"].value_counts(sort=True, ascending=False).index[0]
    return tss.query(f"id == '{longest_id}'")

# %% [markdown]
# ## Time series raw data summary
# We will show how the data looks like during discharge and charge.  
# We will also show that some vehicles have power during charge or discharge or both or none.

# %%
def plt_longest_ts(fleet_info:DF, title:str):
    ts = get_longest_of(fleet_info)
    fig = go.Figure()

    fig = (
        px.line(
        ts,
        x="date",
        y="soc",
        title="State of Charge",
        labels={"soc": "State of Charge", "date": "Date"}
        )
        .add_trace(
            go.Scatter(
                x=ts["date"], 
                y=ts["power"],
                name="Power",
                yaxis="y2"
            )
        )
        .update_layout(
            yaxis=dict(title="State of Charge (%)"),
            yaxis2=dict(title="Power (W)", overlaying='y', side='right'),
            title=title,
            xaxis_title="Date"
        )
        .notebook_only_show()
        .write_html(f"data_cache/plots/{title}.html")
    )


# %%
plt_longest_ts(fleet_info.query("has_power_in_charge"), "Longest time series with power during charge")

# %% [markdown]
# ## What vehicles has power during each period(charge/discharge)

# %%
def print_and_save_ids(fleet_info_query:str, title:str):
    ids = fleet_info.query(fleet_info_query).reset_index()[["id", "max_odo", "min_odo", "travelled_distance"]]
    print(title)
    print(ids)
    ids.to_csv(f"data_cache/tables/{title}.csv".replace(" ", "_"), index=False)
print_and_save_ids("has_power_in_charge", "Has power during charge")
print_and_save_ids("has_power_in_discharge", "Has power during discharge")
print_and_save_ids("has_power_in_charge & has_power_in_discharge", "Has power during charge and discharge")
print_and_save_ids("~has_power_in_charge & ~has_power_in_discharge", "Has no power during charge and discharge")
print_and_save_ids("has_temperature_in_charge", "Has temperature during charge")

id_with_max_dist = (
    fleet_info
    .sort_values("travelled_distance", ascending=False)
    .iloc[0]
)
id_with_max_dist.to_csv("data_cache/tables/max_travelled_distance_vehicle_info.csv", index=True)
# %% [markdown]
# ## soh results

# %%
fig = px.scatter(
    charges.assign(soh=charges["soh"]),
    x="odometer",
    y="soh",
    color="id",
    trendline="ols",
    trendline_scope="overall",
)
fig.write_html("data_cache/plots/sohs_per_charge.html")
fig.notebook_only_show()

# %%
fig = px.scatter(
    soh_per_vehicle.assign(soh=soh_per_vehicle["soh"]),
    x="odometer",
    y="soh",
    trendline="ols",
    trendline_scope="overall",
)
fig.write_html("data_cache/plots/sohs_per_vehicle.html")
fig.notebook_only_show()

# %% [markdown]
# ## Energy consumption over soc and temeperature

# %%
processed_cluster["floored_temperature"] = floor_to(processed_cluster["temperature"], 5)
dist_to_plot = (
    processed_cluster
    .query("temperature < 30 & temperature > 0")
    .groupby(["soc", "floored_temperature"])[["energy_added"]]
    .median()
    .reset_index()
    .sort_values(by=["floored_temperature", "soc"], ascending=[False, True])
)
fig = px.line(
    dist_to_plot,
    x="soc",
    y="energy_added",
    color="floored_temperature",
    color_discrete_sequence=px.colors.sequential.Rainbow,
)
fig.write_html("data_cache/plots/energy_consumption_per_soc_and_temperature.html")
fig.notebook_only_show()

# %%
fig = px.box(
    dist_to_plot,
    "floored_temperature",
    "energy_added",
    color="floored_temperature",
    color_discrete_sequence=px.colors.sequential.Rainbow,
)
fig.write_html("data_cache/plots/energy_consumption_per_soc_and_temperature_boxplot.html")
fig.notebook_only_show()

# %%
POLYNOMIAL_LINEAR_REGRESSION_PIPELINE = Pipeline([
    ('reshape', FunctionTransformer(lambda x: x.reshape(-1, 1))),
    ('poly_features', PolynomialFeatures(degree=10)),
    ('regressor', LinearRegression())
])

energy_by_soc_per_temp = pd.pivot_table(dist_to_plot, columns=["floored_temperature"], values="energy_added", index="soc")
mean_energy_added = energy_by_soc_per_temp.median(axis=1)
energy_by_soc_per_temp = energy_by_soc_per_temp.apply(lambda col: col - mean_energy_added)
energy_by_soc_per_temp = energy_by_soc_per_temp.unstack()
energy_by_soc_per_temp = energy_by_soc_per_temp.reset_index()
energy_by_soc_per_temp

# %%
fig = px.line(
    energy_by_soc_per_temp,
    x="soc",
    y=0,
    color="floored_temperature",
    color_discrete_sequence=px.colors.sequential.Rainbow,
)
fig.notebook_only_show()

# %%
fig = px.box(
    energy_by_soc_per_temp,
    x="floored_temperature",
    y=0,
    color="floored_temperature",
    color_discrete_sequence=px.colors.sequential.Rainbow,
)
fig.write_html("data_cache/plots/box_plot_energy_added_diff.html")
fig.notebook_only_show()

# %%
ratios  = (
    pd.pivot_table(dist_to_plot, columns=["floored_temperature"], values="energy_added", index="soc")
    .agg(["sum", "count"])
    .T
    .eval("ratio = sum / count")
    .eval("abs_ratio = ratio / ratio.min()")
)
ratios.to_csv("data_cache/energy_added_ratios.csv")
ratios


# %%
fig = plt_3d_df(get_preprocessed_charging_points(), "soc", "power", "energy_added", "temperature")
fig.write_html("data_cache/plots/3d_plot_preprocessed_charging_points.html")
fig.notebook_only_show()

# %%

