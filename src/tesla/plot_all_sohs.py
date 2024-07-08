import dotenv

from pandas import Series
from pandas import DataFrame as DF
import matplotlib.pyplot as plt
from matplotlib.axes import Axes

from tesla_constants import *
from processed_tesla_ts import iterate_overs_processed_ts, processed_time_series_of
from core.plt_utils import fill_axs_by_sohs, plt_single_vehicle_sohs
from tesla_perfs import compute_all_perfs
from tesla_fleet_info import fleet_info_df

def main():
    dotenv.load_dotenv()
    plot_all_perfs()
    # only plot PA's parents
    pa_parents_vin = os.getenv("PA_PARENTS_VIN")
    pa_parents_vehicle_df = processed_time_series_of(pa_parents_vin)
    perfs_dict = compute_all_perfs(pa_parents_vehicle_df, pa_parents_vin)
    plt_single_vehicle_sohs(pa_parents_vehicle_df, perfs_dict, plt_variance=True)
    plt_single_vehicle_sohs(pa_parents_vehicle_df, perfs_dict, x_col="date", y_col_periods="mean_date", plt_variance=True)
    histogram_variance_per_period()
    histogram_variance_per_vehicle()

    
def plot_all_perfs(y_col:str="odometer", y_col_periods="mean_odo"):
    axs: list[Axes]
    fig, axs = plt.subplots(4, sharex=True)
    for vin, vehicle_df in iterate_overs_processed_ts(query_str="model=='Model 3 Rear-Wheel Drive'", use_progress_track=False):
        perfs_dict = compute_all_perfs(vehicle_df, vin)
        fill_axs_by_sohs(vehicle_df, perfs_dict, axs, y_col, y_col_periods, time_series_alpha=0.3, perf_alpha=0.7)
    
    fig.suptitle(f"all sohs based on {y_col}")
    plt.show()

def histogram_variance_per_vehicle():
    vars_df = DF({
            "range_soh_var": [mean_normed_var(vehicle_df["range_soh"]) for vin, vehicle_df in iterate_overs_processed_ts()],
            "last_charge_soh_var": [mean_normed_var(vehicle_df["last_charge_soh"]) for vin, vehicle_df in iterate_overs_processed_ts()],
        },
        index=fleet_info_df['vin']
    )
    my_histogram_of_df(vars_df, "number of vehicles per variance range")


def histogram_variance_per_period():
    my_histogram_of_df(DF(rolling_variance_of_fleet(["range_soh", "last_charge_soh"])), "number of 12h periods per variance range")

def rolling_variance_of_fleet(cols:list[str]) -> list[float]:
    data_dict: dict[str, list[float]] = {}
    for vin, vehicle_df in iterate_overs_processed_ts():
        for col in cols:
            new_lst = vehicle_df[col].sub(vehicle_df[col].mean()).rolling("12h", center=True).var().to_list()
            data_dict[f"{col}_var"] = [*data_dict.get(f"{col}_var", []), *new_lst]
    return data_dict

def my_histogram_of_df(df:DF, title:str, **kwargs):    
    axs = df.hist(grid=False, edgecolor="violet", **kwargs)
    ax: Axes
    for (col_name, vars), ax in zip(df.items(), axs.flatten()):
        y_max = ax.get_ylim()[1]
        ax.vlines(x=vars.median(), ymin=0, ymax=y_max, linestyles="--", color="red", label=f"median: {vars.median():.2f}")
        ax.vlines(x=vars.mean(), ymin=0, ymax=y_max, linestyles="--", color="yellow", label=f"mean: {vars.mean():.2f}")
        ax.set_ylim(bottom=0, top=y_max)
        ax.legend()
        for container in ax.containers:
            # Extract the patches (bars)
            left_edges = [patch.get_x() for patch in container.patches]
            right_edges = [patch.get_x() + patch.get_width() for patch in container.patches]
            # Combine left and right edges, ensuring unique values and sorting them
            all_edges = sorted(set(left_edges + right_edges))
            # Set the x-ticks to the edges of the bars
            ax.set_xticks(all_edges)

    axs[0, 0].figure.suptitle(title)
    plt.show()


def mean_normed_var(series: Series) -> float:
    return series.sub(series.mean()).var()

def plt_soh_processing(vehicle_df: DF, vin:str):
    fig, axs = plt.subplots(nrows=4, sharex=True)
    # ax 0
    energy_per_range_km_ax = axs[0].twinx()
    vehicle_df["charge_energy_added"].rolling(EWM_SPAN_ENERGY_TO_RANGE_RATIO, center=True).mean().plot.line(ax=axs[0], legend="charge_energy_added_cleaned", color='red', linestyle='--', marker="x")
    vehicle_df["charge_km_added"].rolling(EWM_SPAN_ENERGY_TO_RANGE_RATIO, center=True).mean().plot.line(ax=energy_per_range_km_ax, legend="charge_km_added_cleaned", color='blue', linestyle='--', marker="x")
    vehicle_df["charge_energy_added"].plot.line(ax=axs[0], legend="charge_energy_added", marker=".", color='red', alpha=0.4)
    vehicle_df["charge_km_added"].plot.line(ax=energy_per_range_km_ax, legend="charge_km_added", color='blue', alpha=0.4)
    # ax 1
    vehicle_df["energy_per_range_km"].plot.line(ax=axs[1], legend="energy_per_range_km", marker=".", color='violet')
    vehicle_df.eval("charge_energy_added / charge_km_added").rolling(EWM_SPAN_ENERGY_TO_RANGE_RATIO, center=True).mean().plot.line(ax=axs[1], legend="energy_per_range_km", marker="x", color='red', linestyle="--")
    # (vehicle_df["charge_energy_added"].ewm(window_size).mean() / vehicle_df["charge_km_added"].ewm(window_size).mean()).plot.line(ax=axs[1], legend="energy_per_range_km", marker="x", color='green', linestyle="--")
    # ax 2
    interpolated_soc_ax = axs[2].twinx()
    vehicle_df["energy"].plot.line(ax=axs[2], legend="energy", marker=".", alpha=0.5)
    vehicle_df.eval("battery_range_km * energy_per_range_km").plot.line(ax=axs[2], legend="energy", marker="x", linestyle="--", color="blue")
    # interpolated_soc.plot.line(ax=interpolated_soc_ax, legend="interpolated soc", marker=".", color="red")
    vehicle_df["soc"].plot.line(ax=interpolated_soc_ax, legend="naned soc", marker=".", color="green")
    vehicle_df["energy"].plot.line(ax=axs[2], legend="energy", marker=".", alpha=0.5)
    axs[2].legend()
    # ax 3
    (100 * (vehicle_df["energy"] / vehicle_df["soc"]) / fleet_info_df.at[vin, "default_kwh_per_soc"]).plot.line(ax=axs[3], legend="raw_last_charge_soh", linestyle="--", color="red")
    vehicle_df["last_charge_soh"].plot.line(ax=axs[3], legend="last_charge_soh", marker=".")
    axs[3].legend()

    fig.suptitle(vin)
    fig.legend()
    plt.show()

if __name__ == "__main__":
    main()
