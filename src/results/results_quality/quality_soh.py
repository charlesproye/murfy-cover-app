import pandas as pd
import plotly.graph_objects as go
from core.stats_utils import compute_confidence_interval
import numpy as np

def analyze_coef_variation(df, cv_threshold=5):
    """
    Calculates the coefficient of variation of soh per vehicle,
    identifies unstable vehicles, and return statistics.

    Parameters:
    - df : DataFrame containing at least the columns 'vin' and 'soh'
    - cv_threshold : CV percentage above which a vehicle is considered unstable

    Returns:
    - cv_stats : DataFrame with statistics (mean, std, CV%)
    - unstable_vehicles : array of unstable vehicles
    """
    # Compute statistics per VIN
    stats = df.groupby('vin', as_index=False, observed=True)['soh'].agg(['mean', 'std', 'count'])
    stats['cv_percent'] = (stats['std'] / stats['mean']) * 100
    unstable_vehicles = stats[stats['cv_percent'] > cv_threshold]

    print(f"Total number of vehicles: {len(stats)}")
    print(f"Vehicles with CV > {cv_threshold}%: {len(unstable_vehicles)}")
    print(f"Proportion of unstable vehicles: {round(len(unstable_vehicles) / len(stats) * 100)}%")

    return stats, unstable_vehicles.vin.unique()


def check_soh_decresing(df):
    """
    Checks whether the SoH (State of Health) values are monotonically decreasing for each VIN over time.
    Args:
        df (pd.DataFrame): A DataFrame containing at least the following columns: 'vin', 'date', 'soh'

    Returns:
        pd.DataFrame: A summary DataFrame with two columns: count/proportion of VINs' soh monotonically decreasing
    """
    def soh_is_monotonically_decreasing(df):
        return df.set_index("date")["soh"].is_monotonic_decreasing
    montonic_mask_per_vin = (
        df.dropna(subset=['soh'])
        .groupby("vin")
        .apply(soh_is_monotonically_decreasing, include_groups=False)
        .to_frame("monotonically_decreasing")
        .reset_index(drop=False)
    )

    value_counts_per_vin = {
    "normalized": montonic_mask_per_vin["monotonically_decreasing"].value_counts(dropna=False, normalize=True),
    "absolute": montonic_mask_per_vin["monotonically_decreasing"].value_counts(dropna=False, normalize=False)
}
    return pd.concat(value_counts_per_vin, axis=1, keys=value_counts_per_vin.keys(), names=["value_counts_type"])
    
  
def coverage_oem(data_df, soh_df, oems=None):
    """
    Computes SoH data coverage per OEM.
    
    Parameters:
        data_df (pd.DataFrame): DataFrame containing at least 'oem' and 'vin' columns.
        soh_df (pd.DataFrame): DataFrame containing at least 'oem_name', 'vin', and 'soh' columns.
        oems (list, optional): List of OEM names to process. If None, a default list is used.
    
    Returns:
        dict: Dictionary with coverage statistics for each OEM.
    """
    if oems is None:
        oems = [
            "bmw", "kia", "stellantis", "mercedes-benz", "volvo-cars",
            "volkswagen", "toyota", "ford", "renault", "mercedes",
            "volvo", "tesla", 'tesla-fleet-telemetry'
        ]

    coverage = {}

    for oem in oems:
        print(oem)
        coverage[oem] = {}

        # Number of VINs associated with this OEM in data_df
        total_vins = data_df[data_df['oem'] == oem]['vin'].nunique()

        # VINs with non-null SoH data
        vins_with_soh = soh_df[
            (soh_df['oem_name'] == oem) & (soh_df['soh'].notna())
        ]['vin'].unique()

        # Number of VINs with SoH that are also in data_df
        vins_with_soh_count = data_df[data_df['vin'].isin(vins_with_soh)]['vin'].nunique()

        coverage[oem]['total_vins'] = total_vins
        coverage[oem]['vins_with_soh'] = vins_with_soh_count
        coverage[oem]['coverage_percent'] = (
            round(vins_with_soh_count * 100 / total_vins)
            if total_vins > 0 else 0
        )

        print(f"For {oem}, data is collected on {total_vins} VINs.")
        print(f"For {oem}, at least one SoH is available for {vins_with_soh_count} VINs.")
        if total_vins > 0:
            print(f"This represents a coverage of {coverage[oem]['coverage_percent']}% for {oem}.")
        print("\n")

    return coverage


def correlation_soc(df):
    """compute soh correlation with soc 
    Args:
        df (pd.DataFrame): Dataframe with at least the columns: soh, soc

    Returns:
        pd.Series: correlation soh with soc, soc_start, soc_max and soc_diff during charges
    """
    correlation_df = df.groupby("in_charge_idx").agg(
        start_soc = ('soc', 'min'),
        end_soc = ('soc', 'max'),
        soh = ("soh", "median")).eval('diff_soc = end_soc - start_soc')
    corr  = correlation_df.corr()
    selected_column = "soh"
    selected_corr = corr[[selected_column]].sort_values(by=selected_column, ascending=False)
    return selected_corr

def proportion_soh_decreasing_per_bin(df: pd.DataFrame, 
                                      bin_size_km: int = 5000) -> pd.Series:
    """
    For each vehicle, compute the proportion of odometer bins (of size bin_size_km)
    in which the SoH is non-increasing.

    Args:
        df (pd.DataFrame): DataFrame with 'vehicle_id', 'odometer', 'SoH'.
        bin_size_km (int): Size of each odometer bin in kilometers.
    Returns:
        pd.Series: Proportion of non-increasing SoH bins per vehicle.
    """
    required_columns = {'vin', 'odometer', 'soh'}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"DataFrame must contain columns: {required_columns}")
    
    results = {}

    for vehicle_id, group in df.groupby('vin'):
        group = group.sort_values('odometer').reset_index(drop=True)
        group['bin'] = (group['odometer'] // bin_size_km).astype(int)

        decreasing_flags = []

        for _, bin_df in group.groupby('bin'):
            if len(bin_df) < 2:
                continue  # Not enough points to determine a trend

            soh_diff = bin_df['soh'].diff().dropna()
            is_decreasing = (soh_diff < 0).any()
            decreasing_flags.append(is_decreasing)

        if decreasing_flags:
            proportion = sum(decreasing_flags) / len(decreasing_flags)
        else:
            proportion = np.nan  # or 0.0, depending on your use case

        results[vehicle_id] = proportion

    return pd.Series(results, name=f'proportion_non_increasing_per_{bin_size_km//1000}k_km')


def compute_ic_per_vin(df, plot=False):
    ic_df = df.groupby('vin', observed=True)['soh'].apply(compute_confidence_interval).reset_index()
    def plot_distrib_ic():
        hist_values, bin_edges = pd.cut(ic_df[ic_df['number_charges'] > 3]['ic_point_diff'], bins=[0, .02, .05, .1, .2, .3, .4, .5, 1], right=False, retbins=True)
        hist_counts = hist_values.value_counts().sort_index()
        fig = go.Figure(data=[go.Bar(
            x=[f"{round(bin_edges[i], 3)} - {round(bin_edges[i+1], 3)}" for i in range(len(bin_edges)-1)],
            y=hist_counts.values,
            marker=dict(color='blue'),
            text=hist_counts
        )])
        fig.update_layout(
            title="répartitions des tailles d'IC",
            xaxis_title="Intervale",
            yaxis_title="Frecuence"
        )
        return fig

    if plot is True:
        fig = plot_distrib_ic()
        fig.show()
    return ic_df
    
