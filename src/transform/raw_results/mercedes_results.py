from logging import getLogger

import plotly.express as px

from core.pandas_utils import *
from core.stats_utils import estimate_cycles
from core.constants import KWH_TO_KJ
from core.caching_utils import cache_result
from core.console_utils import main_decorator
from core.logging_utils import set_level_of_loggers_with_prefix
from transform.processed_tss.ProcessedTimeSeries import ProcessedTimeSeries
from transform.raw_results.config import *

logger = getLogger("transform.raw_results.mercedes_results")

@main_decorator
def main():
    set_level_of_loggers_with_prefix("DEBUG", "transform.raw_results")
    results = get_results(force_update=True)
    assert not results.empty, "Results dataframe is empty, something went wrong..."
    soh_per_vehicle = (
        results
        .dropna(subset=["odometer", "soh"])
        .eval("date = date.dt.date")
        .groupby(["vin", "date"])
        .agg({
            "soh": "median",
            "odometer": "last",
        })
        .reset_index()
    )
    # Plottting results
    for level in ["level_1", "level_2", "level_3"]:
        px.histogram(
            results.dropna(subset=[level]),
            x=level,
            log_y=True,
            title=f"{level} histogram in log scale"
        ).show()
    px.scatter(
        soh_per_vehicle,
        x="odometer",
        y="soh",
        color="model",
        title="SoH per vehicle over odometer"
    ).show()


@cache_result(RAW_RESULTS_CACHE_KEY_TEMPLATE.format(make="mercedes-benz"), "s3")
def get_results() -> DF:
    logger.info("Processing raw mercedes-benz results.")
    results = (
        ProcessedTimeSeries("mercedes-benz")
        .pipe(fill_vars, cols=["soc", "estimated_range", "range"])
        .assign(discharge_size = lambda df: df.groupby(["vin", "in_discharge_idx"]).transform("size"))
        .groupby('model')
        .apply(apply_soh_model_calculation, include_groups=False)
        .reset_index()
        # Set to NaN the SoH if the SOC is not between 0.7 and 0.98 or the discharge size is less than 10
        .eval("soh = soh.where(soc.between(0.7, 0.98) & discharge_size > 10)")
        .sort_values(["vin", "date"])
        .pipe(hot_fix_in_charge_idx)
        .pipe(compute_charging_power)
        .pipe(charge_levels)
    )
    results['cycles'] = results.apply(lambda x: estimate_cycles(x['odometer'], x['range'], x['soh']), axis=1)
    logger.debug("Sanity check of the results:")
    logger.debug(sanity_check(results))
    return results

def apply_soh_model_calculation(group:DF) -> DF:
    model = group.name
    calculation = MERCEDES_SOH_MODEL_CALCULATIONS.get(model, MERCEDES_SOH_MODEL_CALCULATIONS['default'])
    group['soh'] = group.eval(calculation)
    return group

def charge_levels(tss:DF) -> DF:
    return (
        tss
        .eval("level_1 = soc_diff.where(charging_power < @LEVEL_1_MAX_POWER & soc_diff > 0)")
        .eval("level_2 = soc_diff.where(charging_power.between(@LEVEL_1_MAX_POWER, @LEVEL_2_MAX_POWER) & soc_diff > 0)")
        .eval("level_3 = soc_diff.where(charging_power > @LEVEL_2_MAX_POWER & soc_diff > 0)")
    )

def fill_vars(tss:DF, cols:list[str]) -> DF:
    tss_grouped = tss.groupby("vin")
    for col in cols:
        tss[col] = tss_grouped[col].ffill()
        tss[col] = tss_grouped[col].bfill()
    return tss

def compute_charging_power(tss:DF) -> DF:
    tss_grp = tss.groupby("vin")
    tss = (
        tss
        .assign(
            soc_diff=tss_grp["soc"].diff(),
            sec_time_diff=tss_grp["date"].diff().dt.as_unit("s").astype(int),
        )
        .eval("charging_power = capacity * @KWH_TO_KJ * soc_diff / sec_time_diff")
        .eval("charging_power = charging_power.mask(sec_time_diff > 3600, 0)")
    )
    tss["charging_power"] = tss.groupby(["vin", "in_charge_idx"])["charging_power"].transform("median")
    tss[["charging_power", "soc_diff"]] = tss[["charging_power", "soc_diff"]].where(tss["trimmed_in_charge"])
    return tss

def hot_fix_in_charge_idx(tss:DF) -> DF:
    # There are some issues with the in_charge_idx column, this is a hot fix to get the correct values
    # Eventaully we will either fix ProcessedTimeSeries or create a new MercedesProcessedTimeSeries class to get the correct values
    tss = tss.dropna(subset=["soc", "date"])
    tss["in_new_charge"] = tss.groupby("vin")["in_charge"].shift(1, fill_value=False).ne(tss["in_charge"])
    tss["in_charge_idx"] = tss.groupby("vin")["in_new_charge"].cumsum()
    return tss


if __name__ == "__main__":
    main()

