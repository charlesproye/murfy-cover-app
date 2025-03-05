from logging import getLogger

import plotly.express as px

from core.pandas_utils import *
from core.caching_utils import cache_result
from core.console_utils import main_decorator
from core.logging_utils import set_level_of_loggers_with_prefix
from transform.raw_results.config import *
from transform.processed_tss.ProcessedTimeSeries import TeslaProcessedTimeSeries


logger = getLogger("transform.raw_results.tesla_results")

@main_decorator
def main():
    set_level_of_loggers_with_prefix("DEBUG", "transform.raw_results")
    results = get_results(force_update=True)
    print("Saving tesla results as parquet to data_cache/tesla_results.parquet.")
    results.to_parquet("data_cache/tesla_results.parquet")
    # df = (
    #     df
    #     .dropna(subset=["odometer", "soh"])
    #     .eval("date = date.dt.date")
    #     .groupby(["vin", "date"])
    #     .agg({
    #         "soh": "median",
    #         "odometer": "last",
    #         "tesla_code": "first",
    #     })
    #     .reset_index()
    # )
    if not results.empty:
        fig = px.scatter(results, x="odometer", y="soh", color="tesla_code", opacity=0.2)
        fig.show()

USE_COLS = [
    "vin",
    "trimmed_in_charge_idx",
    "trimmed_in_charge",
    "charge_energy_added",
    "soc",
    "inside_temp",
    "capacity",
    "odometer",
    "model",
    "date",
    "tesla_code",
    "battery_heater",
    "charging_power",
    "version",
]

@cache_result(RAW_RESULTS_CACHE_KEY_TEMPLATE.format(make="tesla"), "s3")
def get_results() -> DF:
    logger.info("Processing raw tesla results.")
    results = (
        TeslaProcessedTimeSeries("tesla", columns=USE_COLS, filters=[("trimmed_in_charge", "==", True)])
        .groupby(["vin", "trimmed_in_charge_idx"], observed=True)
        .agg(
            #energy_added=pd.NamedAgg("charge_energy_added", series_start_end_diff),
            energy_added_min=pd.NamedAgg("charge_energy_added", "min"),
            energy_added_end=pd.NamedAgg("charge_energy_added", "last"),
            soc_diff=pd.NamedAgg("soc", series_start_end_diff),
            inside_temp=pd.NamedAgg("inside_temp", "mean"),
            capacity=pd.NamedAgg("capacity", "first"),
            odometer=pd.NamedAgg("odometer", "first"),
            version=pd.NamedAgg("version", "first"),
            size=pd.NamedAgg("soc", "size"),
            model=pd.NamedAgg("model", "first"),
            date=pd.NamedAgg("date", "first"),
            charging_power=pd.NamedAgg("charging_power", "median"),
            tesla_code=pd.NamedAgg("tesla_code", "first"),
        )
        .reset_index(drop=False)
        .eval("energy_added = energy_added_end - energy_added_min")
        .eval("soh = energy_added / (soc_diff / 100.0 * capacity)")
        .query("soc_diff > 40 & soh.between(0.75, 1.05)")
        .eval("level_1 = soc_diff * (charging_power < @LEVEL_1_MAX_POWER) / 100")
        .eval("level_2 = soc_diff * (charging_power.between(@LEVEL_1_MAX_POWER, @LEVEL_2_MAX_POWER)) / 100")
        .eval("level_3 = soc_diff * (charging_power > @LEVEL_2_MAX_POWER) / 100")
	.eval("bottom_soh = soh.between(0.75, 0.9)")
        .eval("fixed_soh_min_end = soh.mask(tesla_code == 'MTY13', soh / 0.96)")
        .eval("fixed_soh_min_end = fixed_soh_min_end.mask(bottom_soh & tesla_code == 'MTY13', fixed_soh_min_end + 0.08)")
        .eval("soh = fixed_soh_min_end")
        .sort_values(["tesla_code", "vin", "date"])
    )
    logger.debug("Sanity check of the results:")
    logger.debug(sanity_check(results))

    return results

if __name__ == "__main__":
    main()

