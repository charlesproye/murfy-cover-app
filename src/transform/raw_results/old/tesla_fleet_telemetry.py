from logging import getLogger

import numpy as np

from core.stats_utils import estimate_cycles
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

@cache_result(RAW_RESULTS_CACHE_KEY_TEMPLATE.format(make="tesla-fleet-telemetry"), "s3")
def get_results() -> DF:
    logger.info("Processing raw tesla fleet telemetry results.")
    results = (TeslaProcessedTimeSeries('tesla-fleet-telemetry',)
               .groupby(["vin", "in_charge_idx"], observed=True, as_index=False).agg(
            ac_energy_added_min=pd.NamedAgg("ac_charge_energy_added", "min"),
            dc_energy_added_min=pd.NamedAgg("dc_charge_energy_added", "min"),
            ac_energy_added_end=pd.NamedAgg("ac_charge_energy_added", "last"),
            dc_energy_added_end=pd.NamedAgg("dc_charge_energy_added", "last"),
            soc_diff=pd.NamedAgg("soc", series_start_end_diff),
            inside_temp=pd.NamedAgg("inside_temp", "mean"),
            net_capacity=pd.NamedAgg("net_capacity", "first"),
            range=pd.NamedAgg("range", "first"),
            odometer=pd.NamedAgg("odometer", "first"),
            version=pd.NamedAgg("version", "first"),
            size=pd.NamedAgg("soc", "size"),
            model=pd.NamedAgg("model", "first"),
            date=pd.NamedAgg("date", "first"),
            ac_charging_power=pd.NamedAgg("ac_charging_power", "median"),
            dc_charging_power=pd.NamedAgg("dc_charging_power", "median"),
            tesla_code=pd.NamedAgg("tesla_code", "first"),
        )
        .assign(charging_power = lambda df: df['ac_charging_power'].replace([np.nan, -np.nan], 0) + df['dc_charging_power'].replace([np.nan, -np.nan], 0))
        .eval("ac_energy_added = ac_energy_added_end  - ac_energy_added_min")
        .eval("dc_energy_added = dc_energy_added_end  - dc_energy_added_min")
        #.assign(energy_added=lambda df: np.maximum(df["ac_energy_added"].replace([np.nan, -np.nan], 0), df["dc_energy_added"].replace([np.nan, -np.nan], 0)))
        .assign(energy_added=lambda df: df["dc_energy_added"])
        .eval("soh = energy_added / (soc_diff / 100.0 * net_capacity)")
        .eval("level_1 = soc_diff * (charging_power < 8) / 100")
        .eval("level_2 = soc_diff * (charging_power.between(8, 45)) / 100")
        .eval("level_3 = soc_diff * (charging_power > 45) / 100")
        .sort_values(["tesla_code", "vin", "date"])
    )
    results['cycles'] = results.apply(lambda x: estimate_cycles(x['odometer'], x['range'], x['soh']), axis=1)
    return results

if __name__ == "__main__":
    main()

