from logging import getLogger

from core.pandas_utils import *
from core.stats_utils import estimate_cycles
from core.console_utils import single_dataframe_script_main
from core.logging_utils import set_level_of_loggers_with_prefix
from core.caching_utils import cache_result
from transform.raw_results.config import *
from transform.processed_tss.ProcessedTimeSeries import ProcessedTimeSeries


logger = getLogger("transform.raw_results.ford_results")

@cache_result(RAW_RESULTS_CACHE_KEY_TEMPLATE.format(make="ford"), "s3")
def get_results() -> DF:
    logger.info("Getting results for Ford.")
    tss = ProcessedTimeSeries("ford")
    max_energy = (
        tss
        .groupby(["net_capacity", "soc"], observed=True)
        .agg(max_battery_energy=pd.NamedAgg("battery_energy", lambda x: x.quantile(0.9)))
        .reset_index(drop=False)
    )
    results = (
        tss
        .pipe(
            left_merge,
            max_energy,
            ["net_capacity", "soc"],
            ["net_capacity", "soc"],
            ["max_battery_energy"],
            logger
        )
        .pipe(compute_consuption)
        .eval("soh = battery_energy / max_battery_energy")
    )
    results['cycles'] = results.apply(lambda x: estimate_cycles(x['odometer'], x['range'], x['soh']), axis=1)
    # # logger.debug("Sanity check of the results:")
    # # logger.debug(sanity_check(results))
    return results


def compute_consuption(tss:DF) -> DF:
    tss_filter = tss.dropna(subset=('odometer', 'soc')).copy()
    consumption =  (tss_filter.groupby(['vin', 'trimmed_in_discharge_idx'], observed=True).agg(
        soc_start=("soc", "first"),
        soc_end=('soc', 'last'),
        odometer_start=("odometer", "first"),
        odometer_end=('odometer', 'last'),
        net_capacity=('net_capacity', 'first')
        )
    .eval('soc_diff=soc_start-soc_end')
    .eval('odometer_diff=odometer_end-odometer_start')
    .eval('consumption=soc_diff * net_capacity * 100 / odometer_diff ')
    .query('consumption > 0')
    .query('odometer_diff > 10'))[["consumption"]]
    consumption.reset_index(inplace=True)
    return tss.merge(consumption, on=['vin', 'trimmed_in_discharge_idx'], how='left')

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform.raw_results")
    single_dataframe_script_main(get_results, force_update=True, logger=logger)

