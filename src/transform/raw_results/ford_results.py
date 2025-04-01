from logging import getLogger

from core.pandas_utils import *
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
        .groupby(["capacity", "soc"])
        .agg(max_battery_energy=pd.NamedAgg("battery_energy", lambda x: x.quantile(0.9)))
        .reset_index(drop=False)
    )
    results = (
        tss
        .pipe(
            left_merge,
            max_energy,
            ["capacity", "soc"],
            ["capacity", "soc"],
            ["max_battery_energy"],
            logger
        )
        .eval("soh = battery_energy / max_battery_energy")
    )
    # logger.debug("Sanity check of the results:")
    # logger.debug(sanity_check(results))
    return results

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform.raw_results")
    single_dataframe_script_main(get_results, force_update=True, logger=logger)

