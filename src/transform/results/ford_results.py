from logging import getLogger

from core.pandas_utils import *
from core.console_utils import single_dataframe_script_main
from core.logging_utils import set_level_of_loggers_with_prefix
from transform.results.config import *
from transform.processed_tss.main import get_processed_tss


logger = getLogger("transform.results.tesla_results")

def get_results() -> DF:
    logger.info("Getting results for Ford.")
    tss = get_processed_tss("ford", force_update=True)
    max_energy = (
        tss
        .groupby(["capacity", "soc"])
        .agg(max_battery_energy=pd.NamedAgg("battery_energy", lambda x: x.quantile(0.9)))
        .reset_index(drop=False)
    )
    return (
        tss
        .pipe(
            left_merge,
            max_energy,
            ["capacity", "soc"],
            ["capacity", "soc"],
            ["max_battery_energy"],
            logger
        )
        .eval("soh = battery_energy / max_battery_energy * 100")
    )

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform.results")
    single_dataframe_script_main(get_results, logger=logger)
