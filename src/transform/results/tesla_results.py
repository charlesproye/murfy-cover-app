from logging import getLogger

from core.pandas_utils import *
from core.console_utils import single_dataframe_script_main
from core.logging_utils import set_level_of_loggers_with_prefix
from transform.results.config import *
from transform.processed_tss.main import get_processed_tss

logger = getLogger("transform.results.tesla_results")

def get_results() -> DF:
    return (
        get_processed_tss("tesla")
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
            date=pd.NamedAgg("date", "first"),
        )
        .reset_index(drop=False)
        .eval("soh = energy_added / (soc_diff / 100 * capacity)")
        .eval("model_version = model + version")
        .query("soc_diff > 40")
    )

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform.results")
    single_dataframe_script_main(get_results, logger=logger)

