from logging import getLogger

from core.pandas_utils import *
from core.logging_utils import set_level_of_loggers_with_prefix
from core.sql_utils import *
from core.ev_models_info import models_info
from core.console_utils import single_dataframe_script_main
from transform.fleet_info.ayvens_fleet_info import fleet_info as ayvens_fleet_info
from transform.fleet_info.tesla_fleet_info import test_tesla_fleet_info, followed_tesla_vehicles_info
from transform.fleet_info.config import *

logger = getLogger("transform.fleet_info")

def get_fleet_info() -> DF:
    return (
        pd.concat((ayvens_fleet_info, test_tesla_fleet_info))
        .pipe(set_all_str_cols_to_lower, but=["vin", "fleet"])
        .pipe(left_merge, followed_tesla_vehicles_info, left_on=["vin"], right_on=["vin"], src_dest_cols=["model", "version"])
        # Hot fix to pervent the name of the versino to break the parsing of models_info.csv
        .assign(version=lambda df: df["version"].mask(df["version"] == "gt première ev 50kwh 136 7,4kw 5d", "gt première ev 50kwh 136 7;4kw 5d"))
        .pipe(left_merge, models_info, left_on=["model", "version"], right_on=["model", "version"])
    )

if __name__ == "__main__":
    set_level_of_loggers_with_prefix("DEBUG", "transform.fleet_info")
    single_dataframe_script_main(get_fleet_info)

fleet_info = get_fleet_info()
