import pandas as pd
from pandas import DataFrame as DF

from core.console_utils import single_dataframe_script_main
from core.config import *

def get_ev_models_infos() -> DF:
    return  (
        pd.read_csv(CSV_EV_MODELS_INFO_PATH)
        .astype({
            "model": "string",
            "manufacturer": "string",
            "kwh_capacity": "float",
        })
    )

if __name__ == "__main__":
    single_dataframe_script_main(get_ev_models_infos, force_update=True)

models_info = get_ev_models_infos()
