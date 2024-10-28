import pandas as pd
from pandas import DataFrame as DF

from core.console_utils import single_dataframe_script_main
from core.config import *

def get_ev_models_infos() -> DF:
    ev_models_info = (
        pd.read_csv(CSV_EV_MODELS_INFO_PATH)
        .astype({
            "model": "string",
            "make": "string",
            "version": "string",
            "kwh_capacity": "float",
        })
    )
    ev_models_info["model"] = ev_models_info["model"].str.lower()
    ev_models_info["version"] = ev_models_info["version"].str.lower()
    ev_models_info["make"] = ev_models_info["make"].str.lower()

    return ev_models_info


if __name__ == "__main__":
    single_dataframe_script_main(get_ev_models_infos, force_update=True)

models_info = get_ev_models_infos()
