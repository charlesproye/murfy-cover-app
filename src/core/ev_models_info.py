import pandas as pd
from pandas import DataFrame as DF

from core.sql_utils import con
from core.pandas_utils import set_all_str_cols_to_lower, safe_locate
from core.console_utils import single_dataframe_script_main
from core.config import *

def get_ev_models_infos() -> DF:
    return (
        pd.read_sql_query(MODEL_INF_SQL_QUERY, con)
        .rename(columns=MODEL_INFO_NAME_MAP)
        .pipe(safe_locate, col_loc=MODEL_INFO_DTYPES.keys())
        .astype(MODEL_INFO_DTYPES)
        .pipe(set_all_str_cols_to_lower, but=["tesla_code"])
        .sort_values(by=["manufacturer", "model", "version", "tesla_code"])
    )

if __name__ == "__main__":
    single_dataframe_script_main(get_ev_models_infos)
    
models_info = get_ev_models_infos()
