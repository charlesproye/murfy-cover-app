from core.pandas_utils import DF, pd
from core.sql_utils import get_sqlalchemy_engine
from transform.fleet_info.config import COL_DTYPES, COLS_NAME_MAPPING, TABLE_QUERY


def get_fleet_info():
    con = get_sqlalchemy_engine()

    fleet_info: DF = (
        pd.read_sql_query(TABLE_QUERY, con)
        .rename(columns=COLS_NAME_MAPPING)
        .loc[:, COL_DTYPES.keys()]
        .astype(COL_DTYPES)
    )
    return fleet_info
