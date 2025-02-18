from rich import print

from core.pandas_utils import *
from core.sql_utils import *
from transform.fleet_info.config import *

fleet_info = (
    pd.read_sql_query(TABLE_QUERY, con)
    .rename(columns=COLS_NAME_MAPPING)
    .loc[:, COL_DTYPES.keys()]
    .astype(COL_DTYPES)
)

if __name__ == "__main__":
    print(fleet_info)
    print(fleet_info.columns)
    print(sanity_check(fleet_info))
