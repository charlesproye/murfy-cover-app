from core.sql_utils import connection
from core.pandas_utils import *
from core.console_utils import single_dataframe_script_main
from transform.fleet_info.ayvens_fleet_info import fleet_info


def get_fleet_info() -> DF:
    # This will eventually turn into a pd.concat call once we have multiple fleet info sources.
    return fleet_info

def update_fleet_info() -> DF:
    fleet_info = get_fleet_info()
    fleet_info.to_sql("vehicle", connection, if_exists="append")
    return fleet_info


if __name__ == "__main__":
    #single_dataframe_script_main(update_fleet_info)
    single_dataframe_script_main(get_fleet_info)

fleet_info = get_fleet_info()

