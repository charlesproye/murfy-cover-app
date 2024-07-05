from os import getenv
import dotenv

from core.plt_utils import plt_time_series_plotly
from core.argparse_utils import parse_kwargs
from processed_tesla_ts import processed_time_series_of

COLS_TO_PLOT = [
    "date",
    "power",
    "speed",
    "battery_range",
    "charge_miles_added_ideal",
    "charger_power",
    "charging_state",
    "odometer",
    "inside_temp",
    "outside_temp",
    "soc",
    "battery_range_km",
    "charge_km_added",
    "range_soh",
    "energy",
    "last_charge_soh",
]

def main():
    dotenv.load_dotenv()
    kwargs = parse_kwargs()
    vin = kwargs.get("vin", getenv("PA_PARENTS_VIN"))
    vehicle_df = processed_time_series_of(vin)
    plt_time_series_plotly(vehicle_df, COLS_TO_PLOT, save_to="parent_pa_time_sereis.html")

if __name__ == "__main__":
    main()
