from logging import getLogger

from scipy.integrate import cumulative_trapezoid
from core.constants import *

from core.pandas_utils import *
from core.caching_utils import CachedETL
from core.logging_utils import set_level_of_loggers_with_prefix
from core.console_utils import main_decorator
from transform.processed_tss.config import *
from transform.raw_tss.main import get_raw_tss
from transform.fleet_info.main import fleet_info


logger = getLogger("transform.processed_tss")

class ProcessedTimeSeries(CachedETL):
    _metadata = ['make', "logger", "id_col", "max_td"]

    def __init__(self, make:str, id_col:str="vin", log_level:str="INFO", max_td:TD=MAX_TD, force_update:bool=False):
        self.make = make
        logger_name = f"transform.processed_tss.{make}"
        self.logger = getLogger(logger_name)
        set_level_of_loggers_with_prefix(log_level, logger_name)
        self.id_col = id_col
        self.max_td = max_td
        super().__init__(S3_PROCESSED_TSS_KEY_FORMAT.format(make=make), "s3", force_update=force_update)

    def run(self) -> DF:
        self.logger.info(f"==================Processing {self.make} raw tss.==================")
        return (
            get_raw_tss(self.make)
            .rename(columns=RENAME_COLS_DICT, errors="ignore")
            .pipe(safe_locate, col_loc=list(COL_DTYPES.keys()), logger=self.logger)
            .pipe(safe_astype, COL_DTYPES, logger=self.logger)
            .pipe(self.metric_normalize)
            .sort_values(by=["vin", "date"])
            .pipe(set_all_str_cols_to_lower, but=["vin"])
            .pipe(self.compute_date_vars)
            .pipe(self.compute_charge_n_discharge_vars)
            .merge(fleet_info, on="vin", how="left")
            .eval("age = date.dt.tz_localize(None) - start_date.dt.tz_localize(None)")
        )

    def compute_charge_n_discharge_vars(self, tss:DF) -> DF:
        return (
            tss
            .pipe(self.compute_charge_n_discharge_masks, IN_CHARGE_CHARGING_STATUS_VALS, IN_DISCHARGE_CHARGING_STATUS_VALS)
            .pipe(self.compute_idx_from_masks, ["in_charge", "in_discharge"])
            .pipe(self.trim_leading_n_trailing_soc_off_masks, ["in_charge", "in_discharge"])
            .pipe(self.compute_idx_from_masks, ["trimmed_in_charge", "trimmed_in_discharge"])
            .pipe(self.compute_cum_var, "power", "cum_energy")
            .pipe(self.compute_cum_var, "charger_power", "cum_charge_energy_added")
        )

    def metric_normalize(self, tss:DF) -> DF:
        tss["odometer"] = tss["odometer"] * ODOMETER_MILES_TO_KM.get(self.make, 1)
        return tss

    def compute_cum_var(self, tss: DF, var_col:str, cum_var_col:str) -> DF:
        if not var_col in tss.columns:
            self.logger.debug(f"{var_col} not found, not computing {cum_var_col}.")
            return tss
        self.logger.debug(f"Computing {cum_var_col} from {var_col}.")
        tss[cum_var_col] = (
            cumulative_trapezoid(
                # Leave the keywords as default order is y x not x y (-_-)
                # Make sure that date time units are in seconds before converting to int
                x=tss["date"].dt.as_unit("s").astype(int),
                y=tss[var_col].fillna(0).values,
                initial=0,
            )            
            .astype("float32")
        )
        tss[cum_var_col] *= KJ_TO_KWH # Convert from kj to kwh
        # Reset value to zero at the start of each vehicle time series
        tss[cum_var_col] -= tss.groupby(self.id_col)[cum_var_col].transform("first")
        return tss

    def compute_date_vars(self, tss:DF) -> DF:
        self.logger.debug(f"Computing sec_date and sec_date_diff.")
        tss["time_diff"] = tss.groupby(self.id_col)["date"].diff()
        tss["sec_time_diff"] = tss["time_diff"].dt.total_seconds()
        return tss

    def compute_charge_n_discharge_masks(self, tss:DF, in_charge_vals:list, in_discharge_vals:list) -> DF:
        self.logger.debug(f"Computing charging and discharging masks.")
        if self.make in CHARGE_MASK_WITH_CHARGING_STATUS_MAKES:
            return self.charge_n_discharging_masks_from_charging_status(tss, in_charge_vals, in_discharge_vals)
        if self.make in CHARGE_MASK_WITH_SOC_DIFFS_MAKES:
            return self.charge_n_discharging_masks_from_soc_diff(tss)
        raise ValueError(MAKE_NOT_SUPPORTED_ERROR.format(make=self.make))

    def charge_n_discharging_masks_from_soc_diff(self, tss:DF) -> DF:
        tss_grp = tss.groupby(self.id_col)
        tss["soc_ffilled"] = tss_grp["soc"].ffill()
        tss["soc_diff"] = tss_grp["soc_ffilled"].diff()
        tss["soc_diff"] /= tss["soc_diff"].abs()
        soc_diff_ffilled = tss_grp["soc_diff"].ffill()
        soc_diff_bfilled = tss_grp["soc_diff"].bfill()
        tss["in_charge"] = soc_diff_ffilled.gt(0, fill_value=False) & soc_diff_bfilled.gt(0, fill_value=False)
        tss["in_discharge"] = soc_diff_ffilled.lt(0, fill_value=False) & soc_diff_bfilled.lt(0, fill_value=False)
        return tss

    def charge_n_discharging_masks_from_charging_status(self, tss:DF, in_charge_vals:list, in_discharge_vals:list) -> DF:
        self.logger.debug(f"Computing charging and discharging vars using charging status dictionary.")
        assert "charging_status" in tss.columns, NO_CHARGING_STATUS_COL_ERROR
        return (
            tss
            .eval(f"in_charge = charging_status in {in_charge_vals}")
            .eval(f"in_discharge = charging_status in {in_discharge_vals}")
        )

    def trim_leading_n_trailing_soc_off_masks(self, tss:DF, masks:list[str]) -> DF:
        self.logger.info(f"Trimming off trailing soc of {masks} masks.")
        tss_grp = tss.groupby(self.id_col)
        for mask in masks:
            trailing_soc = tss_grp[mask].transform("last")
            leading_soc = tss_grp[mask].transform("first")
            tss[f"trimmed_{mask}"] = tss[mask] & (tss["soc"] != trailing_soc) & (tss["soc"] != leading_soc)
        return tss

    def compute_idx_from_masks(self, tss: DF, masks:list[str]) -> DF:
        self.logger.info(f"Computing {masks} idx from masks.")
        for mask in masks:
            idx_col_name = f"{mask}_idx"
            shifted_mask = tss.groupby(self.id_col)[mask].shift(fill_value=False)
            tss["new_period_start_mask"] = shifted_mask.ne(tss[mask]) 
            if self.max_td is not None:
                tss["new_period_start_mask"] |= (tss["time_diff"] > self.max_td)
            tss[idx_col_name] = tss.groupby(self.id_col)["new_period_start_mask"].cumsum().astype("uint16")
            tss.drop(columns=["new_period_start_mask"], inplace=True)
        return tss

    @classmethod
    def update_all_tss(cls, **kwargs):
        for make in ALL_MAKES:
            if make == "tesla":
                cls = TeslaProcessedTimeSeries
            else:
                cls = ProcessedTimeSeries
            print(cls)
            cls(make, force_update=True, **kwargs)

class TeslaProcessedTimeSeries(ProcessedTimeSeries):

    def __init__(self, make:str, id_col:str="vin", log_level:str="INFO", max_td:TD=MAX_TD, force_update:bool=False):
        self.logger = getLogger(make)
        self.logger.info(f"Initializing TeslaProcessedTimeSeries for {make}.")
        super().__init__(make, id_col, log_level, max_td, force_update)
    
    def compute_charge_n_discharge_vars(self, tss:DF) -> DF:
        return (
            tss
            .pipe(self.compute_charge_n_discharge_masks, IN_CHARGE_CHARGING_STATUS_VALS, IN_DISCHARGE_CHARGING_STATUS_VALS)
            .pipe(self.compute_in_charge_idx)
            .pipe(self.compute_idx_from_masks, ["in_discharge"])
            .pipe(self.trim_leading_n_trailing_soc_off_masks, ["in_charge", "in_discharge"])
            .pipe(self.compute_idx_from_masks, ["trimmed_in_charge", "trimmed_in_discharge"])
            .pipe(self.compute_cum_var, "power", "cum_energy")
            .pipe(self.compute_cum_var, "charger_power", "cum_charge_energy_added")
        )
    
    def compute_in_charge_idx(self, tss:DF) -> DF:
        self.logger.info(f"Computing tesla specific in_charge_idx.")
        tss_grp = tss.groupby(self.id_col)
        shifted_vars = tss_grp[["in_charge", "charge_energy_added"]].shift(fill_value=False)
        tss["new_charge_period_mask"] = shifted_vars["in_charge"].ne(tss["in_charge"]) | shifted_vars["charge_energy_added"].lt(tss["charge_energy_added"])
        tss["in_charge_idx"] = tss_grp["new_charge_period_mask"].cumsum().astype("uint16")
        tss = tss.drop(columns=["new_charge_period_mask"])
        monotonically_increasing_charges_value_counts = tss.groupby([self.id_col, "in_charge_idx"])["charge_energy_added"].is_monotonic_increasing.value_counts()
        self.logger.debug(f"All charge periods have monotonically increasing charge energy added:\n{monotonically_increasing_charges_value_counts}")
        return tss

@main_decorator
def main():
    ProcessedTimeSeries.update_all_tss()

if __name__ == "__main__":
    main()

