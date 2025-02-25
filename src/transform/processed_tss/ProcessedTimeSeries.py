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

# Here we have implemented the ETL as a class as most raw time series go through the same processing step.
# To have a processing step specific to a data provider/manufacturer, simply implement a subclass of ProcessedTimeSeries and update update_all_tss.
class ProcessedTimeSeries(CachedETL):
    _metadata = ['make', "logger", "id_col", "max_td"]

    def __init__(self, make:str, id_col:str="vin", log_level:str="INFO", max_td:TD=MAX_TD, force_update:bool=False, **kwargs):
        self.make = make
        logger_name = f"transform.processed_tss.{make}"
        self.logger = getLogger(logger_name)
        set_level_of_loggers_with_prefix(log_level, logger_name)
        self.id_col = id_col
        self.max_td = max_td
        super().__init__(S3_PROCESSED_TSS_KEY_FORMAT.format(make=make), "s3", force_update=force_update, **kwargs)

    # No need to call run it will be called in CachedETL init.
    def run(self) -> DF:
        self.logger.info(f"==================Processing {self.make} raw tss.==================")
        return (
            get_raw_tss(self.make)
            .rename(columns=RENAME_COLS_DICT, errors="ignore")
            .pipe(safe_locate, col_loc=list(COL_DTYPES.keys()), logger=self.logger)
            .pipe(safe_astype, COL_DTYPES, logger=self.logger)
            .pipe(self.normalize_units_to_metric)
            .sort_values(by=["vin", "date"])
            .pipe(str_lower_columns, COLS_TO_STR_LOWER)
            .pipe(self.compute_date_vars)
            .pipe(self.compute_charge_n_discharge_vars)
            .merge(fleet_info, on="vin", how="left")
            .eval("age = date.dt.tz_localize(None) - start_date.dt.tz_localize(None)")
            # It seems that the reset_index calls doesn't reset the id_col into a category if the groupby's by argument was categorical.
            # So we recall astype on the id_col  in case it is supposed to be categorical.
            .astype({self.id_col: COL_DTYPES[self.id_col]})
        )

    def compute_charge_n_discharge_vars(self, tss:DF) -> DF:
        return (
            tss
            # Compute the in_charge and in_discharge masks 
            .pipe(self.compute_charge_n_discharge_masks, IN_CHARGE_CHARGING_STATUS_VALS, IN_DISCHARGE_CHARGING_STATUS_VALS)
            # Compute the correspding indices to perfrom split-apply-combine ops
            .pipe(self.compute_idx_from_masks, ["in_charge", "in_discharge"])
            # We recompute the masks by trimming off the points that have the first and last soc values
            # This is done to reduce the noise in the output due to measurments noise.
            .pipe(self.trim_leading_n_trailing_soc_off_masks, ["in_charge", "in_discharge"]) 
            .pipe(self.compute_idx_from_masks, ["trimmed_in_charge", "trimmed_in_discharge"])
            .pipe(self.compute_cum_var, "power", "cum_energy")
            .pipe(self.compute_cum_var, "charger_power", "cum_charge_energy_added")
            .pipe(self.compute_status_col)
        )

    def normalize_units_to_metric(self, tss:DF) -> DF:
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
        tss[cum_var_col] -= tss.groupby(self.id_col, observed=True)[cum_var_col].transform("first")
        return tss

    def compute_date_vars(self, tss:DF) -> DF:
        self.logger.debug(f"Computing time_diff and sec_time_diff.")
        tss["time_diff"] = tss.groupby(self.id_col, observed=False)["date"].diff()
        tss["sec_time_diff"] = tss["time_diff"].dt.total_seconds()
        return tss

    def compute_charge_n_discharge_masks(self, tss:DF, in_charge_vals:list, in_discharge_vals:list) -> DF:
        """Computes the `in_charge` and `in_discharge` masks either from the charging_status column or from the evolution of the soc over time."""
        self.logger.debug("Computing charging and discharging masks.")
        if self.make in CHARGE_MASK_WITH_CHARGING_STATUS_MAKES:
            return self.charge_n_discharging_masks_from_charging_status(tss, in_charge_vals, in_discharge_vals)
        if self.make in CHARGE_MASK_WITH_SOC_DIFFS_MAKES:
            return self.charge_n_discharging_masks_from_soc_diff(tss)
        raise ValueError(MAKE_NOT_SUPPORTED_ERROR.format(make=self.make))

    def charge_n_discharging_masks_from_soc_diff(self, tss:DF) -> DF:
        tss_grp = tss.groupby(self.id_col, observed=True)
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
        self.logger.debug(f"Computing trimmed masks of{masks}.")
        for mask in masks:
            tss["naned_soc"] = tss["soc"].where(tss[mask])
            soc_grp = tss.groupby(["vin", mask + "_idx"], observed=True)["naned_soc"]
            trailing_soc = soc_grp.transform("first")
            leading_soc = soc_grp.transform("last")
            tss["trailing_soc"] = trailing_soc
            tss["leading_soc"] = leading_soc
            tss[f"trimmed_{mask}"] = tss[mask] & (tss["soc"] != trailing_soc) & (tss["soc"] != leading_soc)
        tss = tss.drop(columns="naned_soc")
        return tss
    
    def compute_idx_from_masks(self, tss: DF, masks:list[str]) -> DF:
        self.logger.info(f"Computing {masks} idx from masks.")
        for mask in masks:
            idx_col_name = f"{mask}_idx"
            shifted_mask = tss.groupby(self.id_col, observed=True)[mask].shift(fill_value=False)
            tss["new_period_start_mask"] = shifted_mask.ne(tss[mask]) 
            if self.max_td is not None:
                tss["new_period_start_mask"] |= (tss["time_diff"] > self.max_td)
            tss[idx_col_name] = tss.groupby(self.id_col, observed=True)["new_period_start_mask"].cumsum().astype("uint16")
            tss.drop(columns=["new_period_start_mask"], inplace=True)
        return tss

    def compute_status_col(self, tss:DF) -> DF:
        self.logger.debug("Computing status column.")
        tss_grp = tss.groupby("vin", observed=True)
        status = tss["in_charge"].map({True: "charging", False:"discharging", pd.NA:"unknown"})
        tss["status"] = status.mask(
            tss["in_charge"].eq(False, fill_value=True),
            np.where(tss_grp["odometer"].diff() > 0, "moving", "idle_discharging"),
        )
        return tss

    @classmethod
    def update_all_tss(cls, **kwargs):
        for make in ALL_MAKES:
            if make == "tesla":
                cls = TeslaProcessedTimeSeries
            else:
                cls = ProcessedTimeSeries
            cls(make, force_update=True, **kwargs)

class TeslaProcessedTimeSeries(ProcessedTimeSeries):

    def __init__(self, make:str="tesla", id_col:str="vin", log_level:str="INFO", max_td:TD=MAX_TD, force_update:bool=False, **kwargs):
        self.logger = getLogger("tesla")
        set_level_of_loggers_with_prefix(log_level, "tesla")
        super().__init__("tesla", id_col, log_level, max_td, force_update, **kwargs)

    def compute_charge_n_discharge_vars(self, tss:DF) -> DF:
        return (
            tss
            .pipe(self.compute_charge_n_discharge_masks)
            .pipe(self.compute_charge_idx)
            .pipe(self.compute_idx_from_masks, ["in_discharge"])
            .pipe(self.trim_leading_n_trailing_soc_off_masks, ["in_charge", "in_discharge"])
            .pipe(self.compute_idx_from_masks, ["trimmed_in_charge", "trimmed_in_discharge"])
            .pipe(self.compute_cum_var, "power", "cum_energy")
        )

    def compute_charge_n_discharge_masks(self, tss:DF) -> DF:
        charging = (
            Series(pd.NA, index=tss.index, dtype="boolean")
            .mask(tss["charging_status"].isin(IN_CHARGE_CHARGING_STATUS_VALS), True)
            .mask(tss["charging_status"].isin(IN_DISCHARGE_CHARGING_STATUS_VALS), False)
        )
        ffill_base = charging.groupby(tss["vin"], observed=True).ffill()
        bfill_base = charging.groupby(tss["vin"], observed=True).bfill()
        charging = charging.mask(ffill_base.eq(bfill_base), ffill_base)
        charging = charging.mask(tss["soc"] >= 98)
        tss["in_charge"] = charging.notna() & charging
        tss["in_discharge"] = charging.notna() & ~charging
        return tss

    def compute_charge_idx(self, tss:DF) -> DF:
        self.logger.debug("Computing Tesla specific charge index.")
        tss_grp = tss.groupby(self.id_col, observed=True)
        tss["charge_energy_added"] = tss_grp["charge_energy_added"].ffill()
        power_loss = tss_grp['charge_energy_added'].diff().div(tss["sec_time_diff"].values)
        MIN_POWER_LOSS = 0.0001
        new_charge_mask = tss["in_charge"] & (power_loss.lt(MIN_POWER_LOSS, fill_value=0) | tss["time_diff"].gt(TD(days=1)))
        tss["in_charge_idx"] = new_charge_mask.groupby(tss["vin"], observed=True).cumsum()
        return tss

@main_decorator
def main():
    ProcessedTimeSeries.update_all_tss()

if __name__ == "__main__":
    main()

