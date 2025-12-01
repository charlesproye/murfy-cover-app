import logging
from logging import getLogger

import numpy as np
import pandas as pd

from core.logging_utils import set_level_of_loggers_with_prefix
from core.s3.s3_utils import S3Service
from core.s3.settings import S3Settings
from core.stats_utils import (
    estimate_cycles,
    force_decay,
    mask_out_outliers_by_interquartile_range,
    weighted_mean,
)
from transform.result_phases.config import RESULT_PHASES_CACHE_KEY_TEMPLATE
from transform.result_week.config import SOH_FILTER_EVAL_STRINGS, UPDATE_FREQUENCY


class ResultPhaseToResultWeek:
    def __init__(
        self,
        make: str,
        log_level: str = "INFO",
        has_soh: bool = False,
        has_levels: bool = False,
        logger: logging.Logger | None = None,
        **kwargs,
    ):
        self.make = make
        logger_name = f"transform.result_week.{make}"
        self.logger = logger or getLogger(logger_name)
        set_level_of_loggers_with_prefix(log_level, logger_name)
        self.bucket = S3Service()
        self.settings = S3Settings()
        self.has_soh = has_soh
        self.has_levels = has_levels

    def run(self, results_phases_path: str | None = None):
        self.logger.info(f"Running {self.make}.")
        if results_phases_path is None:
            rph: pd.DataFrame = self.bucket.read_parquet_df(
                key=RESULT_PHASES_CACHE_KEY_TEMPLATE.format(
                    make=self.make.replace("-", "_")
                )
                + "/"
            )
        else:
            rph: pd.DataFrame = pd.read_parquet(results_phases_path)

        rweek = (
            rph
            # Some raw estimations may have inf values, this will make mask_out_outliers_by_interquartile_range and force_monotonic_decrease fail
            # So we replace them by NaNs.
            .pipe(self._replace_inf_soh)
            .sort_values(["VIN", "DATETIME_BEGIN"])
            .pipe(
                self._make_charge_levels_presentable
                if self.has_levels
                else (lambda rph: rph)
            )
            .eval(SOH_FILTER_EVAL_STRINGS[self.make])
            .pipe(self._agg_results_by_update_frequency)
            .groupby("VIN", observed=True)
            .apply(self._make_soh_presentable_per_vehicle, include_groups=False)
            .pipe(self._replace_soh_over_one_hundred)
            .reset_index(level=0)
            .sort_values(["VIN", "DATE"])
        )

        rweek = self.compute_cycles(rweek)

        if self.has_soh:
            rweek["SOH"] = rweek.groupby("VIN", observed=True)["SOH"].ffill()
            rweek["SOH"] = rweek.groupby("VIN", observed=True)["SOH"].bfill()
        rweek["ODOMETER"] = rweek.groupby("VIN", observed=True)["ODOMETER"].ffill()
        rweek["ODOMETER"] = rweek.groupby("VIN", observed=True)["ODOMETER"].bfill()

        return rweek

    def _replace_inf_soh(self, df):
        if "SOH" in df.columns:
            df["SOH"] = df["SOH"].replace([np.inf, -np.inf], np.nan)
        else:
            df["SOH"] = None
        return df

    def _make_charge_levels_presentable(self, results: pd.DataFrame) -> pd.DataFrame:
        level_columns = ["LEVEL_1", "LEVEL_2", "LEVEL_3"]
        existing_level_columns = [
            col for col in level_columns if col in results.columns
        ]

        if not existing_level_columns:
            return results

        negative_charge_levels = results[["LEVEL_1", "LEVEL_2", "LEVEL_3"]].lt(0)

        nb_negative_levels = negative_charge_levels.sum().sum()
        if nb_negative_levels > 0:
            self.logger.warning(
                f"[{self.make}] There are {nb_negative_levels}({100 * nb_negative_levels / len(results):2f}%) negative charge levels, setting them to 0."
            )
        results[["LEVEL_1", "LEVEL_2", "LEVEL_3"]] = results[
            ["LEVEL_1", "LEVEL_2", "LEVEL_3"]
        ].mask(negative_charge_levels, 0)

        return results

    def _agg_results_by_update_frequency(self, results: pd.DataFrame) -> pd.DataFrame:
        results.reset_index(drop=True, inplace=True)
        results["DATE"] = (
            pd.to_datetime(results["DATETIME_BEGIN"], format="mixed")
            .dt.floor(UPDATE_FREQUENCY)
            .dt.tz_localize(None)
            .dt.date.astype("datetime64[ns]")
        )

        agg_spec = {
            "ODOMETER_LAST": ("ODOMETER", "last"),
            "SOH": ("SOH", "median"),
            "SOH_OEM": ("SOH_OEM", "median"),
            "MODEL": ("MODEL", "first"),
            "VERSION": ("VERSION", "first"),
            "LEVEL_1": ("LEVEL_1", "sum"),
            "LEVEL_2": ("LEVEL_2", "sum"),
            "LEVEL_3": ("LEVEL_3", "sum"),
            "CONSUMPTION": ("CONSUMPTION", "median"),
            "RANGE": ("RANGE", "first"),
        }

        agg_dict = {
            new_col: pd.NamedAgg(src_col, func)
            for src_col, (new_col, func) in agg_spec.items()
            if src_col in results.columns
        }

        if self.make != "bmw":
            mask = (results["CONSUMPTION"] > 0) & (
                results["CONSUMPTION"] <= 100
            )  # mask to drop negative and over 100 consumption
            agg_dict["CONSUMPTION"] = pd.NamedAgg(
                "CONSUMPTION",
                lambda x: weighted_mean(
                    x[mask], results.loc[x.index, "ODOMETER_DIFF"][mask]
                ),
            )
        else:
            agg_dict["CONSUMPTION"] = pd.NamedAgg("CONSUMPTION", "median")

        assign_cols = ["LEVEL_1", "LEVEL_2", "LEVEL_3"]
        num_cols = [
            "SOH",
            "SOH_OEM",
            "CONSUMPTION",
            "LEVEL_1",
            "LEVEL_2",
            "LEVEL_3",
            "ODOMETER_LAST",
            "ODOMETER_DIFF",
        ]

        for col in num_cols:
            if col in results.columns:
                results[col] = pd.to_numeric(results[col], errors="coerce")

        return (
            results.assign(
                **{
                    col: results[col].fillna(0)
                    for col in assign_cols
                    if col in results.columns
                }
            )
            .groupby(["VIN", "DATE"], observed=True, as_index=False)
            .agg(**agg_dict)
        )

    def _make_soh_presentable_per_vehicle(self, df: pd.DataFrame) -> pd.DataFrame:
        if df["SOH"].isna().all():
            return df
        if df["SOH"].count() > 3:
            outliser_mask = mask_out_outliers_by_interquartile_range(df["SOH"])
            assert outliser_mask.any(), (
                f"There seems to be only outliers???:\n{df['SOH']}."
            )
            df = df[outliser_mask].copy()
        if df["SOH"].count() >= 2:
            df["SOH"] = force_decay(df[["SOH", "ODOMETER"]])
        return df

    def _replace_soh_over_one_hundred(self, df: pd.DataFrame) -> pd.DataFrame:
        if "SOH" in df.columns:
            df["SOH"] = df["SOH"].mask(df["SOH"] > 1, 1)
        return df

    def compute_cycles(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute the estimated number of cycles
        """

        if "SOH" in df.columns:
            df["ESTIMATED_CYCLES"] = df.apply(
                lambda row: estimate_cycles(row["ODOMETER"], row["RANGE"], row["SOH"]),
                axis=1,
            )
        else:
            df["ESTIMATED_CYCLES"] = df.apply(
                lambda row: estimate_cycles(row["ODOMETER"], row["RANGE"]), axis=1
            )

        return df
