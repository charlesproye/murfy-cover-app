import logging
from datetime import datetime as DT
from datetime import timedelta as TD
from dateutil import parser

from rich import print
import pandas as pd
from pandas import Series
from pandas import DataFrame as DF
from apscheduler.triggers.interval import IntervalTrigger

from core.s3_utils import S3_Bucket
from transform.base_jobs.job_interval import Jobinterval
from core.constants import *
from core.time_series_processing import process_date, estimate_dummy_soh

class HighMobilityProcessedTS(Jobinterval):

    def __init__(self, brand:str) -> None:
        super().__init__()

        self.brand = brand
        self.name = brand + "-ProcessedTS"
        self.id = self.name
        self.logger = logging.getLogger(self.name)
        self.trigger = IntervalTrigger(days=1, start_date=DT.now() - TD(days=1) + TD(seconds=1))

    async def func(self):
        self.bucket = S3_Bucket()
        self.process_tss_of_all_vins()
        # self.compute_raw_ts_meta_data()

    def compute_raw_ts_meta_data(self):
        # Get list of raw ts in response folder
        keys = Series(self.bucket.list_keys(f"raw_ts/{self.brand}/time_series/"), dtype="string")
        if len(keys) == 0:
            self.logger.info(f"""
                No time series found in the 'raw_ts/{self.brand}/time_series)' folder.
                No processed time series have been generated.
            """)
            return
        # Only retain .parquet files
        keys = keys[keys.str.endswith(".parquet")]
        keys = (
            pd.concat((keys, keys.str.split("/", expand=True).loc[:, 1:]), axis="columns")
            .rename(columns={0:"key", 3:"vin"})
            .loc[:, ["key", "vin"]]
            .assign(vin=lambda df: df["vin"].str.split(".", expand=True).iloc[:, 0])
        )
        meta_data = keys.apply(lambda src_key: self.bucket.read_parquet_df(src_key["key"]).columns, axis="columns")
        meta_data = DF(meta_data.to_list(), index=keys["vin"])
        print(self.brand)
        print(set(meta_data.values.ravel()))

    def process_tss_of_all_vins(self):
        """
        ### Description:
        process raw time series.
        """
        # Get list of raw ts in response folder
        keys = Series(self.bucket.list_keys(f"raw_ts/{self.brand}/time_series/"), dtype="string")
        if len(keys) == 0:
            self.logger.info(f"""
                No time series found in the 'raw_ts/{self.brand}/time_series)' folder.
                No processed time series have been generated.
            """)
            return
        # Only retain .parquet files
        keys = keys[keys.str.endswith(".parquet")]
        keys = (
            pd.concat((keys, keys.str.split("/", expand=True).loc[:, 1:]), axis="columns")
            .rename(columns={0:"key", 3:"vin"})
            .loc[:, ["key", "vin"]]
            .assign(vin=lambda df: df["vin"].str.split(".", expand=True).iloc[:, 0])
        )
        # self.logger.debug(keys)
        if self.brand == "stellantis":
            keys.apply(self.process_raw_ts_stellantis, axis="columns")
        else: 
            keys.apply(self.process_raw_ts, axis="columns")

    def process_raw_ts(self, src_key:DF):
        raw_ts = self.bucket.read_parquet_df(src_key["key"])
        # print(raw_ts)
        if "diagnostics.odometer" in raw_ts.columns:
            processed_ts =  DF({"odometer": raw_ts["diagnostics.odometer"], "date":raw_ts["date"]})
        elif "diagnostics.odometer.miles" in raw_ts.columns:
            processed_ts = DF({"odometer": raw_ts["diagnostics.odometer"] * MILES_TO_KM, "date":raw_ts["date"]})
        else: # Ignore df if it does not contain the odometer for now
            return
        
        processed_ts = (
            processed_ts
            .dropna(axis="index")
            .pipe(process_date, add_sec_time_diff_col=False)
            .pipe(estimate_dummy_soh)
        )

        processed_ts_key = "/".join(["processed_ts", self.brand, "time_series", src_key["vin"]]) + ".parquet"
        # print(processed_ts)
        print(processed_ts_key)
        # print("============")
        
        self.bucket.save_df_as_parquet(processed_ts, processed_ts_key)
    
    def process_raw_ts_stellantis(self, src_key:DF):
        raw_ts = self.bucket.read_parquet_df(src_key["key"])
        if "odometer.value" in raw_ts.columns:
            processed_ts =  DF({"odometer": raw_ts["odometer.value"], "date":raw_ts["date"]})
        else: # Ignore df if it does not contain the odometer for now
            return
        
        processed_ts = (
            processed_ts
            .dropna(axis="index")
            .pipe(process_date, add_sec_time_diff_col=False)
            .pipe(estimate_dummy_soh)
        )

        processed_ts_key = "/".join(["processed_ts", self.brand, "time_series", src_key["vin"]]) + ".parquet"
        # print(processed_ts)
        # print(processed_ts_key)
        # print("============")
        
        self.bucket.save_df_as_parquet(processed_ts, processed_ts_key)


