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
from jobs.base_job import Jobinterval
from core.constants import *

class HighMobilityProcessedTS(Jobinterval):

    def __init__(self, brand:str) -> None:
        super().__init__()

        self.brand = brand
        self.bucket = S3_Bucket()
        self.name = brand + "-ProcessedTS"
        self.id = self.name
        self.logger = logging.getLogger(self.name)
        self.trigger = IntervalTrigger(days=1, start_date=DT.now() - TD(days=1) + TD(seconds=1))

    async def func(self):
        self.process_tss_of_all_vins()

    def process_tss_of_all_vins(self):
        """
        ### Description:
        process raw time series.
        """
        # Get list of objects in response folder
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
        print(keys)
        keys.apply(self.process_raw_ts, axis="columns")

    def process_raw_ts(self, src_key:DF):
        # print(src_key["key"])
        raw_ts = self.bucket.read_parquet(src_key["key"]).set_index("date")
        # print(raw_ts.index)
        if "diagnostics.odometer" in raw_ts.columns:
            processed_ts =  DF({"odometer": raw_ts["diagnostics.odometer"]})
        elif "diagnostics.odometer.miles" in raw_ts.columns:
            processed_ts = DF({"odometer": raw_ts["diagnostics.odometer"] * MILES_TO_KM})
        else: # Ignore df if it does not contain the odometer for now
            return
        
        processed_ts = processed_ts.dropna(axis="index")

        # print(processed_ts)
        
        processed_ts_key = "/".join(["processed_ts", self.brand, "time_series", src_key["vin"]])
        self.bucket.save_df_as_parquet(processed_ts, processed_ts_key)

