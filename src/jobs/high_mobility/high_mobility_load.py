import logging
from datetime import datetime as DT
from datetime import timedelta as TD
from dateutil import parser
import importlib

from rich import print
import pandas as pd
from pandas import Series
from pandas import DataFrame as DF
from apscheduler.triggers.interval import IntervalTrigger

from core.s3_utils import S3_Bucket
from jobs.base_job import Jobinterval
from core.constants import *

# from bib_models.db.session import session_by_schema
from sqlalchemy import insert

class HighMobilityLoad(Jobinterval):

    def __init__(self, brand:str) -> None:
        super().__init__()

        self.brand = brand
        self.name = brand + "-ProcessedTS"
        self.id = self.name
        self.logger = logging.getLogger(self.name)
        self.trigger = IntervalTrigger(days=1, start_date=DT.now() - TD(days=1) + TD(seconds=1))

    async def func(self):
        self.bucket = S3_Bucket()
        self.load_process_of_all_vins()

    def load_process_of_all_vins(self):
        """
        ### Description:
        Load data into the sql db .
        """
        # Get list of objects in response folder
        keys = Series(self.bucket.list_keys(f"processed_ts/{self.brand}/time_series/"), dtype="string")
        
        
        if len(keys) == 0:
            self.logger.info(f"""
                No time series found in the 'processed_ts/{self.brand}/time_series)' folder.
                No data have been loaded.
            """)
            return
        # Only retain .parquet files
        keys = keys[keys.str.endswith(".parquet")]
        print(keys)
        # One file per VIN so no concat needed
        # keys = (
        #     pd.concat((keys, keys.str.split("/", expand=True).loc[:, 1:]), axis="columns")
        #     .rename(columns={0:"key", 3:"vin"})
        #     .loc[:, ["key", "vin"]]
        #     .assign(vin=lambda df: df["vin"].str.split(".", expand=True).iloc[:, 0])
        # )
        # keys.apply(self.load_processed_ts, axis="columns")

    async def load_processed_ts(self, src_key:DF):
        # print(src_key["key"])
        processed_ts = self.bucket.read_parquet(src_key["key"]).set_index("date")
        print(processed_ts)

        # module = importlib.import_module(f"bib_models_data_ev.{self.brand}")
        # brand = module.brand
        # print(brand)
        # # print(raw_ts.index)
        # if "diagnostics.odometer" in processed_ts.columns:
        #     processed_ts =  DF({"odometer": raw_ts["diagnostics.odometer"]})
        # elif "diagnostics.odometer.miles" in raw_ts.columns:
        #     processed_ts = DF({"odometer": raw_ts["diagnostics.odometer"] * MILES_TO_KM})
        # else: # Ignore df if it does not contain the odometer for now
        #     return
        
        # processed_ts = processed_ts.dropna(axis="index")

        # # print(processed_ts)
        
        # processed_ts_key = "/".join(["processed_ts", self.brand, "time_series", src_key["vin"]])
        # self.bucket.save_df_as_parquet(processed_ts, processed_ts_key)
        # async def load(self, battery_review_mappings, list_delete_ids):
        # Insert the data into a single transaction
        # async with session_by_schema("ayvens") as session:
        #     try:
        #         for processed_ts_chunk in processed_ts:
        #             query = insert(self.BatteryReview).where(
        #             self.BatteryReview.id.in_(list_delete_ids_chunk)
        #         )
        #             await session.execute(query)
        #         # insert
        #         await session.run_sync(
        #             lambda sync_session: sync_session.bulk_insert_mappings(
        #                 self.BatteryReview, processed_ts
        #             )
        #         )

        #         await session.commit()
        #     except Exception as e:
        #         await session.rollback()
        #         raise e
        #     finally:
        #         await session.close()
