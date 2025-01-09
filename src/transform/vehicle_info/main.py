import boto3
import os
from datetime import datetime
import pandas as pd
from sqlalchemy.orm import sessionmaker
from bib_models_data_ev.vehicle import Vehicle
from typing import Dict, List
import logging

from sqlalchemy import Connection as Con

from core.s3_utils import S3_Bucket
from core.singleton_s3_bucket import bucket
from core.sql_utils import con, right_union_merge_rdb_table

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VehicleInfoProcessor:
    # def __init__(self, bucket:S3_Bucket=bucket, con:Con=con):

    def get_vehicle_vins(self) -> pd.DataFrame:
        """Retrieve all VINs from the vehicle table"""
        try:
            vehicles = pd.read_sql_table("vehicle", con)
            return vehicles
        except Exception as e:
            logger.error(f"Error fetching VINs from database: {e}")
            raise

    def get_last_file_date(self, brand: str) -> pd.DataFrame:
        """Get the date of the most recent file for a specific VIN"""
        try:
            # List objects in the VIN's folder
            # prefix = f"response/{brand}/{vin}/"
            keys = bucket.list_responses_keys_of_brand(brand)

            keys["date"] = pd.to_datetime(keys["file"].str[:-5])
            keys = keys.groupby("vin").agg({"date": "max"})
            print(type(keys))
            # last_date = keys.groupby("date").last()
            # print(type(last_date))

            if not keys:
                logger.warning(f"No files found for the brand {brand}")
                return None
            
            return last_date

        except Exception as e:
            logger.error(f"Error checking S3 for brand {brand}: {e}")
            return None

    def process_all_vehicles(self) -> Dict[str, datetime]:
        """Process all vehicles and return their last file dates"""
        results = {}
        vins = self.get_vehicle_vins()
        # Once we have all the vins, we want to check all the brands and get the last file date
        brands = pd.read_sql_table("oem", con)
        final_last_date = pd.DataFrame()
        for brand in brands['oem_name'].values:
            # Determine brand from VIN or database
            # brand = self.determine_brand(vin)
            print(brand)

            last_date = self.get_last_file_date(brand)
            # results[vin] = last_date

            final_last_date = pd.concat([final_last_date, last_date])
            # logger.info(f"VIN: {vin}, Last file date: {last_date}")
        
        # right_union_merge_rdb_table(final_last_date, "vehicle", "vin", "vin", ["last_date"])
        print(final_last_date)

        return results

    

def main():
    # Get configuration from environment variables
    s3_bucket = bucket

    df = VehicleInfoProcessor().process_all_vehicles()
    print(df)


if __name__ == "__main__":
    main()

