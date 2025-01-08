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
from core.sql_utils import con

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

    def get_last_file_date(self, brand: str, vin: str) -> datetime:
        """Get the date of the most recent file for a specific VIN"""
        try:
            # List objects in the VIN's folder
            # prefix = f"response/{brand}/{vin}/"
            keys = bucket.list_responses_keys_of_brand(brand)

            keys["date"] = pd.to_datetime(keys["file"].str[:-5])
            keys = keys.groupby("vin").agg({"date": "max"})
            print("laa", keys)
            last_date = keys.groupby("date").last()

            print("get last file", keys)
            if not keys:
                logger.warning(f"No files found for VIN {vin}")
                return None

            # Get the most recent file
            latest_file = max(
                keys=lambda x: x['LastModified']
            )
            
            return latest_file['LastModified']

        except Exception as e:
            logger.error(f"Error checking S3 for VIN {vin}: {e}")
            return None

    def process_all_vehicles(self) -> Dict[str, datetime]:
        """Process all vehicles and return their last file dates"""
        results = {}
        vins = self.get_vehicle_vins()
        for vin in vins['vin'].values:
            # Determine brand from VIN or database
            # This is a simplified example - you'll need to implement proper brand determination
            brand = self.determine_brand(vin)
            print(brand)
            last_date = self.get_last_file_date(brand, vin)
            results[vin] = last_date
            
            logger.info(f"VIN: {vin}, Last file date: {last_date}")
        
        return results

    def determine_brand(self, vin: str) -> str:
        """Determine the brand from VIN"""
        # This is a placeholder - implement your brand determination logic
        # You might want to get this from the vehicle table or parse the VIN
        try:
            vehicle_model_id = pd.read_sql_table("vehicle", con).query("vin == @vin")["vehicle_model_id"].values[0]
            oem_id = pd.read_sql_table("vehicle_model", con).query("id == @vehicle_model_id")["oem_id"].values[0]
            brand = pd.read_sql_table("oem", con).query("id == @oem_id")["oem_name"].values[0]
            return brand
        except Exception as e:
            logger.error(f"Error determining brand for VIN {vin}: {e}")
            raise

def main():
    # Get configuration from environment variables
    s3_bucket = bucket
    
    # if not s3_bucket:
    #     raise ValueError("S3_BUCKET_NAME environment variable is required")


    # df = VehicleInfoProcessor().get_vehicle_vins()
    # print(df)

    df = VehicleInfoProcessor().process_all_vehicles()
    print(df)
    # processor = VehicleInfoProcessor(s3_bucket)
    # results = processor.process_all_vehicles()

    # You can add additional processing here, such as:
    # - Saving results to a database
    # - Generating reports
    # - Triggering alerts for vehicles with old data

if __name__ == "__main__":
    main()

