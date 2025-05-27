import boto3
import os
from datetime import datetime
import pandas as pd
from sqlalchemy.orm import sessionmaker
from bib_models_data_ev.vehicle import Vehicle
from typing import Dict, List
import logging

from sqlalchemy import Connection as Con
from core.sql_utils import get_connection

from core.s3.s3_utils import S3Service
from core.singleton_s3_bucket import S3
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

    def get_last_file_date(self, brand: str) -> pd.DataFrame:
        """Get the date of the most recent file for a specific VIN"""
        try:
            # List objects in the VIN's folder
            keys = S3.list_responses_keys_of_brand(brand)
            
            # Check if keys is empty before processing
            if keys.empty:
                logger.warning(f"No files found for the brand {brand}")
                return None
                
            # Process the data only if we have keys
            keys["last_date_data"] = pd.to_datetime(keys["file"].str[:-5])
            last_date_data = keys.groupby("vin").agg({"last_date_data": "max"})
            
            return last_date_data.reset_index()

        except Exception as e:
            logger.error(f"Error checking S3 for brand {brand}: {e}")
            return None

    def process_all_vehicles(self):
        try:
            # Get all brands except Tesla
            with get_connection() as conn:
                brands = pd.read_sql("SELECT DISTINCT oem_name FROM oem", conn)
                
                final_last_date = pd.DataFrame()
                
                for brand in brands['oem_name'].values:
                    try:
                        last_date_data = self.get_last_file_date(brand)
                        if last_date_data is not None and not last_date_data.empty:
                            print(brand)
                            final_last_date = pd.concat([final_last_date, last_date_data])
                    except Exception as e:
                        logging.error(f"Error processing brand {brand}: {str(e)}")
                        continue
                        
                if not final_last_date.empty:
                    # Create cursor and execute updates within the same connection context
                    with conn.cursor() as cursor:
                        for index, row in final_last_date.iterrows():
                            cursor.execute("""
                                UPDATE vehicle 
                                SET last_date_data = %s 
                                WHERE vin = %s
                            """, (row['last_date_data'], row['vin']))
                        conn.commit()
                        
        except Exception as e:
            logging.error(f"Error in process_all_vehicles: {str(e)}")
            raise
    

def main():
    # Get configuration from environment variables
    s3_bucket = S3

    df = VehicleInfoProcessor().process_all_vehicles()
    print(df)


if __name__ == "__main__":
    main()

