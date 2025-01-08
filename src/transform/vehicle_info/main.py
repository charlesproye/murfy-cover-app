import boto3
import os
from datetime import datetime

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
    def __init__(self, bucket:S3_Bucket=bucket, con:Con=con):
        self.db_engine = create_engine(os.getenv('DATABASE_URL'))
        Session = sessionmaker(bind=self.db_engine)
        self.session = Session()

    def get_vehicle_vins(self) -> List[str]:
        """Retrieve all VINs from the vehicle table"""
        try:
            vehicles = self.session.query(Vehicle.vin).all()
            return [v[0] for v in vehicles]
        except Exception as e:
            logger.error(f"Error fetching VINs from database: {e}")
            raise

    def get_last_file_date(self, brand: str, vin: str) -> datetime:
        """Get the date of the most recent file for a specific VIN"""
        try:
            # List objects in the VIN's folder
            prefix = f"{brand}/{vin}/"
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix=prefix
            )

            if 'Contents' not in response:
                logger.warning(f"No files found for VIN {vin}")
                return None

            # Get the most recent file
            latest_file = max(
                response['Contents'],
                key=lambda x: x['LastModified']
            )
            
            return latest_file['LastModified']

        except Exception as e:
            logger.error(f"Error checking S3 for VIN {vin}: {e}")
            return None

    def process_all_vehicles(self) -> Dict[str, datetime]:
        """Process all vehicles and return their last file dates"""
        results = {}
        vins = self.get_vehicle_vins()
        
        for vin in vins:
            # Determine brand from VIN or database
            # This is a simplified example - you'll need to implement proper brand determination
            brand = self.determine_brand(vin)
            
            last_date = self.get_last_file_date(brand, vin)
            results[vin] = last_date
            
            logger.info(f"VIN: {vin}, Last file date: {last_date}")
        
        return results

    def determine_brand(self, vin: str) -> str:
        """Determine the brand from VIN"""
        # This is a placeholder - implement your brand determination logic
        # You might want to get this from the vehicle table or parse the VIN
        try:
            vehicle = self.session.query(Vehicle).filter(Vehicle.vin == vin).first()
            return vehicle.brand.lower()
        except Exception as e:
            logger.error(f"Error determining brand for VIN {vin}: {e}")
            raise

def main():
    # Get configuration from environment variables
    s3_bucket = bucket
    
    if not s3_bucket:
        raise ValueError("S3_BUCKET_NAME environment variable is required")

    processor = VehicleInfoProcessor(s3_bucket)
    results = processor.process_all_vehicles()

    # You can add additional processing here, such as:
    # - Saving results to a database
    # - Generating reports
    # - Triggering alerts for vehicles with old data

if __name__ == "__main__":
    main()

