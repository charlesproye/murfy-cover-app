import logging
from io import BytesIO

import pandas as pd
from joblib import load as joblib_load
from sqlalchemy.ext.asyncio import AsyncSession

from external_api.core.config import settings
from external_api.schemas.vehicle import DynamicVehicleData, StaticVehicleData
from external_api.services.s3 import s3_client
from external_api.services.vehicle import (
    get_dynamic_vehicle_data,
    get_static_vehicle_data,
)

logger = logging.getLogger(__name__)


def load_model():
    try:
        response = s3_client.get_object(
            Bucket=settings.S3_BUCKET_NAME, Key="models/model_price.pkl"
        )
        buffer = BytesIO(response["Body"].read())
        model = joblib_load(buffer)
        return model
    except Exception as e:
        logger.error(
            f"Error reading pickle file s3://{settings.S3_BUCKET_NAME}/models/model_price.pkl: {e}"
        )
        raise


async def get_price_forecast(vin: str, db: AsyncSession | None = None):
    try:
        """
        Get back the price of a vehicle by its VIN, and the possible discount according to SoH.
        """
        model = load_model()

        # Get information about the vehicle
        static_data: StaticVehicleData = await get_static_vehicle_data(db, vin)
        dynamic_data: DynamicVehicleData = await get_dynamic_vehicle_data(db, vin)
        year = (
            static_data.start_date.year if static_data.start_date is not None else None
        )

        # Apply the model
        model_response = model.predict(
            pd.DataFrame(
                [
                    [
                        static_data.make_name,
                        static_data.autonomy,
                        static_data.battery_chemistry,
                        static_data.net_capacity,
                        dynamic_data.odometer,
                        year,
                        dynamic_data.soh * 100,
                    ]
                ],
                columns=[
                    "make",
                    "autonomy",
                    "battery_chemistry",
                    "net_capacity",
                    "odometer",
                    "year",
                    "soh",
                ],
            )
        )
        if model_response is not None and len(model_response) > 0:
            price = model_response[0]
        else:
            raise Exception("Model response is empty")

        # Get the discount
        def soh_comparison_to_percentage(soh_comparison):
            if soh_comparison >= 3.50:
                return 0.02
            elif soh_comparison >= 2.00:
                return 0.01
            elif soh_comparison >= 1.45:
                return 0.005
            elif soh_comparison > -0.74:
                return -0.005
            elif soh_comparison > -4.00:
                return -0.01
            else:
                return -0.02

        discount_percentage = soh_comparison_to_percentage(dynamic_data.soh_comparison)
        price_discount = discount_percentage * price

        return {
            "price": price,
            "price_discount": price_discount,
        }

    except Exception as e:
        logger.error(f"Error predicting a price: {e}")
        return {"price": None, "price_discount": None}

