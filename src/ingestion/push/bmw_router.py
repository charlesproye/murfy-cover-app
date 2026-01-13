import logging
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import Field
from pydantic_settings import BaseSettings

from core.models import MakeEnum
from ingestion.kafka_producer import KafkaProducerDep

from .response_storage import ResponseStorageDep
from .schemas import PushDataRequest

LOGGER = logging.getLogger(__name__)


class BmwSettings(BaseSettings):
    BMW_PUSH_API_KEY: str = Field(default=...)


settings = BmwSettings()


def authenticate(request: Request):
    api_key = request.headers.get("x-push-payload-key")
    LOGGER.info(f"BMW {api_key = }")
    if api_key != settings.BMW_PUSH_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="api-key missing",
        )
    return api_key


bmw_router = APIRouter(
    prefix="/bmw",
    dependencies=[Depends(authenticate)],
    tags=["BMW"],
)


@bmw_router.post("/api/push")
async def receive_data(
    data: PushDataRequest,
    storage_service: ResponseStorageDep,
    kafka_producer: KafkaProducerDep,
):
    await storage_service.store_basemodels_with_vin(
        MakeEnum.bmw.value, data.vehiclePushKeyValues
    )

    # Send filtered data to Kafka for each vehicle
    for vehicle_data in data.vehiclePushKeyValues:
        # Convert BMW format to dict for filtering
        vin = vehicle_data.vin
        if vin is None:
            LOGGER.error("No VIN found in vehicle data")
            continue

        converted_data = {kv.key: kv.value for kv in vehicle_data.pushKeyValues}

        await kafka_producer.send_filtered_data(
            MakeEnum.bmw,
            converted_data,
            vin,
            message_datetime=vehicle_data.received_date.isoformat()
            if vehicle_data.received_date
            else datetime.now(UTC).replace(tzinfo=None).isoformat(),
        )
