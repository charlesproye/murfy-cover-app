import logging

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import Field
from pydantic_settings import BaseSettings

from .response_storage import ResponseStorageDep
from .schemas import (
    ChargingRemainingTime,
    ChargingState,
    CruisingRange,
    DashboardErrorWarning,
    EnergyLevel,
    EventSelector,
    Location,
    Maintenance,
    Trip,
)


class VolkswagenSettings(BaseSettings):
    VW_PUSH_API_KEY: str = Field(default=...)


settings = VolkswagenSettings()


def authenticate(request: Request):
    api_key = request.headers.get("x-api-key")
    if api_key != settings.VW_PUSH_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="api-key missing",
        )
    return api_key


volkswagen_router = APIRouter(
    prefix="",
    dependencies=[Depends(authenticate)],
    tags=["Volkswagen"],
)


# Test route
@volkswagen_router.post("/")
async def post_data(request: Request, storage_service: ResponseStorageDep):
    EVENT_TO_MODEL = {
        "TRIP": Trip,
        "MAINTENANCE": Maintenance,
        "LOCATION": Location,
        "CRUISING_RANGE": CruisingRange,
        "DASHBOARD_ERROR_WARNING": DashboardErrorWarning,
        "ENERGY_LEVEL": EnergyLevel,
        "CHARGING_STATE": ChargingState,
        "CHARGING_REMAINING_TIME": ChargingRemainingTime,
    }

    json_data = await request.json()

    if json_data == []:
        logging.info("No data to store")
        return {}

    try:
        event_type = EventSelector(**json_data[0]).event.upper()
        model_class = EVENT_TO_MODEL.get(event_type)

        if not model_class:
            logging.error(f"Unknown event type: {event_type}")
            return {}

        models = [model_class(**item) for item in json_data]

        await storage_service.store_basemodels_with_vin("volkswagen", models)
        return {}
    except Exception as e:
        logging.error(f"Error parsing data: {json_data}, error: {e}")
        return {}

