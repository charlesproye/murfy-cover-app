import logging
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import Field
from pydantic_settings import BaseSettings

from .response_storage import ResponseStorageDep
from .schemas import PushDataRequest


class BmwSettings(BaseSettings):
    BMW_PUSH_API_KEY: str = Field(default=...)


def authenticate(request: Request):
    settings = BmwSettings()
    api_key = request.headers.get("x-push-payload-key")
    logging.info(f"BMW {api_key = }")
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
):
    logging.info(f"CONTENT = {data}")
    await storage_service.store_basemodels_with_vin("bmw", data.vehiclePushKeyValues)

