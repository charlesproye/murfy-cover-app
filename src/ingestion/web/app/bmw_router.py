from pprint import pprint
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import Field
from pydantic_settings import BaseSettings
from core.logging_utils import LoggerDep


class BmwSettings(BaseSettings):
    BMW_PUSH_API_KEY: str = Field(default=...)


def authenticate(request: Request, logger: LoggerDep):
    settings = BmwSettings()
    logger.debug(f"GETTING AUTH")
    api_key = request.headers.get("x-push-payload-key")
    logger.debug(f"{api_key = }")
    if api_key != settings.BMW_PUSH_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="api-key header missing",
        )
    return api_key


bmw_router = APIRouter(
    prefix="/bmw", dependencies=[Depends(authenticate)], tags=["BMW"]
)


@bmw_router.post("/api/push")
async def receive_data(request: Request, logger: LoggerDep):
    logger.debug(f"BODY")
    pprint(await request.body())
    return

