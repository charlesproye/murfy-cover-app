import logging
from pprint import pprint
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import Field
from pydantic_settings import BaseSettings

# TODO
# - enforce security when we have th proof BMW have the api key
# - build a parser for the body when we know th format and store the infos


class BmwSettings(BaseSettings):
    BMW_PUSH_API_KEY: str = Field(default=...)


def authenticate(request: Request):
    settings = BmwSettings()
    api_key = request.headers.get("x-push-payload-key")
    logging.info(f"BMW {api_key = }")
    # if api_key != settings.BMW_PUSH_API_KEY:
    #     raise HTTPException(
    #         status_code=status.HTTP_401_UNAUTHORIZED,
    #         detail="api-key header missing",
    #     )
    return api_key


bmw_router = APIRouter(
    prefix="/bmw",
    dependencies=[Depends(authenticate)],
    tags=["BMW"],
)


@bmw_router.post("/api/push")
async def receive_data(request: Request):
    # TODO: waiting to have sample of what bodies look like for this requests to build a parser to store them.
    logging.info(f"BODY")
    pprint(await request.body())
    return

