import base64
import gzip
import json
import logging
import secrets
from contextlib import asynccontextmanager

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import Field
from pydantic_settings import BaseSettings

from ingestion.ingestion_cache import IngestionCache
from ingestion.push.config import KIA_KEYS_TO_IGNORE

from .response_storage import ResponseStorageDep
from .token_utils import create_access_token, verify_token

security = HTTPBasic()

LOGGER = logging.getLogger(__name__)


class KiaSettings(BaseSettings):
    KIA_CLIENT_ID: str = Field(default="kia")
    KIA_PUSH_API_KEY: str = Field(default=...)


@asynccontextmanager
async def kia_lifespan(app):
    LOGGER.info("Initialisation du cache KIA...")
    kia_cache = IngestionCache(
        make="kia", keys_to_ignore=KIA_KEYS_TO_IGNORE, min_change_interval=30
    )

    app.state.kia_cache = kia_cache

    yield


kia_router = APIRouter(prefix="/kia", tags=["KIA"], lifespan=kia_lifespan)


@kia_router.post("/data")
async def receive_kia_data(
    request: Request,
    storage_service: ResponseStorageDep,
    x_amz_firehose_access_key: str = Header(...),
):
    # Check token format
    if not x_amz_firehose_access_key.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid token format")

    token = x_amz_firehose_access_key[len("Bearer ") :]

    # Validate JWT token
    if not verify_token(token):
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    try:
        raw_bytes = await request.body()

        decompressed_data = gzip.decompress(raw_bytes)
        decoded_json = json.loads(decompressed_data.decode("utf-8"))

        kia_cache: IngestionCache = request.app.state.kia_cache

        for record in decoded_json["records"]:
            record_data = json.loads(base64.b64decode(record["data"]))

            vin = record_data.get("header", {}).get("vin", "unknown")
            if kia_cache.json_in_db(vin, record_data):
                continue

            kia_cache.set_json_in_db(vin, record_data)
            await storage_service.store_raw_json("kia", record_data)
            LOGGER.info(f"Stored new record for VIN: {vin}")

    except Exception as e:
        LOGGER.exception(f"Error decoding Kia data: {e}")

    return {}


@kia_router.post("/token")
async def token_endpoint(
    request: Request, credentials: HTTPBasicCredentials = Depends(security)
):
    settings = KiaSettings()

    if not (
        secrets.compare_digest(credentials.username, settings.KIA_CLIENT_ID)
        and secrets.compare_digest(credentials.password, settings.KIA_PUSH_API_KEY)
    ):
        raise HTTPException(status_code=401, detail="Invalid client credentials")

    access_token = create_access_token(credentials.username)

    return {"access_token": access_token, "token_type": "Bearer", "expires_in": 60 * 60}

