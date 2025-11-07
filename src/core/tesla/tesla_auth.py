import aiohttp
from fastapi import HTTPException
from pydantic import BaseModel

from core.tesla.tesla_utils import FLEET_URLS, TeslaRegions


class TeslaTokens(BaseModel):
    access_token: str
    refresh_token: str
    expires_in: int


async def tesla_tokens_from_code(
    session: aiohttp.ClientSession,
    code: str,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    region: TeslaRegions,
) -> TeslaTokens:
    payload = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code,
        "audience": FLEET_URLS[region],
        "redirect_uri": redirect_uri,
    }

    response = await session.post(
        "https://fleet-auth.prd.vn.cloud.tesla.com/oauth2/v3/token",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data=payload,
    )

    if response.status != 200:
        raise HTTPException(
            status_code=response.status,
            detail=f"Error getting tokens from code: {await response.text()}",
        )

    response_data = await response.json()

    return TeslaTokens(
        access_token=response_data["access_token"],
        refresh_token=response_data["refresh_token"],
        expires_in=response_data["expires_in"],
    )


async def refresh_token(
    session: aiohttp.ClientSession,
    client_id: str,
    refresh_token: str,
) -> TeslaTokens:
    payload = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "refresh_token": refresh_token,
    }

    response = await session.post(
        "https://fleet-auth.prd.vn.cloud.tesla.com/oauth2/v3/token",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data=payload,
    )

    if response.status != 200:
        raise HTTPException(
            status_code=response.status,
            detail=f"Error refreshing tokens: {await response.text()}",
        )

    response_data = await response.json()

    return TeslaTokens(
        access_token=response_data["access_token"],
        refresh_token=response_data["refresh_token"],
        expires_in=response_data["expires_in"],
    )
