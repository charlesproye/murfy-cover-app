"""Tests for premium report endpoints."""

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from tests.external_api.utils import create_authenticated_user_with_fleet
from tests.factories import (
    BatteryFactory,
    MakeFactory,
    OemFactory,
    RegionFactory,
    VehicleDataFactory,
    VehicleFactory,
    VehicleModelFactory,
)


@pytest.mark.asyncio
async def test_get_premium_data(
    app_client: httpx.AsyncClient, db_session: AsyncSession
):
    """Test get_premium_data endpoint returns premium report data for a vehicle."""
    # Create required database objects
    auth_context = await create_authenticated_user_with_fleet(db_session)
    fleet = auth_context.fleet
    access_token = auth_context.access_token

    # Create vehicle-related data
    region = await RegionFactory.create_async(session=db_session)
    oem = await OemFactory.create_async(session=db_session)
    make = await MakeFactory.create_async(session=db_session, oem_id=oem.id)
    battery = await BatteryFactory.create_async(session=db_session)

    vehicle_model = await VehicleModelFactory.create_async(
        session=db_session,
        oem_id=oem.id,
        make_id=make.id,
        battery_id=battery.id,
    )

    vehicle = await VehicleFactory.create_async(
        session=db_session,
        fleet_id=fleet.id,
        region_id=region.id,
        vehicle_model_id=vehicle_model.id,
    )

    await VehicleDataFactory.create_async(
        session=db_session,
        vehicle_id=vehicle.id,
        soh=95.5,
        odometer=50000.0,
    )

    await db_session.commit()

    response = await app_client.get(
        f"/v1/premium/{vehicle.vin}/data",
        headers={"Authorization": f"Bearer {access_token}"},
    )

    assert response.status_code == 200

    data = response.json()
    assert data["vehicle"] is not None
    assert data["soh_data"] is not None
    assert data["vehicle"]["vin"] == vehicle.vin
