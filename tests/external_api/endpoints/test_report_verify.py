"""Tests for report verification endpoint."""

from typing import cast
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from tests.external_api.utils import create_authenticated_user_with_fleet
from tests.factories import (
    BatteryFactory,
    MakeFactory,
    OemFactory,
    PremiumReportFactory,
    RegionFactory,
    VehicleFactory,
    VehicleModelFactory,
)


@pytest.mark.asyncio
async def test_verify_report_success(
    app_client: httpx.AsyncClient, db_session: AsyncSession
):
    """Test successful report verification with correct VIN last 4 digits."""
    auth_context = await create_authenticated_user_with_fleet(db_session)
    fleet = auth_context.fleet

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

    report = await PremiumReportFactory.create_async(
        session=db_session,
        vehicle_id=vehicle.id,
    )

    await db_session.commit()

    mock_presigned_url = "https://s3.amazonaws.com/test-bucket/presigned-url"

    with patch(
        "core.s3.async_s3.AsyncS3.get_presigned_url",
        new_callable=AsyncMock,
    ) as mock_get_presigned_url:
        mock_get_presigned_url.return_value = mock_presigned_url
        last_4 = cast(str, vehicle.vin)[-4:]
        response = await app_client.post(
            f"/v1/verify-report/{report.id}",
            json={"vin_last_4": last_4},
        )

        assert response.status_code == 200

        data = response.json()
        assert data["verified"] is True
        assert data["pdf_url"] == mock_presigned_url
        assert data["report_date"] is not None
        assert data["report_type"] == report.report_type.value

        mock_get_presigned_url.assert_called_once()
