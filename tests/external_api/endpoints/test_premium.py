"""Tests for premium report endpoints."""

from unittest.mock import AsyncMock, patch

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


@pytest.mark.asyncio
async def test_generate_premium_report_sync_endpoint(
    app_client: httpx.AsyncClient, db_session: AsyncSession
):
    """Test generate_premium_report_sync_endpoint successfully generates and uploads a PDF."""
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

    # Mock the PDF generation, S3 upload, and presigned URL generation
    mock_pdf_bytes = b"fake_pdf_content"
    mock_presigned_url = "https://s3.amazonaws.com/test-bucket/fake-presigned-url"

    with (
        patch(
            "reports.report_render.report_generator.ReportGenerator.generate_pdf",
            new_callable=AsyncMock,
        ) as mock_generate_pdf,
        patch(
            "core.s3.async_s3.AsyncS3.upload_file_fast",
            new_callable=AsyncMock,
        ) as mock_upload_file_fast,
        patch(
            "core.s3.async_s3.AsyncS3.get_presigned_url",
            new_callable=AsyncMock,
        ) as mock_get_presigned_url,
    ):
        mock_generate_pdf.return_value = mock_pdf_bytes
        mock_get_presigned_url.return_value = mock_presigned_url

        response = await app_client.post(
            f"/v1/premium/{vehicle.vin}/generate_report_sync",
            headers={"Authorization": f"Bearer {access_token}"},
        )

        assert response.status_code == 200

        data = response.json()
        assert data["vin"] == vehicle.vin
        assert data["url"] == mock_presigned_url
        assert data["message"] == "PDF generated and uploaded successfully"

        # Verify mocked functions were called
        mock_generate_pdf.assert_called_once()
        mock_upload_file_fast.assert_called_once()
        mock_get_presigned_url.assert_called_once()
