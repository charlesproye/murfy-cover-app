"""Tests for flash_report_utils module."""

from unittest.mock import AsyncMock, patch
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db_models import Report
from db_models.enums import LanguageEnum
from db_models.report import ReportType
from reports.flash_report_utils import generate_flash_report_pdf
from tests.factories import (
    BatteryFactory,
    FlashReportCombinationFactory,
    MakeFactory,
    OemFactory,
    VehicleModelFactory,
)


@pytest.mark.asyncio
async def test_generate_flash_report_pdf(db_session: AsyncSession):
    """Test flash report PDF generation runs the full code path.

    This test runs all business logic including:
    - Fetching FlashReportCombination by token
    - Fetching VehicleModel, Battery, Oem data
    - Creating temporary Vehicle and VehicleData objects
    - Generating report data with trendline calculations
    - Rendering the HTML template
    - Saving the Report to the database

    Only S3 upload and Gotenberg PDF generation are mocked.
    """
    # Create required test data
    oem = await OemFactory.create_async(session=db_session, oem_name="tesla")
    make = await MakeFactory.create_async(
        session=db_session, oem_id=oem.id, make_name="tesla"
    )
    battery = await BatteryFactory.create_async(
        session=db_session,
        capacity=75.0,
        net_capacity=72.5,
        battery_oem="Panasonic",
        battery_chemistry="NCA",
    )
    vehicle_model = await VehicleModelFactory.create_async(
        session=db_session,
        oem_id=oem.id,
        make_id=make.id,
        battery_id=battery.id,
        model_name="model 3",
        type="long range awd",
        version="2021",
        trendline_bib="1 - 0.03 * np.log1p(x/10000)",
        trendline_oem="1 - 0.03 * np.log1p(x/10000)",
        warranty_km=160000,
        warranty_date=8,
    )

    # Create flash report combination (uses unique VIN from factory)
    flash_report = await FlashReportCombinationFactory.create_async(
        session=db_session,
        make=make.make_name,
        model=vehicle_model.model_name,
        type=vehicle_model.type,
        version=vehicle_model.version,
        odometer=50000,
        language=LanguageEnum.EN,
    )

    # Mock only: S3 upload and Gotenberg PDF generation
    mock_s3 = AsyncMock()
    mock_s3.upload_file_fast = AsyncMock()

    fake_pdf_content = b"%PDF-1.4 fake pdf content for testing"

    assert flash_report.token is not None  # Type narrowing for mypy

    with (
        patch(
            "reports.flash_report_utils.AsyncS3",
            return_value=mock_s3,
        ),
        patch(
            "reports.report_render.gotenberg_client.GotenbergClient.generate_pdf_from_html",
            new_callable=AsyncMock,
            return_value=fake_pdf_content,
        ),
    ):
        result = await generate_flash_report_pdf(
            token=flash_report.token, db=db_session
        )

    # Verify the result is a valid UUID
    assert result is not None
    assert isinstance(result, UUID)

    # Verify the Report was saved to the database
    report_query = select(Report).where(Report.id == result)
    report_result = await db_session.execute(report_query)
    saved_report = report_result.scalar_one_or_none()

    assert saved_report is not None
    assert saved_report.report_type == ReportType.flash
    assert saved_report.flash_report_combination_id == flash_report.id
    assert "flash" in saved_report.report_url
    assert flash_report.vin in saved_report.report_url

    # Verify S3 upload was called with correct parameters
    mock_s3.upload_file_fast.assert_called_once()
    call_kwargs = mock_s3.upload_file_fast.call_args
    assert call_kwargs.kwargs["file"] == fake_pdf_content
    assert "flash" in call_kwargs.kwargs["s3_uri"]
    assert flash_report.vin in call_kwargs.kwargs["s3_uri"]


@pytest.mark.asyncio
async def test_generate_flash_report_pdf_missing_vehicle_model(
    db_session: AsyncSession,
):
    """Test error when VehicleModel cannot be found for the flash report data."""
    # Create flash report with data that doesn't match any VehicleModel
    flash_report = await FlashReportCombinationFactory.create_async(
        session=db_session,
        vin="5YJ3E1EA1KF654321",
        make="nonexistent_make",
        model="nonexistent_model",
        type="nonexistent_type",
        version="2099",
        odometer=50000,
        language=LanguageEnum.EN,
    )

    assert flash_report.token is not None  # Type narrowing for mypy

    with (
        patch(
            "reports.flash_report_utils.AsyncS3",
            return_value=AsyncMock(),
        ),
        pytest.raises(ValueError, match="Cannot find vehicle model"),
    ):
        await generate_flash_report_pdf(token=flash_report.token, db=db_session)
