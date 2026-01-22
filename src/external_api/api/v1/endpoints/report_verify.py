"""Public endpoint for report verification."""

import logging
import uuid

from fastapi import APIRouter, Depends, HTTPException, Path
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import select

from core.s3.async_s3 import AsyncS3, get_async_s3
from core.sql_utils import get_async_db
from db_models import FlashReportCombination
from db_models.report import Report
from db_models.vehicle import Vehicle
from external_api.schemas.report import (
    ReportVerificationRequest,
    ReportVerificationResponse,
)

logger = logging.getLogger(__name__)

verify_router = APIRouter(include_in_schema=False)


@verify_router.post(
    "/{report_uuid}",
    response_model=ReportVerificationResponse,
    summary="Verify a report",
    description="Verify a report by providing the report UUID and the last 4 digits of the VIN. "
    "This endpoint is public and does not require authentication.",
)
async def verify_report(
    request: ReportVerificationRequest,
    report_uuid: str = Path(..., description="Report UUID from the verification URL"),
    db: AsyncSession = Depends(get_async_db),
    s3_client: AsyncS3 = Depends(get_async_s3),
) -> ReportVerificationResponse:
    """
    Verify a report by checking the last 4 digits of the VIN.

    This is a public endpoint that allows anyone with a report verification URL
    to view the report by providing the last 4 digits of the associated VIN.
    """
    invalid_reponse = ReportVerificationResponse(
        verified=False,
        pdf_url=None,
        report_date=None,
        report_type=None,
    )

    try:
        uuid_casted = uuid.UUID(report_uuid)
    except ValueError as e:
        raise HTTPException(status_code=400, detail="Invalid report UUID") from e

    # Find the report and vehicle in a single query
    stmt = (
        select(Report, Vehicle, FlashReportCombination)
        .join(Vehicle, Report.vehicle_id == Vehicle.id, isouter=True)
        .join(
            FlashReportCombination,
            Report.flash_report_combination_id == FlashReportCombination.id,
            isouter=True,
        )
        .where(Report.id == uuid_casted)
    )
    result = await db.execute(stmt)
    row = result.first()

    if not row:
        return invalid_reponse

    report, vehicle, flash_report_combination = row

    if flash_report_combination and flash_report_combination.vin:
        vin = flash_report_combination.vin
    elif vehicle and vehicle.vin:
        vin = vehicle.vin
    else:
        raise HTTPException(
            status_code=404, detail=f"Vehicle not found for report {report_uuid}"
        )

    if vin[-4:] != request.vin_last_4:
        return invalid_reponse

    if not report.report_url:
        raise HTTPException(status_code=404, detail="Report PDF not available")

    pdf_url = await s3_client.get_presigned_url(
        s3_uri=report.report_url,
        expires_in=3600,  # 1 hour
    )
    return ReportVerificationResponse(
        verified=True,
        pdf_url=pdf_url,
        report_date=report.created_at.strftime("%d/%m/%Y"),
        report_type=report.report_type,
    )
