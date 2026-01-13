import logging
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, Path
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession

from core.s3.async_s3 import AsyncS3
from core.sql_utils import get_async_db
from db_models.report import ReportType
from external_api.core.cookie_auth import (
    get_current_user_from_cookie,
    get_user_with_fleets,
)
from external_api.core.dependencies import get_s3_client_fast
from external_api.schemas.report import (
    ReportData,
    ReportGeneration,
    ReportPDFUrl,
    ReportSync,
)
from external_api.schemas.user import GetCurrentUser
from external_api.services.report import report_mutualized_endpoints

readout_router = APIRouter()
readout_router_hidden = APIRouter(include_in_schema=False)

logger = logging.getLogger(__name__)


@readout_router.get(
    "/{vin}/data",
    response_model=ReportData,
    summary="Get data by vehicle",
    description="Returns the complete readout report data collected by Bib for an activated vehicle.",
)
async def get_readout_data(
    db=Depends(get_async_db),
    vin: str = Path(..., description="VIN requested"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
) -> ReportData:
    return await report_mutualized_endpoints.get_report_data(
        db, vin, user, report_type=ReportType.readout
    )


@readout_router.post(
    "/{vin}/report_job",
    status_code=202,
    summary="Generate readout report",
    description="Triggers the generation of a readout report for a vehicle by VIN. The report will only be generated if the vehicle is activated and has SoH data available, it can take up to 2 minutes to generate the report. "
    + "Use the `Get readout report` endpoint to retrieve the status and URL of the report.",
)
async def generate_readout_report(
    db=Depends(get_async_db),
    vin: str = Path(..., description="VIN requested"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
) -> ReportGeneration:
    return await report_mutualized_endpoints.submit_report_job(
        db, vin, user, report_type=ReportType.readout
    )


@readout_router.get(
    "/{vin}/report_job/{job_id}",
    summary="Get readout report",
    description="Returns the status and URL of a readout report for a vehicle using the job_id returned by the `Generate readout report` endpoint.",
)
async def get_report_status(
    vin: str = Path(..., description="The VIN"),
    job_id: str = Path(..., description="The Celery job ID"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
    db: AsyncSession = Depends(get_async_db),
    s3_client: AsyncS3 = Depends(get_s3_client_fast),
) -> ReportPDFUrl:
    return await report_mutualized_endpoints.get_report_job_status(
        vin, job_id, user, db, s3_client
    )


@readout_router.get(
    "/{vin}/report",
    summary="Get readout report for today",
    description="Returns the readout report for a vehicle using the VIN, defaults to today's date.",
)
async def get_readout_report_today(
    vin: str = Path(..., description="VIN requested"),
    db: AsyncSession = Depends(get_async_db),
    s3_client: AsyncS3 = Depends(get_s3_client_fast),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
) -> ReportPDFUrl:
    return await report_mutualized_endpoints.get_report_for_date(
        vin,
        datetime.now(UTC).date().isoformat(),
        db,
        s3_client,
        user,
        report_type=ReportType.readout,
    )


@readout_router.get(
    "/{vin}/report/{iso_date}",
    summary="Get readout report for a specific date",
    description="Returns the readout report for a vehicle using the VIN and date (format: YYYY-MM-DD).",
)
async def get_readout_report_for_date(
    vin: str = Path(..., description="VIN requested"),
    iso_date: str = Path(..., description="Date requested in YYYY-MM-DD format"),
    db: AsyncSession = Depends(get_async_db),
    s3_client: AsyncS3 = Depends(get_s3_client_fast),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
) -> ReportPDFUrl:
    return await report_mutualized_endpoints.get_report_for_date(
        vin, iso_date, db, s3_client, user, report_type=ReportType.readout
    )


@readout_router_hidden.get(
    "/{vin}/report_html",
    response_class=HTMLResponse,
    summary="Get readout report HTML",
    description="Generates the HTML content for a readout report by VIN.",
)
async def get_readout_report_html_endpoint(
    vin: str = Path(..., description="VIN requested"),
    db: AsyncSession = Depends(get_async_db),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
) -> HTMLResponse:
    return await report_mutualized_endpoints.generate_html_report(
        vin, db, user, report_type=ReportType.readout
    )


@readout_router_hidden.post(
    "/{vin}/generate_report_sync",
    response_model=ReportSync,
    summary="Generate readout report synchronously",
    description="Generates a readout report for a vehicle by VIN synchronously. The report will be generated immediately and uploaded to S3. This endpoint may take up to 30 seconds to respond.",
)
async def generate_readout_report_sync_endpoint(
    vin: str = Path(..., description="VIN requested"),
    db: AsyncSession = Depends(get_async_db),
    s3_client: AsyncS3 = Depends(get_s3_client_fast),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
) -> ReportSync:
    return await report_mutualized_endpoints.genereate_pdf_report_sync(
        vin, db, s3_client, user, report_type=ReportType.readout
    )
