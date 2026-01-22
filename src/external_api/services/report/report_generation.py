"""Service for generating premium reports synchronously."""

import logging
from datetime import UTC, date, datetime, timedelta

from fastapi.exceptions import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from core.s3.async_s3 import AsyncS3
from db_models.report import ReportType
from external_api.core.config import settings
from external_api.schemas.report import ReportPDFUrl
from reports import reports_utils

logger = logging.getLogger(__name__)


async def get_report_by_date(
    vin: str,
    report_date: date,
    db: AsyncSession,
    s3_client: AsyncS3,
    report_type: ReportType,
) -> ReportPDFUrl:
    existing_report = await reports_utils.get_db_report_by_date(
        vin, report_date, db, report_type=report_type
    )
    if not existing_report:
        raise HTTPException(
            status_code=404,
            detail=f"Report not found for VIN={vin} and date={report_date.isoformat()}",
        )

    url = await s3_client.get_presigned_url(
        s3_uri=existing_report.report_url,
        expires_in=settings.PREMIUM_REPORT_S3_SIGNED_URI_EXPIRES_IN,
    )

    expires_at = datetime.now(UTC) + timedelta(
        seconds=settings.PREMIUM_REPORT_S3_SIGNED_URI_EXPIRES_IN
    )

    return ReportPDFUrl(
        vin=vin,
        url=url,
        expires_at=expires_at,
    )
