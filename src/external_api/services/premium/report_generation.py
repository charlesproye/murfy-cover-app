"""Service for generating premium reports synchronously."""

import logging
import time
import uuid
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from core.s3.async_s3 import S3ACL, AsyncS3
from db_models.company import Oem
from db_models.report import PremiumReport
from db_models.vehicle import (
    Battery,
    Vehicle,
    VehicleData,
    VehicleModel,
)
from external_api.core.config import settings
from reports.report_render.premium_report_generator import PremiumReportGenerator

logger = logging.getLogger(__name__)


async def generate_premium_report_sync(
    vehicle: Vehicle,
    vehicle_model: VehicleModel,
    battery: Battery,
    oem: Oem,
    vehicle_data: VehicleData,
    image_url: str | None,
    db: AsyncSession,
    s3_client: AsyncS3,
    gotenberg_url: str = settings.GOTENBERG_URL,
    s3_bucket: str = settings.PREMIUM_REPORT_S3_BUCKET,
) -> str:
    """
    Generate a premium PDF report synchronously and upload to S3.

    Args:
        vehicle: Vehicle DB model
        vehicle_model: VehicleModel DB model
        battery: Battery DB model
        oem: Oem DB model
        vehicle_data: Latest VehicleData DB model
        db: Database session
        s3_client: AsyncS3 client instance (injected dependency)
        gotenberg_url: URL of the Gotenberg service
        s3_bucket: S3 bucket name for storing PDFs

    Returns:
        S3 URL of the generated PDF

    Raises:
        ValueError: If required data is missing
        Exception: If PDF generation or upload fails
    """
    vin = vehicle.vin
    logger.info(f"Starting synchronous PDF generation for VIN: {vin}")

    # Initialize the premium report generator
    generator = PremiumReportGenerator(gotenberg_url=gotenberg_url)

    report_uuid = str(uuid.uuid4())
    report_data = await generator.generate_premium_report_data(
        vehicle=vehicle,
        vehicle_model=vehicle_model,
        battery=battery,
        oem=oem,
        vehicle_data=vehicle_data,
        image_url=image_url,
        report_uuid=report_uuid,
    )

    # Use the main template which includes both pages with proper page breaks
    html_content = generator.render_template(
        data=report_data,
        embed_assets=True,
    )

    # Generate PDF asynchronously (non-blocking)
    pdf_bytes = await generator.generate_pdf(
        html_content=html_content,
    )

    logger.info(
        f"PDF generated successfully for VIN {vin}, size: {len(pdf_bytes) / 1024:.2f} KB"
    )

    start = time.time()
    # Upload to S3 using persistent client (reused across all requests)
    s3_path = f"sync/{datetime.now().strftime('%Y%m%d')}/{vin}_{uuid.uuid4()}.pdf"
    await s3_client.upload_file_fast(
        s3_path, pdf_bytes, acl=S3ACL.PUBLIC_READ, bucket=s3_bucket
    )

    # Construct public URL
    url = f"https://{s3_bucket}.s3.fr-par.scw.cloud/{s3_path}"

    # Save to database
    premium_report = PremiumReport(
        vehicle_id=vehicle.id,
        report_url=url,
        id=uuid.UUID(report_uuid),
        task_id=None,  # No task ID for sync generation
    )
    db.add(premium_report)
    await db.commit()

    logger.info(
        f"PDF uploaded to S3 and saved to database: {url} in {time.time() - start:.2f} seconds"
    )

    return url
