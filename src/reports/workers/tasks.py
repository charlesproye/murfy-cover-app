import asyncio
import logging

from sqlalchemy.sql import select

from db_models import PremiumReport
from reports.workers.celery_config import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True,
    name="reports.workers.tasks.generate_pdf_task",
    max_retries=2,
    default_retry_delay=15,
)
def generate_pdf_task(self, vin: str) -> str | None:
    try:
        logger.info(f"Starting report generation for VIN: {vin}")

        pdf_uri = asyncio.run(async_generate_pdf_task(vin, self.request.id))

        logger.info(f"PDF generated successfully for VIN {vin}: {pdf_uri}")
        return pdf_uri
    except Exception as exc:
        logger.error(f"Error generating PDF for VIN {vin}: {exc}", exc_info=True)
        logger.warning(f"Retry in 15s (attempt {self.request.retries + 1}/2)")
        raise self.retry(exc=exc) from exc


async def async_generate_pdf_task(vin: str, task_id: str) -> str | None:
    from reports.report_generator import (
        ReportGenerator,  # Import here to avoid importing in external_api
    )

    report_generator = ReportGenerator()

    try:
        urls = await report_generator.download_pdfs_for_vins([vin])

        if not urls or len(urls) == 0:
            logger.error(f"No PDF generated for VIN: {vin}")
            raise ValueError(f"No PDF generated for VIN: {vin}")

        await report_generator.add_reports_to_db(urls, task_id)

        async with report_generator.async_session_factory() as session:
            pdf_uri = await session.execute(
                select(PremiumReport.report_url).where(PremiumReport.task_id == task_id)
            )
            pdf_uri = pdf_uri.scalar_one_or_none()
            return pdf_uri
    finally:
        await report_generator.close()
