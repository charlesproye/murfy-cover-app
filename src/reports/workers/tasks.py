import asyncio
import logging

from sqlalchemy.sql import select

from db_models import Report
from db_models.report import ReportType
from reports.workers.celery_config import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True,
    name="reports.workers.tasks.generate_pdf_task",
    max_retries=2,
    default_retry_delay=15,
)
def generate_pdf_task(self, vin: str, report_type: ReportType) -> str | None:
    try:
        logger.info(f"Starting report generation for VIN: {vin}")

        pdf_uri = asyncio.run(
            async_generate_pdf_task(
                vin=vin, task_id=self.request.id, report_type=report_type
            )
        )

        logger.info(f"PDF generated successfully for VIN {vin}: {pdf_uri}")
        return pdf_uri
    except Exception as exc:
        logger.error(f"Error generating PDF for VIN {vin}: {exc}", exc_info=True)
        logger.warning(f"Retry in 15s (attempt {self.request.retries + 1}/2)")
        raise self.retry(exc=exc) from exc


async def async_generate_pdf_task(
    vin: str, task_id: str, report_type: ReportType
) -> str | None:
    from reports.gsheet_report_generator import (
        GSheetReportGenerator,  # Import here to avoid importing in external_api
    )

    report_generator = GSheetReportGenerator()

    try:
        urls = await report_generator.download_pdfs_for_vins([vin])

        if not urls or len(urls) == 0:
            logger.error(f"No PDF generated for VIN: {vin}")
            raise ValueError(f"No PDF generated for VIN: {vin}")

        await report_generator.add_reports_to_db(urls, task_id)

        async with report_generator.async_session_factory() as session:
            pdf_uri = await session.execute(
                select(Report.report_url).where(Report.task_id == task_id)
            )
            pdf_uri = pdf_uri.scalar_one_or_none()
            return pdf_uri
    finally:
        await report_generator.close()
