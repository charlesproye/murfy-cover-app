import asyncio
import logging

from db_models import PremiumReport
from reports.workers.celery_config import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True,
    name="reports.workers.tasks.generate_pdf_task",
    max_retries=3,
    default_retry_delay=60,
)
def generate_pdf_task(self, vin: str) -> str:
    try:
        logger.info(f"Starting report generation for VIN: {vin}")

        from reports.report_generator import (
            ReportGenerator,  # Import here to avoid importing in external_api
        )

        report_generator = ReportGenerator()

        urls = asyncio.run(report_generator.download_pdfs_for_vins([vin]))

        if not urls or len(urls) == 0:
            logger.error(f"No PDF generated for VIN: {vin}")
            raise ValueError(f"No PDF generated for VIN: {vin}")

        task_id = self.request.id
        asyncio.run(report_generator.load_to_db(urls, task_id))

        with report_generator.session_factory() as session:
            pdf_path = (
                session.query(PremiumReport)
                .filter_by(task_id=task_id)
                .first()
                .report_url
            )

        logger.info(f"PDF generated successfully for VIN {vin}: {pdf_path}")
        return pdf_path

    except Exception as exc:
        logger.error(f"Error generating PDF for VIN {vin}: {exc}", exc_info=True)
        logger.warning(f"Retry in 60ss (attempt {self.request.retries + 1}/3)")
        raise self.retry(exc=exc) from exc
