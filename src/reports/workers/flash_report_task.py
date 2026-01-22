import asyncio
import logging
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import cast
from uuid import UUID

from sqlalchemy.sql import select

from core.sql_utils import get_async_session_maker
from db_models import FlashReportCombination
from db_models.enums import LanguageEnum
from reports.flash_report_utils import generate_flash_report_pdf
from reports.report_config import VERIFY_REPORT_BASE_URL
from reports.workers.celery_config import (
    SMTP_EMAIL,
    SMTP_HOST,
    SMTP_PASSWORD,
    SMTP_PORT,
    SMTP_USER,
    celery_app,
)

logger = logging.getLogger(__name__)


# https://github.com/celery/celery/issues/6552


async def generate_flash_report_pdf_and_send_email_task(
    token: str, email: str, language: str
) -> UUID:
    async with get_async_session_maker()() as db:
        report_uuid = await generate_flash_report_pdf(token=token, db=db)
        vin = (
            await db.execute(
                select(FlashReportCombination.vin).where(
                    FlashReportCombination.token == token
                )
            )
        ).scalar_one_or_none()

        if not vin:
            raise ValueError(f"Vin not found for token {token}")

        await send_email(
            is_french=language == LanguageEnum.FR,
            email=email,
            report_uuid=report_uuid,
            vin=vin,
        )
        return report_uuid


@celery_app.task(
    bind=True,
    name="reports.workers.tasks.generate_pdf_send_email_task",
    max_retries=1,
    default_retry_delay=15,
)
def generate_pdf_send_email_task(
    self, token: str, email: str, language: str
) -> UUID | None:
    try:
        logger.info(f"Starting flash report generation {token}")

        asyncio.run(
            generate_flash_report_pdf_and_send_email_task(
                token=token, email=email, language=language
            )
        )
    except Exception as exc:
        logger.error(f"Error generating PDF for token {token}: {exc}", exc_info=True)
        logger.warning(f"Retry in 15s (attempt {self.request.retries + 1}/2)")
        raise self.retry(exc=exc) from exc


async def send_email(is_french: bool, email: str, report_uuid: UUID, vin: str) -> None:
    sender_email = cast(str, SMTP_EMAIL)  # Checked at worker_init
    password = cast(str, SMTP_PASSWORD)
    smtp_server = cast(str, SMTP_HOST)
    port = cast(int, SMTP_PORT)
    smtp_user = cast(str, SMTP_USER)

    link = f"{VERIFY_REPORT_BASE_URL}/{report_uuid}"

    vin = vin[:-4] + "<strong>" + vin[-4:] + "</strong>"
    # /!\ Link is not sent if frontend does not include "https":
    # in dev the <a> element won't have a href in the email

    message = MIMEMultipart("alternative")
    message["Subject"] = (
        "Bib a estimé l'état de santé de la batterie de votre véhicule"
        if is_french
        else "Bib estimated your battery state-of-health"
    )
    message["From"] = sender_email
    message["To"] = email
    message["Content-Type"] = "text/html; charset=UTF-8"
    message["X-Mailer"] = "Python SMTP"

    html_en = f"""\
    <!DOCTYPE html>
    <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
        </head>
        <body style="font-family: Arial, sans-serif; line-height: 1.2; color: #333;">
            <div style="max-width: 650px; margin: 0; padding: 20px;">
                <p>Hello,</p>
                <p>Good news, your battery SoH estimation is ready!</p>
                <p>This report was generated for the VIN: {vin}</p>
                <div style="text-align: center; margin: 20px 0;">
                    <a
                        href="{link}"
                        style="
                            display: inline-block;
                            padding: 12px 24px;
                            font-size: 16px;
                            color: white !important;
                            background-color: #2d6d49;
                            text-decoration: none;
                            border-radius: 5px;
                            margin-bottom: 10px;
                            font-weight: bold;
                        "
                        target="_blank"
                    >
                        Download my report
                    </a>
                </div>
                <p>
                    ➤ Learn more about battery SoH
                    <a href="https://bib-batteries.com/" style="color: #2d6d49;">
                        on our website.
                    </a>
                </p>
                <p>
                    If you have any questions or encounter any issues, please
                    <a href="mailto:support@bib-batteries.fr" style="color: #2d6d49;">
                        contact our support team.
                    </a>
                </p>
                <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;">
                <p style="color: #666; font-size: 14px; line-height: 1.6;">
                    Best regards,<br>
                    The Bib Team<br>
                </p>
            </div>
        </body>
    </html>
    """

    html_fr = f"""\
    <!DOCTYPE html>
    <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
        </head>
        <body style="font-family: Arial, sans-serif; line-height: 1.2; color: #333;">
            <div style="max-width: 800px; margin: 0; padding: 20px;">
                <p>Bonjour,</p>
                <p>Bonne nouvelle, l'estimation de l'état de santé de votre batterie est prête !</p>
                <p>Ce rapport a été generé pour le VIN: {vin}</p>
                <div style="text-align: center; margin: 20px 0;">
                    <a
                        href="{link}"
                        style="
                            display: inline-block;
                            padding: 12px 24px;
                            font-size: 16px;
                            color: white !important;
                            background-color: #2d6d49;
                            text-decoration: none;
                            border-radius: 5px;
                            margin-bottom: 10px;
                            font-weight: bold;
                        "
                        target="_blank"
                    >
                        Télécharger mon rapport
                    </a>
                </div>
                <p>
                    ➤ Apprenez en plus sur l'état de santé des batteries
                    <a href="https://bib-batteries.com/" style="color: #2d6d49;">
                        sur notre site web.
                    </a>
                </p>
                <p>
                    Pour toute question ou si vous rencontrez un problème,
                    <a href="mailto:support@bib-batteries.fr" style="color: #2d6d49;">
                    contactez notre équipe de support.
                    </a>
                </p>
                <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;">
                <p style="color: #666; font-size: 14px; line-height: 1.6;">
                    Cordialement,<br>
                    L'équipe Bib<br>
                </p>
            </div>
        </body>
    </html>
    """

    part = MIMEText(html_fr if is_french else html_en, "html")
    message.attach(part)
    context = ssl.create_default_context()

    with smtplib.SMTP(smtp_server, port, timeout=10) as server:
        server.starttls(context=context)
        server.login(smtp_user, password)
        server.sendmail(sender_email, email, message.as_string())
