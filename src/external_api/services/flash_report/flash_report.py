import smtplib
import ssl
import uuid
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
from fastapi import HTTPException, status
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from activation.config.mappings import mapping_vehicle_type
from db_models.vehicle import Battery, FlashReportCombination, Make, VehicleModel
from external_api.core import utils
from external_api.core.config import settings
from external_api.schemas.flash_report import GenerationData, VehicleSpecs
from external_api.services.flash_report.vin_decoder.tesla_vin_decoder import (
    TeslaVinDecoder,
)
from external_api.services.flash_report.vin_decoder.vin_decoder import VinDecoder


async def get_make_has_trendline(make_name: str, db: AsyncSession) -> bool:
    query_has_trendline = """
        SELECT count(*)
        FROM vehicle_model vm
        left join make m on m.id = vm.make_id
        WHERE vm.trendline->>'trendline' is not null AND m.make_name = :make_name
    """
    result = await db.execute(text(query_has_trendline), {"make_name": make_name})
    return result.fetchone()[0] > 0


async def get_db_names(
    make: str,
    model: str,
    type: str | None = None,
    db: AsyncSession = None,
) -> tuple[str | None, str | None]:
    query_all_models = (
        select(
            VehicleModel.model_name,
            VehicleModel.id,
            VehicleModel.type,
            VehicleModel.commissioning_date,
            VehicleModel.end_of_life_date,
            Make.make_name,
            Battery.capacity,
        )
        .select_from(VehicleModel)
        .join(Make, VehicleModel.make_id == Make.id, isouter=True)
        .join(Battery, VehicleModel.battery_id == Battery.id, isouter=True)
    )

    result = await db.execute(query_all_models)
    rows = result.fetchall()
    columns = result.keys()
    model_existing = pd.DataFrame(rows, columns=columns)

    vehicle_model_id = mapping_vehicle_type(
        type or "", make, model, model_existing, None
    )

    if vehicle_model_id is None:
        return None, None

    query_model = """
        SELECT m.make_name, vm.model_name
        FROM vehicle_model vm
        LEFT JOIN make m ON vm.make_id = m.id
        LEFT JOIN battery b ON b.id = vm.battery_id
        WHERE vm.id = :vehicle_model_id
    """
    result = await db.execute(text(query_model), {"vehicle_model_id": vehicle_model_id})
    record = result.mappings().first()

    if not record:
        return None, None

    return record["make_name"], record["model_name"]


async def send_vehicle_specs(vin: str, db: AsyncSession) -> VehicleSpecs:
    if len(vin) != 17:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="VIN must be 17 characters long",
        )

    tesla_vin_decoder = TeslaVinDecoder()
    result = tesla_vin_decoder.decode(vin)

    if result and result.get("Country"):
        make_db_name, model_db_name = await get_db_names(
            "tesla",
            result.get("Model"),
            result.get("Type"),
            db=db,
        )

        has_trendline = await get_make_has_trendline(make_db_name, db)

        return VehicleSpecs(
            has_trendline=has_trendline,
            make="tesla",
            model=model_db_name,
            type=result.get("Type"),
            version=result.get("Version"),
        )
    else:
        vin_decoder = VinDecoder()
        make, model = vin_decoder.decode(vin)

        make_db_name, model_db_name = await get_db_names(make, model, type=None, db=db)

        has_trendline = await get_make_has_trendline(make_db_name, db)

        return VehicleSpecs(
            has_trendline=has_trendline,
            make=make_db_name,
            model=model_db_name,
            type=None,
            version=None,
        )


async def insert_combination(
    vin: str,
    make: str,
    model: str,
    vehicle_type: str,
    version: str | None,
    odometer: int,
    db: AsyncSession,
) -> str:
    token = str(uuid.uuid4())
    db.add(
        FlashReportCombination(
            vin=vin,
            make=make,
            model=model,
            type=vehicle_type,
            version=version,
            odometer=odometer,
            token=token,
        )
    )
    await db.flush()
    await db.commit()

    return token


async def send_email(is_french: bool, email: str, token: str) -> None:
    sender_email = settings.SMTP_EMAIL
    password = settings.SMTP_PASSWORD
    smtp_server = settings.SMTP_HOST
    port = settings.SMTP_PORT
    frontend_url = settings.FRONTEND_URL
    link = f"{frontend_url}/flash-report/generation?token={token}"
    # /!\ Link is not sent if frontend does not include "https":
    # in dev the <a> element won't have a href in the email

    message = MIMEMultipart("alternative")
    message["Subject"] = (
        "Bib a estimé l'état de santé de la batterie de votre véhicule"
        if is_french
        else "Bib has estimated the health status of your battery"
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
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
                <p>Hello,</p>
                <p>Good news! The health status estimation of your battery is ready!</p>
                <div style="text-align: center; margin: 20px 0;">
                    <a
                        href="{link}"
                        style="
                            display: inline-block;
                            padding: 12px 24px;
                            font-size: 16px;
                            color: white !important;
                            background-color: #007bff;
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
                    If you have any questions or encounter any issues, please don't hesitate to
                    <a href="mailto:support@bib-batteries.fr" style="color: #007bff;">
                        contact our support team.
                    </a>
                </p>
                <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;">
                <p style="color: #666; font-size: 14px;">
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
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
                <p>Bonjour,</p>
                <p>Bonne nouvelle ! L'estimation de l'état de santé de votre batterie est prête !</p>
                <div style="text-align: center; margin: 20px 0;">
                    <a
                        href="{link}"
                        style="
                            display: inline-block;
                            padding: 12px 24px;
                            font-size: 16px;
                            color: white !important;
                            background-color: #007bff;
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
                    Pour toutes questions ou si vous rencontrez un problème,
                    <a href="mailto:support@bib-batteries.fr" style="color: #007bff;">
                    contactez notre équipe de support.
                    </a>
                </p>
                <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;">
                <p style="color: #666; font-size: 14px;">
                    Cordialement,<br>
                    The Bib Team<br>
                </p>
            </div>
        </body>
    </html>
    """

    part = MIMEText(html_fr if is_french else html_en, "html")
    message.attach(part)
    context = ssl.create_default_context()

    try:
        with smtplib.SMTP(smtp_server, port, timeout=10) as server:
            server.starttls(context=context)
            server.login(settings.SMTP_USER, password)
            server.sendmail(sender_email, email, message.as_string())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Email error: {e!s}") from e


def get_soh_from_trendline(trendline: str, odometer: int):
    return utils.numpy_safe_eval(trendline, x=odometer)


async def get_flash_report_data(
    flash_report_combination: FlashReportCombination,
    db: AsyncSession = None,
) -> GenerationData:
    version = flash_report_combination.version

    # If Tesla and no version, get version from vehicle_model
    if flash_report_combination.make == "tesla" and not version:
        stmt = (
            select(VehicleModel.version)
            .join(Make, Make.id == VehicleModel.make_id)
            .where(
                (Make.make_name == flash_report_combination.make)
                & (VehicleModel.model_name == flash_report_combination.model)
                & (VehicleModel.type == flash_report_combination.type)
                & VehicleModel.trendline.is_not(None)
            )
        )
        result = await db.execute(stmt)
        version = result.scalar_one_or_none()

    version = version or "unknown"

    # Get vehicle model with battery info
    stmt = (
        select(VehicleModel, Battery)
        .outerjoin(Make, Make.id == VehicleModel.make_id)
        .outerjoin(Battery, Battery.id == VehicleModel.battery_id)
        .where(
            (Make.make_name == flash_report_combination.make)
            & (VehicleModel.model_name == flash_report_combination.model)
            & (VehicleModel.type == flash_report_combination.type)
            & (VehicleModel.version == version)
        )
    )
    result = await db.execute(stmt)
    row = result.first()

    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"Cannot get back vehicle model from token {flash_report_combination.make=}, {flash_report_combination.model=}, {flash_report_combination.type=}, {version=}.",
        )

    vehicle_model, battery = row

    if not vehicle_model.trendline:
        trendline = None
        trendline_min = None
        trendline_max = None
        status = False
        soh = None
    else:
        trendline = vehicle_model.trendline["trendline"]
        trendline_min = vehicle_model.trendline_min["trendline"]
        trendline_max = vehicle_model.trendline_max["trendline"]
        status = True
        soh = get_soh_from_trendline(trendline, flash_report_combination.odometer)

    return GenerationData(
        has_trendline=status,
        vehicle_info={
            "vin": flash_report_combination.vin,
            "brand": flash_report_combination.make,
            "model": flash_report_combination.model,
            "version": version if version and version != "unknown" else None,
            "type": flash_report_combination.type
            if flash_report_combination.type
            and flash_report_combination.type != "unknown"
            else None,
            "mileage": flash_report_combination.odometer,
            "image_url": vehicle_model.url_image,
            "warranty_date": vehicle_model.warranty_date,
            "warranty_km": vehicle_model.warranty_km,
        },
        battery_info={
            "oem": battery.battery_oem if battery else None,
            "chemistry": battery.battery_chemistry if battery else None,
            "net_capacity": battery.net_capacity if battery else None,
            "capacity": battery.capacity if battery else None,
            "consumption": None,
            "range": vehicle_model.autonomy,
            "trendline": trendline,
            "trendline_min": trendline_min,
            "trendline_max": trendline_max,
            "soh": soh,
        },
    )

