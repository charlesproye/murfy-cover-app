import hashlib
import os
import smtplib
import ssl
import uuid
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import numpy as np
import pandas as pd
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from activation.config.mappings import mapping_vehicle_type
from external_api.services.flash_report.vin_decoder.tesla_vin_decoder import (
    TeslaVinDecoder,
)
from external_api.services.flash_report.vin_decoder.vin_decoder import VinDecoder


async def get_has_trendline(make: str, db: AsyncSession):
    query_has_trendline = """
        SELECT count(*)
        FROM vehicle_model vm
        left join oem o
        on o.id = vm.oem_id
    """
    result = await db.execute(text(query_has_trendline), {"make": make})
    return result.fetchone()[0] > 0


async def get_db_names(
    make: str,
    model: str,
    type: str | None = None,
    db: AsyncSession = None,
):
    query_all_models = """
        SELECT vm.model_name, vm.id, vm.type, vm.commissioning_date, vm.end_of_life_date,
               m.make_name, b.capacity
        FROM vehicle_model vm
        LEFT JOIN make m ON vm.make_id = m.id
        LEFT JOIN battery b ON b.id = vm.battery_id
        LEFT JOIN oem o ON o.id = vm.oem_id
    """
    result = await db.execute(text(query_all_models))
    rows = result.fetchall()
    columns = result.keys()
    model_existing = pd.DataFrame(rows, columns=columns)

    vehicle_model_id = mapping_vehicle_type(
        type or "", make, model, model_existing, None
    )

    if vehicle_model_id is None:
        return None, None

    query_model = """
        SELECT o.oem_name, vm.model_name
        FROM vehicle_model vm
        LEFT JOIN make m ON vm.make_id = m.id
        LEFT JOIN battery b ON b.id = vm.battery_id
        LEFT JOIN oem o ON o.id = vm.oem_id
        WHERE vm.id = :vehicle_model_id
    """
    result = await db.execute(text(query_model), {"vehicle_model_id": vehicle_model_id})
    record = result.mappings().first()

    if not record:
        return None, None

    return record["oem_name"], record["model_name"]


async def send_vehicle_specs(vin: str, db: AsyncSession):
    tesla_vin_decoder = TeslaVinDecoder()
    result = tesla_vin_decoder.decode(vin)

    if not result or not result.get("Country"):
        vin_decoder = VinDecoder()
        make, model = vin_decoder.decode(vin)

        oem_name, model_name = await get_db_names(make, model, type=None, db=db)

        has_trendline = await get_has_trendline(oem_name, db)

        return {
            "has_trendline": has_trendline,
            "make": oem_name,
            "model": model_name,
            "type": None,
            "version": None,
        }

    oem_name, model_name = await get_db_names(
        "tesla",
        result.get("Model"),
        result.get("Type"),
        db=db,
    )

    has_trendline = await get_has_trendline(oem_name, db)

    return {
        "has_trendline": has_trendline,
        "make": "tesla",
        "model": model_name,
        "type": result.get("Type"),
        "version": result.get("Version"),
    }


async def insert_combination(
    make: str, model: str, type: str, version: str, odometer: int, db: AsyncSession
):
    # Générer un token unique basé sur les valeurs de la combinaison
    token_input = f"{make}-{model}-{type}-{version}-{odometer}"
    token = hashlib.sha256(token_input.encode("utf-8")).hexdigest()

    id = uuid.uuid4()

    insert_query = """
        INSERT INTO flash_report_combination (id, make, model, type, version, odometer, token)
        VALUES (:id, :make, :model, :type, :version, :odometer, :token)
    """
    try:
        await db.execute(
            text(insert_query),
            {
                "id": id,
                "make": make,
                "model": model,
                "type": type,
                "version": version,
                "odometer": odometer,
                "token": token,
            },
        )
    except IntegrityError:
        return token

    await db.commit()

    return token


# TODO:
# - Demander à Anna ou Jo une repasse sur le mail ->  En cours
async def send_email(email: str, token: str):
    sender_email = os.getenv("SMTP_EMAIL")
    password = os.getenv("SMTP_PASSWORD")
    smtp_server = os.getenv("SMTP_HOST")
    port = os.getenv("SMTP_PORT")
    frontend_url = os.getenv("FRONTEND_URL")
    link = f"{frontend_url}/flash-report/generation?token={token}"

    message = MIMEMultipart("alternative")
    message["Subject"] = "Bib a estimé l'état de santé de la batterie de votre véhicule"
    message["From"] = sender_email
    message["To"] = email

    html = f"""\
    <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
                    <p>Bonjour,</p>
                    <p>Bonne nouvelle ! L'estimation de l'état de santé de votre batterie est prête !</p>
                    <p>
                    <a href="{link}" style="
                        display: inline-block;
                        padding: 10px 20px;
                        font-size: 16px;
                        color: white;
                        background-color: #007bff;
                        text-decoration: none;
                        border-radius: 5px;">
                        Télécharger mon rapport
                    </a>
                    </p>
                <p>If you have any questions or encounter any issues, please don't hesitate to contact our support team.</p>
                <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;">
                <p style="color: #666; font-size: 14px;">
                    Best regards,<br>
                    The Bib BMT Team<br>
                </p>
            </div>
        </body>
    </html>
    """

    part = MIMEText(html, "html")
    message.attach(part)
    context = ssl.create_default_context()

    try:
        with smtplib.SMTP(smtp_server, port, timeout=10) as server:
            server.starttls(context=context)
            server.login(sender_email, password)
            server.sendmail(sender_email, email, message.as_string())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Email error: {e!s}") from e


async def get_soh_from_trendline(trendline: str, odometer: int):
    soh_estimated = eval(trendline, {"np": np, "x": odometer})

    return soh_estimated


async def send_flash_report_data(
    vin: str,
    make: str,
    model: str,
    type: str,
    odometer: int,
    version: str | None = None,
    db: AsyncSession = None,
):
    if make == "tesla" and not version:
        select_version_query = """
            SELECT version
            FROM vehicle_model vm
            left join oem o
            on o.id = vm.oem_id
            where o.oem_name = :make
            and vm.model_name = :model
            and vm.type = :type
            and vm.trendline is not null
        """

        result = await db.execute(
            text(select_version_query),
            {"make": make, "model": model, "type": type},
        )

        version = result.fetchone()[0]

    if version:
        select_query = """
            SELECT oem_name,
                   model_name,
                   type,
                   version,
                   vm.url_image,
                   vm.warranty_date,
                   vm.warranty_km,
                   autonomy,
                   battery_chemistry,
                   net_capacity,
                   capacity,
                   vm.trendline,
                   vm.trendline_min, vm.trendline_max
            FROM vehicle_model vm
            left join oem o
            on o.id = vm.oem_id
            left join battery b
            on b.id = vm.battery_id
            where 1=1
            and o.oem_name=:make
            and vm.model_name = :model
            and vm.type = :type
            and vm.version = :version
        """

        result = await db.execute(
            text(select_query),
            {"make": make, "model": model, "type": type, "version": version},
        )
    else:
        select_query = """
            SELECT oem_name,
                   model_name,
                   type,
                   version,
                   vm.url_image,
                   vm.warranty_date,
                   vm.warranty_km,
                   autonomy,
                   battery_chemistry,
                   net_capacity,
                   capacity,
                   vm.trendline,
                   vm.trendline_min, vm.trendline_max
            FROM vehicle_model vm
            left join oem o
            on o.id = vm.oem_id
            left join battery b
            on b.id = vm.battery_id
            where 1=1
            and o.oem_name=:make
            and vm.model_name = :model
            and vm.type = :type
        """

        result = await db.execute(
            text(select_query),
            {"make": make, "model": model, "type": type},
        )

    row = result.mappings().first()
    if not row:
        return None

    if not row["trendline"]:
        trendline = None
        trendline_min = None
        trendline_max = None
        status = False
        soh_estimated = None
    else:
        trendline = row["trendline"]["trendline"]
        trendline_min = row["trendline_min"]["trendline"]
        trendline_max = row["trendline_max"]["trendline"]
        status = True
        soh_estimated = await get_soh_from_trendline(trendline, odometer)

    return {
        "has_trendline": status,
        "vehicle_info": {
            "vin": vin,
            "brand": row["oem_name"],
            "model": row["model_name"],
            "version": None if row["version"] == "unknown" else row["version"],
            "type": row["type"],
            "mileage": odometer,
            "image": row["url_image"],
            "warranty_date": row["warranty_date"],
            "warranty_km": row["warranty_km"],
        },
        "battery_info": {
            "chemistry": row["battery_chemistry"],
            "net_capacity": row["net_capacity"],
            "capacity": row["capacity"],
            "range": row["autonomy"],
            "trendline": trendline,
            "trendline_min": trendline_min,
            "trendline_max": trendline_max,
            "soh_estimated": soh_estimated,
        },
    }

