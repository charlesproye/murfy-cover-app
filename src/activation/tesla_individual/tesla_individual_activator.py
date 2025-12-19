import logging
from datetime import datetime
from pathlib import Path

import aiohttp
import requests
from sqlalchemy import insert, select
from sqlalchemy.orm import Session

from activation.tesla_individual.api.credentials import (
    FLEET_TELEMETRY_CERT_PATH,
    TESLA_CLIENT_ID,
    TESLA_CLIENT_SECRET,
    TESLA_VEHICLE_COMMAND_CERT_PATH,
    TESLA_VEHICLE_COMMAND_KEY_PATH,
)
from activation.tesla_individual.config import TESLA_TELEMETRY_CONFIG
from core.encrypt_utils import Encrypter
from core.env_utils import get_env_var
from core.slack_utils import send_slack_message
from core.tesla.tesla_individual_api import TeslaIndividualApi
from core.tesla.tesla_utils import TeslaRegions
from db_models import Vehicle, VehicleModel
from db_models.user_tokens import User, UserToken

logger = logging.getLogger(__name__)


class TeslaIndividualActivator:
    def __init__(self):
        self.client_id = TESLA_CLIENT_ID
        self.client_secret = TESLA_CLIENT_SECRET
        self.region = TeslaRegions.EUROPE
        self.encrypter = Encrypter()

    def _format_error_slack_notification(self, vin: str, reason: str):
        if datetime.now().hour >= 16 and datetime.now().hour < 17:
            slack_channel_id = get_env_var("METRIC_CHANNEL_ID")
            message = (
                f"❌ Information - Particulier Tesla\nVIN: {vin}\nMotif: {reason}\n"
            )
            send_slack_message(slack_channel_id, message)

    def _format_validation_slack_notification(self, vin: str, reason: str):
        if datetime.now().hour >= 16 or datetime.now().hour < 17:
            slack_channel_id = get_env_var("METRIC_CHANNEL_ID")
            message = (
                f"✅ Information - Particulier Tesla\nVIN: {vin}\nMotif: {reason}\n"
            )
            send_slack_message(slack_channel_id, message)

    async def check_refresh_token(
        self, vin: str, db: Session, tesla_api: TeslaIndividualApi
    ) -> str | None:
        user_query = select(User).where(User.vin == vin)
        user_result = db.execute(user_query)
        user = user_result.scalar_one_or_none()

        if user is None:
            logger.error(f"User not found for VIN: {vin}")
            self._format_error_slack_notification(
                vin, f"Ce véhicule {vin} n'a pas d'utilisateur dans la base User."
            )
            return None

        user_token_query = (
            select(UserToken)
            .where(UserToken.user_id == user.id)
            .order_by(UserToken.created_at.desc())
            .limit(1)
        )
        user_token: UserToken | None = db.execute(user_token_query).scalar_one_or_none()

        if user_token is None:
            self._format_error_slack_notification(
                vin,
                f"Ce véhicule {vin} n'a pas encore reçu le consentement de l'utilisateur.",
            )
            return None

        if user_token.refresh_token is None:
            tokens = await tesla_api.get_token(
                code=self.encrypter.decrypt(user_token.code),
                redirect_uri=user_token.callback_url,
                region=user.region,
            )
        else:
            tokens = await tesla_api.refresh_token(
                refresh_token=user_token.refresh_token,
            )

        if tokens is None:
            self._format_error_slack_notification(
                vin,
                f"Le refresh token de l'utilisateur {user.id} pour le véhicule {vin} n'a pas pu être rafraîchi. Le token est probablement expiré.",
            )
            return None

        user_token.refresh_token = tokens.refresh_token
        user_token.access_token = tokens.access_token

        db.commit()

        return user_token.access_token

    def activate_vehicle(self, vin: str, db: Session, token: str):
        service_url = get_env_var("TESLA_VEHICLE_COMMAND_SERVICE_URL")
        # The file itself is a PEM file (multiple lines), we just reaad it as it is
        fleet_cert = Path(FLEET_TELEMETRY_CERT_PATH).read_text()

        payload = {
            "config": {
                **TESLA_TELEMETRY_CONFIG,
                "hostname": get_env_var("FLEET_TELEMETRY_HOSTNAME"),
                "ca": fleet_cert,
            },
            "vins": [vin],
        }

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        error_msg = None

        try:
            response = requests.post(
                f"{service_url}/api/1/vehicles/fleet_telemetry_config",
                json=payload,
                headers=headers,
                cert=(TESLA_VEHICLE_COMMAND_CERT_PATH, TESLA_VEHICLE_COMMAND_KEY_PATH),
                verify=False,
                timeout=30,
            )

            if "error" in response.json():
                error_msg = response.json()["error"]

            response.raise_for_status()

            return {
                "success": True,
                "vin": vin,
                "response": response.json() if response.text else None,
            }

        except requests.exceptions.SSLError as e:
            logger.error(f"SSL error activating vehicle {vin}: {e}")
            self._format_error_slack_notification(vin, f"Erreur SSL: {e}")
            return {"success": False, "error": f"SSL error: {e}", "vin": vin}
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error activating vehicle {vin}: {e} - {error_msg}")
            if error_msg:
                self._format_error_slack_notification(
                    vin,
                    f"En effectuant la requête d'activation du véhicule {vin}, une erreur est survenue: {error_msg}",
                )
            else:
                self._format_error_slack_notification(vin, f"{e}")
            return {
                "success": False,
                "error": f"Request error: {e} - {error_msg}",
                "vin": vin,
            }

    async def insert_vehicle_in_db(
        self, vin: str, db: Session, token: str, tesla_api: TeslaIndividualApi
    ):
        version = await tesla_api.get_version(vin, token)
        start_date = await tesla_api.get_start_date(vin, token)
        vehicle_model_query = select(VehicleModel.id).where(
            VehicleModel.version == version
        )
        vehicle_model_id = db.execute(vehicle_model_query).scalar_one_or_none()
        fleet_id = "7821479c-c38c-494c-bd7d-98153b952605"  # Bib
        region_id = "4032de92-ae96-4061-980e-2d633f7228a8"  # Undefinied

        if not vehicle_model_id:
            logger.error(f"No vehicle model found for version {version}")
            self._format_error_slack_notification(
                vin,
                f"Pas de modèle trouvé pour la version {version} de la tesla {vin}",
            )
            return None

        vehicle_query = insert(Vehicle).values(
            vin=vin,
            vehicle_model_id=vehicle_model_id,
            fleet_id=fleet_id,
            start_date=start_date,
            activation_status=True,
            is_eligible=True,
            region_id=region_id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        db.execute(vehicle_query)
        db.commit()

        self._format_validation_slack_notification(vin, "Véhicule activé avec succès")

        logger.info(f"Vehicle {vin} inserted in DB")

    async def run(self, db: Session):
        user_query = select(User.vin)
        user_result = db.execute(user_query)
        vins = user_result.scalars().all()

        async with aiohttp.ClientSession() as session:
            tesla_api = TeslaIndividualApi(
                client_id=self.client_id,
                client_secret=self.client_secret,
                region=self.region,
                session=session,
            )

            for vin in vins:
                vehicle_query = select(Vehicle).where(Vehicle.vin == vin)
                vehicle_result = db.execute(vehicle_query)
                vehicle: Vehicle | None = vehicle_result.scalar_one_or_none()

                if vehicle:
                    logger.info(f"Vehicle {vin} already exists in DB")
                    continue

                try:
                    token: str | None = await self.check_refresh_token(
                        vin, db, tesla_api
                    )
                except Exception as e:
                    logger.error(f"Error checking refresh token for vehicle {vin}: {e}")
                    continue

                if token is None:
                    continue

                activation_response = self.activate_vehicle(vin, db, token)

                if not activation_response["success"]:
                    continue
                else:
                    await self.insert_vehicle_in_db(vin, db, token, tesla_api)
