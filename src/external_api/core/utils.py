"""Service for Google Sheets interaction"""

import logging
import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import gspread
import requests
from fastapi import Depends, HTTPException, status
from fastapi.datastructures import URL
from gspread.exceptions import APIError, SpreadsheetNotFound, WorksheetNotFound
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import select

from db_models import User
from external_api.core.cookie_auth import get_current_user_from_cookie, get_user
from external_api.db.session import get_db
from external_api.schemas.user import GetCurrentUser
from external_api.services.api_pricing import get_api_user_pricing, log_api_call
from external_api.services.redis import (
    add_distinct_vin_and_check_limit,
    increment_and_check_rate_limit,
)
from external_api.services.user import get_user_by_id

logger = logging.getLogger(__name__)

# Chemin vers le fichier de credentials
CREDENTIALS_FILE = (
    Path(os.path.dirname(os.path.dirname(__file__))) / "config" / "credential.json"
)
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ID de la feuille Google Sheets où les véhicules seront activés
# Vous devrez remplacer cette valeur par l'ID réel de votre feuille Google Sheets
SPREADSHEET_ID = "1zGwSY41eN00YQbaNf9HNk3g5g6KQaAD1FY7-XS8Uf9w"
WORKSHEET_NAME = "Liste VIN"  # Nom de l'onglet
HEADERS = [
    "vin",
    "Licence plate",
    "Oem",
    "Make",
    "Model",
    "Type",
    "End of Contract",
    "Country",
    "Activation",
    "Ownership",
    "Start Date",
    "Comment",
    "Eligibility",
    "Real Activation",
    "Activation Error",
    "Rapport Basique",
]  # En-têtes de la feuille

# Si TRUE, le service créera un nouveau Spreadsheet s'il n'existe pas déjà
CREATE_SPREADSHEET_IF_NOT_EXISTS = True


def get_google_sheets_client():
    """
    Initialise et retourne un client Google Sheets

    Returns:
        Client Google Sheets authentifié

    Raises:
        HTTPException: En cas d'erreur d'authentification
    """
    try:
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            CREDENTIALS_FILE, SCOPES
        )
        client = gspread.authorize(credentials)
        return client
    except Exception as e:
        logger.error(f"Erreur lors de l'initialisation du client Google Sheets: {e!s}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erreur lors de l'initialisation du client Google Sheets: {e!s}",
        ) from e


def get_or_create_spreadsheet(client, spreadsheet_id: str) -> gspread.Spreadsheet:
    """
    Récupère la feuille Google Sheets, ou en crée une nouvelle si elle n'existe pas

    Args:
        client: Client Google Sheets authentifié
        spreadsheet_id: ID de la feuille Google Sheets

    Returns:
        La feuille Google Sheets

    Raises:
        HTTPException: En cas d'erreur lors de la récupération ou création de la feuille
    """
    try:
        # Tenter d'ouvrir la feuille existante
        logger.info(f"Tentative d'ouverture de la feuille avec l'ID {spreadsheet_id}")
        try:
            spreadsheet = client.open_by_key(spreadsheet_id)
            logger.info(f"Feuille existante trouvée avec l'ID {spreadsheet_id}")
            return spreadsheet
        except SpreadsheetNotFound:
            # Si la feuille n'existe pas, tenter de l'ouvrir par titre
            logger.warning(
                f"Feuille avec ID {spreadsheet_id} non trouvée, recherche par URL..."
            )

            # Tenter d'ouvrir par URL complète au cas où l'ID serait une URL complète
            try:
                if "/" in spreadsheet_id:
                    url_parts = spreadsheet_id.split("/")
                    # Essayer de trouver un ID valide dans l'URL
                    for part in url_parts:
                        if (
                            len(part) > 20
                        ):  # IDs Google ont généralement plus de 20 caractères
                            try:
                                logger.info(f"Tentative avec l'ID extrait: {part}")
                                spreadsheet = client.open_by_key(part)
                                logger.info(
                                    f"Feuille trouvée avec l'ID extrait: {part}"
                                )
                                return spreadsheet
                            except Exception as e:
                                logger.error(
                                    f"Error opening spreadsheet with extracted ID: {e!s}"
                                )
                                continue
            except Exception as e:
                logger.error(f"Error opening spreadsheet with URL: {e!s}")

            # Si l'ouverture par URL échoue aussi
            if CREATE_SPREADSHEET_IF_NOT_EXISTS:
                logger.info(
                    "Création d'une nouvelle feuille 'Activations de véhicules'"
                )
                spreadsheet = client.create("Activations de véhicules")
                # Partager la feuille pour la rendre accessible
                spreadsheet.share(None, perm_type="anyone", role="reader")
                logger.info(f"Nouvelle feuille créée avec l'ID: {spreadsheet.id}")
                logger.info(
                    f"URL de la feuille: https://docs.google.com/spreadsheets/d/{spreadsheet.id}"
                )
                return spreadsheet
            else:
                logger.error(
                    f"La feuille Google Sheets avec l'ID {spreadsheet_id} n'existe pas"
                )
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"La feuille Google Sheets avec l'ID {spreadsheet_id} n'existe pas",
                ) from None
    except SpreadsheetNotFound:
        if CREATE_SPREADSHEET_IF_NOT_EXISTS:
            logger.info("Création d'une nouvelle feuille 'Activations de véhicules'")
            spreadsheet = client.create("Activations de véhicules")
            # Partager la feuille pour la rendre accessible
            spreadsheet.share(None, perm_type="anyone", role="reader")
            logger.info(f"Nouvelle feuille créée avec l'ID: {spreadsheet.id}")
            logger.info(
                f"URL de la feuille: https://docs.google.com/spreadsheets/d/{spreadsheet.id}"
            )
            return spreadsheet
        else:
            logger.error(
                f"La feuille Google Sheets avec l'ID {spreadsheet_id} n'existe pas"
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"La feuille Google Sheets avec l'ID {spreadsheet_id} n'existe pas",
            ) from None
    except Exception as e:
        logger.error(f"Erreur lors de l'ouverture de la feuille Google Sheets: {e!s}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erreur lors de l'ouverture de la feuille Google Sheets: {e!s}",
        ) from e


def get_or_create_worksheet(
    spreadsheet: gspread.Spreadsheet, worksheet_name: str
) -> gspread.Worksheet:
    """
    Récupère l'onglet dans la feuille Google Sheets, ou en crée un nouveau s'il n'existe pas

    Args:
        spreadsheet: Feuille Google Sheets
        worksheet_name: Nom de l'onglet

    Returns:
        L'onglet de la feuille Google Sheets

    Raises:
        HTTPException: En cas d'erreur lors de la récupération ou création de l'onglet
    """
    try:
        # Tenter d'ouvrir l'onglet existant
        logger.info(f"Tentative d'ouverture de l'onglet {worksheet_name}")
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            logger.info(f"Onglet {worksheet_name} trouvé")
        except WorksheetNotFound:
            # Si l'onglet n'existe pas, le créer
            logger.info(f"L'onglet {worksheet_name} n'existe pas, création de l'onglet")
            worksheet = spreadsheet.add_worksheet(
                title=worksheet_name, rows=1, cols=len(HEADERS)
            )
            # Ajouter les en-têtes
            worksheet.append_row(HEADERS)
            logger.info(
                f"Onglet {worksheet_name} créé avec succès avec les en-têtes: {HEADERS}"
            )

        # Vérifier que l'en-tête est correct
        headers = worksheet.row_values(1)
        if not headers or headers != HEADERS:
            logger.warning(
                f"En-têtes incorrects dans l'onglet {worksheet_name}: {headers}"
            )
            if not headers:
                # Si l'onglet est vide, ajouter les en-têtes
                worksheet.append_row(HEADERS)
                logger.info(f"En-têtes ajoutés à l'onglet {worksheet_name}: {HEADERS}")

        return worksheet
    except Exception as e:
        logger.error(
            f"Erreur lors de la récupération/création de l'onglet {worksheet_name}: {e!s}"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erreur lors de la récupération/création de l'onglet: {e!s}",
        ) from e


async def get_company_name_from_user_id(db: AsyncSession, user_id: uuid.UUID) -> str:
    """
    Get company name from user ID

    Args:
        db: Asynchronous database session
        user_id: User ID

    Returns:
        Company name or "Unknown Company" if not found

    Raises:
        HTTPException: If error occurs while retrieving data
    """
    try:
        # First get the user to obtain company_id
        user = await get_user_by_id(db, user_id)

        if not user or not user.company_id:
            logger.warning(f"User {user_id} not found or without company_id")
            return "Unknown Company"

        # Get company name from company_id
        query = text("""
        SELECT name FROM company WHERE id = :company_id
        """)

        result = await db.execute(query, {"company_id": user.company_id})
        company_data = result.fetchone()

        if not company_data:
            logger.warning(f"Company with ID {user.company_id} not found")
            return "Unknown Company"

        return company_data.name
    except Exception as e:
        logger.error(f"Error while retrieving company name: {e!s}")
        return "Unknown Company"


async def add_vehicles_to_google_sheet(
    vins: list[str], user_id: str, db: AsyncSession = None
) -> bool:
    """
    Add VINs to the activation Google Sheet

    Args:
        vins: List of VINs to activate
        user_id: ID of the user performing the activation
        db: Asynchronous database session (optional)

    Returns:
        True if operation succeeded

    Raises:
        HTTPException: If error occurs while adding to Google Sheet
    """
    client = None
    try:
        logger.info(f"Starting activation process for {len(vins)} vehicles")
        # Get Google Sheets client
        client = get_google_sheets_client()

        # Get company name if db is provided
        company_name = "Unknown Company"
        if db:
            company_name = await get_company_name_from_user_id(db, user_id)
            logger.info(f"Retrieved company name: {company_name}")
        else:
            logger.warning("DB session not provided, unable to retrieve company name")

        # Get or create sheet - direct access without listing all sheets
        spreadsheet = client.open_by_key(SPREADSHEET_ID)

        # Get worksheet without checking headers (since they are already correct)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)

        # Prepare data to insert
        rows_to_add = []
        for vin in vins:
            # Create a row with empty values for all columns except the ones we're setting
            row = [""] * len(HEADERS)
            # Set specific values at their correct positions
            row[0] = vin  # vin
            row[8] = "TRUE"  # Activation
            row[9] = "FALSE"  # EValue
            row[10] = company_name  # Ownership
            row[16] = "TRUE"  # Rapport Basique
            rows_to_add.append(row)

        # Add data to Google Sheet directly without checks
        logger.info(f"Adding {len(rows_to_add)} rows to Google Sheet")
        worksheet.append_rows(rows_to_add)

        logger.info(f"Activation of {len(vins)} vehicles in Google Sheet succeeded")
        return True

    except SpreadsheetNotFound:
        logger.error(f"The Google Sheets with ID {SPREADSHEET_ID} does not exist")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"The Google Sheets with ID {SPREADSHEET_ID} does not exist",
        ) from None
    except WorksheetNotFound:
        logger.error(f"Worksheet {WORKSHEET_NAME} does not exist in the sheet")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Worksheet {WORKSHEET_NAME} does not exist in the sheet",
        ) from None
    except APIError as e:
        # Extract error message from Google Sheets API
        error_message = str(e)
        logger.error(f"Google Sheets API error: {error_message}")

        # Check API error type to give a more specific message
        if "404" in error_message:
            logger.error(
                f"Resource not found (404): Check sheet ID ({SPREADSHEET_ID}) and permissions"
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Google Sheets not found. Check ID and permissions. Details: {error_message}",
            ) from e
        elif "403" in error_message:
            logger.error(
                "Permission error (403): Service account does not have necessary permissions"
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied to Google Sheets. Check service account permissions. Details: {error_message}",
            ) from e
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error while interacting with Google Sheets: {error_message}",
            ) from e
    except Exception as e:
        error_message = str(e)
        logger.error(f"Error while adding vehicles to Google Sheet: {error_message}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error while adding vehicles to Google Sheet: {error_message}",
        ) from e


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


############### This is for the personnal tesla vehicle activation ###############
class CrudTesla:
    async def insert_code_tesla(
        self,
        *,
        body,
        db_session: AsyncSession | None = None,
    ):
        try:
            logger.info("Inside insert_code_tesla")
            # Environment variables
            CLIENT_ID = os.getenv("CLIENT_ID")
            CLIENT_SECRET = os.getenv("CLIENT_SECRET")
            FLEET_API_URL = os.getenv("FLEET_API_URL")
            SCOPES = os.getenv("SCOPES")
            REDIRECT_URI = os.getenv("REDIRECT_URI")

            # Prepare the payload for the token request
            payload = {
                "grant_type": "authorization_code",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "audience": FLEET_API_URL,
                "scope": SCOPES,
                "code": body["code"],
                "redirect_uri": REDIRECT_URI,
            }

            # Check if user exists before making the token request
            try:
                user_exists = await db_session.exec(
                    text(f"""
                    SELECT EXISTS(SELECT 1 FROM "tesla".user WHERE id = '{body["state"]}')
                """)
                )
                user_exists = user_exists.scalar()

                if not user_exists:
                    logger.error(f"User with id {body['state']} does not exist")
                    await db_session.rollback()
                    return False

            except Exception as e:
                logger.error(f"Database error checking user: {e!s}")
                await db_session.rollback()
                return False

            # Make token request
            try:
                response = requests.post(
                    "https://auth.tesla.com/oauth2/v3/token", json=payload
                )
                logger.info(f"Response received: {response.status_code}")
                logger.info(f"Response content: {response.json()}")
            except Exception as e:
                logger.error(f"Error making token request: {e!s}")
                await db_session.rollback()
                return False

            if response.status_code == 400:
                logger.warning(f"Invalid authorization code for user {body['state']}")
                try:
                    await db_session.exec(
                        text(f"""
                        INSERT INTO "tesla".user_tokens (id, user_id, code)
                        VALUES ('{uuid()}', '{body["state"]}', '{body["code"]}')
                    """)
                    )
                    await db_session.commit()
                except Exception as e:
                    logger.error(f"Error inserting failed token attempt: {e!s}")
                    await db_session.rollback()
                return False

            response.raise_for_status()
            tokens = response.json()

            if "error" in tokens:
                await db_session.rollback()
                return False

            # Calculate expiration date and insert tokens
            try:
                expires_at = datetime.now(UTC) + timedelta(seconds=tokens["expires_in"])
                await db_session.exec(
                    text(f"""
                    INSERT INTO "tesla".user_tokens (id, user_id, code, access_token, refresh_token, expires_at)
                    VALUES ('{uuid.uuid4()}', '{body["state"]}', '{body["code"]}', '{tokens["access_token"]}', '{tokens["refresh_token"]}', '{expires_at}')
                """)
                )
                await db_session.commit()
                logger.info(
                    f"Successfully inserted tokens for user {body['state']} and code {body['code']}"
                )
                return True

            except Exception as e:
                logger.error(f"Error inserting tokens: {e!s}")
                await db_session.rollback()
                return False

        except Exception as e:
            logger.error(f"Error in insert_code_tesla: {e!s}", exc_info=True)
            await db_session.rollback()
            try:
                await db_session.exec(
                    text(f"""
                    INSERT INTO "tesla".user_tokens (id, user_id, code)
                    VALUES ('{uuid()}', '{body["state"]}', '{body["code"]}')
                """)
                )
                await db_session.commit()
            except Exception as insert_error:
                logger.error(f"Error inserting error state: {insert_error!s}")
                await db_session.rollback()
            return False


teslaData = CrudTesla()


async def get_flash_report_user(
    db: AsyncSession = Depends(get_db),
) -> User | None:
    query = select(User).where(User.email == "flash-report@bib-batteries.fr")
    flash_report_user = (await db.execute(query)).scalar_one_or_none()

    if not flash_report_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Flash report user not found",
        )

    return flash_report_user


def strip_params_from_url(url: URL) -> str:
    """
    Return the base URL without the query parameters.
    """
    return f"{url.scheme}://{url.netloc}{url.path}"


def replace_in_url(url: URL, old: str, new: str, with_params: bool = False) -> str:
    """
    Replace the old string in the URL with the new string.
    """

    new_path = url.path.replace(old, new)

    new_url = f"{url.scheme}://{url.netloc}{new_path}"

    if with_params:
        new_url += f"?{url.query}"

    return new_url


async def check_rate_limit(
    vin: str,
    endpoint: str,
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user)),
    db: AsyncSession = Depends(get_db),
) -> None:
    """
    Checks rate limits and logs API calls.
    Checks two limits:
    1. Total number of requests per day
    2. Number of distinct VINs per day
    """
    start_time = time.time()

    try:
        # Get pricing plan
        pricing_info = await get_api_user_pricing(db, user.id)

        # 1. Check total requests limit
        current_count, limit_exceeded = await increment_and_check_rate_limit(
            user.id, pricing_info["requests_limit"]
        )

        if limit_exceeded:
            logger.warning(
                f"Request limit exceeded for API user {user.id}, endpoint {endpoint}"
            )
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Daily request limit exceeded. Please upgrade your plan or try again tomorrow.",
            )

        # 2. Check distinct VINs limit
        distinct_count, vin_limit_exceeded = await add_distinct_vin_and_check_limit(
            user.id, vin, pricing_info["max_distinct_vins"]
        )

        if vin_limit_exceeded:
            logger.warning(f"Distinct VINs limit exceeded for API user {user.id}")
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Distinct VINs limit exceeded ({pricing_info['max_distinct_vins']}). Please upgrade your plan or try again tomorrow.",
            )

        # Calculate response time
        response_time = (time.time() - start_time) * 1000  # in milliseconds

        # Log this API call
        await log_api_call(db, user.id, endpoint, vin, response_time=response_time)

        logger.info(
            f"API Call logged: API user {user.id}, endpoint {endpoint}, "
            f"VIN {vin}, requests {current_count}/{pricing_info['requests_limit']}, "
            f"distinct VINs {distinct_count}/{pricing_info['max_distinct_vins']}"
        )
    except Exception as e:
        logger.warning(
            f"Error during rate limit check: {e!s}. Continuing without limits."
        )
        response_time = (time.time() - start_time) * 1000
        logger.info(
            f"API Call logged (without limits): API user {user.id}, endpoint {endpoint}, "
            f"VIN {vin}"
        )
