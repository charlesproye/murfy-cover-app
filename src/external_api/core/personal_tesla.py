import logging
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import requests
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

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
                        VALUES ('{uuid4()}', '{body["state"]}', '{body["code"]}')
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
                    VALUES ('{uuid4()}', '{body["state"]}', '{body["code"]}', '{tokens["access_token"]}', '{tokens["refresh_token"]}', '{expires_at}')
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
                    VALUES ('{uuid4()}', '{body["state"]}', '{body["code"]}')
                """)
                )
                await db_session.commit()
            except Exception as insert_error:
                logger.error(f"Error inserting error state: {insert_error!s}")
                await db_session.rollback()
            return False


teslaData = CrudTesla()
