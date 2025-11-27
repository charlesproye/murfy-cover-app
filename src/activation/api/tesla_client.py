import asyncio
import json
import logging
import re
from datetime import UTC, datetime
from typing import ClassVar

from dateutil.relativedelta import relativedelta

from core.tesla.tesla_utils import FLEET_URLS, TeslaRegions


class TeslaApi:
    """Tesla Fleet API client for vehicle management."""

    ACCOUNT_TOKEN_KEYS: ClassVar[dict[str, str]] = {
        "OLINO": "ACCESS_TOKEN_OLINO",
        "AYVENS_SLBV": "ACCESS_TOKEN_AYVENS_SLBV",
        "AYVENS_BLBV": "ACCESS_TOKEN_AYVENS_BLBV",
        "AYVENS": "ACCESS_TOKEN_AYVENS",
        "AYVENS_NV": "ACCESS_TOKEN_AYVENS_NV",
        "AYVENS_NVA": "ACCESS_TOKEN_AYVENS_NVA",
        "CAPFM": "ACCESS_TOKEN_CAPFM",
    }

    TESLA_PATTERNS: ClassVar[dict[str, dict[str, list[tuple[str, str]]]]] = {
        "model 3": {
            "patterns": [
                (
                    r".*standard range.*plus.*rear.?wheel.*|.*standard range.*plus.*rwd.*|.*rear.?wheel drive.*",
                    "rwd",
                ),
                (r".*performance.*dual motor.*|.*performance.*", "performance"),
                (r".*long range.*all.?wheel drive.*", "long range awd"),
            ]
        },
        "model s": {
            "patterns": [
                (r".*100d.*", "100d"),
                (r".*75d.*", "75d"),
                (r".*long range.*plus.*", "long range plus"),
                (r".*long range.*", "long range"),
                (r".*plaid.*", "plaid"),
                (r".*performance.*", "performance"),
                (r".*standard range.*", "standard range"),
            ]
        },
        "model x": {
            "patterns": [
                (r".*long range.*plus.*", "long range plus"),
                (r".*long range.*", "long range"),
            ]
        },
        "model y": {
            "patterns": [
                (r".*long range.*rwd.*", "long range rwd"),
                (r".*long range.*all.?wheel drive.*", "long range awd"),
                (r".*performance.*awd.*", "performance"),
                (r".*rear.?wheel drive.*", "rwd"),
            ]
        },
    }

    RATE_LIMIT_DELAY: ClassVar[float] = 0.5
    MAX_RETRIES: ClassVar[int] = 3

    def __init__(self, slack_token: str, slack_channel_id: str):
        self.base_url = FLEET_URLS[TeslaRegions.EUROPE]
        self.slack_token = slack_token
        self.slack_channel_id = slack_channel_id
        self._tokens = {}
        self._vin_to_account = {}
        self._cache_timestamp = None
        self._cache_duration = 24 * 3600

    async def _fetch_slack_messages(self, session) -> list[dict]:
        """Récupère les messages de Slack contenant les tokens."""
        url = f"https://slack.com/api/conversations.history?channel={self.slack_channel_id}"
        headers = {
            "Authorization": f"Bearer {self.slack_token}",
            "Content-Type": "application/json",
        }

        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("ok"):
                        return data.get("messages", [])
                    else:
                        logging.error(
                            f"Error fetching Slack messages: {data.get('error')}"
                        )
                        return []
                else:
                    logging.error(
                        f"Failed to fetch Slack messages: HTTP {response.status}"
                    )
                    return []
        except Exception as e:
            logging.error(f"Error fetching Slack messages: {e!s}")
            return []

    async def _get_token_from_slack(self, session, account_key: str) -> str | None:
        """Récupère le token d'un compte spécifique depuis Slack."""
        try:
            logging.info(f"Getting token for {account_key} from Slack...")
            messages = await self._fetch_slack_messages(session)

            for i in range(7):
                try:
                    message_text = messages[i]["blocks"][0]["elements"][0]["elements"][
                        3
                    ]["text"]
                    response_key = messages[i]["blocks"][0]["elements"][0]["elements"][
                        0
                    ]["text"].split(":")[0]
                    response_data = json.loads(message_text)

                    if response_key == account_key:
                        logging.info(f"Found token for {account_key}")
                        return response_data["access_token"]
                except (KeyError, IndexError, json.JSONDecodeError):
                    continue

            logging.warning(f"No token found for {account_key}")
            return None

        except Exception as e:
            logging.error(f"Error getting token from Slack: {e!s}")
            return None

    async def _get_headers(self, session, account_name: str) -> dict[str, str]:
        """Get headers with authentication token for a specific account."""
        if account_name not in self.ACCOUNT_TOKEN_KEYS:
            raise ValueError(f"Invalid account name: {account_name}")

        token_key = self.ACCOUNT_TOKEN_KEYS[account_name]

        if token_key not in self._tokens:
            self._tokens[token_key] = await self._get_token_from_slack(
                session, token_key
            )

        if not self._tokens[token_key]:
            raise ValueError(f"Could not get token for account {account_name}")

        return {
            "Authorization": f"Bearer {self._tokens[token_key]}",
            "Content-Type": "application/json",
        }

    def _is_cache_valid(self) -> bool:
        """Vérifie si le cache des VINs est encore valide."""
        if not self._cache_timestamp:
            return False
        current_time = datetime.now(UTC).timestamp()
        return (current_time - self._cache_timestamp) < self._cache_duration

    async def _fetch_account_vehicles(self, session, account_name: str) -> list[str]:
        """Récupère tous les VINs des véhicules pour un compte donné."""
        all_vins = []
        page = 1
        retries = 0

        while True:
            url = f"{self.base_url}/api/1/vehicles?page={page}&per_page=100"

            try:
                headers = await self._get_headers(session, account_name)
                async with session.get(url, headers=headers) as response:
                    if response.status == 429:
                        if retries < self.MAX_RETRIES:
                            retries += 1
                            retry_delay = self.RATE_LIMIT_DELAY * 2**retries
                            logging.warning(
                                f"Rate limit hit, waiting {retry_delay}s before retry {retries}/{self.MAX_RETRIES}"
                            )
                            await asyncio.sleep(retry_delay)
                            continue
                        else:
                            logging.error("Max retries reached for rate limiting")
                            break

                    if response.status == 401:
                        if retries < self.MAX_RETRIES:
                            retries += 1
                            self._tokens.pop(
                                self.ACCOUNT_TOKEN_KEYS[account_name], None
                            )
                            continue
                        else:
                            logging.error("Max retries reached for authentication")
                            break

                    if response.status != 200:
                        response_text = await response.text()
                        logging.error(
                            f"Error fetching vehicles: HTTP {response.status}\nURL: {url}\nResponse: {response_text}"
                        )
                        break

                    retries = 0
                    data = await response.json()
                    vehicles = data.get("response", [])

                    if not vehicles:
                        break

                    vins = [vehicle["vin"] for vehicle in vehicles]
                    all_vins.extend(vins)
                    logging.info(
                        f"Found {len(vins)} vehicles on page {page} for {account_name}. Total: {len(all_vins)}"
                    )

                    page += 1
                    await asyncio.sleep(self.RATE_LIMIT_DELAY)

            except Exception as e:
                logging.error(f"Error fetching vehicles for {account_name}: {e!s}")
                break

        return all_vins

    async def _build_vin_mapping(self, session) -> list[tuple[str, str]]:
        """Construit le mapping VIN -> compte Tesla en interrogeant tous les comptes.

        Returns:
            List of tuples, where each tuple contains (vin, account_name)
        """
        self._vin_to_account = {}
        self._cache_timestamp = datetime.now(UTC).timestamp()
        vin_account_pairs = []

        for account_name in self.ACCOUNT_TOKEN_KEYS:
            try:
                vins = await self._fetch_account_vehicles(session, account_name)
                for vin in vins:
                    self._vin_to_account[vin] = account_name
                    vin_account_pairs.append((vin, account_name))
                logging.info(f"Mapped {len(vins)} VINs to account {account_name}")
            except Exception as e:
                logging.error(f"Failed to fetch VINs for account {account_name}: {e!s}")
        return vin_account_pairs

    async def get_account_for_vin(self, session, vin: str) -> str | None:
        """Trouve le compte Tesla associé à un VIN donné.

        Args:
            session: Session aiohttp
            vin: VIN du véhicule

        Returns:
            Le nom du compte Tesla associé, ou None si non trouvé
        """
        if not self._is_cache_valid():
            await self._build_vin_mapping(session)

        account = self._vin_to_account.get(vin)
        if not account:
            logging.warning(f"No Tesla account found for VIN: {vin}")
        return account

    async def get_vehicle_options(
        self, session, vin: str, model_name: str
    ) -> tuple[str, str]:
        """Get vehicle options from Tesla API.

        Args:
            session: aiohttp client session
            vin: Vehicle Identification Number
            model_name: Name of Tesla model (e.g., 'model 3', 'model y')

        Returns:
            Tuple[str, str]: (version_code, vehicle_type)
            where version_code is the Tesla model version (e.g., 'MTY13B')
            and vehicle_type is the specific variant (e.g., 'Performance', 'Long Range AWD')
        """
        account = await self.get_account_for_vin(session, vin)
        url = f"{self.base_url}/api/1/dx/vehicles/options?vin={vin}"
        for _ in range(self.MAX_RETRIES):
            try:
                headers = await self._get_headers(session, account)
                async with session.get(url, headers=headers) as response:
                    if response.status != 200:
                        logging.warning(
                            f"Failed to fetch options for VIN {vin}: HTTP {response.status}"
                        )
                        continue

                    data = await response.json()
                    model_info = next(
                        (
                            item
                            for item in data.get("codes", [])
                            if item["code"].startswith("$MT")
                        ),
                        None,
                    )

                    if not model_info:
                        return "MTU", "unknown"

                    # Extract version code
                    version = model_info["code"][1:]
                    if version == "MTY13":
                        version = "MTY13C" if vin[10] == "C" else "MTY13B"

                    # Determine vehicle type
                    display_name = model_info["displayName"].lower()
                    if model_name not in self.TESLA_PATTERNS:
                        return version, "unknown"

                    # Match vehicle type using patterns
                    vehicle_type = next(
                        (
                            type_name
                            for pattern, type_name in self.TESLA_PATTERNS[model_name][
                                "patterns"
                            ]
                            if re.match(pattern, display_name, re.IGNORECASE)
                        ),
                        "unknown",
                    )

                    return version, vehicle_type

            except Exception as e:
                logging.error(f"Error fetching options for VIN {vin}: {e!s}")
                await asyncio.sleep(self.RATE_LIMIT_DELAY)

        logging.error(
            f"Failed to fetch options for VIN {vin} after {self.MAX_RETRIES} retries"
        )
        return "MTU", "unknown"

    async def get_warranty_info(
        self, session, vin: str
    ) -> tuple[int | None, int | None, str | None]:
        """Récupère la date de début basée sur les informations de garantie."""
        account = await self.get_account_for_vin(session, vin)
        url = f"{self.base_url}/api/1/dx/warranty/details?vin={vin}"
        retries = self.MAX_RETRIES

        while retries > 0:
            try:
                headers = await self._get_headers(session, account)
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        active_warranty = data.get("activeWarranty", [])

                        if active_warranty:
                            warranty = active_warranty[1]
                            expiration_date = warranty.get("expirationDate")
                            warranty_date = warranty.get("coverageAgeInYears")
                            warranty_km = int(warranty.get("expirationOdometer"))
                            warranty_km = (
                                240000 if warranty_km == 9999999 else warranty_km
                            )

                            expiration_date_obj = datetime.fromisoformat(
                                expiration_date.replace("Z", "+00:00")
                            )
                            start_date_obj = expiration_date_obj - relativedelta(
                                years=int(warranty_date)
                            )
                            start_date = start_date_obj.strftime("%Y-%m-%d")
                            return warranty_km, warranty_date, start_date
                        else:
                            logging.warning(f"No valid warranty data for VIN {vin}")
                            return None, None, None
                    elif response.status == 401:
                        self._tokens.pop(self.ACCOUNT_TOKEN_KEYS[account], None)

                    logging.warning(
                        f"HTTP {response.status} error fetching warranty for VIN {vin}, retries left: {retries - 1}"
                    )
                    retries -= 1
                    await asyncio.sleep(self.RATE_LIMIT_DELAY)

            except Exception as e:
                logging.error(f"Error getting warranty info for VIN {vin}: {e!s}")
                retries -= 1
                await asyncio.sleep(self.RATE_LIMIT_DELAY)

        logging.error(
            f"Failed to fetch warranty info for VIN {vin} after {self.MAX_RETRIES} retries"
        )
        return None, None, None
