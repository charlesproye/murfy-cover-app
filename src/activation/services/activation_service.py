import json
import logging
import time
from typing import Optional, Tuple

import aiohttp
import pandas as pd

from activation.api.bmw_client import BMWApi
from activation.api.hm_client import HMApi
from activation.api.kia_client import KiaApi
from activation.api.renault_client import RenaultApi
from activation.api.stellantis_client import StellantisApi
from activation.api.volkswagen_client import VolkswagenApi
from activation.config.config import REASON_MAPPING
from activation.config.settings import ACTIVATION_TIMEOUT
from activation.services.google_sheet_service import update_vehicle_activation_data
from core.sql_utils import get_connection


class VehicleActivationService:
    def __init__(
        self,
        bmw_api: BMWApi,
        hm_api: HMApi,
        stellantis_api: StellantisApi,
        renault_api: RenaultApi,
        volkswagen_api: VolkswagenApi,
        kia_api: KiaApi,
        fleet_info_df: pd.DataFrame,
    ):
        self.bmw_api = bmw_api
        self.hm_api = hm_api
        self.stellantis_api = stellantis_api
        self.renault_api = renault_api
        self.kia_api = kia_api
        self.fleet_info_df = fleet_info_df
        self.volkswagen_api = volkswagen_api

    async def _get_current_states_kia(
        self, session: aiohttp.ClientSession, vins: list[str]
    ):
        """Get the current states of a list of vins."""

        current_states_vehicle = await self.kia_api._get_status_consent_type(
            session, ["vehicle"]
        )

        current_states_dtc = await self.kia_api._get_status_consent_type(
            session, ["dtcInfo"]
        )

        merged = {}

        for vin in vins:
            merged[vin] = {"vehicle": False, "dtcInfo": False}

        for el in current_states_vehicle:
            merged[el["vin"]]["vehicle"] = True
        for el in current_states_dtc:
            merged[el["vin"]]["dtcInfo"] = True

        for vin in merged.keys():
            merged[vin]["status_bool"] = (
                True if merged[vin]["vehicle"] and merged[vin]["dtcInfo"] else False
            )
            if merged[vin]["vehicle"] and (not merged[vin]["dtcInfo"]):
                merged[vin]["reason"] = "Package véhicule activé mais pas dtcInfo"
            elif merged[vin]["dtcInfo"] and (not merged[vin]["vehicle"]):
                merged[vin]["reason"] = "Package dtcInfo activé mais pas véhicule"
            elif (not merged[vin]["dtcInfo"]) and (not merged[vin]["vehicle"]):
                merged[vin]["reason"] = "Package dtcInfo activé mais pas véhicule"
            else:
                merged[vin]["reason"] = ""

        return merged

    async def _add_to_fleet(
        self, vin: str, session: aiohttp.ClientSession
    ) -> Tuple[bool, Optional[str]]:
        """Add a vehicle to the appropriate fleet based on ownership.

        Args:
            vin: Vehicle VIN
        """
        try:
            vehicle_info = self.fleet_info_df[self.fleet_info_df["vin"] == vin]
            target_fleet_name = (
                str(vehicle_info["owner"].iloc[0])
                if "owner" in vehicle_info.columns
                else "bib"
            )

            status_code, result = await self.bmw_api.get_fleets(session)

            if status_code != 200:
                error_msg = f"Failed to get fleets: HTTP {status_code}"
                logging.error(error_msg)
                return False, error_msg

            target_fleet_id = None
            for fleet in result.get("fleets", []):
                if fleet.get("name", "").lower() == target_fleet_name.lower():
                    target_fleet_id = fleet["fleet_id"]
                    break

            if not target_fleet_id:
                for fleet in result.get("fleets", []):
                    if fleet.get("name", "").lower() == "bib":
                        target_fleet_id_bib = fleet["fleet_id"]
                        break
                warning_msg = (
                    f"Fleet {target_fleet_name} not found, adding to bib fleet"
                )
                logging.warning(warning_msg)
                target_fleet_id = target_fleet_id_bib

            status_code, result = await self.bmw_api.add_vehicle_to_fleet(
                target_fleet_id, vin, session
            )

            result = json.loads(result)

            if status_code in [200, 201, 204]:
                logging.info(
                    f"Successfully added vehicle {vin} to {target_fleet_name} fleet"
                )
                return True, None
            if status_code == 422:
                logging.info(f"Vehicle {vin} already in fleet")
                return True, None
            if status_code == 429:
                if result["message"] == "Too Many Requests":
                    time.sleep(1)
                    return await self._add_to_fleet(vin, session)
            else:
                error_msg = f"Failed to add vehicle to fleet: HTTP {status_code}"
                logging.error(error_msg)
                return False, error_msg

        except Exception as e:
            error_msg = f"Error adding vehicle to fleet: {str(e)}"
            logging.error(error_msg)
            return False, error_msg

    async def _activate_bmw(
        self, session: aiohttp.ClientSession, vin: str
    ) -> Tuple[bool, Optional[str]]:
        """Activate a BMW vehicle using BMW's API."""
        try:
            license_plate = ""
            end_date = ""

            if self.fleet_info_df is not None:
                vehicle_info = self.fleet_info_df[self.fleet_info_df["vin"] == vin]
                if not vehicle_info.empty:
                    license_plate = (
                        str(vehicle_info["licence_plate"].iloc[0])
                        if "licence_plate" in vehicle_info.columns
                        else ""
                    )
                    end_date = (
                        str(vehicle_info["end_of_contract"].iloc[0])
                        if "end_of_contract" in vehicle_info.columns
                        else ""
                    )

                    license_plate = "" if pd.isna(license_plate) else license_plate
                    end_date = "" if pd.isna(end_date) else end_date

                    logging.info(
                        f"Found vehicle info - VIN: {vin}, License Plate: {license_plate}, End Date: {end_date}"
                    )
                else:
                    logging.warning(
                        f"No vehicle info found in fleet_info for VIN: {vin}"
                    )
            else:
                logging.warning(
                    "fleet_info_df is None, using empty values for license_plate and end_date"
                )

            payload = {
                "vin": vin,
                "licence_plate": license_plate,
                "note": "",
                "contract": {
                    "end_date": end_date,
                },
            }

            status_code, result = await self.bmw_api.create_clearance(payload, session)

            result = json.loads(result)

            logging.info(f"Create clearance response")

            if status_code in [200, 201, 204]:
                fleet_success, fleet_error = await self._add_to_fleet(vin, session)
                if not fleet_success:
                    return False, fleet_error
                return True, None
            elif "message" in result.keys():
                if result["message"] == "Too Many Requests":
                    time.sleep(1)
                    return await self._activate_bmw(session, vin)
                else:
                    pass
            elif (
                status_code == 403
                and result["logErrorId"] == "BMWFD_VEHICLE_ALREADY_EXISTS"
            ):
                logging.info(f"BMW vehicle {vin} already added")
                fleet_success, fleet_error = await self._add_to_fleet(vin, session)
                return True, None
            elif (
                status_code == 412
                and result["logErrorId"] == "BMWFD_VEHICLE_NOT_ALLOWED"
            ):
                logging.info(f"BMW vehicle {vin} is not allowed to be activated")
                return False, REASON_MAPPING["bmw"]["BMWFD_VEHICLE_NOT_ALLOWED"]
            else:
                error_msg = f"L'activation du véhicule a échoué: Code d'erreur {status_code}, Response: {result['logErrorId']}"
                return False, error_msg

        except Exception as e:
            error_msg = f"Error activating BMW vehicle: {str(e)}"
            logging.error(error_msg)
            return False, error_msg

    async def activation_stellantis(self):
        """Process Stellantis vehicle activation/deactivation."""
        df_stellantis = self.fleet_info_df[(self.fleet_info_df["oem"] == "stellantis")]
        status_data = []
        async with aiohttp.ClientSession() as session:
            for _, row in df_stellantis.iterrows():
                vin = row["vin"]
                desired_state = row["activation"]
                is_eligible = await self.stellantis_api.is_eligible(vin, session)

                if not is_eligible:
                    vehicle_data = {
                        "vin": vin,
                        "Eligibility": False,
                        "Real_Activation": False,
                        "Activation_Error": "Not eligible",
                        "API_Detail": "Le VIN est non éligible à l'activation sur Stellantis",
                    }
                    if desired_state:
                        logging.info(
                            f"Stellantis vehicle {vin} should be activated but is not eligible"
                        )
                    else:
                        logging.info(
                            f"Stellantis vehicle {vin} is already in desired state: {desired_state}"
                        )
                    status_data.append(vehicle_data)
                    continue

                current_state, contract_id, _ = await self.stellantis_api.get_status(
                    vin, session
                )

                if current_state == desired_state:
                    logging.info(
                        f"Stellantis vehicle {vin} is already in desired state: {desired_state}"
                    )
                    vehicle_data = {
                        "vin": vin,
                        "Eligibility": current_state,
                        "Real_Activation": current_state,
                        "Activation_Error": None,
                    }
                    status_data.append(vehicle_data)
                    continue

                elif not desired_state:
                    if contract_id:
                        status_code, error_msg = await self.stellantis_api.deactivate(
                            contract_id, session
                        )
                        (
                            real_state,
                            contract_id,
                            _,
                        ) = await self.stellantis_api.get_status(vin, session)
                        if status_code in [200, 204]:
                            logging.info(
                                f"Stellantis vehicle {vin} deactivated successfully"
                            )
                            vehicle_data = {
                                "vin": vin,
                                "Eligibility": real_state,
                                "Real_Activation": False,
                                "Activation_Error": None,
                            }
                            status_data.append(vehicle_data)
                            continue
                        else:
                            logging.info(
                                f"Failed to deactivate Stellantis vehicle {vin}: HTTP {status_code} - {error_msg}"
                            )
                            vehicle_data = {
                                "vin": vin,
                                "Eligibility": real_state,
                                "Real_Activation": False,
                                "Activation_Error": "Failed to deactivate",
                            }
                            status_data.append(vehicle_data)
                            continue
                    else:
                        logging.info(
                            f"Failed to deactivate Stellantis vehicle {vin} has no contract ID"
                        )
                        vehicle_data = {
                            "vin": vin,
                            "Eligibility": real_state,
                            "Real_Activation": False,
                            "Activation_Error": "No contract ID found",
                        }
                        status_data.append(vehicle_data)
                        continue

                elif desired_state:
                    status_code, result = await self.stellantis_api.activate(
                        vin, session
                    )
                    (
                        real_state,
                        contract_id,
                        status,
                    ) = await self.stellantis_api.get_status(vin, session)
                    if status_code in [200, 201, 204]:
                        logging.info(f"Stellantis vehicle {vin} activated successfully")
                        vehicle_data = {
                            "vin": vin,
                            "Eligibility": real_state,
                            "Real_Activation": real_state,
                            "Activation_Error": None,
                        }
                        status_data.append(vehicle_data)
                        continue
                    elif status_code == 409:
                        logging.info(f"Stellantis vehicle {vin} activation in progress")
                        vehicle_data = {
                            "vin": vin,
                            "Eligibility": real_state,
                            "Real_Activation": real_state,
                            "Activation_Error": status,
                            "API_Detail": REASON_MAPPING["stellantis"][status]
                            if status in REASON_MAPPING["stellantis"].keys()
                            else f"La description du statut {status} est inconnue, voir avec l'équipe TECH.",
                        }
                        status_data.append(vehicle_data)
                        continue
                    else:
                        error_msg = f"Failed to activate Stellantis vehicle {vin}: HTTP {status_code} - {result}"
                        logging.error(error_msg)
                        vehicle_data = {
                            "vin": vin,
                            "Eligibility": real_state,
                            "Real_Activation": real_state,
                            "Activation_Error": error_msg,
                            "API_Detail": f"Erreur de l'API lors de l'activation du véhicule.",
                        }
                        status_data.append(vehicle_data)
                        continue

        status_df = pd.DataFrame(status_data)
        await update_vehicle_activation_data(status_df)

    async def activation_bmw(self):
        """Process BMW vehicle activation/deactivation."""
        df_bmw = self.fleet_info_df[self.fleet_info_df["oem"] == "bmw"]
        status_data = []
        async with aiohttp.ClientSession() as session:
            for _, row in df_bmw.iterrows():
                vin = row["vin"]
                desired_state = row["activation"]
                current_state, fleet = await self.bmw_api.check_vehicle_status(
                    vin, session
                )

                if desired_state == current_state:
                    if not fleet and desired_state:
                        await self._add_to_fleet(vin, session)

                    logging.info(
                        f"BMW vehicle {vin} already in desired state: {desired_state}"
                    )
                    vehicle_data = {
                        "vin": vin,
                        "Eligibility": current_state,
                        "Real_Activation": current_state,
                        "Activation_Error": None,
                    }
                    status_data.append(vehicle_data)
                    continue

                elif desired_state:
                    success, api_detail = await self._activate_bmw(session, vin)
                    if success:
                        logging.info(f"BMW vehicle {vin} activated successfully")
                        vehicle_data = {
                            "vin": vin,
                            "Eligibility": True,
                            "Real_Activation": True,
                            "Activation_Error": None,
                        }
                        status_data.append(vehicle_data)
                        continue
                    else:
                        logging.info(f"BMW vehicle {vin} activation failed")
                        vehicle_data = {
                            "vin": vin,
                            "Eligibility": False,
                            "Real_Activation": False,
                            "Activation_Error": "Activation failed",
                            "API_Detail": api_detail,
                        }
                        status_data.append(vehicle_data)
                        continue

                else:
                    try:
                        deactivation_success = await self.bmw_api.deactivate(
                            vin, session
                        )

                        if deactivation_success:
                            current_state = await self.bmw_api.check_vehicle_status(
                                vin, session
                            )

                            if current_state == False:
                                logging.info(
                                    f"BMW vehicle {vin} deactivated successfully"
                                )
                                vehicle_data = {
                                    "vin": vin,
                                    "Eligibility": False,
                                    "Real_Activation": False,
                                    "Activation_Error": None,
                                }
                                status_data.append(vehicle_data)
                                continue
                            else:
                                logging.info(
                                    f"BMW vehicle {vin} deactivation seemed successful but vehicle is still active"
                                )
                                vehicle_data = {
                                    "vin": vin,
                                    "Eligibility": True,
                                    "Real_Activation": False,
                                    "Activation_Error": "Deactivation seemed successful but vehicle is still active",
                                }
                                status_data.append(vehicle_data)
                                continue
                        else:
                            logging.info(
                                f"BMW vehicle {vin} deactivation failed - API returned failure"
                            )
                            vehicle_data = {
                                "vin": vin,
                                "Eligibility": True,
                                "Real_Activation": False,
                                "Activation_Error": "Failed to deactivate BMW vehicle - API returned failure",
                            }
                            status_data.append(vehicle_data)
                            continue
                    except Exception as e:
                        logging.info(
                            f"BMW vehicle {vin} deactivation failed - Error: {str(e)}"
                        )
                        vehicle_data = {
                            "vin": vin,
                            "Eligibility": True,
                            "Real_Activation": False,
                            "Activation_Error": "Failed to deactivate BMW vehicle",
                        }
                        status_data.append(vehicle_data)
                        continue

            status_df = pd.DataFrame(status_data)
            await update_vehicle_activation_data(status_df)

    async def activation_hm(self):
        """Process High Mobility vehicle activation/deactivation."""
        df_hm = self.fleet_info_df[
            self.fleet_info_df["oem"].isin(
                ["renault", "ford", "mercedes", "volvo", "kia"]
            )
        ]
        status_data = []
        async with aiohttp.ClientSession() as session:
            for _, row in df_hm.iterrows():
                vin = row["vin"]
                make = row["make"]
                make_lower = make.lower()

                desired_state = row["activation"]

                try:
                    current_state, _ = await self.hm_api.get_status(vin, session)
                    if current_state == desired_state:
                        logging.info(
                            f"High Mobility vehicle {vin} already in desired state: {desired_state}"
                        )
                        vehicle_data = {
                            "vin": vin,
                            "Eligibility": current_state,
                            "Real_Activation": current_state,
                            "Activation_Error": None,
                        }
                        status_data.append(vehicle_data)
                        continue

                    elif desired_state:
                        try:
                            activation_success = await self.hm_api.create_clearance(
                                vin, make, session
                            )
                            if activation_success:
                                logging.info(
                                    f"High Mobility vehicle {vin} activated successfully"
                                )
                                vehicle_data = {
                                    "vin": vin,
                                    "Eligibility": True,
                                    "Real_Activation": True,
                                    "Activation_Error": None,
                                }
                                status_data.append(vehicle_data)
                                continue
                            else:
                                logging.info(
                                    f"High Mobility vehicle {vin} activation failed"
                                )
                                _, response = await self.hm_api.get_status(vin, session)

                                response["status"] if response[
                                    "status"
                                ] is not None else ""
                                api_detail = (
                                    response["changelog"][-1]["reason"]
                                    if "reason" in response["changelog"][-1] is not None
                                    else ""
                                )

                                if api_detail in REASON_MAPPING["high-mobility"].keys():
                                    api_detail_trad = REASON_MAPPING["high-mobility"][
                                        api_detail
                                    ]
                                elif response["status"] == "pending":
                                    api_detail_trad = "En attente que le véhicule émette des données pour qu'il soit activé"
                                elif response["status"] == "rejected":
                                    api_detail_trad = REASON_MAPPING["high-mobility"][
                                        "unspecified"
                                    ]
                                else:
                                    api_detail_trad = ""

                                vehicle_data = {
                                    "vin": vin,
                                    "Eligibility": False,
                                    "Real_Activation": False,
                                    "Activation_Error": response["status"]
                                    if response["status"] is not None
                                    else "",
                                    "API_Detail": api_detail_trad,
                                }

                                status_data.append(vehicle_data)
                                continue
                        except Exception as e:
                            logging.error(
                                f"Error activating High Mobility vehicle: {str(e)}"
                            )
                            vehicle_data = {
                                "vin": vin,
                                "Eligibility": True,
                                "Real_Activation": False,
                                "Activation_Error": "Erreur d'activation, le VIN est faux ou non activable sur High Mobility",
                            }
                            status_data.append(vehicle_data)
                            continue
                    else:
                        try:
                            deactivation_success = await self.hm_api.delete_clearance(
                                vin, session
                            )
                            if not deactivation_success:
                                logging.error(
                                    f"Failed to deactivate High Mobility vehicle"
                                )
                                vehicle_data = {
                                    "vin": vin,
                                    "Eligibility": True,
                                    "Real_Activation": False,
                                    "Activation_Error": "Failed to deactivate or pending approval",
                                }
                                status_data.append(vehicle_data)
                                continue
                            else:
                                logging.info(
                                    f"High Mobility vehicle {vin} deactivated successfully"
                                )
                                vehicle_data = {
                                    "vin": vin,
                                    "Eligibility": False,
                                    "Real_Activation": False,
                                    "Activation_Error": None,
                                }
                                status_data.append(vehicle_data)
                                continue
                        except Exception as e:
                            logging.error(
                                f"Error deactivating High Mobility vehicle: {str(e)}"
                            )
                            vehicle_data = {
                                "vin": vin,
                                "Eligibility": True,
                                "Real_Activation": False,
                                "Activation_Error": "Failed to deactivate or pending approval",
                            }
                            status_data.append(vehicle_data)
                            continue

                except Exception as e:
                    logging.error(f"Error processing High Mobility vehicle: {str(e)}")
                    vehicle_data = {
                        "vin": vin,
                        "Eligibility": True,
                        "Real_Activation": False,
                        "Activation_Error": "Failed to process High Mobility vehicle",
                    }
                    status_data.append(vehicle_data)
                    continue
        status_df = pd.DataFrame(status_data)
        await update_vehicle_activation_data(status_df)

    async def activation_volkswagen(self):
        """Process Volkswagen vehicle activation/deactivation with safe values for Google Sheets."""

        def safe_bool(val):
            """Convert None/NaN to empty string, keep True/False as-is."""
            if val is True:
                return True
            if val is False:
                return False
            return ""

        df_vw = self.fleet_info_df[self.fleet_info_df["oem"] == "volkswagen"]

        status_data = []

        async with aiohttp.ClientSession() as session:
            desired_states = {
                vin: {"desired_state": desired_state}
                for vin, desired_state in zip(
                    df_vw["vin"].tolist(), df_vw["activation"].tolist()
                )
            }

            current_states = await self.volkswagen_api.check_vehicle_status(
                session, desired_states.keys()
            )

            merged_states = {
                vin: {
                    "desired_state": desired_states[vin]["desired_state"],
                    "current_state": current_states.get(vin, {}).get("status_bool"),
                    "current_state_status": current_states.get(vin, {}).get("status"),
                    "reason": current_states.get(vin, {}).get("reason"),
                }
                for vin in desired_states
            }

            vins_in_desired_state = {
                vin: info
                for vin, info in merged_states.items()
                if info["desired_state"] == info["current_state"]
            }

            vins_to_activate = [
                vin
                for vin, info in merged_states.items()
                if info["desired_state"] is True and info["current_state"] is False
            ]

            vins_to_deactivate = [
                vin
                for vin, info in merged_states.items()
                if info["desired_state"] is False and info["current_state"] is True
            ]

            logging.info("\nVOLKSWAGEN STATUS UPDATE : Starting")
            for vin, info in vins_in_desired_state.items():
                logging.info(
                    f"Volkswagen vehicle {vin} already in desired state: {info['desired_state']}"
                )
                vehicle_data = {
                    "vin": vin,
                    "Eligibility": safe_bool(
                        info["current_state"]
                    ),  # VW confond eligibility & activation
                    "Real_Activation": safe_bool(info["current_state"]),
                    "Activation_Error": "",
                    "API_Detail": "",
                }
                status_data.append(vehicle_data)

            # --- Activation ---
            logging.info("\nVOLKSWAGEN ACTIVATION : Starting")
            await self.volkswagen_api.activate_vehicles(session, vins_to_activate)
            activated_current_states = await self.volkswagen_api.check_vehicle_status(
                session, vins_to_activate
            )

            for vin, info in activated_current_states.items():
                logging.info(
                    f"Update Volkswagen vehicle that has just been activated {vin} : {info['status_bool']}"
                )

                if not info.get("status_bool"):
                    if info.get("status") in REASON_MAPPING["volkswagen"].keys():
                        api_detail = REASON_MAPPING["volkswagen"][info.get("status")]
                    else:
                        api_detail = (
                            "Statut inconnu, voir avec l'équipe TECH pour le définir."
                        )
                else:
                    api_detail = ""

                vehicle_data = {
                    "vin": vin,
                    "Eligibility": safe_bool(info.get("status_bool")),
                    "Real_Activation": safe_bool(info.get("status_bool")),
                    "Activation_Error": (
                        str(info.get("status")) if not info.get("status_bool") else ""
                    ),
                    "API_Detail": (api_detail),
                }
                status_data.append(vehicle_data)

            # --- Deactivation ---
            logging.info("\nVOLKSWAGEN DEACTIVATION : Starting")
            await self.volkswagen_api.deactivate_vehicles(session, vins_to_deactivate)
            deactivated_current_states = await self.volkswagen_api.check_vehicle_status(
                session, vins_to_deactivate
            )

            for vin, info in deactivated_current_states.items():
                logging.info(
                    f"Update Volkswagen vehicle that has just been deactivated {vin} : {info['status_bool']}"
                )
                vehicle_data = {
                    "vin": vin,
                    "Eligibility": safe_bool(info.get("status_bool")),
                    "Real_Activation": safe_bool(info.get("status_bool")),
                    "Activation_Error": "",
                    "API_Detail": "",
                }
                status_data.append(vehicle_data)

        # --- Cleaning before sending to Google Sheets ---
        status_df = pd.DataFrame(status_data)
        status_df = status_df.replace([None, float("inf"), float("-inf")], "")
        status_df = status_df.where(pd.notnull(status_df), "")

        await update_vehicle_activation_data(status_df)

    async def activation_kia(self):
        """Process KIA vehicle activation/deactivation with safe values for Google Sheets."""

        def safe_bool(val):
            """Convert None/NaN to empty string, keep True/False as-is."""
            if val is True:
                return True
            if val is False:
                return False
            return ""

        df_kia = self.fleet_info_df[self.fleet_info_df["oem"] == "kia"]

        status_data = []

        async with aiohttp.ClientSession() as session:
            desired_states = {
                vin: {"desired_state": desired_state}
                for vin, desired_state in zip(
                    df_kia["vin"].tolist(), df_kia["activation"].tolist()
                )
            }

            merged = await self._get_current_states_kia(session, desired_states.keys())

            merged_states = {
                vin: {
                    "desired_state": desired_states[vin]["desired_state"],
                    "current_state": merged[vin]["status_bool"],
                    "current_state_status": merged[vin]["reason"],
                }
                for vin in merged.keys()
            }

            vins_in_desired_state = {
                vin: info
                for vin, info in merged_states.items()
                if info["desired_state"] == info["current_state"]
            }

            vins_to_activate = [
                vin
                for vin, info in merged_states.items()
                if info["desired_state"] is True and info["current_state"] is False
            ]

            vins_to_deactivate = [
                vin
                for vin, info in merged_states.items()
                if info["desired_state"] is False and info["current_state"] is True
            ]

            logging.info("KIA STATUS UPDATE : Starting")
            for vin, info in vins_in_desired_state.items():
                logging.info(
                    f"KIA vehicle {vin} already in desired state: {info['desired_state']}"
                )
                vehicle_data = {
                    "vin": vin,
                    "Eligibility": safe_bool(
                        info["current_state"]
                    ),  # VW confond eligibility & activation
                    "Real_Activation": safe_bool(info["current_state"]),
                    "Activation_Error": "",
                    "API_Detail": "",
                }
                status_data.append(vehicle_data)

            # --- Activation ---
            logging.info("KIA ACTIVATION : Starting")

            await self.kia_api._activate_vins(session, vins_to_activate)

            activated_current_states = await self._get_current_states_kia(
                session, vins_to_activate
            )

            for vin in vins_to_activate:
                logging.info(
                    f"Update Kia vehicle that has just been activated {vin} : {info['status_bool']}"
                )

                if vin not in activated_current_states.keys():
                    vehicle_data = {
                        "vin": vin,
                        "Eligibility": False,
                        "Real_Activation": False,
                        "Activation_Error": "vin not eligible for data sharing",
                        "API_Detail": "Le vin n'est pas éligible pour le partage de données, VIN erroné ou d'une version non prise en charge",
                    }

                else:
                    info = activated_current_states[vin]

                    if info["status_bool"]:
                        vehicle_data = {
                            "vin": vin,
                            "Eligibility": True,
                            "Real_Activation": True,
                            "Activation_Error": "",
                            "API_Detail": "",
                        }
                    else:
                        vehicle_data = {
                            "vin": vin,
                            "Eligibility": True,
                            "Real_Activation": False,
                            "Activation_Error": info["reason"],
                            "API_Detail": info["reason"],
                        }
                status_data.append(vehicle_data)

            # --- Deactivation ---
            logging.info("\nKIA DEACTIVATION : Starting")
            await self.kia_api.delete_consent(session, vins_to_deactivate)

            for vin, info in deactivated_current_states.items():
                logging.info(
                    f"Update KIA vehicle that has just been deactivated {vin} : {info['status_bool']}"
                )
                vehicle_data = {
                    "vin": vin,
                    "Eligibility": True,
                    "Real_Activation": False,
                    "Activation_Error": "",
                    "API_Detail": "",
                }
                status_data.append(vehicle_data)

        # --- Cleaning before sending to Google Sheets ---
        status_df = pd.DataFrame(status_data)
        status_df = status_df.replace([None, float("inf"), float("-inf")], "")
        status_df = status_df.where(pd.notnull(status_df), "")

        await update_vehicle_activation_data(status_df)

    # async def activation_tesla(self):
    #     """Process Tesla vehicle activation/deactivation.

    #     Returns:
    #         pd.DataFrame: DataFrame containing vehicle status with columns:
    #             - vin: Vehicle identification number
    #             - Eligibility: Whether the vehicle is eligible for activation
    #             - Real_Activation: Current activation status
    #             - Activation_Error: Any error messages
    #             - account_owner: Tesla account owner name
    #     """
    #     logging.info("Checking eligibility of Tesla vehicles")

    #     # Get Tesla vehicles from fleet info
    #     ggsheet_tesla = self.fleet_info_df[self.fleet_info_df["oem"] == "tesla"]

    #     # Get Tesla API data
    #     async with aiohttp.ClientSession() as session:
    #         api_tesla = await self.tesla_api._build_vin_mapping(session)

    #         # Create DataFrame for vehicles in API
    #         api_vehicles = pd.DataFrame(
    #             [
    #                 {
    #                     "vin": vin,
    #                     "Eligibility": True,
    #                     "Real_Activation": (
    #                         ggsheet_tesla[ggsheet_tesla["vin"] == vin][
    #                             "activation"
    #                         ].iloc[0]
    #                         == True
    #                         if not ggsheet_tesla[ggsheet_tesla["vin"] == vin].empty
    #                         else False
    #                     ),
    #                     "Activation_Error": None,
    #                     "account_owner": account_name,
    #                 }
    #                 for vin, account_name in api_tesla
    #             ]
    #         )
    #         print(f"Tesla vehicles in API: {len(api_vehicles)}")

    #         # Create DataFrame for vehicles not in API
    #         missing_vehicles = pd.DataFrame(
    #             [
    #                 {
    #                     "vin": vin,
    #                     "Eligibility": False,
    #                     "Real_Activation": False,
    #                     "Activation_Error": "Vehicle not found in Tesla accounts",
    #                     "account_owner": None,
    #                 }
    #                 for vin in ggsheet_tesla["vin"]
    #                 if vin
    #                 not in [
    #                     v[0] for v in api_tesla
    #                 ]  # Extract VINs from tuples for comparison
    #             ]
    #         )
    #         print(f"Missing tesla vehicles): {len(missing_vehicles)}")

    #         # Combine both DataFrames
    #         status_df = pd.concat([api_vehicles, missing_vehicles], ignore_index=True)
    #         print(f"Total tesla vehicles: {len(status_df)}")
    #         await update_vehicle_activation_data(status_df)

    # async def activation_tesla_particulier(self):
    #     """Process Tesla particulier vehicle activation/deactivation"""
    #     status_data = []

    #     async with aiohttp.ClientSession() as session:
    #         with get_connection() as con:
    #             cursor = con.cursor()
    #             cursor.execute("SELECT vin,full_name FROM tesla.user")
    #             vins = cursor.fetchall()

    #             for vin, full_name in vins:
    #                 try:
    #                     actual_state = await self.tesla_particulier_api.get_status(
    #                         vin, session, cursor
    #                     )
    #                     print(f"Actual state: {actual_state}")

    #                     if actual_state:
    #                         vehicle_data = {
    #                             "vin": vin,
    #                             "Eligibility": True,
    #                             "Real_Activation": True,
    #                             "Activation_Error": None,
    #                             "account_owner": full_name,
    #                         }
    #                         status_data.append(vehicle_data)
    #                     else:
    #                         vehicle_data = {
    #                             "vin": vin,
    #                             "Eligibility": False,
    #                             "Real_Activation": False,
    #                             "Activation_Error": "Particulier Tesla account not found",
    #                             "account_owner": full_name,
    #                         }
    #                         status_data.append(vehicle_data)

    #                 except Exception as e:
    #                     status_data.append(
    #                         {
    #                             "vin": vin,
    #                             "Eligibility": False,
    #                             "Real_Activation": False,
    #                             "Activation_Error": f"Error processing vehicle: {str(e)}",
    #                             "account_owner": full_name,
    #                         }
    #                     )

    #             status_df = pd.DataFrame(status_data)
    #             await update_vehicle_activation_data(status_df)

