import json
import logging
import time

import aiohttp
import pandas as pd

from activation.api.bmw_client import BMWApi
from activation.api.hm_client import HMApi
from activation.api.kia_client import KiaApi
from activation.api.renault_client import RenaultApi
from activation.api.stellantis_client import StellantisApi
from activation.api.volkswagen_client import VolkswagenApi
from activation.config.config import REASON_MAPPING
from activation.services.google_sheet_service import update_vehicle_activation_data


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

    async def _get_vehicle_status_hm(self, data: list[dict], vin: str) -> dict | None:
        vehicle = next((v for v in data if v.get("vin") == vin), None)
        if not vehicle:
            return None

        status = vehicle.get("status")
        reason = None

        changelog = vehicle.get("changelog", [])
        if changelog and isinstance(changelog[-1], dict):
            reason = changelog[-1].get("reason")

        return {"status": status, "reason": reason}

    async def _get_current_states_kia(
        self, session: aiohttp.ClientSession, vins: list[str]
    ):
        """Get the current states of a list of vins."""

        current_states_vehicle = await self.kia_api._get_status_consent_type(
            session, "vehicle"
        )

        current_states_dtc = await self.kia_api._get_status_consent_type(
            session, "dtcInfo"
        )

        merged = {}

        for vin in vins:
            merged[vin] = {"vehicle": False, "dtcInfo": False}

        for el in current_states_vehicle:
            if el["vin"] in merged:
                merged[el["vin"]]["vehicle"] = True
        for el in current_states_dtc:
            if el["vin"] in merged:
                merged[el["vin"]]["dtcInfo"] = True

        for vin in merged:
            merged[vin]["status_bool"] = bool(
                merged[vin]["vehicle"] and merged[vin]["dtcInfo"]
            )
            if merged[vin]["vehicle"] and (not merged[vin]["dtcInfo"]):
                merged[vin]["reason"] = "Package véhicule activé mais pas dtcInfo"
            elif merged[vin]["dtcInfo"] and (not merged[vin]["vehicle"]):
                merged[vin]["reason"] = "Package dtcInfo activé mais pas véhicule"
            elif (not merged[vin]["dtcInfo"]) and (not merged[vin]["vehicle"]):
                merged[vin]["reason"] = "Aucun package activé"
            else:
                merged[vin]["reason"] = ""

        return merged

    async def _add_to_fleet(
        self, vin: str, session: aiohttp.ClientSession
    ) -> tuple[bool, str | None]:
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

            if isinstance(result, str):
                result = json.loads(result) if result.strip() else {}

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
            error_msg = f"Error adding vehicle to fleet: {e!s}"
            logging.error(error_msg)
            return False, error_msg

    async def _activate_bmw(
        self, session: aiohttp.ClientSession, vin: str
    ) -> tuple[bool, str | None]:
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

            if not status_code:
                return False, result

            logging.info("Create clearance response")

            if isinstance(result, str):
                result = json.loads(result) if result.strip() else {}

            if status_code in [200, 201, 204]:
                fleet_success, fleet_error = await self._add_to_fleet(vin, session)
                if not fleet_success:
                    return False, fleet_error
                return True, None
            elif "message" in result:
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
            error_msg = f"Error activating BMW vehicle: {e!s}"
            logging.error(error_msg)
            return False, error_msg

    async def activation_stellantis(self):
        """Process Stellantis vehicle activation/deactivation."""
        df_stellantis = self.fleet_info_df[(self.fleet_info_df["oem"] == "stellantis")]

        vin_list = df_stellantis["vin"].tolist()

        status_data = []
        async with aiohttp.ClientSession() as session:
            eligibilities = await self.stellantis_api.is_eligible_batch(
                vin_list, session
            )
            statuses = await self.stellantis_api.get_status_batch(vin_list, session)

        to_activate = {}
        to_deactivate = {}
        status_equal = {}

        for _, row in df_stellantis.iterrows():
            vin = row["vin"]
            desired_status = row["activation"]
            current_status = statuses.get(vin, {}).get("status", "") == "activated"
            reason = statuses.get(vin, {}).get("status", "")
            contract_id = statuses.get(vin, {}).get("id", "")

            entry = {
                "eligibility": eligibilities.get(vin, False),
                "status_desired": desired_status,
                "current_status": current_status,
                "reason": reason,
                "id": contract_id,
            }

            if not current_status and desired_status:
                to_activate[vin] = entry
            elif current_status and not desired_status:
                to_deactivate[vin] = entry
            elif current_status == desired_status:
                status_equal[vin] = entry
            else:
                pass

        if len(status_equal) > 0:
            for key, values in status_equal.items():
                if values["status_desired"]:
                    vehicle_data = {
                        "vin": key,
                        "Eligibility": values["eligibility"],
                        "Real_Activation": values["status_desired"],
                        "Activation_Error": "",
                        "API_Detail": "",
                    }
                    status_data.append(vehicle_data)
                else:
                    vehicle_data = {
                        "vin": key,
                        "Eligibility": values["eligibility"],
                        "Real_Activation": values["status_desired"],
                        "Activation_Error": values["reason"],
                        "API_Detail": "",
                    }
                    status_data.append(vehicle_data)

        # Activation & Deactivation per batch unavailable with Mobilisight
        if len(to_activate) > 0:
            async with aiohttp.ClientSession() as session:
                for vin, entry in to_activate.items():
                    if entry["eligibility"]:
                        status_code, result = await self.stellantis_api.activate(
                            vin, session
                        )
                        if status_code in [200, 201, 204]:
                            if result["status"] == "activated":
                                vehicle_data = {
                                    "vin": vin,
                                    "Eligibility": entry["eligibility"],
                                    "Real_Activation": entry["status_desired"],
                                    "Activation_Error": result["status"],
                                    "API_Detail": "",
                                }
                                status_data.append(vehicle_data)
                            else:
                                vehicle_data = {
                                    "vin": vin,
                                    "Eligibility": entry["eligibility"],
                                    "Real_Activation": False,
                                    "Activation_Error": result["status"],
                                    "API_Detail": "",
                                }
                                status_data.append(vehicle_data)
                            logging.info(
                                f"Stellantis vehicle {vin} activated successfully"
                            )
                        elif status_code == 409:
                            logging.info(
                                f"Stellantis vehicle {vin} activation already in progress"
                            )
                            vehicle_data = {
                                "vin": vin,
                                "Eligibility": entry["eligibility"],
                                "Real_Activation": entry["current_status"],
                                "Activation_Error": entry["reason"],
                                "API_Detail": "",
                            }
                            status_data.append(vehicle_data)
                        else:
                            vehicle_data = {
                                "vin": vin,
                                "Eligibility": entry["eligibility"],
                                "Real_Activation": False,
                                "Activation_Error": result,
                                "API_Detail": "",
                            }
                            status_data.append(vehicle_data)
                            logging.error(
                                f"Failed to activate Stellantis vehicle {vin}: HTTP {status_code} - {result}"
                            )
        # Deactivation
        if len(to_deactivate) > 0:
            async with aiohttp.ClientSession() as session:
                for vin, entry in to_deactivate.items():
                    status_code, error_msg = await self.stellantis_api.deactivate(
                        entry["id"], session
                    )
                    if status_code in [200, 204]:
                        logging.info(
                            f"Stellantis vehicle {vin} deactivated successfully"
                        )
                        vehicle_data = {
                            "vin": vin,
                            "Eligibility": entry["eligibility"],
                            "Real_Activation": False,
                            "Activation_Error": None,
                            "API_Detail": None,
                        }
                        status_data.append(vehicle_data)
                    else:
                        logging.info(
                            f"Failed to deactivate Stellantis vehicle {vin}: HTTP {status_code} - {error_msg}"
                        )
                        vehicle_data = {
                            "vin": vin,
                            "Eligibility": entry["eligibility"],
                            "Real_Activation": entry["current_status"],
                            "Activation_Error": "Failed to deactivate",
                            "API_Detail": error_msg,
                        }
                        status_data.append(vehicle_data)
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
                        "API_Detail": None,
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
                            "API_Detail": None,
                        }
                        status_data.append(vehicle_data)
                        continue
                    elif api_detail and "header unit" in api_detail:
                        logging.info(
                            f"BMW vehicle {vin} header unit version is too old to have a sufficient data frequency"
                        )
                        vehicle_data = {
                            "vin": vin,
                            "Eligibility": True,
                            "Real_Activation": False,
                            "Activation_Error": "Activation failed",
                            "API_Detail": api_detail,
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

                            if not current_state:
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
                                    "API_Detail": None,
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
                            f"BMW vehicle {vin} deactivation failed - Error: {e!s}"
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

        status_data = []

        df_hm = self.fleet_info_df[
            self.fleet_info_df["oem"].isin(
                ["renault", "ford", "mercedes", "volvo-cars"]
            )
        ][["vin", "oem", "activation", "eligibility"]]

        # Get all status from HM
        async with aiohttp.ClientSession() as session:
            data = await self.hm_api.get_all_status(session)

        status_dict = {
            vehicle["vin"]: {
                "status": vehicle["status"] == "approved",
                "status_hm": vehicle["status"],
            }
            for vehicle in data
        }

        to_activate = {}
        to_deactivate = {}
        status_equal = {}
        async with aiohttp.ClientSession() as session:
            for _, row in df_hm.iterrows():
                if row["activation"] and not row["eligibility"]:
                    eligibility = await self.hm_api.get_eligibility(
                        row["vin"], row["oem"], session
                    )
                    if eligibility:
                        row["eligibility"] = eligibility
                    else:
                        row["eligibility"] = False

                vin = row["vin"]
                oem = row["oem"]
                desired_status = row["activation"]
                current_status_info = status_dict.get(
                    vin, {"status": False, "status_hm": "unknown"}
                )
                current_status = current_status_info["status"]
                status_hm = current_status_info["status_hm"]

                entry = {
                    "oem": oem,
                    "status_desired": desired_status,
                    "current_status": current_status,
                    "reason": status_hm,
                    "eligibility": row["eligibility"],
                }

                if current_status is False and desired_status is True:
                    to_activate[vin] = entry
                elif current_status is True and desired_status is False:
                    to_deactivate[vin] = entry
                elif current_status == desired_status:
                    status_equal[vin] = entry
                else:
                    pass

        # Update status when desired status is equal to current status
        logging.info("HM Status update started")
        if len(status_equal) > 0:
            for key, values in status_equal.items():
                if values["status_desired"]:
                    vehicle_data = {
                        "vin": key,
                        "Eligibility": values["eligibility"],
                        "Real_Activation": values["status_desired"],
                        "Activation_Error": "",
                        "API_Detail": "",
                    }
                    status_data.append(vehicle_data)
                elif values["reason"] == "unknown":
                    vehicle_data = {
                        "vin": key,
                        "Eligibility": values["eligibility"],
                        "Real_Activation": False,
                        "Activation_Error": "Activation non demandée sur le Gsheet",
                        "API_Detail": "",
                    }
                    status_data.append(vehicle_data)
                else:
                    vehicle_data = {
                        "vin": key,
                        "Eligibility": values["eligibility"],
                        "Real_Activation": values["status_desired"],
                        "Activation_Error": values["reason"],
                        "API_Detail": "",
                    }
                    status_data.append(vehicle_data)
        # Activation
        logging.info("HM Activation started")
        if len(to_activate) > 0:
            async with aiohttp.ClientSession() as session:
                results_activation = await self.hm_api.create_clearance_batch(
                    to_activate, session
                )
            # Recheck the status of the vehicles as it can be pending as a result of the batch activation
            # and switch to rejected immediately
            async with aiohttp.ClientSession() as session:
                time.sleep(3)
                data = await self.hm_api.get_all_status(session)
            for vin_dict in results_activation["vehicles"]:
                status = await self._get_vehicle_status_hm(data, vin_dict["vin"])
                if status:
                    vehicle_data = {
                        "vin": vin_dict["vin"],
                        "Eligibility": to_activate[vin_dict["vin"]]["eligibility"],
                        "Real_Activation": status["status"] == "approved",
                        "Activation_Error": status["status"],
                        "API_Detail": status["reason"],
                    }
                    status_data.append(vehicle_data)
                elif vin_dict["status"] == "error":
                    vehicle_data = {
                        "vin": vin_dict["vin"],
                        "Eligibility": to_activate[vin_dict["vin"]]["eligibility"],
                        "Real_Activation": False,
                        "Activation_Error": vin_dict.get("status"),
                        "API_Detail": vin_dict.get("description"),
                    }
                    status_data.append(vehicle_data)

        # Deactivation - HM does not support batch deactivation
        logging.info("HM Deactivation started")
        if len(to_deactivate) > 0:
            for vin in to_deactivate:
                async with aiohttp.ClientSession() as session:
                    deactivation_success = await self.hm_api.delete_clearance(
                        vin, session
                    )
                if not deactivation_success:
                    logging.info(f"Failed to deactivate High Mobility vehicle : {vin}")
                    vehicle_data = {
                        "vin": vin,
                        "Eligibility": True,
                        "Real_Activation": True,
                        "Activation_Error": "La désactivation a échoué",
                    }
                    status_data.append(vehicle_data)
                    continue
                else:
                    logging.info(
                        f"Successfully deactivated High Mobility vehicle : {vin}"
                    )
                    vehicle_data = {
                        "vin": vin,
                        "Eligibility": True,
                        "Real_Activation": False,
                        "Activation_Error": None,
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
                    df_vw["vin"].tolist(), df_vw["activation"].tolist(), strict=False
                )
            }

            current_states = await self.volkswagen_api.check_vehicle_status(
                session, desired_states
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
                    if info.get("status") in REASON_MAPPING["volkswagen"]:
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
                    "API_Detail": api_detail,
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
                    df_kia["vin"].tolist(), df_kia["activation"].tolist(), strict=False
                )
            }

            merged = await self._get_current_states_kia(session, desired_states.keys())

            merged_states = {
                vin: {
                    "desired_state": desired_states[vin]["desired_state"],
                    "current_state": merged[vin]["status_bool"],
                    "current_state_status": merged[vin]["reason"],
                }
                for vin in merged
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

            logging.info(f"Vins to activate: {vins_to_activate}")

            if len(vins_to_activate) > 0:
                # Par batch de 50 vins
                for i in range(0, len(vins_to_activate), 50):
                    batch = vins_to_activate[i : i + 50]
                    await self.kia_api._activate_vins(session, batch)

                    activated_current_states = await self._get_current_states_kia(
                        session, batch
                    )

                    for vin in batch:
                        if vin not in activated_current_states:
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
            else:
                logging.info("No vins to activate")

            # --- Deactivation ---
            logging.info("\nKIA DEACTIVATION : Starting")
            logging.info(f"Vins to deactivate: {vins_to_deactivate}")

            if len(vins_to_deactivate) > 0:
                # Par batch de 50 vins
                for i in range(0, len(vins_to_deactivate), 50):
                    batch = vins_to_deactivate[i : i + 50]
                    await self.kia_api.delete_consent(session, batch)

                    deactivated_current_states = await self._get_current_states_kia(
                        session, batch
                    )

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
            else:
                logging.info("No vins to deactivate")

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
    #             - account_owner_tesla: Tesla account owner name
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
    #                     "account_owner_tesla": account_name,
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
    #                     "account_owner_tesla": None,
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
    #                             "account_owner_tesla": full_name,
    #                         }
    #                         status_data.append(vehicle_data)
    #                     else:
    #                         vehicle_data = {
    #                             "vin": vin,
    #                             "Eligibility": False,
    #                             "Real_Activation": False,
    #                             "Activation_Error": "Particulier Tesla account not found",
    #                             "account_owner_tesla": full_name,
    #                         }
    #                         status_data.append(vehicle_data)

    #                 except Exception as e:
    #                     status_data.append(
    #                         {
    #                             "vin": vin,
    #                             "Eligibility": False,
    #                             "Real_Activation": False,
    #                             "Activation_Error": f"Error processing vehicle: {str(e)}",
    #                             "account_owner_tesla": full_name,
    #                         }
    #                     )

    #             status_df = pd.DataFrame(status_data)
    #             await update_vehicle_activation_data(status_df)
