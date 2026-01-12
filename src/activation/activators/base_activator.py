"""Base activator for OEM-specific activation and processing."""

import logging
from abc import ABC, abstractmethod

import pandas as pd
from sqlalchemy import select

from activation.config.config import MAKES_WITH_SOH_BIB_WO_MODEL_API
from activation.config.credentials import METRIC_SLACK_CHANNEL_ID
from core.slack_utils import send_slack_message
from core.sql_utils import get_async_session_maker
from db_models.vehicle import Vehicle


class BaseOEMActivator(ABC):
    """Abstract base class for OEM activators.

    Each OEM activator is responsible for:
    1. Activating/deactivating vehicles via the OEM API
    2. Processing activated vehicles to enrich their data
    """

    def __init__(
        self,
        api_client,
        vehicle_with_command: pd.DataFrame,
        vehicle_to_process: pd.DataFrame,
    ):
        """
        Initialize the activator.

        Args:
            api_client: The API client for this OEM
            vehicle_with_command: DataFrame with vehicles that need activation changes
            vehicle_to_process: DataFrame with vehicles that need processing
        """
        self.api = api_client
        self.vehicle_with_command = vehicle_with_command
        self.vehicle_to_process = vehicle_to_process
        self.oem_name = self.get_oem_name()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @abstractmethod
    def get_oem_name(self) -> str:
        """Return the OEM name used in the database."""
        ...

    @abstractmethod
    async def activate(self) -> None:
        """
        Handle activation/deactivation of vehicles for this OEM.

        This method should:
        1. Filter vehicles for this OEM
        2. Check current activation status
        3. Activate or deactivate as needed
        4. Update vehicle status in database
        """
        ...

    async def process(self) -> None:
        """
        Process activated vehicles to enrich their data.

        Default implementation for OEMs without special processing logic.
        Override this method if your OEM needs custom processing.
        """

        df_oem = self.get_vehicles_for_processing()

        if df_oem.empty:
            self.logger.info(f"No {self.get_oem_name()} vehicles to process")
            return

        vins_wo_vehicle_model_id = []
        vins_with_vehicle_model_id = []

        async with get_async_session_maker()() as db:
            for _, row in df_oem.iterrows():
                result = await db.execute(
                    select(Vehicle).where(Vehicle.vin == row["vin"])
                )
                vehicle = result.scalar_one_or_none()
                if vehicle.vehicle_model_id is None:
                    vins_wo_vehicle_model_id.append(row["vin"])
                else:
                    vins_with_vehicle_model_id.append(row["vin"])
                    vehicle.is_processed = True
                await db.commit()

            if self.get_oem_name() in MAKES_WITH_SOH_BIB_WO_MODEL_API:
                if len(vins_wo_vehicle_model_id) > 0:
                    send_slack_message(
                        METRIC_SLACK_CHANNEL_ID,
                        f"🚨 Alerte - Association VIN - Modèle\nLes vins suivants {self.get_oem_name()} n'ont pas de modèle de véhicule associé et le SoH ne pourra être calculé sans une capacité de batterie: {vins_wo_vehicle_model_id}. Il est conseillé de les désactiver.",
                    )

                if len(vins_with_vehicle_model_id) > 0:
                    send_slack_message(
                        METRIC_SLACK_CHANNEL_ID,
                        f"🚨 Alerte - Association VIN - Modèle\nLes vins suivants {self.get_oem_name()} ont un modèle de véhicule associé mais il vient d'une entrée manuelle (Bib ou client), la précision du SoH est donc incertaine: {vins_with_vehicle_model_id}.\n Cette alerte ne sonnera pas de nouveau et les véhicules seront intégrés dans le système.",
                    )

    def _create_history_entry(
        self,
        vehicle: Vehicle,
        activation_status: bool,
        message: str,
        oem_detail: str | None = None,
        override_requested_status: bool | None = None,
    ) -> dict:
        """Create a history entry dict for bulk insert."""
        return {
            "vehicle_id": vehicle.id,
            "activation_requested_status": (
                override_requested_status
                if override_requested_status is not None
                else vehicle.activation_requested_status
            ),
            "activation_status": activation_status,
            "activation_status_message": message,
            "oem_detail": oem_detail,
        }

    def _update_vehicle_and_create_history(
        self,
        vehicle: Vehicle,
        activation_status: bool,
        message: str,
        oem_detail: str | None = None,
        set_requested_status: bool | None = None,
    ) -> dict:
        """Update vehicle in memory and return history entry."""
        vehicle.activation_status = activation_status
        vehicle.activation_status_message = message
        if set_requested_status is not None:
            vehicle.activation_requested_status = set_requested_status

        # Pass override if set_requested_status was provided
        return self._create_history_entry(
            vehicle,
            activation_status,
            message,
            oem_detail,
            override_requested_status=set_requested_status,
        )

    def get_vehicles_for_activation(self) -> pd.DataFrame:
        """Filter vehicles for this OEM that need activation changes."""
        return self.vehicle_with_command[
            self.vehicle_with_command["oem"] == self.oem_name
        ]

    def get_vehicles_for_processing(self) -> pd.DataFrame:
        """Filter vehicles for this OEM that need processing."""
        return self.vehicle_to_process[self.vehicle_to_process["oem"] == self.oem_name]

    async def run(self) -> None:
        """
        Run the complete cycle: activation + processing.

        Args:
            vehicle_to_process: DataFrame with all activated vehicles needing processing
        """
        self.logger.info(f"Starting {self.oem_name.upper()} activator...")

        # Step 1: Activation/Deactivation
        try:
            await self.activate()
            self.logger.info(f"{self.oem_name.upper()} activation completed")
        except Exception as e:
            self.logger.error(f"Error during {self.oem_name} activation: {e!s}")
            raise

        # Step 2: Processing
        try:
            await self.process()
            self.logger.info(f"{self.oem_name.upper()} processing completed")
        except Exception as e:
            self.logger.error(f"Error during {self.oem_name} processing: {e!s}")
            raise

        self.logger.info(f"{self.oem_name.upper()} activator completed successfully ✓")
