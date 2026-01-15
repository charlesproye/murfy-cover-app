import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from core.s3.s3_utils import S3Service
from core.sql_utils import get_sqlalchemy_engine
from db_models import Vehicle, VehicleData, VehicleStatus


class VehicleStatusTracker:
    def __init__(self, logger: logging.Logger):
        self.s3 = S3Service()
        self.engine = get_sqlalchemy_engine()
        self.session = sessionmaker(bind=self.engine)
        self.makes = [
            "ford",
            "renault",
            "stellantis",
            "volkswagen",
            "volvo_cars",
            "mercedes_benz",
            "bmw",
            "kia",
            "tesla_fleet_telemetry",
        ]
        self.logger = logger

    def _get_table(self, model) -> pd.DataFrame:
        with self.session() as session:
            subquery = select(model)
            data = session.execute(subquery).scalars().all()
            data = pd.DataFrame(
                [
                    {c.name: getattr(obj, c.name) for c in obj.__table__.columns}
                    for obj in data
                ]
            )
        return data

    def _get_parquet_as_pd(self, make: str):
        return self.s3.read_parquet_df(f"result_phases/result_phases_{make}.parquet")

    def _upsert_vehicle_status_batch(self, vehicle_statuses: list[VehicleStatus]):
        """Upsert a list of vehicle statuses efficiently."""
        with self.session() as session:
            keys = [
                (vs.vin, vs.status_name, vs.process_step) for vs in vehicle_statuses
            ]
            existing_records = (
                session.query(VehicleStatus)
                .filter(VehicleStatus.vin.in_([k[0] for k in keys]))
                .all()
            )

            existing_dict = {
                (rec.vin, rec.status_name, rec.process_step): rec
                for rec in existing_records
            }

            for vehicle_status in vehicle_statuses:
                key = (
                    vehicle_status.vin,
                    vehicle_status.status_name,
                    vehicle_status.process_step,
                )
                existing = existing_dict.get(key)

                if existing:
                    existing.status_value = vehicle_status.status_value
                    existing.updated_at = vehicle_status.updated_at
                else:
                    session.add(vehicle_status)

            session.commit()

    def check_required_activation(self):
        # Delete all required activation statuses to get only wished activation statuses
        with self.session() as session:
            session.query(VehicleStatus).filter(
                VehicleStatus.status_name == "REQUIRED_ACTIVATION"
            ).delete()
            session.commit()
        vehicle = self._get_table(Vehicle)

        vehicle_statuses = []
        for _, row in vehicle.iterrows():
            vehicle_statuses.append(
                VehicleStatus(
                    vehicle_id=row["id"],
                    vin=row["vin"],
                    status_name="REQUIRED_ACTIVATION",
                    status_value=row["activation_requested_status"]
                    if row["activation_requested_status"]
                    else False,
                    process_step="ACTIVATION",
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            )

        self._upsert_vehicle_status_batch(vehicle_statuses)

    def check_actual_activation(self):
        vehicle = self._get_table(Vehicle)
        vehicle = vehicle[["id", "vin", "activation_status"]]

        vehicle_statuses = []
        for _, row in vehicle.iterrows():
            vehicle_statuses.append(
                VehicleStatus(
                    vehicle_id=row["id"],
                    vin=row["vin"],
                    status_name="ACTUAL_ACTIVATION",
                    status_value=row["activation_status"]
                    if row["activation_status"]
                    else False,
                    process_step="ACTIVATION",
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            )
        self._upsert_vehicle_status_batch(vehicle_statuses)

    def check_soh_rph_availability(self, column_soh: str):
        # Update all SOH RPH availability statuses to False to get only reality in case of deletion of SOH in rph
        with self.session() as session:
            session.query(VehicleStatus).filter(
                VehicleStatus.status_name == f"{column_soh.upper()}_AVAILABILITY_AT_RPH"
            ).update({"status_value": False, "updated_at": datetime.now()})
            session.commit()
        for make in self.makes:
            self.logger.info(f"Checking {make} for SOH RPH availability")
            rph = self._get_parquet_as_pd(make)
            if column_soh in rph.columns:
                if "VIN" in rph.columns:
                    rph = rph.rename(columns={"VIN": "vin"})

                data = rph[rph[column_soh].notna()]
                vehicle = self._get_table(Vehicle)
                data = data.merge(vehicle[["vin", "id"]], on="vin", how="left")
                data = data.groupby(["vin", "id"]).size().reset_index(name="count")

                vehicle_statuses = []
                for _, row in data.iterrows():
                    vehicle_statuses.append(
                        VehicleStatus(
                            vehicle_id=row["id"] if pd.notna(row["id"]) else None,
                            vin=row["vin"],
                            status_name=f"{column_soh.upper()}_AVAILABILITY_AT_RPH",
                            status_value=True,
                            process_step="TRANSFORM",
                            created_at=datetime.now(),
                            updated_at=datetime.now(),
                        )
                    )
                self._upsert_vehicle_status_batch(vehicle_statuses)

    def check_column_availability(self, column_name: str):
        # Update all column availability statuses to False to get only reality in case of deletion of column in postgres
        with self.session() as session:
            session.query(VehicleStatus).filter(
                VehicleStatus.status_name == f"{column_name.upper()}_AVAILABILITY"
            ).update({"status_value": False, "updated_at": datetime.now()})
            session.commit()
        vehicle = self._get_table(Vehicle)
        vehicle_activated = vehicle
        vehicle_data = self._get_table(VehicleData)
        data = vehicle_activated[["vin", "id"]].merge(
            vehicle_data[["vehicle_id", column_name.lower()]],
            left_on="id",
            right_on="vehicle_id",
            how="left",
        )
        data = data[data[column_name.lower()].notna()]
        data = data.groupby(["vin", "id"]).size().reset_index(name="count")

        vehicle_statuses = []
        for _, row in data.iterrows():
            vehicle_statuses.append(
                VehicleStatus(
                    vehicle_id=row["id"],
                    vin=row["vin"],
                    status_name=f"{column_name.upper()}_AVAILABILITY",
                    status_value=True,
                    process_step="TRANSFORM",
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            )
        self._upsert_vehicle_status_batch(vehicle_statuses)

    def check_soh_overall_availability(self, at_rph: bool = False):
        # Update all SOH overall availability statuses to False to get only reality in case of deletion of SOH in postgres
        with self.session() as session:
            session.query(VehicleStatus).filter(
                VehicleStatus.status_name
                == f"SOH_OVERALL_AVAILABILITY{'_AT_RPH' if at_rph else ''}"
            ).update({"status_value": False, "updated_at": datetime.now()})
            session.commit()
            status_names = [
                f"{column_soh.upper()}_AVAILABILITY{'_AT_RPH' if at_rph else ''}"
                for column_soh in ["SOH", "SOH_OEM"]
            ]

            vehicle_statuses = self._get_table(VehicleStatus)
            data = vehicle_statuses[vehicle_statuses["status_name"].isin(status_names)]
            vins = []
            vehicle_statuses = []
            for _, row in data.iterrows():
                if row["vin"] not in vins:
                    vehicle_statuses.append(
                        VehicleStatus(
                            vehicle_id=row["vehicle_id"],
                            vin=row["vin"],
                            status_name=f"SOH_OVERALL_AVAILABILITY{'_AT_RPH' if at_rph else ''}",
                            status_value=True,
                            process_step="TRANSFORM",
                            created_at=datetime.now(),
                            updated_at=datetime.now(),
                        )
                    )
                    vins.append(row["vin"])
            self._upsert_vehicle_status_batch(vehicle_statuses)

    def run_all_checks(self):
        self.logger.info("Running all checks")
        self.logger.info("> Checking required activation...")
        self.check_required_activation()
        self.logger.info("> Checking actual activation...")
        self.check_actual_activation()
        self.logger.info("> Checking odometer column availability...")
        self.check_column_availability("ODOMETER")
        self.logger.info("> Checking SOH RPH availability...")
        self.check_soh_rph_availability("SOH")
        self.logger.info("> Checking SOH OEM RPH availability...")
        self.check_soh_rph_availability("SOH_OEM")
        self.logger.info("> Checking SOH availability...")
        self.check_column_availability("SOH")
        self.logger.info("> Checking SOH OEM availability...")
        self.check_column_availability("SOH_OEM")
        self.logger.info("> Checking SOH overall availability at RPH...")
        self.check_soh_overall_availability(at_rph=True)
        self.logger.info("> Checking SOH overall availability...")
        self.check_soh_overall_availability()
        self.logger.info("All checks completed")
