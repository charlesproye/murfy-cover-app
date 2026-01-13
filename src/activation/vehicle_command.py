from logging import getLogger

import pandas as pd
from sqlalchemy.orm import sessionmaker

from activation.config.config import MAKES_TO_OEM
from core.sql_utils import get_sqlalchemy_engine
from db_models import Make, Oem
from db_models.vehicle import Vehicle, VehicleModel
from external_api.services.flash_report.vin_decoder.vin_decoder import VinDecoder

logger = getLogger("ingestion.vehicle_command")


def get_vehicle_table() -> pd.DataFrame:
    """Get the vehicle table from the database and transform it into a pandas dataframe."""
    engine = get_sqlalchemy_engine()
    Session = sessionmaker(bind=engine)
    db = Session()
    vehicles = (
        db.query(Vehicle, VehicleModel, Make, Oem)
        .outerjoin(VehicleModel, Vehicle.vehicle_model_id == VehicleModel.id)
        .outerjoin(Make, VehicleModel.make_id == Make.id)
        .outerjoin(Oem, VehicleModel.oem_id == Oem.id)
        .all()
    )

    vehicles_data = [
        {
            "id": str(vehicle.Vehicle.id),
            "vin": vehicle.Vehicle.vin,
            "fleet_id": str(vehicle.Vehicle.fleet_id),
            "region_id": str(vehicle.Vehicle.region_id),
            "vehicle_model_id": str(vehicle.Vehicle.vehicle_model_id)
            if vehicle.Vehicle.vehicle_model_id
            else None,
            "make": vehicle.Make.make_name if vehicle.Make else None,
            "oem": vehicle.Oem.oem_name if vehicle.Oem else None,
            "activation_requested_status": vehicle.Vehicle.activation_requested_status,
            "activation_start_date": vehicle.Vehicle.activation_start_date,
            "activation_end_date": vehicle.Vehicle.activation_end_date,
            "activation_status_message": vehicle.Vehicle.activation_status_message,
            "activation_comment": vehicle.Vehicle.activation_comment,
            "activation_status": vehicle.Vehicle.activation_status,
            "is_processed": vehicle.Vehicle.is_processed,
        }
        for vehicle in vehicles
    ]

    db.close()
    return pd.DataFrame(vehicles_data)


def add_oem_column(df: pd.DataFrame) -> pd.DataFrame:
    """Add the OEM column to the dataframe."""
    vin_decoder = VinDecoder()

    for index, row in df.iterrows():
        if row["make"] is None:
            df.at[index, "make"] = vin_decoder.decode(row["vin"])[0].lower()
            df.at[index, "oem"] = MAKES_TO_OEM.get(df.at[index, "make"], None)

    return df


def update_activation_status(df: pd.DataFrame) -> pd.DataFrame:
    """Update the activation status according to the deactivation date."""
    today = pd.Timestamp.now().normalize()

    df["activation_start_date"] = pd.to_datetime(
        df["activation_start_date"], errors="coerce"
    )
    df["activation_end_date"] = pd.to_datetime(
        df["activation_end_date"], errors="coerce"
    )

    mask_dates_present = (
        (df["activation_start_date"].notna() | df["activation_end_date"].notna())
        & (df["activation_requested_status"] == True)  # noqa: E712
    )

    active = (
        df["activation_start_date"].isna() | (df["activation_start_date"] <= today)
    ) & (df["activation_end_date"].isna() | (df["activation_end_date"] > today))

    df.loc[mask_dates_present, "activation_requested_status"] = active

    return df


async def get_vehicle_command() -> pd.DataFrame:
    df = get_vehicle_table()
    df = add_oem_column(df)
    df = update_activation_status(df)

    return df
