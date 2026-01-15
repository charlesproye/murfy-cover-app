import logging

import pandas as pd
from sqlalchemy import or_, select, update

from activation.config.mappings import mapping_vehicle_type
from activation.config.settings import LOGGING_CONFIG
from core.gsheet_utils import load_excel_data
from core.sql_utils import get_sqlalchemy_engine
from db_models.company import Make, Oem
from db_models.vehicle import Battery, Vehicle, VehicleData, VehicleModel
from results.trendline.trendline_utils import (
    clean_battery_data,
    compute_trendline_functions,
    filter_data,
    filter_trendlines,
    prepare_data_for_fitting,
    update_database_trendlines,
)

logging.basicConfig(**LOGGING_CONFIG)
logger = logging.getLogger(__name__)


def generate_trendline_functions(df, odometer_column, soh_column):
    df_clean = clean_battery_data(df, odometer_column, soh_column)
    if df_clean.shape[0] < 20:
        return "Can't compute trendline"
    x_data, y_data = prepare_data_for_fitting(df_clean)
    mean, upper_bound, lower_bound = compute_trendline_functions(x_data, y_data)
    return mean, upper_bound, lower_bound


def load_and_clean_scrapping_data():
    raw_data = load_excel_data("Courbes de tendance", "Courbes OS")

    df_sheet = pd.DataFrame(
        data=raw_data[1:, :8],
        columns=[
            "make_name",
            "model_name",
            "type",
            "year",
            "odometer",
            "soh",
            "vin",
            "battery_capacity",
        ],
    )
    df_sheet = df_sheet[df_sheet["soh"] != ""]
    df_sheet["soh"] = (
        df_sheet["soh"].apply(lambda x: x.replace("%", "").strip()).astype(float) / 100
    )

    df_sheet = df_sheet[(df_sheet["soh"].notna()) & (df_sheet["soh"] != "")]

    df_sheet["model_name"] = df_sheet["model_name"].apply(str.lower)
    df_sheet["make_name"] = df_sheet["make_name"].apply(str.lower)

    df_sheet["odometer"] = (
        df_sheet["odometer"]
        .apply(lambda x: str(x).replace(",", "").replace(" ", "").strip())
        .astype(float)
    )

    return df_sheet.drop_duplicates(subset=["soh", "odometer", "model_name"])


def load_vehicle_data_from_db():
    engine = get_sqlalchemy_engine()

    stmt = (
        select(
            Vehicle.vin,
            VehicleModel.model_name,
            VehicleModel.id.label("vehicle_model_id"),
            VehicleModel.type,
            VehicleModel.version,
            Make.make_name,
            VehicleData.soh,
            VehicleData.soh_oem,
            VehicleData.odometer,
            Oem.oem_name,
            Oem.id.label("oem_id"),
            Make.id.label("make_id"),
        )
        .select_from(Vehicle)
        .outerjoin(VehicleModel, VehicleModel.id == Vehicle.vehicle_model_id)
        .outerjoin(VehicleData, VehicleData.vehicle_id == Vehicle.id)
        .outerjoin(Oem, Oem.id == VehicleModel.oem_id)
        .outerjoin(Battery, Battery.id == VehicleModel.battery_id)
        .outerjoin(Make, Make.id == VehicleModel.make_id)
        .where(or_(VehicleData.soh.is_not(None), VehicleData.soh_oem.is_not(None)))
    )

    with engine.connect() as connection:
        df = pd.read_sql(stmt, connection)

    df["soh_oem"] = pd.to_numeric(df["soh_oem"], errors="coerce")
    df["soh"] = pd.to_numeric(df["soh"], errors="coerce")

    # Use soh_oem if available
    df["soh"] = df.apply(
        lambda x: x["soh_oem"]
        if pd.notna(x["soh_oem"]) and x["soh_oem"] > 0
        else x["soh"],
        axis=1,
    )

    df = df[df["soh"] < 1.1]
    return df


def load_vehicle_models_from_db():
    engine = get_sqlalchemy_engine()

    stmt = (
        select(
            VehicleModel.model_name,
            VehicleModel.id.label("id"),
            VehicleModel.type,
            Make.make_name,
            Oem.oem_name,
            Oem.id.label("oem_id"),
            Make.id.label("make_id"),
        )
        .join(Oem, Oem.id == VehicleModel.oem_id)
        .join(Make, Make.id == VehicleModel.make_id)
    )

    with engine.connect() as conn:
        return pd.read_sql(stmt, conn)


def load_all_data():
    logger.info("Load data")
    df_sheet = load_and_clean_scrapping_data()
    cars_models = load_vehicle_models_from_db()
    df_sheet = df_sheet.merge(
        cars_models[["make_name", "oem_id", "oem_name"]].drop_duplicates(),
        on=[
            "make_name",
        ],
        how="left",
    )

    df_sheet["vehicle_model_id"] = df_sheet.apply(
        lambda x: mapping_vehicle_type(
            x["type"], x["make_name"], x["model_name"], cars_models
        ),
        axis=1,
    )
    df_prod = load_vehicle_data_from_db()
    df_all = pd.concat((df_prod, df_sheet), ignore_index=True)
    return df_all


def clean_db_trendlines():
    logger.info("Clean db trendlines...")

    stmt = (
        update(VehicleModel)
        .where(VehicleModel.trendline.is_not(None))
        .values(
            trendline=None,
            trendline_min=None,
            trendline_max=None,
        )
    )

    with get_sqlalchemy_engine().begin() as conn:
        conn.execute(stmt)


def update_trendline_oem():
    df_all = load_all_data()

    oem_ids = [oem for oem in df_all.oem_id.unique() if pd.notna(oem)]
    updated_oems = 0
    skipped_oems = 0

    engine = get_sqlalchemy_engine()

    for oem in oem_ids:
        df_temp = df_all[df_all["oem_id"] == oem]
        if filter_data(df_temp, "odometer", "vin", 50_000, 50_000, 50, 10, 10):
            mean_trend, upper_bound, lower_bound = generate_trendline_functions(
                df_temp, "odometer", "soh"
            )

            engine = get_sqlalchemy_engine()
            with engine.begin() as conn:
                update_database_trendlines(
                    [], mean_trend, upper_bound, lower_bound, oem_id=oem, conn=conn
                )
            updated_oems += 1
        else:
            logger.info(f"Can't compute trendline for oem: {oem}")
            skipped_oems += 1

    return {
        "oem_trendlines_attempted": len(oem_ids),
        "oem_trendlines_updated": updated_oems,
        "oem_trendlines_skipped": skipped_oems,
    }


def update_trendline_model():
    logger.info("Update trendline model...")
    df_all = load_all_data()

    model_ids = [mid for mid in df_all["vehicle_model_id"].unique() if pd.notna(mid)]
    updated_models = 0
    skipped_models = 0

    for model_car in model_ids:
        df_temp = df_all[(df_all["vehicle_model_id"] == model_car)].copy()
        try:
            if filter_data(df_temp, "odometer", "vin", 0, 0, 20, 0, 0):
                mean_trend, upper_bound, lower_bound = generate_trendline_functions(
                    df_temp, "odometer", "soh"
                )
                if filter_trendlines(mean_trend, upper_bound, lower_bound):
                    # Get all models with same model_name and battery_id
                    engine = get_sqlalchemy_engine()
                    with engine.begin() as conn:
                        subquery = (
                            select(VehicleModel.model_name, VehicleModel.battery_id)
                            .where(VehicleModel.id == model_car)
                            .subquery()
                        )

                        query = select(VehicleModel.id).where(
                            (VehicleModel.model_name == subquery.c.model_name)
                            & (VehicleModel.battery_id == subquery.c.battery_id)
                        )

                        result = conn.execute(query)
                        vehicle_model_ids = [str(row[0]) for row in result]

                        update_database_trendlines(
                            vehicle_model_ids,
                            mean_trend,
                            upper_bound,
                            lower_bound,
                            False,
                            conn=conn,
                        )

                        updated_models += len(vehicle_model_ids)
                else:
                    logger.info(f"Trendline not updated for car model {model_car}")
                    skipped_models += 1
            else:
                skipped_models += 1
        except Exception as e:
            logger.error(f"Error with car model: {model_car}: {e}")
            skipped_models += 1

    return {
        "model_trendlines_attempted": len(model_ids),
        "model_trendlines_updated": updated_models,
        "model_trendlines_skipped": skipped_models,
    }


def main():
    clean_db_trendlines()
    update_trendline_oem()
    update_trendline_model()


if __name__ == "__main__":
    main()
