import logging

import pandas as pd
from sqlalchemy import text

from core.gsheet_utils import load_excel_data
from core.sql_utils import get_connection, get_sqlalchemy_engine
from results.trendline.trendline_utils import *


def generate_trendline_functions(df, odometer_column, soh_column):
    """

    Parameters:
    -----------
    df : pd.DataFrame
        Dataframe with SoH and Odometer column
    soh_column: str
        Column name with the SoH
    odometer_column: str
       Column name with the odometer
    Returns:
    --------
    tupple
        trendlines mean, upper and lower bound
    """
    df_clean = clean_battery_data(df, odometer_column, soh_column)
    if df_clean.shape[0] < 20:
        return "Can't compute trendline"
    x_data, y_data = prepare_data_for_fitting(df_clean)
    coef_mean, coef_lower, coef_upper, mean, upper_bound, lower_bound = (
        compute_main_trendline(x_data, y_data)
    )
    if coef_upper[0] >= 0:
        upper_bound = compute_upper_bound(df_clean, mean, coef_mean)
    if coef_lower[0] >= 0:
        lower_bound = compute_lower_bound(df_clean, mean, coef_mean)
    return mean, upper_bound, lower_bound


def load_all_data():
    # data from scrapping
    df_sheet = load_excel_data("Courbes de tendance", "Courbes OS")
    df_sheet = pd.DataFrame(
        data=df_sheet[1:, :8],
        columns=[
            "make_name",
            "model_name",
            "type",
            "year",
            "odometer",
            "soh",
            "link",
            "battery_capacity",
        ],
    )
    df_sheet["model_name"] = df_sheet["model_name"].apply(str.lower)
    df_sheet["make_name"] = df_sheet["make_name"].apply(str.lower)
    df_sheet["soh"] = (
        df_sheet["soh"].apply(lambda x: x.replace("%", "").strip()).astype(float) / 100
    )
    df_sheet["odometer"] = (
        df_sheet["odometer"]
        .apply(lambda x: str(x).replace(",", "").strip())
        .astype(float)
    )
    logging.info("Starting trendline update for oem")

    # data from db
    with get_connection() as connection:
        query = """
            SELECT vm.model_name, vm.id as vehicle_model_id, vm.type, m.make_name, vd.soh, vd.soh_oem, vd.odometer, o.oem_name, o.id as oem_id, m.id as make_id FROM vehicle v
            left JOIN vehicle_model vm ON vm.id = v.vehicle_model_id
            left JOIN vehicle_data vd ON vd.vehicle_id = v.id
            left JOIN oem o ON o.id = vm.oem_id
            left JOIN battery b ON b.id = vm.battery_id
            left JOIN make m on m.id = vm.make_id
            where (soh is not NULL or soh_oem is not null)
            and o.id not in (uuid('fe6fba2f-1e10-43a7-b8b0-2aa021f97c3f'), uuid('1b093647-0d1b-409f-8b37-871bc81ac9ca'), uuid('5d2e8c26-d2bb-4c24-812e-59c3a7508213'), uuid('fc285a82-f5ea-44a0-8f2e-706cd5f8c868'))
        """
        df = pd.read_sql(query, connection)
    df["soh_oem"] = pd.to_numeric(df["soh_oem"], errors="coerce")
    df["soh"] = pd.to_numeric(df["soh"], errors="coerce")
    df["soh"] = df.apply(
        lambda x: x["soh_oem"] if float(x["soh_oem"]) > 0 else x["soh"], axis=1
    )
    df = df[df["soh"] < 1.1]

    engine = get_sqlalchemy_engine()
    con = engine.connect()
    with engine.connect() as connection:
        dbeaver_df = pd.read_sql(
            text("""SELECT m.make_name, o.id as oem_id, o.oem_name, m.id as make_id FROM make m
                                    join oem o on o.id=m.oem_id;"""),
            con,
        )
        df_merge = df_sheet.merge(
            dbeaver_df,
            on=[
                "make_name",
            ],
            how="left",
        )

    df_all = pd.concat((df, df_merge), ignore_index=True)
    return df_all


def update_trendline_oem():
    df_all = load_all_data()

    for oem in df_all.oem_id.unique():
        df_temp = df_all[df_all["oem_id"] == oem]
        if filtrer_trendlines(
            df_temp, "odometer", "odometer", 50_000, 50_000, 50, 10, 10
        ):
            mean_trend, upper_bound, lower_bound = generate_trendline_functions(
                df_temp, "odometer", "soh"
            )
            update_database_trendlines(
                None, mean_trend, upper_bound, lower_bound, oem_id=oem
            )
        else:
            logging.info(f"Can't compute trendline for oem: {oem}")


def update_trendline_model():
    df_all = load_all_data()

    logging.info("Starting trendline update from gsheet")

    for model_car in load_all_data()["id"].unique():
        df_temp = load_all_data()[(load_all_data()["id"] == model_car)].copy()
        try:
            if filtrer_trendlines(df_temp, "Odomètre (km)", "lien", 0, 0, 20, 0, 0):
                mean_trend, upper_bound, lower_bound = generate_trendline_functions(
                    df_temp, "Odomètre (km)", "SoH"
                )
                update_database_trendlines(
                    model_car, mean_trend, upper_bound, lower_bound, False
                )
                logging.info(f"Trendline update for car model {model_car}")
        except Exception as e:
            logging.error(f"Error with car model: {model_car}: {e}")


def main():
    update_trendline_oem()
    update_trendline_model()


if __name__ == "__main__":
    main()

