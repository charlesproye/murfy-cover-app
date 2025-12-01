from logging import Logger, getLogger

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    root_mean_squared_error,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from core.gsheet_utils import load_excel_data
from core.s3.s3_utils import S3Service
from core.sql_utils import get_sqlalchemy_engine

LOGGER = getLogger(__name__)


def get_data() -> pd.DataFrame:
    df_scrapping = load_excel_data("Courbes de tendance", "Courbes OS")
    df_scrapping = pd.DataFrame(columns=df_scrapping[:1][0], data=df_scrapping[1:])
    df_scrapping = df_scrapping.rename(
        columns={
            "OEM": "make",
            "SoH": "soh",
            "Odomètre (km)": "odometer",
            "Année": "year",
        }
    )

    engine = get_sqlalchemy_engine()
    df_dbeaver = pd.read_sql(
        """
        SELECT vm.model_name, vm.type, vm.version, vm.autonomy,
               b.battery_chemistry, b.capacity, b.net_capacity
        FROM vehicle_model vm
        JOIN battery b ON b.id = vm.battery_id
        """,
        engine,
    )

    df_scrapping["Modèle"] = df_scrapping["Modèle"].apply(lambda x: x.lower())

    df_info = df_scrapping.merge(
        df_dbeaver,
        right_on=["model_name", "type"],
        left_on=["Modèle", "Type"],
        how="left",
    )[
        [
            "make",
            "Modèle",
            "Type",
            "version",
            "autonomy",
            "battery_chemistry",
            "net_capacity",
            "odometer",
            "year",
            "soh",
            "price",
        ]
    ].drop_duplicates()

    df_info = df_info[
        (df_info["soh"].notna()) & (df_info["soh"] != "")
    ]  # Empty rows in gsheet

    df_info["price"] = df_info["price"].replace("", np.nan).astype(float)
    df_info["soh"] = df_info["soh"].apply(lambda x: float(str(x).replace("%", "")))
    df_info["odometer"] = df_info["odometer"].apply(
        lambda x: float(str(x).replace(",", "").replace(" ", ""))
    )
    df_info["year"] = df_info["year"].astype(int)

    df_info = df_info.dropna(subset=["price", "net_capacity"]).reset_index(drop=True)
    return df_info


def train_and_save(
    model_name: str, target_column: str = "price", logger: Logger = LOGGER
) -> dict[str, float | int | str]:
    logger.info("Fetching training data for price forecast...")
    data = get_data()

    X = data[
        [
            "make",
            "autonomy",
            "battery_chemistry",
            "net_capacity",
            "odometer",
            "year",
            "soh",
        ]
    ]
    y = data[target_column]
    categorical_features = ["make", "battery_chemistry"]

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "cat",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                categorical_features,
            )
        ],
        remainder="passthrough",
    )

    pipeline = Pipeline(
        steps=[
            ("preprocess", preprocessor),
            ("model", RandomForestRegressor(n_estimators=100, random_state=42)),
        ]
    )

    logger.info("Training price forecast model...")
    pipeline.fit(X, y)

    y_pred = pipeline.predict(X)
    mae = float(mean_absolute_error(y, y_pred))
    mape = float(mean_absolute_percentage_error(y, y_pred) * 100)
    rmse = float(root_mean_squared_error(y, y_pred))
    logger.info(
        "Training metrics - MAE: %.4f | MAPE: %.2f%% | RMSE: %.4f",
        mae,
        mape,
        rmse,
    )

    # Sauvegarde
    s3 = S3Service()
    s3.save_as_pickle(pipeline, f"models/{model_name}")
    logger.info(f"✅ Model saved to S3: {model_name}")

    return {
        "model_key": model_name,
        "training_rows": len(data),
        "mae": mae,
        "mape_percent": mape,
        "rmse": rmse,
    }


def load_model(model_name: str):
    s3 = S3Service()
    return s3.load_pickle(f"models/{model_name}")


def predict(model, data: pd.DataFrame):
    return model.predict(data)
