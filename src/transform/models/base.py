"""Base classes for OEM-specific SoH model training."""

from abc import ABC, abstractmethod
from datetime import datetime

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, LinearRegressionModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from core.models import MakeEnum
from db_models.data_eng import get_data_eng_session
from db_models.data_eng.models import SoHModel

PREDICTION_COLUMN_NAME = "pred_soh"
FEATURE_COLUMN_NAME = "features"


class BaseSOHModel(ABC):
    """
    Base class for OEM-specific SoH model training.

    Each OEM implementation should inherit from this class and implement:
    - load_data(): Load and prepare data specific to the OEM
    - get_feature_columns(): Return list of feature column names
    - get_label_column(): Return the target/label column name

    The base class handles:
    - Training models (one per vehicle model)
    - Saving models to S3
    - Loading models from S3
    """

    def __init__(self, spark: SparkSession, make: MakeEnum):
        self.spark = spark
        self.make = make
        self.ml_models: dict[str, LinearRegressionModel] = {}

    @abstractmethod
    def load_data(self) -> DataFrame:
        """
        Load and prepare data for training.

        Returns:
            DataFrame with columns: [vin, date, <features>, <label>, model]
        """
        ...

    @abstractmethod
    def get_feature_columns(self) -> list[str]:
        """Return list of feature column names for training."""
        ...

    def get_label_column(self) -> str:
        """Return the label column name. Default is 'soh'."""
        return "soh"

    def get_model_group_column(self) -> str:
        """Return the column used to group models. Default is 'model'."""
        return "model"

    def train(self, df: DataFrame) -> dict[str, LinearRegressionModel]:
        """
        Train one linear regression model per vehicle model.

        Args:
            df: Prepared DataFrame from load_data()

        Returns:
            Dictionary mapping model names to trained models
        """
        self.ml_models = {}

        feature_cols = self.get_feature_columns()
        label_col = self.get_label_column()
        group_col = self.get_model_group_column()

        assembler = VectorAssembler(
            inputCols=feature_cols, outputCol=FEATURE_COLUMN_NAME
        )
        df = assembler.transform(df)
        df.cache()

        car_models = [
            row[group_col] for row in df.select(group_col).distinct().collect()
        ]

        for car_model in car_models:
            model_df = df.filter(F.col(group_col) == car_model)

            lr = LinearRegression(
                featuresCol=FEATURE_COLUMN_NAME,
                labelCol=label_col,
                predictionCol=PREDICTION_COLUMN_NAME,
            )
            lr_model = lr.fit(model_df)

            self.ml_models[car_model] = lr_model

        return self.ml_models

    def save_all(self, bucket_name: str) -> list[SoHModel]:
        """
        Save all trained models to S3 and return metadata.

        Args:
            bucket_name: S3 bucket name

        Returns:
            List of SoHModel records for database
        """
        ml_model_data = []
        if not self.ml_models:
            raise ValueError("No models to save. Train models first.")

        day_str = datetime.now().strftime("%Y%m%d%H%M")
        for car_model_name, model in self.ml_models.items():
            ml_model_name = f"{self.make.value}_{car_model_name}_temperature"
            save_path = f"s3a://{bucket_name}/ml_models/{self.make.value}/{day_str}/{ml_model_name}"
            model.write().overwrite().save(save_path)
            ml_model_data.append(
                SoHModel(
                    make=self.make.value,
                    model_name=ml_model_name,
                    car_model_name=car_model_name,
                    model_uri=save_path,
                    metrics={
                        "r2": model.summary.r2,
                        "r2_adj": model.summary.r2adj,
                        "mse": model.summary.meanSquaredError,
                        "rmse": model.summary.rootMeanSquaredError,
                        "mae": model.summary.meanAbsoluteError,
                        "total_iterations": model.summary.totalIterations,
                        "pValues": model.summary.pValues,
                        "explainedVariance": model.summary.explainedVariance,
                    },
                )
            )
            print(f"Model saved to {save_path}")
        return ml_model_data

    def load_all(
        self, ml_models_info: list[SoHModel] | None = None
    ) -> dict[str, LinearRegressionModel]:
        """
        Load all models for this OEM from S3.

        Args:
            ml_models_info: Optional list of model metadata. If None, fetch from DB.

        Returns:
            Dictionary mapping car model names to loaded models
        """
        if ml_models_info is None:
            with get_data_eng_session() as session:
                # Get the most recent model for each car_model_name for this make
                ml_models_info = (
                    session.query(SoHModel)
                    .filter(SoHModel.make == self.make.value)
                    .distinct(SoHModel.car_model_name)
                    .order_by(SoHModel.car_model_name, SoHModel.created_at.desc())
                    .all()
                )

        for ml_model in ml_models_info:
            if ml_model.car_model_name is None:
                continue
            load_path = ml_model.model_uri
            model = LinearRegressionModel.load(load_path)
            self.ml_models[ml_model.car_model_name] = model

        return self.ml_models
