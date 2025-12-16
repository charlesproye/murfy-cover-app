from logging import getLogger

from dagster_pipes import open_dagster_pipes
from pyspark.sql import SparkSession

from core.models import MakeEnum
from core.s3.settings import S3Settings
from db_models.data_eng import get_data_eng_session
from db_models.data_eng.models import Make, SoHModel
from transform.base_spark import BaseSpark
from transform.models.base import BaseSOHModel
from transform.models.renault_soh_model import RenaultSOHModel

logger = getLogger(__name__)


def get_soh_model(make: MakeEnum, spark: SparkSession) -> BaseSOHModel:
    """
    Factory function to get the appropriate SoH model for an OEM.

    Args:
        make: OEM identifier
        spark: SparkSession

    Returns:
        OEM-specific SoH model instance
    """
    match make:
        case MakeEnum.renault:
            return RenaultSOHModel(spark)
        case _:
            raise NotImplementedError(f"SoH model not implemented for {make.value}")


class SoHMLModelsPipeline(BaseSpark):
    def __init__(self, make: MakeEnum = MakeEnum.renault):
        super().__init__(
            name="models",
            help="Train and save SOH correction models",
        )
        self.make = make

    def run(self, **kwargs):
        # Run command, called in dagster
        with open_dagster_pipes() as pipes:
            pipes.log.info(f"Loading data for {self.make.value}...")

            soh_model = get_soh_model(self.make, self.spark)
            df = soh_model.load_data()

            pipes.log.info("Training SOH models (one per vehicle model)...")
            models = soh_model.train(df)

            pipes.log.info(f"Trained models: {list[str](models.keys())}")

            pipes.log.info("Saving models to S3...")
            ml_models = self.save_ml_models(soh_model)

            pipes.log.info("Completed: all models have been saved to S3.")
            self.spark.stop()

            # Build metadata with key metrics for each trained model
            metadata = {"models_processed": len(models)}
            for model in ml_models:
                metrics = model.metrics or {}
                metadata[f"{model.model_name}_r2"] = metrics.get("r2")
                metadata[f"{model.model_name}_rmse"] = metrics.get("rmse")

            pipes.report_asset_materialization(metadata=metadata)

            return ml_models

    def save_ml_models(self, model: BaseSOHModel) -> list[SoHModel]:
        settings = S3Settings()
        ml_models = model.save_all(bucket_name=settings.S3_BUCKET)

        with get_data_eng_session() as session:
            if not session.query(Make).filter_by(make=model.make.value).first():
                session.add(Make(make=model.make.value))
            session.add_all(ml_models)
            session.commit()

        return ml_models


# Expose CLI for external access (e.g., Dagster Pipes)
cli = SoHMLModelsPipeline().cli

if __name__ == "__main__":
    cli()
