from logging import getLogger
from typing import Any

from core.s3.settings import S3Settings
from core.spark_utils import create_spark_session
from transform.models.temperature import RenaultSOHModel

logger = getLogger(__name__)


def main():
    print("🚀 Initializing Spark...")
    # Retrieve S3 credentials from environment variables
    settings = S3Settings()
    spark = create_spark_session(settings.S3_KEY, settings.S3_SECRET)

    logger.info("Loading data...")
    model = RenaultSOHModel(spark)
    df = model.load_data()

    logger.info("Training SOH models (one per vehicle model)...")
    models = model.train(df)

    logger.info(f"Trained models: {list[Any](models.keys())}")

    logger.info("Saving models to S3...")
    model.save_all()

    logger.info("Completed: all models have been saved to S3.")
    if spark:
        spark.stop()


if __name__ == "__main__":
    main()
