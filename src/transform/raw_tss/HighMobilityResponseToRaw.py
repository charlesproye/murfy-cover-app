from pyspark.sql.functions import col, explode, expr, udf
from pyspark.sql.types import *
from pyspark.sql.types import (ArrayType, BooleanType, DateType, DoubleType,
                               IntegerType, LongType, StringType, StructField,
                               StructType, TimestampType)
from transform.raw_tss.ResponseToRawTss import ResponseToRawTss
from typing import Optional
from pyspark.sql import SparkSession
from logging import Logger
import logging
from dotenv import load_dotenv
from core.s3.settings import S3Settings
from core.spark_utils import create_spark_session
from core.console_utils import main_decorator
import sys
from pyspark.sql import DataFrame


class HighMobilityResponseToRaw(ResponseToRawTss):
    """
    Classe pour traiter les données renvoyées par les API Tesla Fleet Telemetry 
    stockées dans /response sur Scaleway
    """

    def __init__(
        self,
        make: str = "high-mobility",
        force_update: bool = False,
        writing_mode: Optional[str] = "append",
        spark: SparkSession = None,
        logger: Logger = None,
        **kwargs,
    ):

        super().__init__(
            make=make,
            force_update=force_update,
            writing_mode=writing_mode,
            spark=spark,
            logger=logger,
            **kwargs,
        )

        """
        Parse dict from High Mobility api response

        Args:
            response (dict): Contains data to parse
            spark (SparkSession): spark session active
            vin (str): Vehicle identification number

        Returns:
            spark.DataFrame: Data with every columns
        """

        breakpoint()

        # flattened_response = {}

        # for capability, variables in response.items():
        #     if not isinstance(variables, dict):
        #         continue
        #     for variable, elements in variables.items():
        #         for element in elements:
        #             timestamp = element["timestamp"]
        #             variable_name = capability + "." + variable

        #             if isinstance(element["data"], dict):
        #                 if "value" in element["data"] or "time" in element['data']:
        #                     if "value" in element['data']:
        #                         value = element["data"]["value"]
        #                     else:
        #                         value = get_next_scheduled_timestamp(element['timestamp'], element['data'])
        #                     if "unit" in element:
        #                         variable_name += "." + element["unit"]
        #                 else:
        #                     continue            
        #             else:
        #                 value = element["data"]

        #             if isinstance(value, (int, float)):
        #                 value = float(value)
        #             variable_name = variable_name.replace(".", "_")
        #             flattened_response[timestamp] = flattened_response.get(
        #                 timestamp, {}
        #             ) | {variable_name: value}



        # data_list = []
        # for timestamp, variables in flattened_response.items():
        #     row = {"readable_date": timestamp, "vin": vin}
        #     row.update(variables)
        #     data_list.append(row)


        # raw_ts = spark.createDataFrame(data_list)
        # return raw_ts


@main_decorator
def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stdout
    )

    logger = logging.getLogger('Logger RawTss')

    settings = S3Settings()
    spark = create_spark_session(settings.S3_KEY, settings.S3_SECRET)

    HighMobilityResponseToRaw(make='mercedes-benz', force_update=True, spark=spark, logger=logger)


if __name__ == "__main__":
    main()


