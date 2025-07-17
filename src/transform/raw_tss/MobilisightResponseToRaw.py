import logging
import sys
from functools import reduce
from logging import Logger
from typing import Optional

from config import SCHEMAS
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import ArrayType, StructType

from core.console_utils import main_decorator
from core.s3.settings import S3Settings
from core.spark_utils import create_spark_session
from transform.raw_tss.ResponseToRawTss import ResponseToRawTss
from pyspark.sql import functions as F
from typing import Dict


class MobilisightResponseToRaw(ResponseToRawTss):
    """
    Classe pour traiter les données émises par les API Mobilisight
    stockées dans '/response/bmw/' sur Scaleway
    """

    def __init__(
        self,
        make: str = "stellantis",
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

    def _build_fields_from_schema(
        self,
        schema: StructType,
        prefix: str = "",
        naming_sep: str = "."
    ) -> Dict[str, Dict]:
        """
        Génère un dictionnaire 'fields' pour l'extraction à plat à partir d'un schema StructType,
        en ciblant les ArrayType de StructType contenant un champ 'datetime'.

        - Ignore les champs 'unit'
        - Garde 'value' sans suffixe (ex: 'odometer.value' → 'odometer')
        """
        result = {}

        for field in schema.fields:
            field_name = field.name
            full_path = f"{prefix}.{field_name}" if prefix else field_name

            if isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
                struct = field.dataType.elementType
                has_datetime = any(f.name == "datetime" for f in struct.fields)

                if has_datetime:
                    field_map = {}
                    for f in struct.fields:
                        if f.name in ("datetime", "unit"):
                            continue
                        if f.name == "value":
                            # Exemple: electricity.level.value → electricity_level
                            col_name = full_path.replace(".", naming_sep)
                        else:
                            # Exemple: electricity.level.percentage → electricity_level_percentage
                            col_name = f"{full_path.replace('.', naming_sep)}{naming_sep}{f.name}"
                        field_map[f.name] = col_name

                    result[full_path] = {
                        "path": full_path,
                        "fields": field_map
                    }

                else:
                    # Recurse dans des structs imbriqués
                    result.update(self._build_fields_from_schema(struct, full_path, naming_sep))

            elif isinstance(field.dataType, StructType):
                # Struct simple (non-array), on descend
                result.update(self._build_fields_from_schema(field.dataType, full_path, naming_sep))

            else:
                # Champ non-struct, non-array, on ignore
                continue

        return result



    def parse_data(self, df: DataFrame, optimal_partitions_nb: int) -> DataFrame:
        """
        Parse dict from BMW api response

        Args:
            response (dict): Contains data to parse
            spark (SparkSession): spark session active
            vin (str): Vehicle identification number

        Returns:
            spark.DataFrame: Data with every columns
        """

        df = df.coalesce(32)

        fields = self._build_fields_from_schema(SCHEMAS[self.make])

        # Liste pour stocker tous les DataFrames en format long
        long_dfs = []
        for key, params in fields.items():
            path = params["path"]
            field_mapping = params["fields"]
            
            exploded = df.select(
                "vin",
                F.explode_outer(path).alias("exploded_struct")
            )

            exploded = exploded.cache()
            
            # Créer une ligne pour chaque champ (sauf datetime et unit)
            for field_in_struct, alias in field_mapping.items():
                long_df = exploded.select(
                    "vin",
                    F.col("exploded_struct.datetime").alias("date"),
                    F.lit(alias).alias("key"),  # Nom de la colonne
                    F.col(f"exploded_struct.{field_in_struct}").cast("string").alias("value") 
                ).dropna()
                
                long_dfs.append(long_df)
        exploded.unpersist()


        df_parsed = reduce(lambda a, b: a.unionByName(b), long_dfs)

        df_parsed = df_parsed.cache() 
        # Forcer le cache
        df_parsed.count()

        # Repartitionner pour optimiser le pivot
        df_parsed = df_parsed.repartition("vin").coalesce(optimal_partitions_nb)

        # Pivoter pour avoir une colonne par clé
        pivoted = df_parsed.groupBy("vin", "date").pivot("key").agg(F.first("value")).coalesce(optimal_partitions_nb)

        df_parsed.unpersist()

        return pivoted


@main_decorator
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )

    logger = logging.getLogger("Logger RawTss")

    settings = S3Settings()
    spark = create_spark_session(settings.S3_KEY, settings.S3_SECRET)
    
    MobilisightResponseToRaw(
        make='stellantis', spark=spark, logger=logger
    )


if __name__ == "__main__":
    main()

