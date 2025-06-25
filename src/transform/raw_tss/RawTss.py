from typing import Optional, List
import logging
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, to_timestamp, expr, collect_list
from pyspark.sql import DataFrame, SparkSession
from functools import reduce
from rich.progress import track
from core.s3_utils import S3_Bucket
from core.env_utils import get_env_var
from core.spark_utils import *
from core.caching_utils import CachedETLSpark, cache_result_spark
from core.console_utils import main_decorator
from core.pandas_utils import DF
from transform.raw_tss.config import (
    GET_PARSING_FUNCTIONS,
    S3_RAW_TSS_KEY_FORMAT,
    ALL_MAKES,
)


# Configuration du logger
logger = logging.getLogger(__name__)


class RawTss(CachedETLSpark):
    """
    Classe pour traiter les données renvoyées par les API stocké dans response sur scaleway
    """

    def __init__(
        self,
        make: str,
        force_update: bool = False,
        spark: SparkSession = None,
        **kwargs,
    ):
        """
        Initialise le processeur de télémétrie

        Args:
            make: Marque du véhicule
            bucket: Instance S3_Bucket pour l'accès aux données
            spark: Session Spark pour le traitement des données
        """
        self.spark = spark
        self.make = make
        self.bucket = S3_Bucket()
        self.base_s3_path = f"s3a://{get_env_var('S3_BUCKET')}"
        super().__init__(
            S3_RAW_TSS_KEY_FORMAT.format(brand=make),
            "s3",
            force_update=force_update,
            **kwargs,
        )

    def _ensure_spark_session(self):
        """Vérifie qu'une session Spark est disponible"""
        if self.spark is None:
            raise ValueError(
                "Session Spark non initialisée. Utilisez set_spark_session() d'abord."
            )

    def _parse_response(self, **kwargs) -> DF:
        """Sélectionne la fonction de parsing en fonction de la marque"""
        func = GET_PARSING_FUNCTIONS[self.make]

        return func(**kwargs)

    # No need to call run, it will be called in CachedETLSpark init.
    def run(self) -> DF:
        """
        Point d'entrée principal pour récupérer les données TSS brutes

        Returns:
            DataFrame contenant toutes les données TSS brutes
        """

        self._ensure_spark_session()
        logger.debug(
            "Getting raw tss from responses provided by tesla fleet telemetry."
        )

        self._configure_spark_optimization()

        try:
            keys = self.get_response_keys_to_parse()
            logger.info("keys loaded")
            # Correction: passer self.spark au lieu de spark
            new_raw_tss = self.get_raw_tss_from_keys_spark(keys)
            logger.info("new_raw_tss loaded")
            return new_raw_tss

        except Exception as e:
            logger.error(f"Erreur dans get_raw_tss: {e}")
            return self._create_empty_raw_tss_schema()

    def read_parquet(self, key: str, columns: Optional[List[str]] = None) -> DataFrame:
        """
        Lit un fichier parquet depuis S3

        Args:
            key: Clé S3 du fichier
            columns: Colonnes à sélectionner (optionnel)

        Returns:
            DataFrame Spark
        """
        self._ensure_spark_session()
        logger.info("read_parquet_spark started")

        full_path = f"{self.base_s3_path}/{key}"
        try:
            df = self.spark.read.parquet(full_path)

            if columns is not None:
                # Vérifier que les colonnes existent avant de les sélectionner
                available_columns = df.columns
                valid_columns = [col for col in columns if col in available_columns]
                if valid_columns:
                    df = df.select(*valid_columns)
                else:
                    logger.warning(
                        f"Aucune colonne valide trouvée dans {valid_columns}"
                    )
                    return self._create_empty_raw_tss_schema()

            return df
        except Exception as e:
            logger.error(f"Erreur lors de la lecture du parquet {full_path}: {e}")
            return self._create_empty_raw_tss_schema()

    def _create_empty_raw_tss_schema(self) -> DataFrame:
        """Crée un DataFrame vide avec le schéma raw_tss"""
        self._ensure_spark_session()
        schema = StructType(
            [
                StructField("vin", StringType(), True),
                StructField("readable_date", TimestampType(), True),
            ]
        )
        return self.spark.createDataFrame([], schema)

    def get_response_keys_to_parse(self) -> DataFrame:
        """
        Récupère les clés de réponse à parser en filtrant par date de dernière analyse

        Returns:
            DataFrame contenant les clés à traiter
        """
        self._ensure_spark_session()
        logger.info("get_response_keys_to_parse_spark started")

        try:
            # Récupération des données TSS brutes existantes ou création d'un DataFrame vide
            if self.bucket.check_spark_file_exists(
                f"raw_ts/{self.make}/time_series/spark_raw_tss.parquet"
            ):
                raw_tss_subset = self.read_parquet(
                    f"raw_ts/{self.make}/time_series/spark_raw_tss.parquet",
                    columns=["vin", "readable_date"],
                )

            else:
                raw_tss_subset = self._create_empty_raw_tss_schema()

            # Calcul de la dernière date parsée par VIN
            last_parsed_date = (
                raw_tss_subset.groupBy("vin")  # Correction: groupBy au lieu de groupby
                .agg({"readable_date": "max"})
                .withColumnRenamed("max(readable_date)", "last_parsed_date")
            )

            # Récupération des clés de réponse
            response_keys_list = self.bucket.list_responses_keys_of_brand(self.make)
            response_keys_df = self.spark.createDataFrame(response_keys_list)

            # Vérifier si la colonne 'file' existe
            if "file" not in response_keys_df.columns:
                logger.error("La colonne 'file' n'existe pas dans response_keys_df")
                return self.spark.createDataFrame(
                    [],
                    StructType(
                        [
                            StructField("vin", StringType(), True),
                            StructField("key", StringType(), True),
                            StructField("date", TimestampType(), True),
                        ]
                    ),
                )

            response_keys_df = response_keys_df.withColumn(
                "date", to_timestamp(expr("substring(file, 1, length(file) - 5)"))
            )

            # Filtrage des nouvelles clés à traiter
            result = response_keys_df.join(
                last_parsed_date, on="vin", how="outer"
            ).filter(
                (col("last_parsed_date").isNull())
                | (col("date") > col("last_parsed_date"))
            )

            return result

        except Exception as e:
            logger.error(f"Erreur dans get_response_keys_to_parse: {e}")
            return self.spark.createDataFrame(
                [],
                StructType(
                    [
                        StructField("vin", StringType(), True),
                        StructField("key", StringType(), True),
                        StructField("date", TimestampType(), True),
                    ]
                ),
            )

    def get_raw_tss_from_keys_spark(
        self, keys: DataFrame, max_vins: int = None
    ) -> DataFrame:
        """
        Récupère les données TSS brutes à partir des clés

        Args:
            keys: DataFrame contenant les clés à traiter
            max_vins: Nombre maximum de VINs à traiter (pour les tests)

        Returns:
            DataFrame contenant les données TSS brutes
        """
        try:

            # Cache du DataFrame pour éviter les recalculs
            df = keys.select("vin", "key").distinct().cache()

            # Collecte groupée des données par VIN
            vin_keys_grouped = (
                df.groupBy("vin").agg(collect_list("key").alias("keys")).orderBy("vin")
            )

            # Limit pour test le code
            if max_vins and max_vins > 0:
                vin_keys_grouped = vin_keys_grouped.limit(max_vins)

            # Collecte une seule fois
            vin_data = vin_keys_grouped.collect()

            if not vin_data:
                logger.warning("Aucune donnée VIN trouvée")
                df.unpersist()
                return self._create_empty_raw_tss_schema()

            all_data = []

            # Traitement par batch
            batch_size = 200  # Ajustable
            all_keys_to_process = []
            vin_key_mapping = {}

            # Préparation des keys
            for row in vin_data:
                vin = row["vin"]
                keys_list = row["keys"]
                all_keys_to_process.extend(keys_list)
                for key in keys_list:
                    vin_key_mapping[key] = vin

            logger.info(f"Total keys to process: {len(all_keys_to_process)}")

            if not all_keys_to_process:
                df.unpersist()
                return self._create_empty_raw_tss_schema()

            # Traitement par batch des fichiers S3
            for i in track(
                range(0, len(all_keys_to_process), batch_size),
                description="Processing batches",
            ):
                batch_keys = all_keys_to_process[i : i + batch_size]

                try:
                    responses = self.bucket.read_multiple_json_files(
                        batch_keys, max_workers=128
                    )  # Est ce qu'on ne peut pas agréger les json auparavant pour limiter les partitions ?
                    batch_data = []
                    for response in responses:
                        try:
                            rows = self._parse_response(
                                response=response, spark=self.spark
                            )
                            if rows is not None and rows.count() > 0:
                                batch_data.append(rows)
                        except Exception as e:
                            logger.error(f"Error parsing response: {e}")

                    # Union des données du batch
                    if batch_data:
                        batch_df = reduce(
                            lambda df1, df2: df1.unionByName(
                                df2, allowMissingColumns=True
                            ),
                            batch_data,
                        )
                        all_data.append(batch_df)

                except Exception as e:
                    logger.error(f"Error processing batch {i//batch_size + 1}: {e}")

            # Suppression du cache
            df.unpersist()

            if all_data:
                # Cache le résultat final si besoin plus tard
                final_df = reduce(
                    lambda df1, df2: df1.coalesce(1).unionByName(
                        df2, allowMissingColumns=True
                    ),
                    all_data,
                )
                return final_df.cache()
            else:
                logger.warning("Aucune donnée finale générée")
                return self._create_empty_raw_tss_schema()

        except Exception as e:
            logger.error(f"Erreur dans get_raw_tss_from_keys_spark: {e}")
            return self._create_empty_raw_tss_schema()

    def _configure_spark_optimization(self):
        """Configure les optimisations Spark"""
        self._ensure_spark_session()
        try:
            self.spark.conf.set("spark.sql.adaptive.enabled", "true")
            self.spark.conf.set(
                "spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64MB"
            )
            self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            logger.info("Optimisations Spark configurées")
        except Exception as e:
            logger.error(
                f"Erreur lors de la configuration des optimisations Spark: {e}"
            )

    @classmethod
    def update_all_tss(cls, spark, **kwargs):
        for make in ALL_MAKES:
            cls = RawTss
            cls(make, force_update=True, spark=spark, **kwargs)


@main_decorator
def main():
    """Fonction principale d'exécution"""
    # Configuration du logging
    logging.basicConfig(level=logging.INFO)

    try:
        # Initialisation
        bucket = S3_Bucket()
        # Correction: passer la marque en paramètre

        # Création de la session Spark
        creds = bucket.get_creds_from_dot_env()
        spark_session = create_spark_session(
            creds["aws_access_key_id"], creds["aws_secret_access_key"]
        )

        logger.info("Spark session launched")

        RawTss.update_all_tss(spark=spark_session)

        logger.info("Processing completed successfully")

    except Exception as e:
        logger.error(f"Erreur dans main: {e}")
        raise
    finally:
        if "spark_session" in locals():
            spark_session.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()

