import random
import re
import time
from datetime import datetime
from itertools import islice
from logging import Logger
from typing import Optional

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from core.s3.s3_utils import S3Service
from core.s3.settings import S3Settings
from core.spark_utils import get_optimal_nb_partitions
from transform.raw_tss.config import S3_RAW_TSS_KEY_FORMAT, SCHEMAS


class ResponseToRawTss:
    """
    Classe pour traiter les données renvoyées par les API stockées dans /response sur Scaleway
    """

    def __init__(
        self,
        make: str,
        force_update: bool = False,
        writing_mode: Optional[str] = "append",
        spark: SparkSession = None,
        logger: Logger = None,
        **kwargs,
    ):
        """
        Initialise le processeur de télémétrie

        Args:
            make: Marque du véhicule
            bucket: Instance S3Service pour l'accès aux données
            spark: Session Spark pour le traitement des données
            force_update: Si True, force la mise à jour des données dans CachedETLSpark
        """

        self.logger = logger
        self.spark = spark
        self.make = make
        self.bucket = S3Service()
        self.settings = S3Settings()
        self.base_s3_path = f"s3a://{self.settings.S3_BUCKET}"
        self.raw_tss_path = S3_RAW_TSS_KEY_FORMAT.format(brand=self.make)

    def run(self):

        self.logger.info(f"Traitement débuté pour {self.make}")

        start = time.time()
        keys_to_download_per_vin, paths_to_exclude = (
            self._get_keys_to_download()
        )  # Clés à télécharger par vin
        end = time.time()
        self.logger.info(
            f"Temps écoulé pour récupérer les clés à télécharger: {end - start:.2f} secondes"
        )

        if len(keys_to_download_per_vin) == 0:
            self.logger.info(f"Aucun VIN à traiter pour {self.make}")
        else:
            start = time.time()
            optimal_partitions_nb, batch_size = self._set_optimal_spark_parameters(
                keys_to_download_per_vin, paths_to_exclude
            )
            end = time.time()
            self.logger.info(
                f"Temps écoulé pour déterminer les paramètres Spark: {end - start:.2f} secondes"
            )

            print("Nb de vins", len(list(keys_to_download_per_vin.keys())))
            print(
                "Nb de batches",
                len(list(self._batch_dict_items(keys_to_download_per_vin, batch_size))),
            )

            for batch_num, batch in enumerate(
                self._batch_dict_items(keys_to_download_per_vin, batch_size), 1
            ):  # Boucle pour faire des batchs d'écriture et ne pas saturer la mémoire
                self.logger.info(f"Batch {batch_num}:")

                start = time.time()
                # Extract
                raw_tss_unparsed = self._download_keys(batch)
                end = time.time()
                self.logger.info(
                    f"Temps écoulé pour télécharger les json en spark {batch_num}: {end - start:.2f} secondes"
                )

                start = time.time()
                # Transform
                raw_tss_parsed = self.parse_data(
                    raw_tss_unparsed, optimal_partitions_nb
                )
                end = time.time()
                self.logger.info(
                    f"Temps écoulé pour transformer les données du batch {batch_num}: {end - start:.2f} secondes"
                )

                start = time.time()
                # Load
                self.bucket.append_spark_df_to_parquet(
                    raw_tss_parsed, self.raw_tss_path
                )
                end = time.time()
                self.logger.info(
                    f"Temps écoulé pour écrire les données dans le bucket {batch_num}: {end - start:.2f} secondes"
                )

                start = time.time()
                self._update_last_parsed_date(keys_to_download_per_vin)
                end = time.time()
                self.logger.info(
                    f"Temps écoulé pour actualiser la date de dernière analyse {batch_num}: {end - start:.2f} secondes"
                )

                raw_tss_parsed.unpersist()
                del raw_tss_parsed

            self.logger.info(f"Traitement terminé pour {self.make}")

    def _set_optimal_spark_parameters(
        self,
        keys_to_download_per_vin: dict,
        paths_to_exclude: list[str],
        nb_cores: int = 8,
    ) -> tuple[int, int]:
        """
        Calcule la taille optimale des batches pour le traitement parallèle des VINs.

        Cette méthode détermine le nombre optimal de VINs à traiter par batch en fonction
        de la taille des données, du nombre de VINs et des ressources système disponibles.
        L'optimisation vise à équilibrer la charge de travail entre les cœurs CPU tout
        en maximisant l'utilisation des ressources Spark.

        Args:
            nb_cores (int, optional): Nombre de cœurs CPU disponibles pour le traitement.
                                    Défaut: 4

        Returns:
            int: Nombre optimal de VINs à traiter par batch
        """
        if nb_cores <= 0:
            raise ValueError("Nombre de cœurs doit être un entier positif")

        file_size, _ = self.bucket.get_object_size(
            f"response/{self.make}/", prefix_to_exclude=paths_to_exclude
        )

        nb_vins = len(list(keys_to_download_per_vin.keys()))

        if nb_vins == 0:
            self.logger.warning("Aucun VIN à traiter, retour de batch_size = 1")
            return 1

        optimal_partitions = get_optimal_nb_partitions(file_size, nb_vins)

        vin_per_batch = max(1, int((nb_vins / optimal_partitions) * nb_cores * 4))

        return (4 * nb_cores, vin_per_batch)

    def _group_paths_by_vin(self, paths: list[str]) -> dict[str, list[str]]:
        grouped = {}

        for path in paths:
            if "/temp/" not in path:
                parts = path.strip("/").split("/")
                if len(parts) < 2:
                    continue  # ignorer les paths invalides
                vin = parts[-2]
                # Initialise la liste si vin pas encore vu
                if vin not in grouped:
                    grouped[vin] = []

                grouped[vin].append(path)

        return grouped

    def _batch_dict_items(self, dictionary: dict, batch_size: int):
        """Générateur pour traiter un dictionnaire par lots"""
        total_items = len(dictionary)

        for i in range(0, total_items, batch_size):
            batch = dict(islice(dictionary.items(), i, i + batch_size))
            yield batch

    def _get_keys_to_download(self) -> (dict[str, list[str]], list):
        """
        Récupère les clés S3 des fichiers à télécharger en filtrant par date de dernière analyse.

        Cette méthode compare les dates des fichiers de réponse disponibles avec la date
        de dernière analyse stockée dans les données raw TSS pour déterminer quels fichiers
        doivent être téléchargés et traités.

        Returns:
            dict[str, list[str]]: Dictionnaire où les clés sont les VINs et les valeurs sont
                                les listes des chemins S3 des fichiers à télécharger.
                                Format: {'VIN123': ['response/brand/VIN123/2024-01-01.json', ...]}

        Raises:
            Exception: Si une erreur survient lors de la lecture des données Parquet ou
                    de la liste des fichiers S3
        """

        last_parsed_date_dict = None

        if self.bucket.check_spark_file_exists(
            f"raw_ts/{self.make}/technical/tec_vin_last_parsed_date.parquet"
        ):
            last_parsed_date_df = self.bucket.read_parquet_df_spark(
                self.spark,
                f"raw_ts/{self.make}/technical/tec_vin_last_parsed_date.parquet",
            )
            last_parsed_date_dict = dict(
                last_parsed_date_df.select("vin", "last_parsed_file_date").collect()
            )

        vins_paths = self.bucket.list_files(f"response/{self.make}/", type_file=".json")

        vins_paths_grouped = self._group_paths_by_vin(vins_paths)

        paths_to_exclude = []

        if last_parsed_date_dict:
            for vin, paths in vins_paths_grouped.items():
                if vin in last_parsed_date_dict.keys():
                    vins_paths_grouped[vin] = [
                        path
                        for path in paths
                        if datetime.strptime(path.split("/")[-1], "%Y-%m-%d.json")
                        > datetime.strptime(
                            str(last_parsed_date_dict[vin]).split()[0], "%Y-%m-%d"
                        )
                    ]

                    paths_to_exclude.extend(
                        [
                            path
                            for path in paths
                            if datetime.strptime(path.split("/")[-1], "%Y-%m-%d.json")
                            <= datetime.strptime(
                                str(last_parsed_date_dict[vin]).split()[0], "%Y-%m-%d"
                            )
                        ]
                    )

        vins_paths_grouped = {k: v for k, v in vins_paths_grouped.items() if v}
        vins_paths_grouped = {k: v for k, v in vins_paths_grouped.items() if len(v) > 0}

        # Shuffle the vins to avoid skewness
        vins_paths_grouped = dict(
            random.sample(list(vins_paths_grouped.items()), k=len(vins_paths_grouped))
        )

        return (vins_paths_grouped, paths_to_exclude)

    def _download_keys(self, batch: dict[str, list[str]]) -> DataFrame:
        """
        Télécharge les json et retourne un DataFrame Spark
        """

        keys_to_download = []

        for _, paths in batch.items():
            keys_to_download.extend(paths)

        schema = SCHEMAS[self.make]

        keys_to_download_str = [
            f"s3a://{self.settings.S3_BUCKET}/{key}" for key in keys_to_download
        ]

        return (
            self.spark.read.option("multiline", "true")
            .option(
                "badRecordsPath",
                f"s3a://{self.settings.S3_BUCKET}/response/{self.make}/corrupted_responses/",
            )
            .option("mode", "PERMISSIVE")
            .schema(schema)
            .json(keys_to_download_str)
        )

    def _update_last_parsed_date(self, keys_to_download_per_vin: dict):
        """
        Met à jour la date de dernière date analyse pour les VINs présents dans le DataFrame.
        """

        def extract_date_from_path(path):
            match = re.search(r"(\d{4}-\d{2}-\d{2})\.json$", path)
            return match.group(1) if match else None

        rows = []
        for vin, paths in keys_to_download_per_vin.items():
            dates = [
                extract_date_from_path(p) for p in paths if extract_date_from_path(p)
            ]
            if dates:
                dates = [date for date in dates if date]  # Get rid of None
                max_date_str = max(dates)
                rows.append(Row(vin=vin, last_parsed_file_date=max_date_str))

        schema = StructType(
            [
                StructField("vin", StringType(), False),
                StructField("last_parsed_file_date", StringType(), False),
            ]
        )

        progress_df = self.spark.createDataFrame(rows, schema=schema)

        vins_to_update = list(keys_to_download_per_vin.keys())

        # Lire l'existant
        if self.bucket.check_spark_file_exists(
            f"raw_ts/{self.make}/technical/tec_vin_last_parsed_date.parquet"
        ):
            existing_df = self.bucket.read_parquet_df_spark(
                self.spark,
                f"raw_ts/{self.make}/technical/tec_vin_last_parsed_date.parquet",
            )
            filtered_df = existing_df.filter(~existing_df.vin.isin(vins_to_update))
            if filtered_df.count() > 0:
                final_df = filtered_df.unionByName(progress_df)
                final_df.cache().count()
            else:
                final_df = progress_df
        else:
            final_df = progress_df

        final_df.coalesce(1).write.mode("overwrite").parquet(
            f"s3a://{self.settings.S3_BUCKET}/raw_ts/{self.make}/technical/tec_vin_last_parsed_date.parquet"
        )

        pass

    def parse_data(self, df: DataFrame, optimal_partitions_nb: int) -> DataFrame:
        pass

