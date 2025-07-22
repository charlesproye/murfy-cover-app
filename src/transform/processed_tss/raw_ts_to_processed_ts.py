from abc import abstractmethod
from logging import getLogger

from pyspark.sql import DataFrame as DF
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count_distinct, first
from pyspark.sql.functions import hash as spark_hash
from pyspark.sql.functions import lag, last, lit, pandas_udf
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import unix_timestamp, when

from core.caching_utils import CachedETLSpark
from core.logging_utils import set_level_of_loggers_with_prefix
from core.s3.s3_utils import S3Service
from core.s3.settings import S3Settings
from core.spark_utils import (get_optimal_nb_partitions,
                              safe_astype_spark_with_error_handling)
from transform.fleet_info.main import fleet_info
from transform.processed_tss.config import *
from transform.processed_tss.config import NB_CORES_CLUSTER, SCALE_SOC
from transform.raw_tss.main import *


class RawTsToProcessedTs(CachedETLSpark):

    def __init__(
        self,
        make: str,
        id_col: str = "vin",
        log_level: str = "INFO",
        force_update: bool = False,
        spark: SparkSession = None,
        **kwargs,
    ):
        self.make = make
        logger_name = f"transform.processed_tss.{make}"
        self.logger = getLogger(logger_name)
        set_level_of_loggers_with_prefix(log_level, logger_name)
        self.id_col = id_col
        self.spark = spark
        self.bucket = S3Service()
        self.settings = S3Settings()

        super().__init__(
            S3_PROCESSED_TSS_KEY_FORMAT.format(make=make),
            "s3",
            force_update=force_update,
            spark=spark,
            **kwargs,
        )

    # TODO:
    # Keep overwrite mode as long as performance is acceptable, don't switch to 'append' yet
    # Keep non-batched system as long as memory usage is manageable
    # Trier ce qui est utile ou non dans la config
    # Clean cette classe
    # Faire tourner sur tout

    def run(self):

        # Check if raw_ts exists, if not, run ResponseToRawTs
        if self.bucket.check_spark_file_exists(
            f"raw_ts/{self.make}/time_series/raw_ts_spark.parquet"
        ):
            tss = self.bucket.read_parquet_df_spark(
                spark=self.spark,
                key=f"raw_ts/{self.make}/time_series/raw_ts_spark.parquet",
            )
        else:
            tss = ORCHESTRATED_MAKES[self.make][1](self.make, spark=self.spark).run()

        optimal_partitions_nb, _ = self._set_optimal_spark_parameters(
            tss, NB_CORES_CLUSTER
        )  # Optimize partitions
        tss = tss.repartition("vin").coalesce(optimal_partitions_nb)
        tss = tss.cache()
        tss.count()  # Trigger caching lazy operations

        tss = tss.withColumnsRenamed(RENAME_COLS_DICT)
        tss = tss.select(*NECESSARY_COLS[self.make])  # Reduce column volumetry

        tss = safe_astype_spark_with_error_handling(tss)
        tss = self.normalize_units_to_metric(tss)
        tss = tss.orderBy(["vin", "date"])

        tss = self.compute_charge_idx(tss)  # Charging Status Indew

        tss = self.compute_specific_features(tss)

        tss = tss.cache()
        tss.count()
        tss = tss.join(self.spark.createDataFrame(fleet_info), "vin", "left")

        return tss

    def _set_optimal_spark_parameters(
        self,
        tss,
        nb_cores: int = 8,
    ):
        """
        Calculates the optimal batch size for parallel processing of VINs.

        This method determines the optimal number of VINs to process per batch based on
        data size, number of VINs, and available system resources.
        The goal is to balance the workload across CPU cores while
        maximizing Spark resource utilization.

        Args:
            nb_cores (int, optional): Number of CPU cores available for processing.
                                      Default: 4

        Returns:
            int: Optimal number of VINs to process per batch
        """

        if nb_cores <= 0:
            raise ValueError("Number of cores must be a positive integer")

        file_size, _ = self.bucket.get_object_size(
            f"raw_ts/{self.make}/time_series/raw_ts_spark.parquet"
        )

        nb_vins = tss.select(count_distinct("vin")).collect()[0][0]

        print("Nb vins: ", nb_vins)

        print(nb_vins)

        if nb_vins == 0:
            self.logger.warning("No VINs to process, returning batch_size = 1")
            return (1, nb_vins)

        optimal_partitions = get_optimal_nb_partitions(file_size, nb_vins)

        vin_per_batch = min(
            max(1, int((nb_vins / optimal_partitions) * nb_cores * 4)), nb_vins
        )

        return (4 * nb_cores, vin_per_batch)

    def normalize_units_to_metric(self, tss):
        tss = tss.withColumn("odometer", col("odometer") * 1.609)
        return tss

    def _reassign_short_phases(self, df, min_duration_minutes=3):
        """
        Recalcule les phase_id en fusionnant les phases de moins de `min_duration_minutes`
        avec la phase valide précédente.

        Args:
            df (DataFrame): DataFrame Spark avec les colonnes `phase_id`, `date`, `total_phase_time`
            min_duration_minutes (float): Durée minimale pour conserver une phase (en minutes)

        Returns:
            DataFrame: DataFrame avec la colonne `phase_id` mise à jour
        """

        # 1. Marquer les phases valides
        df = df.withColumn(
            "is_valid_phase",
            F.when(F.col("total_phase_time") >= min_duration_minutes, 1).otherwise(0),
        )

        # 2. Déterminer la dernière phase valide précédemment
        w_time = (
            Window.partitionBy("vin")
            .orderBy("date")
            .rowsBetween(Window.unboundedPreceding, 0)
        )

        df = df.withColumn(
            "last_valid_phase_id",
            F.last(
                F.when(F.col("is_valid_phase") == 1, F.col("phase_id")),
                ignorenulls=True,
            ).over(w_time),
        )

        # 3. Mettre à jour le phase_id
        df = df.withColumn(
            "phase_id_updated",
            F.when(F.col("is_valid_phase") == 1, F.col("phase_id")).otherwise(
                F.col("last_valid_phase_id")
            ),
        )

        # 4. Re-numérotation des phase_id pour compacter (optionnel mais propre)
        df = df.withColumn(
            "phase_id_final",
            F.dense_rank().over(Window.partitionBy("vin").orderBy("phase_id_updated"))
            - 1,
        )

        # 5. Nettoyage final
        df = df.drop(
            "phase_id", "last_valid_phase_id", "is_valid_phase", "phase_id_updated"
        )
        df = df.withColumnRenamed("phase_id_final", "phase_id")

        return df

    def compute_charge_idx(self, tss: DF) -> DF:

        # Définir les fenêtres
        w = Window.partitionBy("vin").orderBy("date")

        # Calculer soc_diff en allant chercher la précédente valeur non nulle
        tss = tss.withColumn(
            "soc_diff",
            F.when(
                F.col("soc").isNotNull(),
                F.col("soc")
                - F.last("soc", ignorenulls=True).over(
                    w.rowsBetween(Window.unboundedPreceding, -1)
                ),
            ).otherwise(None),
        )
        # Calcul du gap en minutes
        df = tss.withColumn("prev_date", lag("date").over(w))
        df = df.withColumn(
            "time_gap_minutes",
            (F.unix_timestamp("date") - F.unix_timestamp("prev_date")) / 60,
        )

        # Calcul de direction avec forward fill
        df = df.withColumn(
            "direction_raw",
            F.when(col("soc_diff").isNull(), None).otherwise(F.signum("soc_diff")),
        )

        # Forward fill de la direction
        df = df.withColumn(
            "direction",
            F.last("direction_raw", ignorenulls=True).over(
                w.partitionBy("vin")
                .orderBy("date")
                .rowsBetween(Window.unboundedPreceding, 0)
            ),
        )

        # Détecter les changements de direction
        df = df.withColumn(
            "direction_change",
            F.when(F.col("direction") != F.lag("direction").over(w), 1).otherwise(0),
        )

        # Créer phase_id en cumulant les changements
        df = df.withColumn(
            "phase_id",
            F.sum("direction_change").over(w.rowsBetween(Window.unboundedPreceding, 0)),
        )

        w_phase = Window.partitionBy("vin", "phase_id")

        df = df.withColumn("total_phase_time", F.sum("time_gap_minutes").over(w_phase))

        df = self._reassign_short_phases(df)

        w_phase = Window.partitionBy("vin", "phase_id")

        df = df.withColumn("total_soc_diff", F.sum("soc_diff").over(w_phase))

        df = df.withColumn("prev_phase", F.lag("direction").over(w)).withColumn(
            "next_phase", F.lead("direction").over(w)
        )

        df = df.withColumn(
            "charging_status",
            F.when(F.col("total_soc_diff") > 0.005 * SCALE_SOC[self.make], "charging")
            .when(
                F.col("total_soc_diff") < -0.005 * SCALE_SOC[self.make], "discharging"
            )
            .when(
                (F.col("prev_phase") == F.col("next_phase"))
                & (F.col("prev_phase") >= 0),
                "charging",
            )
            .when(
                (F.col("prev_phase") == F.col("next_phase"))
                & (F.col("prev_phase") <= 0),
                "discharging",
            )
            .otherwise("idle"),
        )
        # Étape 3: Recréer le phase_id basé sur les changements de phase
        df = df.withColumn(
            "charging_status_change",
            F.when(
                F.col("charging_status") != F.lag("charging_status").over(w), 1
            ).otherwise(0),
        )

        df = df.withColumn(
            "charging_status_idx",
            F.sum("charging_status_change").over(
                w.rowsBetween(Window.unboundedPreceding, 0)
            ),
        )

        return df

    @abstractmethod
    def compute_specific_features(self, tss: DF) -> DF:
        pass

