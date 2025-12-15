from logging import getLogger

from dagster_pipes import PipesContext
from dotenv import load_dotenv
from pyspark.sql import DataFrame as DF
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count_distinct

from core.caching_utils import CachedETLSpark
from core.logging_utils import set_level_of_loggers_with_prefix
from core.models.make import MakeEnum
from core.s3.s3_utils import S3Service
from core.s3.settings import S3Settings
from core.spark_utils import (
    get_optimal_nb_partitions,
    safe_astype_spark_with_error_handling,
)
from transform.fleet_info.main import get_fleet_info
from transform.processed_phases.config import (
    ODOMETER_MILES_TO_KM,
    PROCESSED_PHASES_CACHE_KEY_TEMPLATE,
    SCALE_SOC,
    SOC_DIFF_THRESHOLD,
)
from transform.raw_tss.main import PROVIDERS

load_dotenv()


class RawTsToProcessedPhases(CachedETLSpark):
    def __init__(
        self,
        make: str | MakeEnum,
        id_col: str = "vin",
        log_level: str = "INFO",
        force_update: bool = False,
        spark: SparkSession | None = None,
        pipes: PipesContext | None = None,
        **kwargs,
    ):
        # Normalize make to MakeEnum if it's a string
        if isinstance(make, str):
            self.make = MakeEnum(make)
        else:
            self.make = make
        logger_name = f"transform.processed_tss.{self.make.value}"
        self.logger = getLogger(logger_name)
        set_level_of_loggers_with_prefix(log_level, logger_name)
        self.id_col = id_col
        self.spark = spark
        self.bucket = S3Service()
        self.settings = S3Settings()
        self.pipes = pipes
        self.fleet_info = get_fleet_info()
        self.force_update = force_update

        super().__init__(
            "s3",
            bucket=self.bucket,
            settings=self.settings,
            spark=spark,
            repartition_key="VIN",
            **kwargs,
        )

    # TODO:
    # Keep overwrite mode as long as performance is acceptable, don't switch to 'append' yet
    # Keep non-batched system as long as memory usage is manageable
    # Add config file for the aggregation of the metrics on the phases
    # Check with Hugo how to handle the 'usable phase'
    # Check with Hugo the aggregation criteria for each metric

    def run(self):
        self.pipes.log.info(f"Running for {self.make.value}")

        if self.bucket.check_spark_file_exists(
            f"raw_ts/{self.make.value}/time_series/raw_ts_spark.parquet"
        ):
            tss = self.bucket.read_parquet_df_spark(
                spark=self.spark,
                key=f"raw_ts/{self.make.value}/time_series/raw_ts_spark.parquet",
            )
        else:
            tss = PROVIDERS[self.make](self.make, spark=self.spark).run()

        dynamic_config = self.bucket.read_yaml_file(f"config/{self.make.value}.yaml")
        if dynamic_config is None:
            raise ValueError(f"Config file config/{self.make.value}.yaml not found")
        tss = tss.withColumnsRenamed(
            dynamic_config["raw_tss_to_processed_phase"]["rename"]
        )
        tss = tss.select(*dynamic_config["raw_tss_to_processed_phase"]["keep"])
        tss = safe_astype_spark_with_error_handling(tss)
        tss = self._normalize_units_to_metric(tss)
        tss = tss.orderBy(["vin", "date"])
        tss = self.fill_forward(tss)

        all_vins = [row["vin"] for row in tss.select("vin").distinct().collect()]
        BATCH_SIZE = 200

        batch_results = []

        for i in range(0, len(all_vins), BATCH_SIZE):
            self.logger.info(f"Processing batch {i} of {len(all_vins)}")

            batch_vins = all_vins[i : i + BATCH_SIZE]
            batch_df = tss.filter(F.col("vin").isin(batch_vins))

            tss_phase_idx = self.compute_charge_idx(
                batch_df, SOC_DIFF_THRESHOLD[self.make.value]
            )
            phases = self.generate_phase(tss_phase_idx)

            phase_tss = self.join_metrics_to_phase(phases, batch_df)
            phase_tss = self.compute_specific_features_before_aggregation(phase_tss)
            phase_tss = phase_tss.orderBy("date", "vin")

            phases_enriched_batch = self.aggregate_stats(phase_tss)
            batch_results.append(phases_enriched_batch)

        phases_enriched = batch_results[0]
        for r in batch_results[1:]:
            phases_enriched = phases_enriched.union(r)

        self.save(
            phases_enriched,
            PROCESSED_PHASES_CACHE_KEY_TEMPLATE.format(
                make=self.make.value.replace("-", "_")
            ),
            force_update=self.force_update,
        )

        return phases_enriched

    def _set_optimal_spark_parameters(self, tss, nb_cores: int = 8):
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
            f"raw_ts/{self.make.value}/time_series/raw_ts_spark.parquet"
        )

        nb_vins = tss.select(count_distinct("vin")).collect()[0][0]

        if nb_vins == 0:
            self.logger.warning("No VINs to process, returning batch_size = 1")
            return (1, nb_vins)

        optimal_partitions = get_optimal_nb_partitions(file_size, nb_vins)

        vin_per_batch = min(
            max(1, int((nb_vins / optimal_partitions) * nb_cores * 4)), nb_vins
        )

        return (4 * nb_cores, vin_per_batch)

    def _normalize_units_to_metric(self, tss):
        tss = tss.withColumn(
            "odometer", col("odometer") * ODOMETER_MILES_TO_KM.get(self.make.value, 1)
        )
        tss = tss.withColumn("soc", F.col("soc") * SCALE_SOC[self.make.value])
        return tss

    def fill_forward(self, tss):
        return tss

    def _reassign_short_phases(self, df, min_duration_minutes=3):
        """
        Recalculates phase_id by merging phases shorter than `min_duration_minutes`
        with the previous valid phase.

        Args:
            df (DataFrame): Spark DataFrame with columns `phase_id`, `date`, `total_phase_time`
            min_duration_minutes (float): Minimum duration to keep a phase (in minutes)

        Returns:
            DataFrame: DataFrame with updated `phase_id` column
        """

        df = df.withColumn(
            "is_valid_phase",
            F.when(F.col("total_phase_time") >= min_duration_minutes, 1).otherwise(0),
        )

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

        df = df.withColumn(
            "phase_id_updated",
            F.when(F.col("is_valid_phase") == 1, F.col("phase_id")).otherwise(
                F.col("last_valid_phase_id")
            ),
        )

        df = df.withColumn(
            "phase_id_final",
            F.dense_rank().over(Window.partitionBy("vin").orderBy("phase_id_updated"))
            - 1,
        )

        df = df.drop(
            "phase_id", "last_valid_phase_id", "is_valid_phase", "phase_id_updated"
        )
        df = df.withColumnRenamed("phase_id_final", "phase_id")

        return df

    def compute_charge_idx(
        self,
        tss: DF,
        total_soc_diff_threshold: float = 0.5,
        phase_delimiter_mn: int = 45,
    ) -> DF:
        w = Window.partitionBy("vin").orderBy(
            "date"
        )  # Window partitioned by vin and sorted by date

        # Remove rows with null values for the "soc" column, don't do forward fill to be able to distinguish real idles
        tss = tss.na.drop(subset=["soc"])

        # Calculate soc_diff at each point
        tss = tss.withColumn(
            "soc_diff",
            F.col("soc") - F.lag("soc").over(w),
        )

        # Calculate time between two points
        df = tss.withColumn("prev_date", F.lag("date").over(w)).withColumn(
            "time_gap_minutes",
            (F.unix_timestamp("date") - F.unix_timestamp("prev_date")) / 60,
        )

        # Naively calculate the charging direction
        df = df.withColumn(
            "direction_raw",
            F.when(col("soc_diff").isNull(), None).otherwise(F.signum("soc_diff")),
        )

        # Forward fill the direction
        df = df.withColumn(
            "direction",
            F.last("direction_raw", ignorenulls=True).over(
                w.partitionBy("vin")
                .orderBy("date")
                .rowsBetween(Window.unboundedPreceding, 0)
            ),
        )

        # Calculate moments when direction changes
        df = df.withColumn(
            "direction_change",
            F.when(F.col("direction") != F.lag("direction").over(w), 1).otherwise(0),
        )

        # At this stage we have naively defined all phases regardless of soc points gained or lost and time spent in the phase
        df = df.withColumn(
            "phase_id",
            F.sum("direction_change").over(w.rowsBetween(Window.unboundedPreceding, 0)),
        )

        w_phase = Window.partitionBy("vin", "phase_id")

        # For Tesla
        # Total time of the useful phase, for Tesla
        if self.make == MakeEnum.tesla_fleet_telemetry:
            df = df.withColumn(
                "total_phase_time", F.sum("time_gap_minutes").over(w_phase)
            )

            df = self._reassign_short_phases(
                df
            )  # Reassign short phases to previous valid phase (especially useful for tesla-fleet-telemetry noise)

        # Total soc gained or lost in the naive phase
        df = df.withColumn("total_soc_diff_phase", F.sum("soc_diff").over(w_phase))

        # Direction of the previous and next phase
        df = df.withColumn("prev_phase", F.lag("direction").over(w)).withColumn(
            "next_phase", F.lead("direction").over(w)
        )

        # Core of the reactor: allowing to properly judge the status, a phase to be considered as charging or discharging must bring a minimum soc gain or loss
        # If the phase lasts one point and the previous and next are of the same nature, we reassign them otherwise it's an idle

        df = df.withColumn(
            "charging_status",
            F.when(
                (F.col("total_soc_diff_phase") > total_soc_diff_threshold), "charging"
            )
            .when(
                (F.col("total_soc_diff_phase") < -total_soc_diff_threshold),
                "discharging",
            )
            .when(
                (F.col("prev_phase") == F.col("next_phase"))
                & (F.col("prev_phase") > 0),
                "charging",
            )
            .when(
                (F.col("prev_phase") == F.col("next_phase"))
                & (F.col("prev_phase") < 0),
                "discharging",
            )
            .otherwise("idle"),
        )

        # Make sure the previous phase starts at the right place
        df = df.withColumn("next_status", F.lead("charging_status").over(w))

        # Set the phase before a charging/discharging phase as the phase to get the correct SOC diff
        df = df.withColumn(
            "charging_status",
            F.when(F.col("next_status") == "charging", "charging")
            .when(F.col("next_status") == "discharging", "discharging")
            .otherwise(col("charging_status")),
        )

        # Drop idles because we have no interest in identifying them and it will allow to properly cut phase numbers
        # But important to have identified them to not unnecessarily extend charging or discharging phases as time is important
        df = df.withColumn(
            "charging_status",
            F.when(F.col("charging_status") == "idle", None).otherwise(
                F.col("charging_status")
            ),
        )

        df = df.na.drop(subset=["charging_status"])

        df = df.withColumn(
            "charging_status_change",
            F.when(
                F.col("charging_status") != F.lag("charging_status").over(w), 1
            ).otherwise(0),
        )

        # Separate successive phases that are identical, only if the frequency is sufficient
        if self.make in (
            MakeEnum.bmw,
            MakeEnum.tesla_fleet_telemetry,
            MakeEnum.renault,
        ):
            df = df.withColumn("prev_date", F.lag("date").over(w))
            df = df.withColumn(
                "time_gap_minutes",
                (F.unix_timestamp("date") - F.unix_timestamp("prev_date")) / 60,
            )

            df = df.withColumn(
                "charging_status_change",
                F.when(
                    (F.col("charging_status_change") == 0)
                    & (F.col("time_gap_minutes") > phase_delimiter_mn),
                    F.lit(1),
                ).otherwise(F.col("charging_status_change")),
            )

        df = df.withColumn(
            "charging_status_idx",
            F.sum("charging_status_change").over(
                w.rowsBetween(Window.unboundedPreceding, 0)
            ),
        )

        return df

    def generate_phase(self, df: DF) -> DF:
        """
        Aggregate phases to get the final phase dataframe
        """

        w_phase_static = Window.partitionBy("vin").orderBy("first_date")

        # Aggregate per phase
        phase_stats = (
            df.orderBy("date")
            .groupBy("vin", "charging_status_idx")
            .agg(
                F.first("soc", ignorenulls=True).alias("first_soc"),
                F.last("soc", ignorenulls=True).alias("last_soc"),
                F.min("date").alias("first_date"),
                F.max("date").alias("last_date"),
                F.count("date").alias("count_points"),
                F.first("charging_status").alias("charging_status"),
            )
            .withColumn("next_dt_begin", F.lead("first_date").over(w_phase_static))
            .withColumn("next_first_soc", F.lead("first_soc").over(w_phase_static))
        )

        # Reajust the phase end when charging end = discharging start (resp. start = end)
        condition = (
            (F.col("last_soc") < F.col("next_first_soc"))
            & (F.col("charging_status") == "charging")
        ) | (
            (F.col("last_soc") > F.col("next_first_soc"))
            & (F.col("charging_status") == "discharging")
        )

        phase_stats = phase_stats.withColumn(
            "adjusted_last_date",
            F.when(condition, F.col("next_dt_begin")).otherwise(F.col("last_date")),
        ).withColumn(
            "adjusted_last_soc",
            F.when(condition, F.col("next_first_soc")).otherwise(F.col("last_soc")),
        )

        phase_stats = phase_stats.drop("last_soc", "last_date")

        phase_stats = phase_stats.withColumnRenamed("adjusted_last_soc", "last_soc")
        phase_stats = phase_stats.withColumnRenamed("adjusted_last_date", "last_date")

        # Compute the total soc diff and the total phase time
        phase_stats = phase_stats.withColumn(
            "total_soc_diff", col("last_soc") - col("first_soc")
        )
        phase_stats = phase_stats.withColumn(
            "total_phase_time_minutes",
            (F.unix_timestamp(col("last_date")) - F.unix_timestamp(col("first_date")))
            / 60,
        )

        phase_stats = phase_stats.filter(F.col("total_soc_diff") != 0)

        phase_stats = phase_stats.withColumn(
            "is_usable_phase",
            F.when(
                (F.col("count_points") > 1)
                & (F.col("total_phase_time_minutes") <= 1440),
                F.lit(1),
            ).otherwise(0),
        )

        return phase_stats

    def join_metrics_to_phase(self, phases_df: DF, tss: DF) -> DF:
        """
        Join the time series stats to the tss dataframe
        """
        phases_df = phases_df.select(
            F.col("vin").alias("VIN_ph"),
            F.col("first_date").alias("DATETIME_BEGIN"),
            F.col("last_date").alias("DATETIME_END"),
            F.col("total_phase_time_minutes").alias("PHASE_TIME_MINUTES"),
            F.col("charging_status_idx").alias("PHASE_INDEX"),
            F.col("charging_status").alias("PHASE_STATUS"),
            F.col("first_soc").alias("SOC_FIRST"),
            F.col("last_soc").alias("SOC_LAST"),
            F.col("total_soc_diff").alias("SOC_DIFF"),
            F.col("count_points").alias("NO_SOC_DATAPOINT"),
            F.col("is_usable_phase").alias("IS_USABLE_PHASE"),
        )

        ts = tss.alias("ts")
        ph = phases_df.alias("ph")

        # Join condition
        join_condition = (
            (ph["VIN_ph"] == ts["vin"])
            & (ts["date"] >= ph["datetime_begin"])
            & (ts["date"] <= ph["datetime_end"])
        )

        # Join
        tss_phased = ph.join(ts, on=join_condition, how="left")

        tss_phased = (
            tss_phased.join(self.spark.createDataFrame(self.fleet_info), "vin", "left")
            .drop("vin")
            .withColumnRenamed("VIN_ph", "VIN")
        )

        return tss_phased

    def compute_specific_features_before_aggregation(self, phase_df: DF) -> DF:
        """
        Compute the specific features before aggregation
        """
        return phase_df

    def aggregate_stats(self, phase_df: DF) -> DF:
        """
        Aggregate the time series stats to the phase dataframe
        """
        agg_columns = [
            F.first("make", ignorenulls=True).alias("MAKE"),
            F.first("model", ignorenulls=True).alias("MODEL"),
            F.first("version", ignorenulls=True).alias("VERSION"),
            F.first("net_capacity", ignorenulls=True).alias("BATTERY_NET_CAPACITY"),
            F.first("odometer", ignorenulls=True).alias("ODOMETER_FIRST"),
            F.last("odometer", ignorenulls=True).alias("ODOMETER_LAST"),
            F.first("range", ignorenulls=True).alias("RANGE"),
        ]

        if "consumption" in phase_df.columns:
            agg_columns.append(F.mean("consumption").alias("CONSUMPTION"))

        df_final = phase_df.groupBy(
            "VIN",
            "PHASE_INDEX",
            "DATETIME_BEGIN",
            "DATETIME_END",
            "PHASE_STATUS",
            "SOC_FIRST",
            "SOC_LAST",
            "SOC_DIFF",
            "NO_SOC_DATAPOINT",
            "IS_USABLE_PHASE",
        ).agg(*agg_columns)

        return df_final
