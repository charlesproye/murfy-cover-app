from logging import getLogger

from dotenv import load_dotenv
from pyspark.sql import DataFrame as DF
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from core.caching_utils import CachedETLSpark
from core.logging_utils import set_level_of_loggers_with_prefix
from core.s3.s3_utils import S3Service
from core.s3.settings import S3Settings
from transform.processed_phases.main import PROVIDERS
from transform.result_phases.config import RESULT_PHASES_CACHE_KEY_TEMPLATE

load_dotenv()


class ProcessedPhaseToResultPhase(CachedETLSpark):
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
        logger_name = f"transform.result_phase.{make}"
        self.logger = getLogger(logger_name)
        set_level_of_loggers_with_prefix(log_level, logger_name)
        self.id_col = id_col
        self.spark = spark
        self.bucket = S3Service()
        self.settings = S3Settings()

        super().__init__(
            RESULT_PHASES_CACHE_KEY_TEMPLATE.format(make=make.replace("-", "_")),
            "s3",
            force_update=force_update,
            spark=spark,
            repartition_key="VIN",
            **kwargs,
        )

    def run(self):
        # Load the raw tss dataframe
        self.logger.info(f"Running for {self.make}")

        cls = PROVIDERS[self.make]

        pph = cls(make=self.make, spark=self.spark, logger=self.logger).data

        pph = self.compute_specific_features(pph)
        pph = self.compute_soh(pph)
        pph = self.compute_odometer_diff(pph)
        pph = self.compute_consumption(pph)
        pph = self.run_real_autonomy_compute(pph)
        pph = self.compute_charge_levels(pph)

        return pph

    def compute_specific_features(self, pph: DF) -> DF:
        """
        Compute the specific features
        """
        return pph

    def compute_soh(self, df_aggregated: DF) -> DF:
        """
        Compute the soh
        """
        return df_aggregated

    def compute_charge_levels(self, df_aggregated: DF) -> DF:
        """
        Compute the charge levels
        """
        return df_aggregated

    def compute_odometer_diff(self, phase_df: DF) -> DF:
        """
        Compute the odometer difference
        """
        return phase_df.withColumn(
            "ODOMETER_DIFF", F.col("ODOMETER_LAST") - F.col("ODOMETER_FIRST")
        )

    def compute_consumption(self, phase_df: DF) -> DF:
        """
        Compute the consumption
        """

        if "CONSUMPTION" not in phase_df.columns:
            phase_df = phase_df.withColumn(
                "ODOMETER_DIFF", F.col("ODOMETER_LAST") - F.col("ODOMETER_FIRST")
            ).withColumn(
                "CONSUMPTION",
                F.when(
                    (F.col("PHASE_STATUS") == "discharging")
                    & (F.col("ODOMETER_DIFF") > 5),
                    (-1 * F.col("SOC_DIFF"))
                    * (F.col("BATTERY_NET_CAPACITY"))
                    / F.col("ODOMETER_DIFF"),
                ).otherwise(None),
            )

        return phase_df

    def clean_dataset(self, df):
        """
        Cleans Stellantis data

        Args:
            df (pyspark.sql.DataFrame): Raw Spark DataFrame

        Returns:
            pyspark.sql.DataFrame: Cleaned Spark DataFrame
        """
        # Add ODOMETER_DIFF if necessary
        if "ODOMETER_DIFF" not in df.columns:
            df = df.withColumn(
                "ODOMETER_DIFF", F.col("ODOMETER_LAST") - F.col("ODOMETER_FIRST")
            )

        # Remove null values and filter
        df_clean = df.na.drop(subset=["ODOMETER_DIFF", "RANGE", "SOC_DIFF"])
        df_filtered = df_clean.filter(
            (F.col("SOC_DIFF") < 0) & (F.col("ODOMETER_DIFF") > 0)
        )
        return df_filtered

    def calculate_ratios(self, df):
        """
        Calculates km/SOC ratios and average SOC

        Args:
            df (pyspark.sql.DataFrame): Cleaned Spark DataFrame

        Returns:
            pyspark.sql.DataFrame: Spark DataFrame with calculated ratios
        """
        df_with_ratios = df.withColumn(
            "ratio_km_soc", F.col("ODOMETER_DIFF") / F.abs(F.col("SOC_DIFF"))
        ).withColumn("SOC_MEAN", (F.col("SOC_FIRST") + F.col("SOC_LAST")) / 2)

        return df_with_ratios

    def filter_valid_vins(self, df, min_odometer_diff=500):
        """
        Filters VINs with sufficient data using Spark

        Args:
            df (pyspark.sql.DataFrame): Spark DataFrame with ratios
            min_odometer_diff (int): Minimum required odometer difference

        Returns:
            pyspark.sql.DataFrame: Filtered Spark DataFrame
        """
        # Calculate statistics by VIN
        vin_stats = (
            df.groupBy("VIN")
            .agg(
                F.min("ODOMETER_FIRST").alias("odometer_start"),
                F.max("ODOMETER_LAST").alias("odometer_end"),
            )
            .withColumn(
                "odometer_diff", F.col("odometer_end") - F.col("odometer_start")
            )
        )

        # Filter VINs with sufficient data
        valid_vins = vin_stats.filter(F.col("odometer_diff") > min_odometer_diff)

        # Join with original data
        df_filtered = df.join(valid_vins.select("VIN"), "VIN", "inner")

        return df_filtered

    def create_bins_and_filter_data(self, df):
        """
        Creates bins and filters data according to valid ranges

        Args:
            df (pyspark.sql.DataFrame): Spark DataFrame with ratios

        Returns:
            pyspark.sql.DataFrame: Filtered Spark DataFrame according to valid ranges
        """
        # Remove null values
        df_result = df.na.drop(
            subset=["ratio_km_soc", "MAKE", "SOC_DIFF", "ODOMETER_DIFF"]
        )

        # Normalize MAKE
        df_result = df_result.withColumn("MAKE", F.expr("lower(trim(MAKE))"))

        # Create bins for kilometers
        df_result = df_result.withColumn(
            "km_range",
            F.when(F.col("ODOMETER_DIFF") < 10, "<10 km")
            .when(F.col("ODOMETER_DIFF") < 25, "10-25 km")
            .when(F.col("ODOMETER_DIFF") < 50, "25-50 km")
            .when(F.col("ODOMETER_DIFF") < 100, "50-100 km")
            .when(F.col("ODOMETER_DIFF") < 200, "100-200 km")
            .when(F.col("ODOMETER_DIFF") <= 1000, ">200 km")
            .otherwise(None),
        )

        # Create bins for SOC
        df_result = df_result.withColumn(
            "soc_range",
            F.when(F.col("SOC_DIFF") <= -50, "<-50%")
            .when(F.col("SOC_DIFF") <= -25, "-50% to -25%")
            .when(F.col("SOC_DIFF") <= -15, "-25% to -15%")
            .when(F.col("SOC_DIFF") <= -10, "-15% to -10%")
            .when(F.col("SOC_DIFF") <= -5, "-10% to -5%")
            .otherwise(F.lit("-5% to 0%")),
        )

        # Calculate valid ranges for SOC
        valid_tranches_soc = []
        makes = [
            row["MAKE"] for row in df_result.select(F.col("MAKE")).distinct().collect()
        ]

        for make in makes:
            df_make = df_result.filter(F.col("MAKE") == make)

            # Calculate statistics by SOC range
            stats = (
                df_make.groupBy("soc_range")
                .agg(F.mean("ratio_km_soc").alias("mean_ratio"))
                .collect()
            )

            if stats:
                ratios = [
                    row["mean_ratio"] for row in stats if row["mean_ratio"] is not None
                ]
                if ratios:
                    median_mean = sorted(ratios)[len(ratios) // 2]
                    lower = 0.8 * median_mean
                    upper = 1.2 * median_mean

                    valid_ranges = [
                        row["soc_range"]
                        for row in stats
                        if row["mean_ratio"] is not None
                        and lower <= row["mean_ratio"] <= upper
                    ]

                    for soc_range in valid_ranges:
                        valid_tranches_soc.append((make, soc_range))

        # Calculate valid ranges for kilometers
        valid_tranches_km = []

        for make in makes:
            df_make = df_result.filter(F.col("MAKE") == make)

            stats = (
                df_make.groupBy("km_range")
                .agg(F.mean("ratio_km_soc").alias("mean_ratio"))
                .collect()
            )

            if stats:
                ratios = [
                    row["mean_ratio"] for row in stats if row["mean_ratio"] is not None
                ]
                if ratios:
                    median_mean = sorted(ratios)[len(ratios) // 2]
                    lower = 0.8 * median_mean
                    upper = 1.2 * median_mean

                    valid_ranges = [
                        row["km_range"]
                        for row in stats
                        if row["mean_ratio"] is not None
                        and lower <= row["mean_ratio"] <= upper
                    ]

                    for km_range in valid_ranges:
                        valid_tranches_km.append((make, km_range))

        # Create DataFrames for joins
        if valid_tranches_soc:
            spark_session = SparkSession.getActiveSession()
            soc_df = spark_session.createDataFrame(
                valid_tranches_soc, ["MAKE", "soc_range"]
            )
            df_result = df_result.join(soc_df, ["MAKE", "soc_range"], "left")

        if valid_tranches_km:
            spark_session = SparkSession.getActiveSession()
            km_df = spark_session.createDataFrame(
                valid_tranches_km, ["MAKE", "km_range"]
            )
            df_result = df_result.join(km_df, ["MAKE", "km_range"], "left")

        return df_result

    def calculate_model_bounds(self, df):
        """
        Calculates model/version bounds

        Args:
            df (pyspark.sql.DataFrame): Filtered Spark DataFrame

        Returns:
            pyspark.sql.DataFrame: Spark DataFrame with model/version bounds
        """
        # Calculate statistics by model and version
        model_stats = df.groupBy("MODEL", "VERSION").agg(
            F.median("ratio_km_soc").alias("median"),
            F.count("*").alias("size"),
        )

        # Calculate total number of observations
        total_obs = df.count()

        # Calculate bounds with confidence adjustment
        model_bounds = (
            model_stats.withColumn(
                "confidence_factor",
                F.lit(1.0) - (F.col("size") / F.lit(total_obs)),
            )
            .withColumn(
                "adjustment", F.expr("greatest(0.10, least(0.30, confidence_factor))")
            )
            .withColumn(
                "lower_bound", F.col("median") * (F.lit(1.0) - F.col("adjustment"))
            )
            .withColumn(
                "upper_bound", F.col("median") * (F.lit(1.0) + F.col("adjustment"))
            )
            .withColumn("lower_bound_bis", F.col("median") * F.lit(0.8))
            .withColumn("upper_bound_bis", F.col("median") * F.lit(1.2))
            .select("MODEL", "VERSION", "median", "lower_bound", "upper_bound", "size")
            .orderBy(F.col("size").desc())
        )

        return model_bounds

    def filter_data_by_bounds(self, df, bounds_df):
        """
        Filters data according to calculated bounds

        Args:
            df (pyspark.sql.DataFrame): Spark DataFrame with ratios
            bounds_df (pyspark.sql.DataFrame): Spark DataFrame with bounds

        Returns:
            pyspark.sql.DataFrame: Filtered Spark DataFrame
        """
        # Join with bounds
        df_merged = df.join(
            bounds_df.select("VERSION", "lower_bound", "upper_bound"), "VERSION", "left"
        )

        # Filter according to bounds
        df_filtered = df_merged.filter(
            (F.col("ratio_km_soc") >= F.col("lower_bound"))
            & (F.col("ratio_km_soc") <= F.col("upper_bound"))
        ).drop("lower_bound", "upper_bound")

        return df_filtered

    def calculate_weighted_autonomy(self, df):
        """
        Calculates weighted potential autonomy by VIN using Spark

        Args:
            df (pyspark.sql.DataFrame): Filtered Spark DataFrame

        Returns:
            pyspark.sql.DataFrame: Spark DataFrame with calculated potential autonomy
        """
        # Calculate weighted average by VIN
        weighted_ratio = df.groupBy("VIN").agg(
            (
                F.sum(F.col("ratio_km_soc") * F.col("ODOMETER_DIFF"))
                / F.sum(F.col("ODOMETER_DIFF"))
            ).alias("weighted_avg_ratio_km_soc"),
        )

        # Add brand and model information
        vin_info = df.select(
            "VIN", "MAKE", "MODEL", "VERSION", "RANGE", "BATTERY_NET_CAPACITY"
        ).distinct()

        weighted_ratio_with_info = weighted_ratio.join(vin_info, "VIN", "left")

        # Calculate potential autonomy
        weighted_ratio_final = weighted_ratio_with_info.withColumn(
            "REAL_RANGE", F.round(F.col("weighted_avg_ratio_km_soc") * F.lit(100))
        )
        return weighted_ratio_final[["VIN", "REAL_RANGE"]]

    def run_real_autonomy_compute(self, df):
        """
        Runs the complete analysis

        Args:
            df (pyspark.sql.DataFrame): Raw Spark DataFrame

        Returns:
            pyspark.sql.DataFrame: Spark DataFrame with complete analysis
        """
        phase_df = df
        df_clean = self.clean_dataset(df)
        df_clean = self.calculate_ratios(df_clean)
        df_clean = self.filter_valid_vins(df_clean)
        df_valid = self.create_bins_and_filter_data(df_clean)
        model_bounds = self.calculate_model_bounds(df_valid)
        df_filtered = self.filter_data_by_bounds(df_valid, model_bounds)
        range_df = self.calculate_weighted_autonomy(df_filtered)
        phase_df = phase_df.join(range_df, "VIN", "left")
        return phase_df

