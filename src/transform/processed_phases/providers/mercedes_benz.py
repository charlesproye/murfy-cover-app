from logging import Logger

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from transform.processed_phases.raw_ts_to_processed_phases import RawTsToProcessedPhases
from pyspark.sql import Window


class MercedesBenzRawTsToProcessedPhases(RawTsToProcessedPhases):

    def __init__(
        self,
        make="mercedes-benz",
        spark: SparkSession = None,
        force_update: bool = False,
        logger: Logger = None,
        **kwargs
    ):
        super().__init__(
            make, spark=spark, force_update=force_update, logger=logger, **kwargs
        )
    
    def compute_specific_features_before_aggregation(self, phase_df) :
        """
        Compute the specific features before aggregation
        It computes a weighted charging_rate for each phase
        """
        
        # Fenêtre partitionnée par VIN + PHASE_INDEX, ordonnée par date
        w = Window.partitionBy("VIN", "PHASE_INDEX", "DATETIME_BEGIN", "DATETIME_END").orderBy("date")

        df_cr = phase_df.filter(F.col('charging_rate').isNotNull())


        # Récupérer la prochaine date
        df_cr = df_cr.withColumn("next_date", F.lead("date").over(w))

        # Pour la dernière ligne d'une phase, utiliser DATETIME_END comme borne
        df_cr = df_cr.withColumn(
            "interval_end",
            F.coalesce(F.col("next_date"), F.col("DATETIME_END"))
        )

        # Calculer la durée en minutes entre 2 timestamps
        df_cr = df_cr.withColumn(
            "duration_minutes",
            (F.col("interval_end").cast("long") - F.col("date").cast("long")) / 60
        )

        # Ne garder que les durées positives
        df_cr = df_cr.filter(F.col("duration_minutes") > 0)

        # Calcul pondéré : charging_rate * durée
        df_cr = df_cr.withColumn("weighted_charging_rate", F.col("charging_rate") * F.col("duration_minutes"))

        phase_df = phase_df.join(df_cr.select("VIN", "PHASE_INDEX", "DATETIME_BEGIN", "DATETIME_END", "date", "weighted_charging_rate", "duration_minutes"), on=["VIN", "PHASE_INDEX", "DATETIME_BEGIN", "DATETIME_END", "date"], how="left")

        return phase_df

    def aggregate_stats(self, phase_df):
        agg_columns = [
            # Minimum 
            F.first("make", ignorenulls=True).alias("MAKE"),
            F.first("model", ignorenulls=True).alias("MODEL"),
            F.first("version", ignorenulls=True).alias("VERSION"),
            F.first("net_capacity", ignorenulls=True).alias("BATTERY_NET_CAPACITY"),
            F.first("odometer", ignorenulls=True).alias("ODOMETER_FIRST"),
            F.last("odometer", ignorenulls=True).alias("ODOMETER_LAST"),
            # Mercedes specific features / Might be able to handle with config
            (F.sum("weighted_charging_rate") / F.sum("duration_minutes")).alias("CHARGING_RATE_MEAN"),
            F.last('total_charging_duration', ignorenulls=True).alias('CHARGING_DURATION_OEM'),
            F.last('energy_charged', ignorenulls=True).alias('TOTAL_ENERGY_CHARGED')
        ]

        if "consumption" in phase_df.columns:
            agg_columns.append(F.mean("consumption").alias("CONSUMPTION"))

        

        df_aggregated = (
            phase_df.groupBy("VIN", "PHASE_INDEX", "DATETIME_BEGIN", "DATETIME_END", "PHASE_STATUS", "SOC_FIRST", "SOC_LAST", "SOC_DIFF", "NO_SOC_DATAPOINT", "IS_USABLE_PHASE")
            .agg(*agg_columns)
        )

        return df_aggregated
