from pyspark.sql import functions as F, Window, SparkSession
from pyspark.sql import DataFrame
from core.s3_utils import S3_Bucket
from core.spark_utils import create_spark_session, safe_astype_spark_with_error_handling_results
from transform.raw_results.config import *
from core.caching_utils import CachedETLSpark
from transform.processed_tss.ProcessedTimeSeriesSpark import ProcessedTimeSeries
from transform.raw_results.compute_soh import *

SOH_FUNCTION = {"tesla-fleet-telemetry": tesla_soh}

class RawResult(CachedETLSpark):
    """
    Classe pour traiter les données de charge des véhicules électriques.
    """
    
    def __init__(self, make, spark: SparkSession=None, force_update:bool=False, **kwargs):
        """
        Initialise le processeur de données de charge.
        """
        self.make = make
        self.spark=spark
        super().__init__(RAW_RESULTS_CACHE_KEY_TEMPLATE.format(make=make), "s3", force_update=force_update, spark=spark, **kwargs)
    
    def run(self):
        """
        Traite les données de charge et calcule diverses métriques.
        
        Args:
            df (DataFrame): DataFrame PySpark contenant les données de charge
            
        Returns:
            DataFrame: DataFrame traité avec les métriques calculées
        """
        window_spec = Window.partitionBy("vin", "in_charge_idx").orderBy("date")

        df = ProcessedTimeSeries(self.make, spark=self.spark).data
        # Agrégation des données
        results = df.groupBy("vin", "in_charge_idx").agg(
            F.min("ac_charge_energy_added").alias("ac_energy_added_min"),
            F.min("dc_charge_energy_added").alias("dc_energy_added_min"),
            F.last("ac_charge_energy_added", ignorenulls=True).alias("ac_energy_added_end"),
            F.last("dc_charge_energy_added", ignorenulls=True).alias("dc_energy_added_end"),
            F.first("net_capacity", ignorenulls=True).alias("net_capacity"),
            F.first("range", ignorenulls=True).alias("range"),
            F.first("odometer", ignorenulls=True).alias("odometer"),
            F.first("version", ignorenulls=True).alias("version"),
            F.count("soc").alias("size"),
            F.first("model", ignorenulls=True).alias("model"),
            F.first("date", ignorenulls=True).alias("date"),
            F.expr("percentile_approx(ac_charging_power, 0.5)").alias("ac_charging_power"),
            F.expr("percentile_approx(dc_charging_power, 0.5)").alias("dc_charging_power"),
            F.first("tesla_code", ignorenulls=True).alias("tesla_code"),
        )
        print("agg done")
        # Calcul de la différence SOC
        results = self._compute_soc_diff(df, results, window_spec)
        
        # Calcul de la puissance de charge
        results = self._compute_charging_power(results)
        
        # Calcul de l'énergie ajoutée
        results = self._compute_energy_added(results)
        
        # Calcul du SOH (State of Health)
        print(results)
        results = SOH_FUNCTION[self.make](results)
        
        # Calcul des niveaux de charge
        results = self._compute_charge_levels(results)
        print(results)
        # Tri des résultats
        results = results.orderBy("tesla_code", "vin", "date")
        
        # Calcul des cycles (nécessite la fonction estimate_cycles)
        results = self._estimate_cycles(results)

        # Pour faciliter l'union des dataframes avec Spark
        results = safe_astype_spark_with_error_handling_results(results)
        print('astype done')


        return results.cache()
    
    def _compute_soc_diff(self, df: DataFrame, results: DataFrame,  window_spec) -> DataFrame:
        """
        Calcule la différence de SOC (State of Charge).
        
        Args:
            df: DataFrame original
            df: DataFrame des résultats agrégés
            window_spec: Spécification de la fenêtre
            
        Returns:
            DataFrame: df avec soc_diff ajouté
        """
        df = df.withColumn("soc_start",  F.first("soc").over(window_spec))
        df = df.withColumn("soc_end",  F.last("soc").over(window_spec))
        df = df.withColumn("soc_diff", F.col("soc_start") - F.col("soc_end"))
        print("soc diff done")
        
        # Ajouter soc_diff aux résultats
        soc_diff_df = df.select("vin", "in_charge_idx", "soc_diff").distinct()
        results = results.join(soc_diff_df, on=["vin", "in_charge_idx"], how="left")
        print("soc diff merge done")
        
        return results
    
    def _compute_charging_power(self, df: DataFrame) -> DataFrame:
        """
        Calcule la puissance de charge totale (AC + DC).
        
        Args:
            df: DataFrame des résultats
            
        Returns:
            DataFrame: df avec charging_power ajouté
        """
        df = df.withColumn(
            "charging_power",
            F.coalesce(F.col("ac_charging_power"), F.lit(0)) + F.coalesce(F.col("dc_charging_power"), F.lit(0))
        )
        print("charging power done")
        return df
    
    def _compute_energy_added(self, df: DataFrame) -> DataFrame:
        """
        Calcule l'énergie ajoutée pendant la charge.
        
        Args:
            df: DataFrame des résultats
            
        Returns:
            DataFrame: df avec energy_added ajouté
        """
        df = df.withColumn(
            "ac_energy_added", F.col("ac_energy_added_end") - F.col("ac_energy_added_min")
        ).withColumn(
            "dc_energy_added", F.col("dc_energy_added_end") - F.col("dc_energy_added_min")
        ).withColumn(
            "energy_added", F.col("dc_energy_added")
        )
        print("energy add done")
        return df
    
    def _compute_charge_levels(self, df: DataFrame) -> DataFrame:
        """
        Calcule les niveaux de charge (Level 1, 2, 3) basés sur la puissance.
        
        Args:
            df: DataFrame des résultats
            
        Returns:
            DataFrame: df avec les niveaux de charge ajoutés
        """
        df = df.withColumn(
            "level_1", F.col("soc_diff") * F.when(F.col("charging_power") < 8, 1).otherwise(0) / 100
        ).withColumn(
            "level_2", F.col("soc_diff") * F.when(F.col("charging_power").between(8, 45), 1).otherwise(0) / 100
        ).withColumn(
            "level_3", F.col("soc_diff") * F.when(F.col("charging_power") > 45, 1).otherwise(0) / 100
        )
        print('levels done')
        return df
    
    def _estimate_cycles(self, df, initial_range:float=1, default_soh:float=1.0):
        """Calcule le nombre estimé de cycles

        Args:
            initial_range (float): autonomie initiale du véhicule
            soh (float, optional): SoH du véhicule 

        Returns:
            float: le nombre de cycle de la batterie
        """
        return df.withColumn("estimated_cycles",
                             F.round(F.col("odometer") / 
                                     ((F.when(F.col("range").isNull(), 
                                              F.lit(initial_range)).otherwise(F.col("range"))) * 
                                      ((F.when(F.col("soh").isNull() | F.isnan(F.col("soh")), 
                                               F.lit(default_soh)).otherwise(F.col("soh"))) + F.lit(1)) / F.lit(2))))


    @classmethod
    def update_all_raw_results(cls, spark:SparkSession, **kwargs):
        for make in MAKES_WITHOUT_SOH:
            RawResult(make, spark=spark, **kwargs).run()


if __name__ == "__main__":

    # Initialisation
    bucket = S3_Bucket()

    # Création de la session Spark
    creds = bucket.get_creds_from_dot_env()
    spark_session = create_spark_session(
        creds["aws_access_key_id"], creds["aws_secret_access_key"]
    )
    
    RawResult.update_all_raw_results(spark=spark_session, force_update=True)

    if "spark_session" in locals():
        spark_session.stop()

