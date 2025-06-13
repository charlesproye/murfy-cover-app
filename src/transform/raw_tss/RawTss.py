from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, trunc, to_timestamp, expr
from pyspark.sql import DataFrame, SparkSession
from functools import reduce, partial
from rich.progress import track
from core.s3_utils import S3_Bucket
from core.spark_utils import *
from transform.raw_tss.config import *
from core.caching_utils import cache_result_spark
from typing import Optional, List
from core.caching_utils import CachedETL

class RawTss:
    """
    Classe pour traiter les données de télémétrie de la flotte Tesla
    """
    
    def __init__(self, bucket: S3_Bucket = S3_Bucket(), spark: SparkSession = None):
        """
        Initialise le processeur de télémétrie
        
        Args:
            bucket: Instance S3_Bucket pour l'accès aux données
            spark: Session Spark pour le traitement des données
        """
        self.bucket = bucket
        self.spark = spark
        self.base_s3_path = "s3a://bib-platform-prod-data"
        
    def _ensure_spark_session(self):
        """Vérifie qu'une session Spark est disponible"""
        if self.spark is None:
            raise ValueError("Session Spark non initialisée. Utilisez set_spark_session() d'abord.")
    
    def set_spark_session(self, spark: SparkSession):
        """Définit la session Spark à utiliser"""
        self.spark = spark
        
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
        print("read_parquet_spark started")
        
        full_path = f"{self.base_s3_path}/{key}"
        df = self.spark.read.parquet(full_path)
        
        if columns is not None:
            df = df.select(*columns)
            
        return df
    
    def _create_empty_raw_tss_schema(self) -> DataFrame:
        """Crée un DataFrame vide avec le schéma raw_tss"""
        schema = StructType([
            StructField("vin", StringType(), True),
            StructField("readable_date", TimestampType(), True),
        ])
        return self.spark.createDataFrame([], schema)
    
    def get_response_keys_to_parse(self) -> DataFrame:
        """
        Récupère les clés de réponse à parser en filtrant par date de dernière analyse
        
        Returns:
            DataFrame contenant les clés à traiter
        """
        self._ensure_spark_session()
        print("get_response_keys_to_parse_spark started")
        
        # Récupération des données TSS brutes existantes ou création d'un DataFrame vide
        if self.bucket.check_spark_file_exists(FLEET_TELEMETRY_RAW_TSS_KEY):
            raw_tss_subset = self.read_parquet(
                FLEET_TELEMETRY_RAW_TSS_KEY, 
                columns=["vin", "readable_date"]
            )
        else:
            raw_tss_subset = self._create_empty_raw_tss_schema()
        
        # Calcul de la dernière date parsée par VIN
        last_parsed_date = (
            raw_tss_subset
            .groupby(["vin"])
            .agg({"readable_date": "max"})
            .withColumnRenamed("max(readable_date)", "last_parsed_date")
        )
        
        # Récupération des clés de réponse
        response_keys_df = self.bucket.list_responses_keys_of_brand("tesla-fleet-telemetry")
        response_keys_df = self.spark.createDataFrame(response_keys_df)
        response_keys_df = response_keys_df.withColumn(
            "date",
            to_timestamp(expr("substring(file, 1, length(file) - 5)"))
        )
        
        # Filtrage des nouvelles clés à traiter
        return (
            response_keys_df
            .join(last_parsed_date, on="vin", how="outer")
            .filter((col("last_parsed_date").isNull()) | (col("date") > col("last_parsed_date")))
        )
    
    def _process_weekly_group(self, week_keys: DataFrame) -> List[DataFrame]:
        """
        Traite un groupe de clés pour une semaine donnée
        
        Args:
            week_keys: DataFrame contenant les clés pour une semaine
            
        Returns:
            Liste des DataFrames traités
        """
        key_list = [r["key"] for r in week_keys.select("key").distinct().collect()]
        print(f"Processing {len(key_list)} keys")
        
        weekly_data = []
        responses = self.bucket.read_multiple_json_files(key_list, max_workers=64)
        
        for response in responses:
            try:
                rows = explode_data_spark(response, self.spark)
                weekly_data.append(rows)
            except Exception as e:
                print(f"Error parsing response: {e}")
        
        print("week_raw_tss done")
        return weekly_data
    
    def get_raw_tss_from_keys(self, keys: DataFrame) -> DataFrame:
        """
        Traite les clés pour extraire les données TSS brutes
        
        Args:
            keys: DataFrame contenant les clés à traiter
            
        Returns:
            DataFrame avec les données TSS brutes
        """
        self._ensure_spark_session()
        
        # Ajout de la colonne week_start
        df = keys.withColumn("week_start", trunc(col("date"), "week"))
        df = df.withColumn("week_start", expr("date_sub(trunc(date, 'week'), 6)"))
        
        all_data = []
        
        # Traitement par semaine
        weeks = df.select("week_start").distinct().orderBy("week_start").collect()
        
        for row in track(weeks, description="Processing weekly groups"):
            week_keys = df.filter(df.week_start == row["week_start"])
            weekly_data = self._process_weekly_group(week_keys)
            all_data.extend(weekly_data)
        
        # Union de tous les DataFrames
        if all_data:
            return reduce(
                lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), 
                all_data
            )
        
        return self.spark.createDataFrame([])
    
    @cache_result_spark(SPARK_FLEET_TELEMETRY_RAW_TSS_KEY, on="s3")
    def get_raw_tss(self) -> DataFrame:
        """
        Point d'entrée principal pour récupérer les données TSS brutes
        
        Returns:
            DataFrame contenant toutes les données TSS brutes
        """
        self._ensure_spark_session()
        logger.debug("Getting raw tss from responses provided by tesla fleet telemetry.")
        
        keys = self.get_response_keys_to_parse()
        print("keys loaded")
        
        if self.bucket.check_file_exists(SPARK_FLEET_TELEMETRY_RAW_TSS_KEY):
            # Fusion des données existantes avec les nouvelles
            raw_tss = self.read_parquet(SPARK_FLEET_TELEMETRY_RAW_TSS_KEY)
            print("raw_tss loaded")
            
            new_raw_tss = self.get_raw_tss_from_keys(keys)
            print("new_raw_tss loaded")
            
            return raw_tss.unionByName(new_raw_tss, allowMissingColumns=True)
        else:
            # Première exécution - traitement de toutes les clés
            return self.get_raw_tss_from_keys(keys)
    
    def configure_spark_optimization(self):
        """Configure les optimisations Spark"""
        self._ensure_spark_session()
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64MB")
    
    def write_data_to_s3(self, data: DataFrame, output_path: str, num_partitions: int = 10):
        """
        Écrit les données vers S3
        
        Args:
            data: DataFrame à écrire
            output_path: Chemin de sortie S3
            num_partitions: Nombre de partitions
        """
        self._ensure_spark_session()
        (data.repartition(num_partitions)
         .write
         .option("parquet.block.size", 67108864)
         .mode("overwrite")
         .parquet(output_path))
        print(f"Data written to {output_path}")


def main():
    """Fonction principale d'exécution"""
    # Initialisation
    bucket = S3_Bucket()
    processor = RawTss(bucket)
    
    # Création de la session Spark
    spark_session = create_spark_session(
        bucket.get_creds_from_dot_env()["aws_access_key_id"],
        bucket.get_creds_from_dot_env()["aws_secret_access_key"]
    )
    processor.set_spark_session(spark_session)
    
    print('Spark session launched')
    
    try:

        data = processor.get_raw_tss()
        print("data processing done")
        
        processor.configure_spark_optimization()
        
        print("Processing completed successfully")
        
    finally:
        spark_session.stop()


if __name__ == '__main__':
    main()
