from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, trunc, to_timestamp, expr, collect_list
from pyspark.sql import DataFrame, SparkSession
from functools import reduce, partial
from rich.progress import track
from core.s3_utils import S3_Bucket
from core.spark_utils import *
from transform.processed_tss.config import S3_PROCESSED_TSS_KEY_FORMAT
from transform.raw_tss.config import *
from core.caching_utils import cache_result_spark
from typing import Optional, List
from core.caching_utils import CachedETLSpark

class RawTss(CachedETLSpark):
    """
    Classe pour traiter les données de télémétrie de la flotte Tesla
    """
    
    def __init__(self, make, bucket: S3_Bucket = S3_Bucket(), spark: SparkSession = None):
        """
        Initialise le processeur de télémétrie
        
        Args:
            bucket: Instance S3_Bucket pour l'accès aux données
            spark: Session Spark pour le traitement des données
        """
        self.bucket = bucket
        self.spark = spark
        self.base_s3_path = "s3a://bib-platform-prod-data"
        super().__init__(S3_PROCESSED_TSS_KEY_FORMAT.format(make=make), "s3")

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
    
    def get_raw_tss_from_keys_spark(keys: DataFrame, bucket: S3_Bucket, spark, max_vins: int = None) -> DataFrame:
        # Cache du DataFrame pour éviter les recalculs
        df = keys.select("vin", "key").distinct().cache()
        
        # Collecte groupée des données par VIN
        vin_keys_grouped = (df.groupBy("vin")
                        .agg(collect_list("key").alias("keys"))
                        .orderBy("vin"))
        
        # Limit pour test le code
        if max_vins:
            vin_keys_grouped = vin_keys_grouped.limit(max_vins)
        
        # Collecte une seule fois
        vin_data = vin_keys_grouped.collect()
        
        all_data = []
        
        # Traitement par batch
        batch_size = 10  # Ajustable
        all_keys_to_process = []
        vin_key_mapping = {}
        
        # prépareration des keys
        for row in vin_data:
            vin = row["vin"]
            keys_list = row["keys"]
            all_keys_to_process.extend(keys_list)
            for key in keys_list:
                vin_key_mapping[key] = vin
        
        print(f"Total keys to process: {len(all_keys_to_process)}")
        
        # traitement par batch des fichiers S3
        for i in track(range(0, len(all_keys_to_process), batch_size), 
                    description="Processing batches"):
            batch_keys = all_keys_to_process[i:i + batch_size]
            
            try:
                responses = bucket.read_multiple_json_files(batch_keys, max_workers=128)
                batch_data = []
                for response in responses:
                    try:
                        rows = explode_data_spark(response, spark)
                        batch_data.append(rows)
                    except Exception as e:
                        print(f"Error parsing response: {e}")
                
                # Union des données du batch
                if batch_data:
                    batch_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), 
                                    batch_data)
                    all_data.append(batch_df)
                    
            except Exception as e:
                print(f"Error processing batch {i//batch_size + 1}: {e}")
        
        if all_data:
            # Cache le résultat final si besoin plus tard
            final_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), all_data)
            return final_df.cache()
        
        # Suppression cache
        df.unpersist()
        
        return spark.createDataFrame([], schema=keys.schema)
    
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
            new_raw_tss = self.get_raw_tss_from_keys_spark(keys)
            print("new_raw_tss loaded")
            return new_raw_tss
    
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
