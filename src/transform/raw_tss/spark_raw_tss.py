from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, to_timestamp, expr, collect_list
from pyspark.sql import DataFrame
from functools import reduce
from rich.progress import track
from core.s3_utils import S3_Bucket
from core.spark_utils import explode_data_spark, create_spark_session
from transform.raw_tss.config import *
from core.caching_utils import cache_result_spark

def read_parquet_spark(spark, key: str, columns: list[str] | None = None):
    print("read_parquet_spark started")
    full_path = f"s3a://bib-platform-prod-data/{key}"
    df = spark.read.parquet(full_path)
    if columns is not None:
        df = df.select(*columns)
    return df
    
def get_response_keys_to_parse_spark(bucket:S3_Bucket, spark) -> DF:
    print("get_response_keys_to_parse_spark started")
    if bucket.check_spark_file_exists(FLEET_TELEMETRY_RAW_TSS_KEY):
        raw_tss_subset = read_parquet_spark(spark, FLEET_TELEMETRY_RAW_TSS_KEY, columns=["vin", "readable_date"])
    else:
        schema = StructType([
    StructField("vin", StringType(), True),
    StructField("readable_date", TimestampType(), True),
])
        raw_tss_subset = spark.createDataFrame([], schema)
    last_parsed_date = (
        raw_tss_subset
        .groupby(["vin"])
        .agg({"readable_date": "max"}).withColumnRenamed("max(readable_date)", "last_parsed_date")
    )
    response_keys_df = bucket.list_responses_keys_of_brand("tesla-fleet-telemetry")
    response_keys_df = spark.createDataFrame(response_keys_df)
    response_keys_df = response_keys_df.withColumn(
        "date",
        to_timestamp(expr("substring(file, 1, length(file) - 5)"))
    )
    return (
        response_keys_df
        .join(last_parsed_date, on="vin", how="outer")
        .filter((col("last_parsed_date").isNull()) | (col("date") > col("last_parsed_date")))
    )

def get_raw_tss_from_keys_spark_V2(keys: DataFrame, bucket: S3_Bucket, spark, max_vins: int = None) -> DataFrame:
    
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

@cache_result_spark(SPARK_FLEET_TELEMETRY_RAW_TSS_KEY,  on="s3")
def get_raw_tss(spark, bucket: S3_Bucket = S3_Bucket()) -> DF:
    keys = get_response_keys_to_parse_spark(bucket, spark)
    new_raw_tss = get_raw_tss_from_keys_spark_V2(keys, bucket, spark, 20)
    return new_raw_tss 
    

if __name__ == '__main__':
    spark_session = create_spark_session(S3_Bucket().get_creds_from_dot_env()["aws_access_key_id"],
                                         S3_Bucket().get_creds_from_dot_env()["aws_secret_access_key"])
    print('Spark session launch')
    data = get_raw_tss(spark_session, force_update=True)
    print("data done")
    spark_session.conf.set("spark.sql.adaptive.enabled", "true")
    spark_session.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64MB")
    print("data write in scaleway")
    spark_session.stop()
