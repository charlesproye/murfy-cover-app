from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, to_timestamp, expr
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

def get_raw_tss_from_keys_spark(keys: DataFrame, bucket: S3_Bucket, spark) -> DataFrame:
    print(keys.printSchema())
    df = keys.select("vin", "key").distinct()
    all_data = []
    # Obtenir les vins
    vins = df.select("vin").distinct().orderBy("vin").collect()

   # i =  0
    for row in track(vins, description="Processing weekly groups"):
        print(row)
        #if i < 3:
        vin_keys = df.filter(df.vin == row["vin"])
        key_list = [r["key"] for r in vin_keys.distinct().collect()]
        print(len(key_list))
        responses = bucket.read_multiple_json_files(key_list, max_workers=64)
        for response in responses:
            try:
                rows = explode_data_spark(response, spark)
                all_data.append(rows)
            except Exception as e:
                print(f"Error parsing response: {e}")
        print("week_raw_tss done")
            #i += 1
        
    if all_data:
        return reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), all_data)
    
    return spark.createDataFrame()

@cache_result_spark(SPARK_FLEET_TELEMETRY_RAW_TSS_KEY,  on="s3")
def get_raw_tss(spark, bucket: S3_Bucket = S3_Bucket()) -> DF:
    keys = get_response_keys_to_parse_spark(bucket, spark)
    print("keys loaded")
    if bucket.check_spark_file_exists(SPARK_FLEET_TELEMETRY_RAW_TSS_KEY):
        #raw_tss = read_parquet_spark(spark, SPARK_FLEET_TELEMETRY_RAW_TSS_KEY)
        new_raw_tss = get_raw_tss_from_keys_spark(keys, bucket, spark)
        return new_raw_tss #raw_tss.unionByName(new_raw_tss,  allowMissingColumns=True)
    else:
        new_raw_tss = get_raw_tss_from_keys_spark(keys, bucket, spark)
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
