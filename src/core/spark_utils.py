from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType, TimestampType, StringType
import os
from transform.raw_tss.fleet_telemetry_raw_tss import *



LIST_COL_TO_DROP = []

def create_spark_session(access_key, secret_key):
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.hadoop:hadoop-aws:3.3.4 pyspark-shell"
)
    spark = SparkSession.builder \
    .appName("Scaleway S3 Read JSON") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", "https://s3.fr-par.scw.cloud") \
    .config("spark.hadoop.fs.s3a.access.key", access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.executor.memory", "12g") \
    .config("spark.driver.memory", "12g") \
    .getOrCreate()
    return spark

def explode_data_spark(response: dict, spark: SparkSession):
    flattened_data = []
    for entry in response if isinstance(response, list) else [response]:
        vin = entry.get('vin')
        timestamp = entry.get('timestamp')
        readable_date = entry.get('readable_date')
        base = {
            'vin': vin,
            'timestamp': timestamp,
            'readable_date': readable_date,
        }

        row_data = {}
        for item in entry.get("data", []):
            if item not in LIST_COL_TO_DROP:
                key = item.get("key")
                value_dict = item.get("value", {})
                if not value_dict:
                    continue
                value = list(value_dict.values())[0]  # Récupère la valeur quel que soit le type
                row_data[key] = value
                try:
                    value = float(value)
                except:
                    value = str(value)
                row_data[key] = value
        flattened_data.append({**base, **row_data})
    df_spark = spark.createDataFrame(flattened_data)

    return df_spark


def rename_and_select(tss, rename_col, col_to_select):
    return tss.withColumnsRenamed(rename_col).select(col_to_select)

def safe_astype_spark(tss):
    return tss.withColumn("odometer", col("odometer").cast(FloatType())) \
    .withColumn("soc", col("soc").cast(FloatType())) \
    .withColumn("battery_level", col("battery_level").cast(FloatType())) \
    .withColumn("ac_charge_energy_added", col("ac_charge_energy_added").cast(FloatType())) \
    .withColumn("dc_charge_energy_added", col("dc_charge_energy_added").cast(FloatType())) \
    .withColumn("ac_charging_power", col("ac_charging_power").cast(FloatType())) \
    .withColumn("dc_charging_power", col("dc_charging_power").cast(FloatType())) \
    .withColumn("date", col("date").cast(TimestampType()))\
        #.withColumn("ac_charge_energy_added", col("ac_charge_energy_added").cast(StringType()))
    
