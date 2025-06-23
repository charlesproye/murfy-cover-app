# Spark utils function

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType, TimestampType, StringType, BooleanType, IntegerType, DoubleType
from transform.processed_tss.config import COL_DTYPES


LIST_COL_TO_DROP = ["model"]

def create_spark_session(access_key: str, secret_key: str) -> SparkSession:
    """
    Create a session spark with a connexion to scaleway
    
    """
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
    #### dépolaceer dans transform.raw_tss.parsing -> parsing_fleet_teleemtry
    """
    Parse dict from api response 

    Args:
        response (dict): Contains data to parse
        spark (SparkSession): spark session active 

    Returns:
        spark.DataFrame: Data with every columns
    """
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
                value = str(value)
                row_data[key] = value
        flattened_data.append({**base, **row_data})
    df_spark = spark.createDataFrame(flattened_data)

    return df_spark

    
def timedelta_to_interval(td):
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"INTERVAL '{hours}:{minutes:02d}:{seconds:02d}' HOUR TO SECOND"

def safe_astype_spark_with_error_handling(tss):
    
    existing_columns = set(tss.columns)
    result_df = tss
    COL_DTYPES = {
        "date": TimestampType(), 
        "soc": FloatType(), 
        "odometer": FloatType(), 
        "battery_level": FloatType(), 
        "ac_charge_energy_added": FloatType(), 
        "dc_charge_energy_added": FloatType(), 
        "ac_charging_power": FloatType(), 
        "dc_charging_power": FloatType(), 
        "charging_status": StringType(), 
        "prev_date": TimestampType(), 
        "sec_time_diff": FloatType(), 
        "in_charge": BooleanType(), 
        "in_discharge": BooleanType(), 
        "charge_energy_added": FloatType(), 
        "soc_diff": FloatType(), 
        "in_charge_idx": FloatType(), 
        "end_of_contract_date": TimestampType(), 
        "fleet_name": StringType(), 
        "start_date": TimestampType(), 
        "version": StringType(), 
        "capacity": FloatType(), 
        "net_capacity": FloatType(), 
        "range": FloatType(), 
        "tesla_code": StringType(), 
        "make": StringType(),
        "region_name": StringType(),
        "activation_status": BooleanType(),
        "vin": StringType(),
        'BMSState': StringType(),
        "BatteryHeaterOn": BooleanType(), 
        "BmsFullchargecomplete": StringType(), 
        "ChargeEnableRequest": StringType(),
        "ChargePort": StringType(), 
        "ChargePortColdWeatherMode": StringType(),
        "ChargeState": StringType(),
        "ChargingCableType": StringType(),
        "ClimateKeeperMode": StringType(),
        "DCDCEnable": StringType(),
        "DefrostMode": StringType(),
        "EfficiencyPackage": StringType(),
        "FastChargerPresent": StringType(), 
        "FastChargerType": StringType(),
        "HvacAutoMode": StringType(),
        "HvacPower": StringType(),
        "PreconditioningEnabled": StringType(),   
        "SentryMode": StringType(),
        "status": StringType(),
        "trailing_soc": FloatType(),
        "leading_soc": FloatType(),  
        "BrickVoltageMax": FloatType(), 
        "BrickVoltageMin": FloatType(), 
        "ChargeAmps": FloatType(), 
        "ChargeCurrentRequest": FloatType(), 
        "ChargeCurrentRequestMax": FloatType(), 
        "ChargeLimitSoc": FloatType(), 
        "ChargeRateMilePerHour": FloatType(), 
        "ChargerPhases": FloatType(), 
        "DefrostForPreconditioning": FloatType(), 
        "EnergyRemaining": FloatType(), 
        "EstBatteryRange": FloatType(), 
        "EstimatedHoursToChargeTermination": FloatType(), 
        "EuropeVehicle": FloatType(), 
        "HvacACEnabled": FloatType(), 
        "HvacFanSpeed": FloatType(), 
        "IdealBatteryRange": FloatType(), 
        "InsideTemp": FloatType(), 
        "IsolationResistance": FloatType(), 
        "LifetimeEnergyUsed": FloatType(), 
        "ModuleTempMax": FloatType(), 
        "ModuleTempMin": FloatType(), 
        "odometer": FloatType(), 
        "OutsideTemp": FloatType(), 
        "PackCurrent": FloatType(), 
        "PackVoltage": FloatType(), 
        "RatedRange": FloatType(), 
        "RearDefrostEnabled": FloatType(), 
        "VehicleSpeed": FloatType(), 
        "ChargerVoltage": FloatType(), 
        "sec_time_diff": FloatType(), 
        "in_charge": BooleanType(), 
        "in_discharge": BooleanType(), 
        "in_charge_idx": IntegerType(), 
        "in_discharge_idx": IntegerType(), 
        "trimmed_in_charge": BooleanType(), 
        "trimmed_in_discharge": BooleanType(), 
        "trimmed_in_charge_idx": IntegerType(), 
        "trimmed_in_discharge_idx": IntegerType(), 
    }
    for column_name, target_type in COL_DTYPES.items():
        if column_name in existing_columns:
            try:
                result_df = result_df.withColumn(column_name, col(column_name).cast(target_type))
                print(f"✓ Colonne '{column_name}' convertie en {target_type}")
            except Exception as e:
                print(f"⚠ Erreur lors de la conversion de '{column_name}': {e}")
    result_df = result_df.drop("model")
    return result_df
