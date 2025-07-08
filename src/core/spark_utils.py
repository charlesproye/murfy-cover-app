# Spark utils function

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    FloatType,
    TimestampType,
    StringType,
    BooleanType,
    IntegerType,
    DoubleType,
)


LIST_COL_TO_DROP = ["model"]


def create_spark_session(access_key: str, secret_key: str) -> SparkSession:
    """
    Create a session spark with a connexion to scaleway

    """
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--packages org.apache.hadoop:hadoop-aws:3.3.4 pyspark-shell"
    )

    g1gc_options = (
        "-XX:+UseG1GC "  # Activer G1GC
        "-XX:MaxGCPauseMillis=100 "  # Pause GC max de 100ms
        "-XX:G1HeapRegionSize=32m "  # Taille des régions G1
        "-XX:+UseStringDeduplication "  # Déduplication des chaînes
        "-XX:+UnlockExperimentalVMOptions "  # Débloquer les options expérimentales
        "-XX:+UseZGC "  # ZGC pour les gros heaps (Java 11+)
        "-XX:+DisableExplicitGC "  # Désactiver System.gc()
        "-XX:+UseGCOverheadLimit "  # Activer la limite de surcharge GC
        "-XX:GCTimeRatio=9 "  # Ratio temps GC vs temps application
        "-XX:+PrintGCDetails "  # Logs détaillés du GC (optionnel)
        "-XX:+PrintGCTimeStamps "  # Timestamps dans les logs GC
        "-Xloggc:/tmp/spark-gc.log"  # Fichier de log GC
    )
    spark = (
        SparkSession.builder.appName("Scaleway S3 Read JSON")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.hadoop.fs.s3a.endpoint", "https://s3.fr-par.scw.cloud")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.driver.host", "localhost")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.executor.memory", "10g")  # Garder une mémoire suffisante
        .config("spark.driver.memory", "10g")  # Garder une mémoire suffisante
        .config("spark.driver.maxResultSize", "4g")
        # Configuration G1GC pour les executors
        .config("spark.executor.extraJavaOptions", g1gc_options)
        # Configuration G1GC pour le driver
        .config("spark.driver.extraJavaOptions", g1gc_options)
        .getOrCreate()
    )
    # .config("spark.sql.debug.maxToStringFields", "0") \
    # .config("spark.sql.adaptive.logLevel", "WARN") \

    return spark


def explode_data_spark(response: dict, spark: SparkSession):
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
        vin = entry.get("vin")
        timestamp = entry.get("timestamp")
        readable_date = entry.get("readable_date")
        base = {
            "vin": vin,
            "timestamp": timestamp,
            "readable_date": readable_date,
        }

        row_data = {}
        for item in entry.get("data", []):
            if item not in LIST_COL_TO_DROP:
                key = item.get("key")
                value_dict = item.get("value", {})
                if not value_dict:
                    continue
                value = list(value_dict.values())[
                    0
                ]  # Récupère la valeur quel que soit le type
                value = str(value)
                row_data[key] = value
        flattened_data.append({**base, **row_data})
    df_spark = spark.createDataFrame(flattened_data)

    return df_spark


def timedelta_to_interval(td):
    """
    Convert a timedelta to an interval
    """

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
        "BMSState": StringType(),
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
                result_df = result_df.withColumn(
                    column_name, col(column_name).cast(target_type)
                )
                print(f"✓ Colonne '{column_name}' convertie en {target_type}")
            except Exception as e:
                print(f"⚠ Erreur lors de la conversion de '{column_name}': {e}")
    result_df = result_df.drop("model")
    return result_df


def safe_astype_spark_with_error_handling_results(ptss):
    """
    Conversion sécurisée des types pour les colonnes spécifiées
    """

    existing_columns = set(ptss.columns)
    result_df = ptss

    # Mapping des types pandas vers Spark
    COL_DTYPES = {
        # String/Object types
        "vin": StringType(),
        "version": StringType(),
        "model": StringType(),
        "tesla_code": StringType(),
        # Integer types
        "in_charge_idx": IntegerType(),
        "size": IntegerType(),
        # Float32 -> FloatType (Spark FloatType = 32-bit)
        "ac_energy_added_min": FloatType(),
        "dc_energy_added_min": FloatType(),
        "ac_energy_added_end": FloatType(),
        "dc_energy_added_end": FloatType(),
        "odometer": FloatType(),
        "ac_charging_power": FloatType(),
        "dc_charging_power": FloatType(),
        "charging_power": FloatType(),
        "ac_energy_added": FloatType(),
        "dc_energy_added": FloatType(),
        "energy_added": FloatType(),
        # Float64 -> DoubleType (Spark DoubleType = 64-bit)
        "soc_diff": DoubleType(),
        "net_capacity": DoubleType(),
        "range": DoubleType(),
        "soh": DoubleType(),
        "level_1": DoubleType(),
        "level_2": DoubleType(),
        "level_3": DoubleType(),
        "cycles": DoubleType(),
        # Datetime
        "date": TimestampType(),
    }

    for column_name, target_type in COL_DTYPES.items():
        if column_name in existing_columns:
            try:
                result_df = result_df.withColumn(
                    column_name, col(column_name).cast(target_type)
                )
                print(f"✓ Colonne '{column_name}' convertie en {target_type}")
            except Exception as e:
                print(f"⚠ Erreur lors de la conversion de '{column_name}': {e}")
                # En cas d'erreur, essayer de convertir en string
                try:
                    result_df = result_df.withColumn(
                        column_name, col(column_name).cast(StringType())
                    )
                    print(
                        f"  → Colonne '{column_name}' convertie en StringType (fallback)"
                    )
                except Exception as e2:
                    print(
                        f"  ❌ Impossible de convertir '{column_name}' même en string: {e2}"
                    )

    return result_df



def align_dataframes_for_union(df1, df2, strategy="intersection"):
    """
    Aligne deux DataFrames pour l'union

    Args:
        df1, df2: DataFrames à unir
        strategy: "intersection" (colonnes communes) ou "union" (toutes les colonnes)
    """

    cols1 = set(df1.columns)
    cols2 = set(df2.columns)

    print(f"DataFrame 1: {len(cols1)} colonnes")
    print(f"DataFrame 2: {len(cols2)} colonnes")

    if strategy == "intersection":
        # Utiliser seulement les colonnes communes
        common_cols = cols1 & cols2
        print(f"Colonnes communes: {len(common_cols)}")

        # Colonnes manquantes dans chaque DataFrame
        missing_in_df1 = cols2 - cols1
        missing_in_df2 = cols1 - cols2

        if missing_in_df1:
            print(f"Colonnes manquantes dans df1: {missing_in_df1}")
        if missing_in_df2:
            print(f"Colonnes manquantes dans df2: {missing_in_df2}")

        # Sélectionner seulement les colonnes communes
        df1_aligned = df1.select(*sorted(common_cols))
        df2_aligned = df2.select(*sorted(common_cols))

    elif strategy == "union":
        # Utiliser toutes les colonnes, ajouter des colonnes NULL pour les manquantes
        all_cols = sorted(cols1 | cols2)
        print(f"Toutes les colonnes: {len(all_cols)}")

        # Ajouter les colonnes manquantes à df1
        for col in all_cols:
            if col not in cols1:
                df1 = df1.withColumn(col, lit(None).cast("string"))

        # Ajouter les colonnes manquantes à df2
        for col in all_cols:
            if col not in cols2:
                df2 = df2.withColumn(col, lit(None).cast("string"))

        df1_aligned = df1.select(*all_cols)
        df2_aligned = df2.select(*all_cols)

    return df1_aligned, df2_aligned

