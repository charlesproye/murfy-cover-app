# Spark utils function

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    TimestampType,
)

LIST_COL_TO_DROP = ["model"]


def create_spark_session(access_key: str, secret_key: str) -> SparkSession:
    """
    Create a session spark with a connexion to scaleway
    """
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--packages org.apache.hadoop:hadoop-aws:3.4.1,org.apache.spark:spark-hadoop-cloud_2.13:4.0.1 pyspark-shell"
    )

    spark = (
        SparkSession.builder.appName("Scaleway S3 Read JSON")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.1,org.apache.spark:spark-hadoop-cloud_2.13:4.0.1",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "https://s3.fr-par.scw.cloud")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # S3A optimization configurations
        .config("spark.hadoop.fs.s3a.experimental.input.fadvise", "normal")
        .config("spark.hadoop.fs.s3a.connection.maximum", "1000")
        .config("spark.hadoop.fs.s3a.threads.max", "20")
        .config("spark.hadoop.fs.s3a.threads.core", "10")
        .config("spark.hadoop.fs.s3a.buffer.dir", "/tmp")
        .config("spark.hadoop.fs.s3a.block.size", "134217728")  # 128MB
        .config("spark.hadoop.fs.s3a.multipart.size", "134217728")  # 128MB
        .config("spark.hadoop.fs.s3a.multipart.threshold", "134217728")  # 128MB
        # Spark SQL adaptive configurations
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.default.parallelism", "200")
        # Network configurations
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.port", "0")
        .config("spark.blockManager.port", "0")
        .getOrCreate()
    )

    return spark


def create_spark_session_k8s(
    s3_key: str, s3_secret: str, endpoint: str = "https://s3.fr-par.scw.cloud"
) -> SparkSession:
    """
    Configure le SparkSession existant dans Kubernetes pour accéder à S3.

    Args:
        s3_key (str): Clé d'accès S3.
        s3_secret (str): Secret S3.
        endpoint (str, optional): Endpoint S3. Defaults to Scaleway S3.

    Returns:
        SparkSession: La session Spark configurée.
    """
    spark = SparkSession.builder.getOrCreate()
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    hadoop_conf.set("fs.s3a.access.key", s3_key)
    hadoop_conf.set("fs.s3a.secret.key", s3_secret)
    hadoop_conf.set("fs.s3a.endpoint", endpoint)
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )

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
                value = next(
                    iter(value_dict.values())
                )  # Récupère la valeur quel que soit le type
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
        "OutsideTemp": FloatType(),
        "PackCurrent": FloatType(),
        "PackVoltage": FloatType(),
        "RatedRange": FloatType(),
        "RearDefrostEnabled": FloatType(),
        "VehicleSpeed": FloatType(),
        "ChargerVoltage": FloatType(),
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


def get_optimal_nb_partitions(file_size_bytes: float, nb_vin: int) -> int:
    """
    Calcule le nombre idéal de partitions Spark basé sur la taille du fichier et le nombre de VINs.

    Cette fonction détermine le nombre optimal de partitions pour optimiser les performances
    Spark en fonction de la taille moyenne par VIN et de la recommandation de 128MB par partition.

    Args:
        file_size_bytes (float): Taille totale du fichier en octets
        nb_vin (int): Nombre de VINs (véhicules) dans le fichier

    Returns:
        int: Nombre idéal de partitions Spark

    Raises:
        ValueError: Si file_size_bytes ou nb_vin sont négatifs ou nuls
    """
    # Validation des paramètres
    if file_size_bytes <= 0 or nb_vin <= 0:
        raise ValueError("file_size_bytes et nb_vin doivent être positifs")

    # Calcul de la taille moyenne par VIN
    size_file_mb = file_size_bytes / (1024 * 1024)
    print(f"📁 Taille du fichier: {size_file_mb:.2f} MB")

    avg_size_file_vin_mb = size_file_mb / nb_vin

    # Calcul du nombre idéal de VINs par partition (basé sur 128MB recommandé)
    nb_vin_ideal_size = 128 / avg_size_file_vin_mb

    # Logique de décision
    if nb_vin_ideal_size < 0.5:
        return nb_vin
    if nb_vin_ideal_size < 1:
        return nb_vin

    optimal_partitions = int(nb_vin / nb_vin_ideal_size)
    if int(optimal_partitions) == 0:
        return 1
    if optimal_partitions % 2 == 0:
        pass
    else:
        optimal_partitions += 1
    return optimal_partitions


def weighted_mean_spark(df, group_col: str, value_col: str, weight_col: str):
    """
    Calculates weighted average by group with Spark.

    Args:
        df: Source Spark DataFrame
        group_col: Grouping column
        value_col: Value column to average
        weight_col: Weight column

    Returns:
        Spark DataFrame with weighted averages
    """
    # Calculate weighted sum and sum of weights by group
    weighted_summary = df.groupBy(group_col).agg(
        spark_sum(col(value_col) * col(weight_col)).alias("weighted_sum"),
        spark_sum(col(weight_col)).alias("weight_sum"),
    )

    # Calculate weighted average
    weighted_avg = weighted_summary.withColumn(
        f"weighted_avg_{value_col}",
        when(col("weight_sum") > 0, col("weighted_sum") / col("weight_sum")).otherwise(
            lit(None)
        ),
    ).select(group_col, f"weighted_avg_{value_col}")

    return weighted_avg


# TODO: Check again the relevance of this method as it was vibe coded
def get_spark_available_cores(spark_session: SparkSession, logger=None) -> int:
    """
    Get the number of available cores for the Spark cluster.

    This function tries multiple approaches to determine available cores:
    1. Use environment variable NB_CORES_CLUSTER if set
    2. Calculate from Spark context configuration
    3. Fall back to a reasonable default

    Args:
        spark_session: The active SparkSession
        logger: Optional logger for informational messages

    Returns:
        int: Number of available cores
    """
    import os

    # Method 1: Check environment variable (backwards compatibility)
    env_cores = os.environ.get("NB_CORES_CLUSTER")
    if env_cores:
        try:
            return int(env_cores)
        except ValueError:
            if logger:
                logger.warning(f"Invalid NB_CORES_CLUSTER value: {env_cores}")

    # Method 2: Calculate from Spark context
    if spark_session:
        try:
            # Get executor instances and cores from Spark configuration
            spark_conf = spark_session.sparkContext.getConf()

            # Try to get executor instances and cores from configuration
            executor_instances = spark_conf.get("spark.executor.instances")
            executor_cores = spark_conf.get("spark.executor.cores")
            driver_cores = spark_conf.get("spark.driver.cores", "1")

            if executor_instances and executor_cores:
                total_cores = int(driver_cores) + (
                    int(executor_instances) * int(executor_cores)
                )
                if logger:
                    logger.info(
                        f"Calculated cores from Spark config: {total_cores} "
                        f"(driver: {driver_cores}, executors: {executor_instances}x{executor_cores})"
                    )
                return total_cores

            # Alternative: get default parallelism which is often cores * 2
            default_parallelism = spark_session.sparkContext.defaultParallelism
            if default_parallelism > 0:
                estimated_cores = max(1, default_parallelism // 2)
                if logger:
                    logger.info(
                        f"Estimated cores from default parallelism: {estimated_cores} "
                        f"(parallelism: {default_parallelism})"
                    )
                return estimated_cores

        except Exception as e:
            if logger:
                logger.warning(f"Failed to get cores from Spark context: {e}")

    # Method 3: Fallback to reasonable default
    default_cores = 4
    if logger:
        logger.warning(
            f"Using default cores: {default_cores} "
            f"(set NB_CORES_CLUSTER environment variable for better performance)"
        )
    return default_cores

