from pyspark.sql import SparkSession
from core.pandas_utils import DF
from core.pandas_utils import parse_unstructured_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType

LIST_COL_TO_DROP = ["model"]


def parse_high_mobility(response, spark, vin: str):
    """
    Parse dict from High Mobility api response

    Args:
        response (dict): Contains data to parse
        spark (SparkSession): spark session active
        vin (str): Vehicle identification number

    Returns:
        spark.DataFrame: Data with every columns
    """
    
    flattened_response = {}

    for capability, variables in response.items():
        if not isinstance(variables, dict):
            continue
        for variable, elements in variables.items():
            for element in elements:
                timestamp = element["timestamp"]
                variable_name = capability + "." + variable

                if isinstance(element["data"], dict):
                    if not "value" in element["data"]:
                        continue
                    value = element["data"]["value"]
                    if "unit" in element:
                        variable_name += "." + element["unit"]
                else:
                    value = element["data"]

                if isinstance(value, (int, float)):
                    value = float(value)

                flattened_response[timestamp] = flattened_response.get(
                    timestamp, {}
                ) | {variable_name: value}

    data_list = []
    for timestamp, variables in flattened_response.items():
        row = {"timestamp": timestamp, "vin": vin}
        row.update(variables)
        data_list.append(row)

    raw_ts = spark.createDataFrame(data_list)
    return raw_ts


def parse_fleet_telemetry(response: dict, spark: SparkSession):
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
                row_data[key] = value
                value = str(value)
                row_data[key] = value
        flattened_data.append({**base, **row_data})
        # schema = StructType([StructField('vin', StringType(), True), StructField('timestamp', StringType(), True), StructField('readable_date', StringType(), True), StructField('PackVoltage_stringValue', StringType(), True), StructField('PackCurrent_stringValue', StringType(), True), StructField('LifetimeEnergyUsed_stringValue', StringType(), True), StructField('IdealBatteryRange_stringValue', StringType(), True), StructField('BatteryLevel_stringValue', StringType(), True), StructField('Soc_stringValue', StringType(), True), StructField('EnergyRemaining_stringValue', StringType(), True), StructField('IsolationResistance_stringValue', StringType(), True), StructField('BrickVoltageMax_stringValue', StringType(), True), StructField('BrickVoltageMin_stringValue', StringType(), True), StructField('InsideTemp_stringValue', StringType(), True), StructField('EstBatteryRange_stringValue', StringType(), True), StructField('RatedRange_stringValue', StringType(), True), StructField('ModuleTempMax_stringValue', StringType(), True), StructField('ModuleTempMin_stringValue', StringType(), True), StructField('OutsideTemp_stringValue', StringType(), True), StructField('SentryMode_stringValue', StringType(), True), StructField('HvacPower_hvacPowerValue', StringType(), True), StructField('BMSState_stringValue', StringType(), True), StructField('VehicleSpeed_stringValue', StringType(), True), StructField('Odometer_stringValue', StringType(), True), StructField('BatteryHeaterOn_stringValue', StringType(), True), StructField('DCChargingEnergyIn_stringValue', StringType(), True), StructField('ChargingCableType_cableTypeValue', StringType(), True), StructField('DetailedChargeState_detailedChargeStateValue', StringType(), True), StructField('ChargeState_stringValue', StringType(), True), StructField('EstimatedHoursToChargeTermination_doubleValue', DoubleType(), True), StructField('FastChargerPresent_stringValue', StringType(), True), StructField('FastChargerType_fastChargerValue', StringType(), True), StructField('ChargeRateMilePerHour_doubleValue', DoubleType(), True), StructField('DCChargingPower_stringValue', StringType(), True), StructField('ChargeLimitSoc_stringValue', StringType(), True), StructField('HvacACEnabled_booleanValue', BooleanType(), True), StructField('HvacAutoMode_hvacAutoModeValue', StringType(), True), StructField('HvacFanSpeed_intValue', DoubleType(), True), StructField('ChargingCableType_invalid', BooleanType(), True), StructField('FastChargerType_invalid', BooleanType(), True), StructField('BmsFullchargecomplete_stringValue', StringType(), True), StructField('EstimatedHoursToChargeTermination_invalid', BooleanType(), True), StructField('DCDCEnable_stringValue', StringType(), True), StructField('VehicleSpeed_invalid', BooleanType(), True), StructField('ACChargingPower_invalid', BooleanType(), True), StructField('ChargeState_invalid', BooleanType(), True), StructField('ChargePortColdWeatherMode_invalid', BooleanType(), True), StructField('FastChargerPresent_invalid', BooleanType(), True), StructField('ChargeAmps_invalid', BooleanType(), True), StructField('ChargerVoltage_invalid', BooleanType(), True), StructField('ChargerPhases_invalid', BooleanType(), True), StructField('DefrostForPreconditioning_booleanValue', BooleanType(), True), StructField('ACChargingEnergyIn_stringValue', StringType(), True), StructField('ChargeEnableRequest_stringValue', StringType(), True), StructField('ChargeCurrentRequestMax_stringValue', StringType(), True), StructField('EuropeVehicle_booleanValue', BooleanType(), True), StructField('RearDefrostEnabled_booleanValue', BooleanType(), True), StructField('EfficiencyPackage_stringValue', StringType(), True), StructField('DCDCEnable_invalid', BooleanType(), True), StructField('DefrostMode_defrostModeValue', StringType(), True), StructField('PreconditioningEnabled_stringValue', StringType(), True), StructField('CarType_stringValue', StringType(), True), StructField('ClimateKeeperMode_climateKeeperModeValue', StringType(), True), StructField('ChargePort_stringValue', StringType(), True), StructField('ChargeCurrentRequest_stringValue', StringType(), True), StructField('ChargerVoltage_doubleValue', DoubleType(), True), StructField('ACChargingPower_stringValue', StringType(), True), StructField('ChargePortColdWeatherMode_stringValue', StringType(), True), StructField('ChargeAmps_stringValue', StringType(), True), StructField('BatteryHeaterOn_invalid', BooleanType(), True), StructField('ChargerPhases_stringValue', StringType(), True), StructField('DetailedChargeState_invalid', BooleanType(), True), StructField('ModuleTempMax_invalid', BooleanType(), True), StructField('BMSState_invalid', BooleanType(), True), StructField('HvacPower_invalid', BooleanType(), True), StructField('ModuleTempMin_invalid', BooleanType(), True), StructField('LifetimeEnergyUsed_invalid', BooleanType(), True), StructField('PackCurrent_invalid', BooleanType(), True), StructField('PackVoltage_invalid', BooleanType(), True), StructField('IsolationResistance_invalid', BooleanType(), True), StructField('InsideTemp_invalid', BooleanType(), True), StructField('OutsideTemp_invalid', BooleanType(), True), StructField('ChargeCurrentRequestMax_invalid', BooleanType(), True), StructField('HvacFanSpeed_invalid', BooleanType(), True), StructField('IdealBatteryRange_invalid', BooleanType(), True), StructField('Soc_invalid', BooleanType(), True), StructField('BrickVoltageMin_invalid', BooleanType(), True), StructField('BmsFullchargecomplete_invalid', BooleanType(), True), StructField('RatedRange_invalid', BooleanType(), True), StructField('BrickVoltageMax_invalid', BooleanType(), True), StructField('HvacACEnabled_invalid', BooleanType(), True), StructField('HvacAutoMode_invalid', BooleanType(), True), StructField('RearDefrostEnabled_invalid', BooleanType(), True), StructField('EstBatteryRange_invalid', BooleanType(), True), StructField('BatteryLevel_invalid', BooleanType(), True), StructField('ChargeCurrentRequest_invalid', BooleanType(), True), StructField('DCChargingEnergyIn_invalid', BooleanType(), True), StructField('Odometer_invalid', BooleanType(), True), StructField('ACChargingEnergyIn_invalid', BooleanType(), True), StructField('DCChargingPower_invalid', BooleanType(), True), StructField('__index_level_0__', LongType(), True)])
        # df_pandas = DF(flattened_data)
    df_spark = spark.createDataFrame(flattened_data)

    return df_spark


def parse_bmw(response: dict, spark):
    """
    Parse dict from BMW api response

    Args:
        response (dict): Contains data to parse
        spark (SparkSession): spark session active

    Returns:
        spark.DataFrame: Data with every columns
    """

    parsed_data = []
    
    for item in response["data"]:
        vin = item["vin"]
        
        # Grouper par date_of_value
        date_groups = {}
        
        for key_value in item["pushKeyValues"]:
            date = key_value["date_of_value"]
            key = key_value["key"]
            value = key_value["value"]
            
            if date not in date_groups:
                date_groups[date] = {"vin": vin, "date_of_value": date}
            
            date_groups[date][key] = value
        
        # Ajouter chaque groupe de date
        parsed_data.extend(date_groups.values())

    
    return spark.createDataFrame(parsed_data)


def parse_mobilisight(response: dict, spark: SparkSession):
    """
    Parse dict from Mobilisight api response

    Args:
        response (dict): Contains data to parse
        spark (SparkSession): spark session active

    Returns:
        spark.DataFrame: Data with every columns
    """

    df = (
        parse_unstructured_json(
            response, no_prefix_path=["datetime"], no_suffix_path=["value"]
        )
        .assign(vin=response["vin"])
        .astype(str)
        .rename(columns={"datetime": "readable_date"})
    )

    print(df.shape)

    return spark.createDataFrame(df)

