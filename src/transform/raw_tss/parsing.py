from pyspark.sql import SparkSession
from core.pandas_utils import DF
from core.pandas_utils import parse_unstructured_json

LIST_COL_TO_DROP = ["model"]


def parse_high_mobility(response, spark):
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
        row = {"timestamp": timestamp}
        row.update(variables)
        data_list.append(row)

    raw_ts = spark.createDataFrame(data_list).coalesce(1)
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
    df_spark = spark.createDataFrame(flattened_data).coalesce(1)

    return df_spark


def parse_bmw(response: dict, spark: SparkSession):
    pass


#     # Parsing commenté car le format des responses BMW va changer, à modifier à ce moment là
#     """
#     Parse dict from BMW api response

#     Args:
#         response (dict): Contains data to parse
#         spark (SparkSession): spark session active

#     Returns:
#         spark.DataFrame: Data with every columns
#     """
#     print(response)
#     df = (
#         DF.from_dict(response["data"])
#         .drop(columns=["unit", "info"])
#         .eval("date_of_value = date_of_value.ffill().bfill()")
#         .drop_duplicates(subset=["date_of_value", "key"])
#         .pivot(index="date_of_value", columns="key", values="value")
#         .reset_index(drop=False)
#         .rename(columns={"date_of_value": "readable_date"})
#     )

#     print(df)

#     return spark.createDataFrame(df).coalesce(1)


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

    return spark.createDataFrame(df).coalesce(1)

