from pyspark.sql import functions as F


def tesla_soh(df):
    required_col = {"energy_added", "soc_diff", "net_capacity"}
    missing_col = required_col  - set(df.columns)
    assert not missing_col, f"Missing columns in DataFrame: {missing_col}"
    return df.withColumn(
        "soh", F.col("energy_added") / (F.col("soc_diff") / 100.0 * F.col("net_capacity"))
    )
    
