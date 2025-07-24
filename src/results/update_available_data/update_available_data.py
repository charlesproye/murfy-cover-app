from core.sql_utils import get_sqlalchemy_engine
from core.pandas_utils import DF
import pandas as pd
from core.spark_utils import create_spark_session
from core.s3.s3_utils import S3Service
from core.s3.settings import S3Settings
from config import MAKES, TABLE_QUERY
from core.console_utils import main_decorator
from sqlalchemy.engine import Engine
from pyspark.sql import functions as F
from sqlalchemy import text
import logging





def get_fleet_info(con: Engine) -> DF:
    fleet_info:DF = (
        pd.read_sql_query(TABLE_QUERY, con)
    )

    fleet_info_spark = spark.createDataFrame(fleet_info)

    return fleet_info_spark

def get_presence_per_type(tss: DF, fleet_info_spark: DF, con: Engine, column_to_check: str, column_to_update: str):

    if '.' in column_to_check:
        tss = tss.withColumnsRenamed({column_to_check: column_to_check.replace('.', '_')})
        column_to_check = column_to_check.replace('.', '_')

    merged_df = fleet_info_spark.join(tss, "vin", "left")

    presence_check_df = (
        merged_df
        .groupBy("model_name", "type")
        .agg(
            F.count(F.when(F.col(f"{column_to_check}").isNotNull(), 1)).alias(f"non_null_{column_to_check}"),
        )
        .orderBy("model_name", "type")
    ).filter(F.col(f"non_null_{column_to_check}") > 0)

    df_pd = presence_check_df.coalesce(32).toPandas()

    condition = ""
    for model, version in zip(df_pd['model_name'], df_pd['type']): 
        condition += f"('{model}', '{version}'),"
    
    print('Before query update...')
    query = f"""
    UPDATE vehicle_model
    SET {column_to_update} = TRUE
    WHERE (model_name, type) IN (
    {condition[:-1]}
    );
    """

    with con.begin() as connection:
        print(query)
        connection.execute(text(query)) 
        connection.close()

    return query

@main_decorator
def run(spark, s3: S3Service, con: Engine):
    fleet_info = get_fleet_info(con)
    for make, dic_cols in MAKES.items():
        tss = s3.read_parquet_df_spark(spark, f"raw_ts/{make}/time_series/raw_ts_spark.parquet")
        for column_to_update, column_to_check in dic_cols.items():
            get_presence_per_type(tss, fleet_info, con, column_to_check, column_to_update)




if __name__ == "__main__":
    con = get_sqlalchemy_engine()

    s3 = S3Service()

    spark = create_spark_session(
        S3Settings().S3_KEY,
        S3Settings().S3_SECRET
    )

    run(spark, s3, con)





