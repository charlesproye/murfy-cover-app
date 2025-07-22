from pyspark.sql import DataFrame as DF
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

from transform.processed_tss.raw_ts_to_processed_ts import RawTsToProcessedTs


class TeslaFTRawTsToProcessedTs(RawTsToProcessedTs):
    """
    Compute the specific features for the Tesla Fleet Telemetry data.
    """

    def __init__(
        self,
        make: str,
        id_col: str = "vin",
        log_level: str = "INFO",
        force_update: bool = False,
        spark: SparkSession = None,
        **kwargs,
    ):

        super().__init__(
            make=make,
            id_col=id_col,
            log_level=log_level,
            force_update=force_update,
            spark=spark,
            **kwargs,
        )

    def compute_specific_features(self, tss: DF) -> DF:
        tss = tss.withColumn(
            "charge_energy_added",
            when(
                col("dc_charge_energy_added").isNotNull()
                & (col("dc_charge_energy_added") > 0),
                col("dc_charge_energy_added"),
            ).otherwise(col("ac_charge_energy_added")),
        )
        return tss

    # Pertinent pour Tesla
    # def compute_cum_var(self, tss, var_col: str, cum_var_col: str):
    #     if var_col not in tss.columns:
    #         self.logger.debug(f"{var_col} not found, not computing {cum_var_col}.")
    #         return tss

    #     self.logger.debug(
    #         f"Computing {cum_var_col} from {var_col} using Arrow + Pandas UDF."
    #     )

    #     # Schéma de retour attendu → adapte le type si nécessaire
    #     schema = tss.schema.add(cum_var_col, DoubleType())

    #     @pandas_udf(schema, functionType="grouped_map")
    #     def integrate_trapezoid(df: pd.DataFrame) -> pd.DataFrame:

    #         df = df.sort_values("date").copy()

    #         x = df["date"].astype("int64") // 10**9  # Convertit ns → s
    #         y = df[var_col].fillna(0).astype("float64")

    #         cum = cumulative_trapezoid(y=y.values, x=x.values, initial=0) * KJ_TO_KWH

    #         # Ajuste pour que ça commence à zéro
    #         cum = cum - cum[0]

    #         df[cum_var_col] = cum
    #         return df

    #     return tss.groupBy(self.id_col).apply(integrate_trapezoid)

