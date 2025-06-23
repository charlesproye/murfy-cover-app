from logging import getLogger
import argparse
import logging

from pyspark.sql import DataFrame as DF, Window
from pyspark.sql.functions import (
    col, lag, unix_timestamp, when, lit, last, first,
    expr, coalesce, sum as spark_sum, pandas_udf
)
from pyspark.sql.types import DoubleType
from pyspark.sql import SparkSession
from scipy.integrate import cumulative_trapezoid
import pandas as pd
from core.console_utils import main_decorator
from core.constants import KJ_TO_KWH
from core.caching_utils import CachedETLSpark
from core.logging_utils import set_level_of_loggers_with_prefix
from core.spark_utils import safe_astype_spark_with_error_handling, create_spark_session, timedelta_to_interval
from transform.raw_tss.main import get_raw_tss
from transform.fleet_info.main import fleet_info
from transform.processed_tss.config import *


# Here we have implemented the ETL as a class as most raw time series go through the same processing step.
# To have a processing step specific to a data provider/manufacturer, simply implement a subclass of ProcessedTimeSeries and update update_all_tss.
class ProcessedTimeSeries(CachedETLSpark):
    # Declare that the following variable names are not dataframe(parent class) columns
    _metadata = ['make', "logger", "id_col", "max_td"]

    def __init__(self, make:str, id_col:str="vin", log_level:str="INFO", max_td:TD=MAX_TD, force_update:bool=False, spark: SparkSession = None,  **kwargs):
        self.make = make
        logger_name = f"transform.processed_tss.{make}"
        self.logger = getLogger(logger_name)
        set_level_of_loggers_with_prefix(log_level, logger_name)
        self.id_col = id_col
        self.max_td = max_td
        self.spark = spark
        super().__init__(S3_PROCESSED_TSS_KEY_FORMAT.format(make=make), "s3", force_update=force_update, spark=spark, **kwargs)
    
    # No need to call run, it will be called in CachedETL init.
    def run(self):
        #self.logger.info(f"{'Processing ' + self.make + ' raw tss.':=^{50}}")
        tss = get_raw_tss(self.make, spark=self.spark)
        print('load data')
        tss = tss.withColumnsRenamed(RENAME_COLS_DICT)
        tss = safe_astype_spark_with_error_handling(tss)
        tss = self.normalize_units_to_metric(tss)
        tss = tss.orderBy(["vin", "date"])
        tss = self.compute_date_vars(tss)
        tss = self.compute_charge_n_discharge_vars(tss)
        tss = tss.join(self.spark.createDataFrame(fleet_info), 'vin', 'left')
        tss = tss.sort("vin", ascending=True)
        print("process done")
        return tss
    
    def compute_charge_n_discharge_vars(self, tss):
        tss = self.compute_charge_n_discharge_masks(tss, IN_CHARGE_CHARGING_STATUS_VALS, IN_DISCHARGE_CHARGING_STATUS_VALS)
        # Compute the correspding indices to perfrom split-apply-combine ops
        tss = self.compute_idx_from_masks(tss, ["in_charge", "in_discharge"])
        # We recompute the masks by trimming off the points that have the first and last soc values
        # This is done to reduce the noise in the output due to measurments noise.
        tss = self.trim_leading_n_trailing_soc_off_masks(tss, ["in_charge", "in_discharge"]) 
        tss = self.compute_idx_from_masks(tss, ["trimmed_in_charge", "trimmed_in_discharge"])
        tss = self.compute_cum_var(tss, "power", "cum_energy")
        tss = self.compute_cum_var(tss, "charger_power", "cum_charge_energy_added")
        tss = self.compute_status_col(tss)
        return tss
    
    def normalize_units_to_metric(self, tss):
        tss = tss.withColumn("odometer", col("odometer") * 1.609)
        return tss


    def compute_cum_var(self, tss, var_col: str, cum_var_col: str):
        if var_col not in tss.columns:
            self.logger.debug(f"{var_col} not found, not computing {cum_var_col}.")
            return tss

        self.logger.debug(f"Computing {cum_var_col} from {var_col} using Arrow + Pandas UDF.")

        # Schéma de retour attendu → adapte le type si nécessaire
        schema = tss.schema.add(cum_var_col, DoubleType())

        @pandas_udf(schema, functionType="grouped_map")
        def integrate_trapezoid(df: pd.DataFrame) -> pd.DataFrame:

            df = df.sort_values("date").copy()

            x = df["date"].astype('int64') // 10**9  # Convertit ns → s
            y = df[var_col].fillna(0).astype("float64")

            cum = cumulative_trapezoid(y=y.values, x=x.values, initial=0) * KJ_TO_KWH

            # Ajuste pour que ça commence à zéro
            cum = cum - cum[0]

            df[cum_var_col] = cum
            return df

        return tss.groupBy(self.id_col).apply(integrate_trapezoid)

    def compute_date_vars(self, tss: DF) -> DF:
        # Créer une fenêtre par vin, ordonnée par date
        window_spec = Window.partitionBy("vin").orderBy("date")
        
        # Calculer le lag de date (valeur précédente)
        tss = tss.withColumn("prev_date", lag(col("date")).over(window_spec))
        
        # Différence en secondes entre les deux timestamps
        tss = tss.withColumn(
            "sec_time_diff",
            (unix_timestamp(col("date")) - unix_timestamp(col("prev_date"))).cast("double")
        )
        
        return tss

    def compute_charge_n_discharge_masks(self, tss:DF, in_charge_vals:list, in_discharge_vals:list) -> DF:
        """Computes the `in_charge` and `in_discharge` masks either from the charging_status column or from the evolution of the soc over time."""
        self.logger.debug("Computing charging and discharging masks.")
        if self.make in CHARGE_MASK_WITH_CHARGING_STATUS_MAKES:
            return self.charge_n_discharging_masks_from_charging_status(tss, in_charge_vals, in_discharge_vals)
        if self.make in CHARGE_MASK_WITH_SOC_DIFFS_MAKES:
            return self.charge_n_discharging_masks_from_soc_diff(tss)
        raise ValueError(MAKE_NOT_SUPPORTED_ERROR.format(make=self.make))

    def charge_n_discharging_masks_from_soc_diff(self, tss):
        w = Window.partitionBy(self.id_col).orderBy("date").rowsBetween(Window.unboundedPreceding, 0)

        # Forward fill soc
        tss = tss.withColumn("soc_ffilled", last("soc", ignorenulls=True).over(w))

        # Window for diff calculation
        w_diff = Window.partitionBy(self.id_col).orderBy("date")

        soc_prev = lag("soc_ffilled").over(w_diff)
        soc_diff = col("soc_ffilled") - soc_prev

        # Normalisation du signe → {-1, 0, 1}
        soc_sign = when(soc_diff.isNull(), lit(0)).otherwise(soc_diff / abs(soc_diff))

        tss = tss.withColumn("soc_diff", soc_sign)

        # Forward fill and backward fill equivalents
        tss = tss.withColumn("soc_diff_ffill", last("soc_diff", ignorenulls=True).over(w))
        w_rev = Window.partitionBy(self.id_col).orderBy(col("date").desc()).rowsBetween(Window.unboundedPreceding, 0)
        tss = tss.withColumn("soc_diff_bfill", last("soc_diff", ignorenulls=True).over(w_rev))

        # Définition des masques
        tss = tss.withColumn("in_charge", (col("soc_diff_ffill") > 0) & (col("soc_diff_bfill") > 0))
        tss = tss.withColumn("in_discharge", (col("soc_diff_ffill") < 0) & (col("soc_diff_bfill") < 0))

        return tss

    def charge_n_discharging_masks_from_charging_status(self, tss:DF, in_charge_vals:list, in_discharge_vals:list) -> DF:
        self.logger.debug(f"Computing charging and discharging vars using charging status dictionary.")
        assert "charging_status" in tss.columns, NO_CHARGING_STATUS_COL_ERROR
        return (
        tss
        .withColumn("in_charge", col("charging_status").isin(in_charge_vals))
        .withColumn("in_discharge", col("charging_status").isin(in_discharge_vals))
    )

    def trim_leading_n_trailing_soc_off_masks(self, tss:DF, masks:list[str]) -> DF:
        self.logger.debug(f"Computing trimmed masks of{masks}.")
        for mask in masks:
            # Créer une colonne temporaire contenant 'soc' uniquement lorsque le masque est vrai
            tss = tss.withColumn("naned_soc", when(col(mask), col("soc")))
            # Fenêtre pour grouper par 'vin' et l'index associé au masque
            w = Window.partitionBy("vin", col(f"{mask}_idx")).orderBy("date")  # assuming you have a 'timestamp' for ordering
            # Calcul des premières et dernières valeurs non nulles de 'naned_soc' dans chaque groupe
            trailing_soc = first("naned_soc", ignorenulls=True).over(w)
            leading_soc = last("naned_soc", ignorenulls=True).over(w)
            # Ajouter ces colonnes
            tss = (
                tss
                .withColumn("trailing_soc", trailing_soc)
                .withColumn("leading_soc", leading_soc)
                .withColumn(
                    f"trimmed_{mask}",
                    (col(mask)) & (col("soc") != col("trailing_soc")) & (col("soc") != col("leading_soc"))
                )
                .drop("naned_soc")
            )

        return tss
    
    
    def compute_idx_from_masks(self, tss, masks: list[str]):
        """
        Spark version of compute_idx_from_masks.
        
        Args:
            tss (DataFrame): Spark DataFrame.
            masks (list): List of boolean column names to compute idx on.
        
        Returns:
            DataFrame: Transformed Spark DataFrame.
        """
        self.logger.info(f"Computing {masks} idx from masks.")
        
        for mask in masks:
            idx_col_name = f"{mask}_idx"

            w = Window.partitionBy(self.id_col).orderBy("date")

            # Décalage de mask par groupe
            shifted_mask = lag(col(mask), 1).over(w)

            # new_period_start_mask = shifted_mask != mask
            new_period_start_mask = (shifted_mask.isNull() | (shifted_mask != col(mask)))

            # Si max_td est défini, on ajoute aussi condition sur time_diff
            if self.max_td is not None:
                new_period_start_mask = new_period_start_mask | (col("sec_time_diff") > lit(timedelta_to_interval(self.max_td)))

            # Génère l'index via cumul
            tss = tss.withColumn(
                "new_period_start_mask", when(new_period_start_mask, lit(1)).otherwise(lit(0))
            )

            tss = tss.withColumn(
                idx_col_name, spark_sum("new_period_start_mask").over(w)
            ).drop("new_period_start_mask")

        return tss

    def compute_status_col(self, tss):
        self.logger.debug("Computing status column.")

        # Fenêtre ordonnée par date pour chaque VIN
        w = Window.partitionBy("vin").orderBy("date")

        # Décalage pour calculer diff(odometer)
        prev_odo = lag("odometer").over(w)
        delta_odo = col("odometer") - prev_odo

        # Première base de status
        status = when(col("in_charge") == True, lit("charging")) \
                .when(col("in_charge") == False, lit("discharging")) \
                .otherwise(lit("unknown"))

        # Raffinement → si in_charge == False → "moving" ou "idle_discharging"
        status = when(col("in_charge") == True, lit("charging")) \
                .when(col("in_charge") == False, 
                    when(delta_odo > 0, lit("moving"))
                    .otherwise(lit("idle_discharging"))
                    ) \
                .otherwise(lit("unknown"))

        return tss.withColumn("status", status)

    @classmethod
    def update_all_tss(cls, spark, **kwargs):
        for make in ALL_MAKES:
            if make in ["tesla", "tesla-fleet-telemery"]:
                print('ici')
                cls = TeslaProcessedTimeSeries(spark)
            else:
                cls = ProcessedTimeSeries(spark)
            cls(make, force_update=True, **kwargs)

            
class TeslaProcessedTimeSeries(ProcessedTimeSeries):
    def __init__(self, make:str="tesla-fleet-telemery", id_col:str="vin", log_level:str="INFO", max_td:TD=MAX_TD, force_update:bool=False, spark=None, **kwargs):
        self.logger = getLogger(make)
        set_level_of_loggers_with_prefix(log_level, make)
        super().__init__(make, id_col, log_level, max_td, force_update, spark=spark, **kwargs)

        
    def compute_charge_n_discharge_vars(self, tss:DF) -> DF:
        tss = self.compute_charge_n_discharge_masks(tss, IN_CHARGE_CHARGING_STATUS_VALS, IN_DISCHARGE_CHARGING_STATUS_VALS)
        tss = self.compute_charge_idx_bis(tss)
        return tss

    def compute_charge_n_discharge_masks(self, tss:DF, in_charge_vals:list, in_discharge_vals:list) -> DF:
        """Computes the `in_charge` and `in_discharge` masks either from the charging_status column or from the evolution of the soc over time."""
        if self.make in CHARGE_MASK_WITH_CHARGING_STATUS_MAKES:
            return self.charge_n_discharging_masks_from_charging_status(tss, in_charge_vals, in_discharge_vals)

    def charge_n_discharging_masks_from_charging_status(self, tss: DF, in_charge_vals: list, in_discharge_vals: list) -> DF:
        assert "charging_status" in tss.columns, NO_CHARGING_STATUS_COL_ERROR
        
        # Masques booléens Spark
        tss = tss.withColumn(
            "in_charge",
            when(col("charging_status").isin(in_charge_vals), lit(True)).otherwise(lit(False))
        )

        tss = tss.withColumn(
            "in_discharge",
            when(col("charging_status").isin(in_discharge_vals), lit(True)).otherwise(lit(False))
        )
    
        return tss

    def compute_energy_added(self, tss: DF) -> DF:
        tss = tss.withColumn(
            "charge_energy_added",
            when(
                col("dc_charge_energy_added").isNotNull() & (col("dc_charge_energy_added") > 0),
                col("dc_charge_energy_added")
            ).otherwise(col("ac_charge_energy_added"))
        )
        return tss

    
    def compute_charge_idx_bis(self, tss: DF) -> DF:
        
        tss = self.compute_energy_added(tss)
        
        # 1. Filtrer les lignes où soc n'est pas null
        tss_na = tss.filter(col("soc").isNotNull())

        # 2. Créer une fenêtre ordonnée par date par VIN
        vin_window = Window.partitionBy("vin").orderBy("date")

        # 3. Calcul des différences
        tss_na = tss_na \
            .withColumn("soc_diff", col("soc") - lag("soc", 1).over(vin_window)) \
            .withColumn("trend", when(col("soc_diff") > 0, lit(1))
                                .when(col("soc_diff") < 0, lit(-1))
                                .otherwise(lit(0))) \
            .withColumn("prev_trend", lag("trend", 1).over(vin_window)) \
            .withColumn("prev_prev_trend", lag("trend", 2).over(vin_window)) \
            .withColumn("prev_date", lag("date", 1).over(vin_window)) \
            .withColumn("time_diff_min", 
                        (unix_timestamp(col("date")) - unix_timestamp(col("prev_date"))) / 60) \
            .withColumn("time_gap", col("time_diff_min") > 60) \
            .withColumn("trend_change",
                        when(
                            ((col("trend") != col("prev_trend")) & 
                            (col("prev_trend") == col("prev_prev_trend"))) | 
                            col("time_gap"), 
                            lit(1)
                        ).otherwise(lit(0)))

        # 4. Initialiser les premières lignes à 0
        tss_na = tss_na.withColumn(
            "trend_change",
            when(col("date") == lag("date", 1).over(vin_window), lit(0)).otherwise(col("trend_change"))
        )

        # 5. Cumulative sum (session index)
        tss_na = tss_na.withColumn("in_charge_idx", spark_sum("trend_change").over(vin_window.rowsBetween(Window.unboundedPreceding, 0)))

        # 6. Join avec le DataFrame original
        tss = tss.join(
            tss_na.select("vin", "date", "soc", "soc_diff", "in_charge_idx"),
            on=["vin", "date", "soc"],
            how="left"
        )

        # 7. Forward-fill `odometer` et `in_charge_idx` (non-natif en Spark, mais on peut approximer)
        fill_window = Window.partitionBy("vin").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
        tss = tss \
            .withColumn("odometer", coalesce(col("odometer"), expr("last(odometer, true)").over(fill_window))) \
            .withColumn("in_charge_idx", coalesce(col("in_charge_idx"), expr("last(in_charge_idx, true)").over(fill_window)))

        return tss
    
    

        
@main_decorator
def main():
    parser = argparse.ArgumentParser(description="Process time series data.")
    parser.add_argument('--log_level', type=str, default='INFO', help='Set the logging level (default: INFO)')
    args = parser.parse_args()

    log_level = getattr(logging, args.log_level.upper(), logging.INFO)

    ProcessedTimeSeries.update_all_tss(log_level=log_level)

if __name__ == "__main__":
    main()

