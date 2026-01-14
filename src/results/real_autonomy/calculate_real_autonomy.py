import logging

import numpy as np
import pandas as pd
from sqlalchemy import text

from core.s3.s3_utils import S3Service
from core.s3.settings import S3Settings
from core.sql_utils import get_connection, get_sqlalchemy_engine

# Logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class RealAutonomyCalculator:
    """Calculates the real autonomy of electric vehicles."""

    def __init__(
        self,
        min_odometer_followed: float = 500,
        min_soc_diff: float = 5,
        min_odometer_diff: float = 5,
        wltp_ratio_max: float = 1.3,
        max_ratio_km_soc: float = 10,
    ):
        """
        Initializes the real autonomy calculator.

        Args:
            min_odometer_followed: Minimum tracked distance for a VIN (km)
            min_soc_diff: Minimum SOC difference for a discharge (%)
            min_odometer_diff: Minimum distance for a discharge (km)
            wltp_ratio_max: Maximum ratio compared to WLTP autonomy
            max_ratio_km_soc: Maximum acceptable km/SOC ratio
        """
        self.min_odometer_followed = min_odometer_followed
        self.min_soc_diff = min_soc_diff
        self.min_odometer_diff = min_odometer_diff
        self.wltp_ratio_max = wltp_ratio_max
        self.max_ratio_km_soc = max_ratio_km_soc

        # Connection initialization
        self.s3_settings = S3Settings()
        self.s3 = S3Service()
        self.engine = get_sqlalchemy_engine()

        logger.info("RealAutonomyCalculator initialized with parameters:")
        logger.info(f"  - min_odometer_followed: {min_odometer_followed} km")
        logger.info(f"  - min_soc_diff: {min_soc_diff} %")
        logger.info(f"  - min_odometer_diff: {min_odometer_diff} km")
        logger.info(f"  - wltp_ratio_max: {wltp_ratio_max}")
        logger.info(f"  - max_ratio_km_soc: {max_ratio_km_soc}")

    def load_vehicle_data_from_db(self) -> pd.DataFrame:
        query = text("""
            SELECT 
                v.vin,
                vm.autonomy,
                MIN(vd.soh) as soh,
                vm.id as model_id
            FROM vehicle_data vd
            JOIN vehicle v ON vd.vehicle_id = v.id
            JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
            GROUP BY v.vin, vm.id, vm.autonomy
        """)

        with self.engine.connect() as con:
            df = pd.read_sql(query, con)

        logger.info(f"Loaded {len(df)} vehicles from database")
        return df

    def load_result_phases(self, oem: str) -> pd.DataFrame:
        logger.info(f"Loading phases from S3 for {oem}...")

        path = f"result_phases/result_phases_{oem}.parquet"
        df = self.s3.read_parquet_df(path)
        return df

    def filter_valid_vins(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filters VINs with sufficient tracked mileage.

        Args:
            df: Pandas DataFrame with phases

        Returns:
            Filtered DataFrame
        """
        logger.info(
            f"Filtering VINs with minimum {self.min_odometer_followed} km tracked..."
        )

        vin_stats = (
            df.groupby("VIN")
            .agg(
                odometer_start=("ODOMETER_FIRST", "min"),
                odometer_end=("ODOMETER_LAST", "max"),
            )
            .reset_index()
        )

        vin_stats["odometer_diff"] = (
            vin_stats["odometer_end"] - vin_stats["odometer_start"]
        )

        valid_vins = vin_stats[vin_stats["odometer_diff"] > self.min_odometer_followed][
            "VIN"
        ]

        df_filtered = df[df["VIN"].isin(valid_vins)].copy()

        initial_count = df["VIN"].nunique()
        filtered_count = df_filtered["VIN"].nunique()
        logger.info(f"VINs retained: {filtered_count}/{initial_count}")

        return df_filtered

    def prepare_dataframe(
        self, df: pd.DataFrame, vehicle_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Prepares the DataFrame for analysis by merging phases and DB data.

        Args:
            df: Pandas DataFrame with filtered phases
            vehicle_df: Pandas DataFrame with vehicle data

        Returns:
            Prepared pandas DataFrame
        """
        df_pd = (
            df[df["SOC_DIFF"] < 0][
                [
                    "VIN",
                    "BATTERY_NET_CAPACITY",
                    "SOC_DIFF",
                    "ODOMETER_DIFF",
                    "RATIO_KM_SOC",
                    "ODOMETER_FIRST",
                    "ODOMETER_LAST",
                    "SOC_FIRST",
                    "SOC_LAST",
                ]
            ]
            .dropna(subset=["RATIO_KM_SOC"])
            .copy()
        )

        df_merged = df_pd.merge(
            vehicle_df[["vin", "autonomy", "soh", "model_id"]].dropna(
                subset=["model_id"]
            ),
            left_on="VIN",
            right_on="vin",
            how="left",
        )

        return df_merged

    def apply_quality_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies quality filters on discharges.

        Args:
            df: Pandas DataFrame with discharges

        Returns:
            Filtered DataFrame
        """
        logger.info("Applying quality filters...")

        # Filter 1: Minimum SOC_DIFF
        mask_soc = df["SOC_DIFF"] < -self.min_soc_diff

        # Filter 2: Minimum ODOMETER_DIFF
        mask_odo_min = df["ODOMETER_DIFF"] > self.min_odometer_diff

        # Filter 3: ODOMETER_DIFF not too high compared to WLTP
        mask_odo_max = df["autonomy"].isna() | (
            df["ODOMETER_DIFF"] < df["autonomy"] * self.wltp_ratio_max
        )

        # Filter 4: Reasonable RATIO_KM_SOC
        df_filtered = df[mask_soc & mask_odo_min & mask_odo_max].copy()
        df_filtered = df_filtered[df_filtered["RATIO_KM_SOC"] < self.max_ratio_km_soc]

        logger.info(f"  - SOC filter > {self.min_soc_diff}%: {mask_soc.sum()}")
        logger.info(
            f"  - Min distance filter {self.min_odometer_diff} km: {mask_odo_min.sum()}"
        )
        logger.info(
            f"  - WLTP ratio filter < {self.wltp_ratio_max}: {mask_odo_max.sum()}"
        )
        logger.info(
            f"  - km/SOC ratio filter < {self.max_ratio_km_soc}: {len(df_filtered)}"
        )

        return df_filtered

    def calculate_real_autonomy(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates real autonomy and autonomy normalized to 100% SOH.

        Args:
            df: Pandas DataFrame with filtered discharges

        Returns:
            DataFrame with calculated autonomies
        """
        logger.info("Calculating real autonomies...")

        df = df.copy()

        # Real autonomy observed (km for 100% SOC)
        df["REAL_AUTONOMY"] = df["RATIO_KM_SOC"] * 100

        # Autonomy normalized to 100% SOH
        # If SOH is missing or 0, we consider it to be 100%
        soh_normalized = df["soh"].replace(0, np.nan).fillna(1.0)
        df["real_autonomy_100"] = df["REAL_AUTONOMY"] * (1 / soh_normalized)

        logger.info(f"Autonomies calculated for {len(df)} discharges")
        return df

    def aggregate_by_model(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregates results by model and version.

        Args:
            df: Pandas DataFrame with calculated autonomies

        Returns:
            DataFrame aggregated by model/version
        """
        logger.info("Aggregating results by model/version...")
        agg_df = df.groupby(["model_id"], as_index=False).agg(
            {
                "real_autonomy_100": "mean",
            }
        )

        agg_df["real_autonomy_100"] = agg_df["real_autonomy_100"].round()
        return agg_df

    def save_to_database(self, df: pd.DataFrame):
        """
        Saves results to the database.

        Args:
            df: Pandas DataFrame with aggregated results
            table_name: Destination table name
        """
        logger.info(f"Saving results to vehicle_model table...")
        df.to_sql(
            "tmp_real_autonomy",
            self.engine,
            if_exists="replace",
            index=False,
        )
        with get_connection() as con:
            cur = con.cursor()
            cur.execute("""
                UPDATE vehicle_model vm
                SET real_autonomy = s.real_autonomy_100
                FROM tmp_real_autonomy s
                WHERE vm.id = s.model_id::uuid
            """)
            cur.execute("DROP TABLE IF EXISTS tmp_real_autonomy;")
            con.commit()

    def run(
        self,
        oem: str,
    ):
        """
        Executes the complete real autonomy calculation pipeline.

        Args:
            oem: OEM name for loading phases
        """
        try:
            # Load data
            vehicle_df = self.load_vehicle_data_from_db()
            result_phases = self.load_result_phases(oem)

            # Filter valid VINs
            filtered_phases = self.filter_valid_vins(result_phases)

            # Prepare DataFrame
            df = self.prepare_dataframe(filtered_phases, vehicle_df)

            # Apply quality filters
            df_filtered = self.apply_quality_filters(df)

            # Calculate autonomies
            df_with_autonomy = self.calculate_real_autonomy(df_filtered)
            # Aggregate by model
            df_aggregated = self.aggregate_by_model(df_with_autonomy)

            # Save to database
            self.save_to_database(df_aggregated)
        except Exception as e:
            logger.error(f"Error during execution: {e}", exc_info=True)
            raise


def update_real_autonomy():
    calculator = RealAutonomyCalculator(
        min_odometer_followed=500,
        min_soc_diff=5,
        min_odometer_diff=5,
        wltp_ratio_max=1.3,
        max_ratio_km_soc=10,
    )

    oems = [
        "tesla_fleet_telemetry",
        "bmw",
        "renault",
        "stellantis",
        "volvo_cars",
        "volkswagen",
        "kia",
        "ford",
        "mercedes_benz",
    ]
    for oem in oems:
        logger.info(f"Updating real autonomy for {oem}...")
        calculator.run(oem=oem)
