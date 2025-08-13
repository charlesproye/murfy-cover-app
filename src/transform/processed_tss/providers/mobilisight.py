from pyspark.sql import DataFrame as DF
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

from transform.processed_tss.raw_ts_to_processed_ts import RawTsToProcessedTs


class MobilisightRawTsToProcessedTs(RawTsToProcessedTs):
    """
    Compute the specific features for the Mobilisight data.
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
        return tss

