import io

import pyarrow.parquet as pq

from core.sql_utils import get_sqlalchemy_engine, text
from visualization.catalog.config import OEM_LIST

STEPS = {
    "raw_ts": "raw_ts/{oem}/time_series/raw_ts_spark.parquet/",
    "processed_phases": "processed_phases/processed_phases_{oem}.parquet/",
    "result_phases": "result_phases/result_phases_{oem}.parquet/",
}

engine = get_sqlalchemy_engine()


def get_parquet_schema_from_s3(s3_key: str, s3_service) -> list[tuple[str, str]]:
    """
    Retourne [(col_name, col_type)] depuis un fichier parquet S3
    """
    client = s3_service._s3_client
    bucket = s3_service.bucket_name

    try:
        if s3_key.endswith("/"):
            response = client.list_objects_v2(Bucket=bucket, Prefix=s3_key)
            contents = response.get("Contents", [])
            parquet_keys = [
                obj["Key"]
                for obj in contents
                if obj["Key"].endswith(".parquet")
                and not obj["Key"].endswith("_metadata.parquet")
            ]
            if not parquet_keys:
                return [("Erreur", "Aucun fichier parquet trouvé")]
            s3_key = parquet_keys[0]
        else:
            client.head_object(Bucket=bucket, Key=s3_key)  # check existence

        response = client.get_object(Bucket=bucket, Key=s3_key)
        buffer = io.BytesIO(response["Body"].read())

        schema = pq.read_schema(buffer)

        return [(name, str(schema.field(name).type)) for name in schema.names]

    except Exception as e:
        s3_service.logger.warning(f"Erreur lecture Parquet sur {s3_key}: {e}")
        return [("Erreur", str(e))]


def insert_into_data_catalog(columns: list[tuple[str, str]], step: str, oem: str):
    """Insère les colonnes dans data_catalog"""
    with engine.begin() as conn:
        for col_name, col_type in columns:
            conn.execute(
                text("""
                INSERT INTO data_catalog (column_name, type, oem_name, step)
                VALUES (:col_name, :col_type, :oem_name, :step)
                ON CONFLICT (column_name, oem_name, step) DO NOTHING
                """),
                {
                    "col_name": col_name,
                    "col_type": col_type,
                    "oem_name": oem,
                    "step": step,
                },
            )


def process_parquet_to_catalog(s3_service):
    for oem in OEM_LIST:
        for step, path in STEPS.items():
            print(f"Processing {step} for {oem}")
            path = path.format(oem=oem.replace("-", "_"))
            columns = get_parquet_schema_from_s3(path, s3_service)
            if columns and columns[0][0] != "Erreur":
                insert_into_data_catalog(columns, step, oem)
            else:
                print(f"⚠️ Problème avec {columns}")

