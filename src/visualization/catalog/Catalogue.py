import io

import pandas as pd
import streamlit as st
import yaml

from core.s3.s3_utils import S3Service
from core.s3.settings import S3Settings
from visualization.catalog.config import OEM_LIST, STEPS

settings = S3Settings()
s3_service = S3Service()


st.set_page_config(page_title="Catalogue", page_icon="📊")


def get_parquet_columns(s3_key: str, s3_service: S3Service):
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
                return [("Erreur", f"Aucun fichier .parquet trouvé dans {s3_key}")]
            s3_key = parquet_keys[0]
        else:
            client.head_object(Bucket=bucket, Key=s3_key)

        response = client.get_object(Bucket=bucket, Key=s3_key)
        buffer = io.BytesIO(response["Body"].read())

        df = pd.read_parquet(buffer)

        columns = [
            (col, str(dtype)) for col, dtype in zip(df.columns, df.dtypes, strict=False)
        ]

        na_percent = (df.isna().mean() * 100).round(2)

        info = [(col, typ, f"{na_percent[col]}%") for col, typ in columns]

        return info

    except Exception as e:
        s3_service.logger.warning(f"Erreur lecture Parquet sur {s3_key}: {e}")
        return [("Erreur", str(e), "")]


def get_last_modified(s3_key: str, s3_service: S3Service):
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
                return "Aucun fichier .parquet trouvé"
            s3_key = parquet_keys[0]

        response = client.head_object(Bucket=bucket, Key=s3_key)
        return response["LastModified"].strftime("%Y-%m-%d %H:%M")

    except Exception:
        return "Non trouvé"


def read_config(oem, s3_service):
    config_file = f"config/{oem}.yaml"
    response = s3_service._s3_client.get_object(
        Bucket=s3_service.bucket_name, Key=config_file
    )

    config_data = yaml.safe_load(response["Body"])
    last_mod = response["LastModified"].strftime("%Y-%m-%d %H:%M")
    return config_data, last_mod


st.title("Data Catalog")

selected_steps = st.multiselect("Étapes", list(STEPS.keys()))
selected_oems = st.multiselect("OEMs", [oem.capitalize() for oem in OEM_LIST])

for step in selected_steps:
    if step == "raw_ts":
        st.header(f"🪨 Étape : {step.capitalize()}")
    elif step == "processed_phases":
        st.header(f"⚙️ Étape : {step.capitalize()}")
    elif step == "result_phases":
        st.header(f"📊 Etape : {step.capitalize()}")

    for oem in [oem.lower() for oem in selected_oems]:
        s3_path = STEPS[step].format(oem=oem.replace("-", "_"))

        st.subheader(f"{oem.upper()}")

        columns = get_parquet_columns(s3_path, s3_service)
        last_mod = get_last_modified(s3_path, s3_service)
        st.markdown(f"🕒 **Dernier upload** : `{last_mod}`")

        if columns and columns[0][0] != "Erreur":
            with st.expander("**COLONNES, TYPES & % NA**"):
                df_columns = pd.DataFrame(columns, columns=["Nom", "Type", "% de NA"])
                st.markdown(f"**Nombre de colonnes** : `{df_columns.shape[0]}`")
                st.dataframe(df_columns, use_container_width=True)
        else:
            st.error(f"❌ {columns[0][1]}")


for oem in [oem.lower() for oem in selected_oems]:
    s3_path_rts = STEPS["raw_ts"].format(oem=oem.replace("-", "_"))
    s3_path_pph = STEPS["processed_phases"].format(oem=oem)
    s3_path_rph = STEPS["result_phases"].format(oem=oem)
    last_mod_rts = get_last_modified(s3_path_rts, s3_service)
    last_mod_pph = get_last_modified(s3_path_pph, s3_service)
    last_mod_rph = get_last_modified(s3_path_rph, s3_service)

    st.header("🧮 Fichiers de configuration")
    config_data, config_time = read_config(oem, s3_service)
    with st.expander(f"**{oem.upper()}**", expanded=False):
        st.markdown(f"🕒 **Dernier upload config** : `{config_time}`")
        if (
            (last_mod_rts > config_time)
            and (last_mod_rph > config_time)
            and (last_mod_pph > config_time)
        ):
            st.markdown(
                " **Tous les fichiers ont été upload après dernière modification de la config** ✅"
            )
        else:
            st.markdown(
                " **Au moins un des fichiers a été upload avant dernière modification de la config** ❌"
            )
        st.json(config_data)

