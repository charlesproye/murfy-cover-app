import pandas as pd
import streamlit as st

from core.s3.s3_utils import S3Service
from core.s3.settings import S3Settings
from core.sql_utils import get_sqlalchemy_engine, text
from visualization.catalog.config import OEM_LIST, STEPS
from visualization.catalog.utils import (
    get_parquet_schema_from_s3,
    insert_into_data_catalog,
)

engine = get_sqlalchemy_engine()
s3_service = S3Service()
settings = S3Settings()


st.set_page_config(page_title="Description des colonnes", page_icon="📋")


def get_oems():
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT DISTINCT oem_name FROM data_catalog ORDER BY oem_name")
        )
        return [row[0] for row in result if row[0] is not None]


def get_steps(oem):
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT DISTINCT step FROM data_catalog WHERE oem_name = :oem_name ORDER BY step"
            ),
            {"oem_name": oem},
        )
        return [row[0] for row in result if row[0] is not None]


def get_columns(oem, step):
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT column_name, type, description
                FROM data_catalog
                WHERE oem_name = :oem_name AND step = :step
                ORDER BY column_name
            """),
            {"oem_name": oem, "step": step},
        )
        return pd.DataFrame(
            result.fetchall(), columns=["column_name", "type", "description"]
        )


def update_description(oem, step, column_name, new_desc):
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE data_catalog
                SET description = :desc
                WHERE oem_name = :oem_name AND step = :step AND column_name = :col
            """),
            {"desc": new_desc, "oem_name": oem, "step": step, "col": column_name},
        )


st.title("Description des colonnes")

oems = get_oems()
oem = st.selectbox("Sélectionne l'OEM :", oems) if oems else None

step = None
if oem:
    steps = get_steps(oem)
    step = st.selectbox("Sélectionne la step :", steps) if steps else None

if oem and step:
    df = get_columns(oem, step)
    if df.empty:
        st.warning("Aucune colonne trouvée pour cette combinaison OEM/step.")
    else:
        # Construire un tableau avec colonne + description
        display_df = df[["column_name", "description"]].copy()
        display_df["description"] = display_df["description"].fillna(
            "Pas de description"
        )
        display_df.reset_index(drop=True, inplace=True)  # supprime l'index

        # Injecter un peu de CSS pour réduire la police
        st.markdown(
            """
            <style>
            .dataframe td, .dataframe th {
                font-size: 12px;
                max-width: 400px;  /* élargit un peu la colonne */
                text-overflow: ellipsis;
                white-space: nowrap;
                overflow: hidden;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        st.subheader("📋 Colonnes existantes")
        st.dataframe(display_df, use_container_width=True)

        col_name = st.selectbox(
            "Sélectionne la colonne à modifier :", df["column_name"]
        )
        current_desc = df.loc[df["column_name"] == col_name, "description"].values[0]

        new_desc = st.text_area(
            "Nouvelle description :", current_desc if current_desc else ""
        )

        if st.button("💾 Mettre à jour"):
            update_description(oem, step, col_name, new_desc)
            st.success(f"Description mise à jour pour `{col_name}` ✅")

if st.button("🚀 Lancer l'actualisation de la base de données"):
    for oem in OEM_LIST:
        for step, path in STEPS.items():
            if step == "raw_ts":
                path_formatted = path.format(oem=oem)
            else:
                path_formatted = path.format(oem=oem.replace("-", "_"))
            st.write(f"Processing {step} for {oem}...")

            columns = get_parquet_schema_from_s3(path_formatted, s3_service)
            if columns and columns[0][0] != "Erreur":
                with engine.connect() as conn:
                    existing = conn.execute(
                        text("""
                        SELECT column_name
                        FROM data_catalog
                        WHERE oem_name = :oem_name AND step = :step
                        """),
                        {"oem_name": oem, "step": step},
                    )
                    existing_cols = {row[0] for row in existing}

                new_columns = [
                    (col, typ) for col, typ in columns if col not in existing_cols
                ]

                if new_columns:
                    insert_into_data_catalog(new_columns, step, oem)
                    st.success(
                        f"✅ {len(new_columns)} nouvelles colonnes insérées pour {step} / {oem}"
                    )
                else:
                    st.info(f"⚠️ Aucune nouvelle colonne à insérer pour {step} / {oem}")
            else:
                st.error(f"⚠️ Problème avec {columns}")

