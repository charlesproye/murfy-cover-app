import pandas as pd
import streamlit as st

from core.gsheet_utils import load_excel_data
from core.plt_utils import show_trendline
from core.sql_utils import get_connection

# page config
st.set_page_config(page_title="Liste des modèles de véhicules", layout="wide")
st.title("📊 Données de la table vehicle_model")


def get_vehicle_models():
    with get_connection() as con:
        cursor = con.cursor()
        cursor.execute("""SELECT vm.model_name, vm.type, vm.version, o.oem_name, m.make_name,
                       vm.trendline_bib, vm.trendline_bib_min, vm.trendline_bib_max,
                       vm.trendline_oem, vm.trendline_oem_min, vm.trendline_oem_max,
                       vm.odometer_data ,vm.soh_data , vm.soh_oem_data ,vm.has_trendline_bib , vm.has_trendline_oem , vm.commissioning_date , vm.end_of_life_date
                            FROM vehicle_model vm
                            join oem o  on o.id = vm.oem_id
                            join make m  on m.id = vm.make_id
                        """)
        df = cursor.fetchall()
        return df


vehicle_models = pd.DataFrame(
    get_vehicle_models(),
    columns=[
        "model_name",
        "type",
        "version",
        "oem_name",
        "make_name",
        "trendline_bib",
        "trendline_bib_min",
        "trendline_bib_max",
        "trendline_oem",
        "trendline_oem_min",
        "trendline_oem_max",
        "odometer_data",
        "soh_data",
        "soh_oem_data",
        "has_trendline_bib",
        "has_trendline_oem",
        "commissioning_date",
        "end_of_life_date",
    ],
)

filtered_df = vehicle_models.copy()

oem_choices = sorted(filtered_df["oem_name"].dropna().unique())
selected_oem = st.selectbox("Choisir un OEM", ["", *oem_choices])

if selected_oem:
    filtered_df = filtered_df[filtered_df["oem_name"] == selected_oem]

make_choices = sorted(filtered_df["make_name"].dropna().unique())
selected_make = st.selectbox("Choisir un Make", ["", *make_choices])

if selected_make:
    filtered_df = filtered_df[filtered_df["make_name"] == selected_make]

model_choices = sorted(filtered_df["model_name"].dropna().unique())
selected_model = st.selectbox("Choisir un Modèle", ["", *model_choices])

if selected_model:
    filtered_df = filtered_df[filtered_df["model_name"] == selected_model]

type_choices = sorted(filtered_df["type"].dropna().unique())
selected_type = st.selectbox("Choisir un Type", ["", *type_choices])

print(filtered_df.version.unique())

if selected_type:
    filtered_df = filtered_df[filtered_df["type"] == selected_type]

print(filtered_df.version.unique())

version_choices = sorted(filtered_df["version"].dropna().unique())

selected_version = st.selectbox("Choisir une Version", ["", *version_choices])
if selected_version:
    filtered_df = filtered_df[filtered_df["version"] == selected_version]

st.subheader("📋 Données filtrées")
st.dataframe(filtered_df, use_container_width=True)


if st.button("show graph bib"):
    with get_connection() as con:
        cursor = con.cursor()
        if selected_oem == "tesla":
            cursor.execute(
                """SELECT vd.soh_bib, vd.odometer FROM vehicle v
                                JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
                                JOIN battery b ON vm.battery_id = b.id
                                JOIN oem o ON vm.oem_id = o.id
                                join vehicle_data vd on vd.vehicle_id = v.id
                                WHERE vm.version = %s""",
                (selected_version,),
            )

        else:
            cursor.execute(
                """SELECT vd.soh_bib, vd.odometer FROM vehicle v
                                JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
                                JOIN battery b ON vm.battery_id = b.id
                                JOIN oem o ON vm.oem_id = o.id
                                join vehicle_data vd on vd.vehicle_id = v.id
                                where vm.model_name = %s
                                and vm.type = %s""",
                (selected_model, selected_type),
            )
        df = pd.DataFrame(
            cursor.fetchall(), columns=["soh_bib", "odometer"], dtype=float
        ).dropna()
        fig = show_trendline(
            df,
            filtered_df["trendline_bib"].values[0],
            filtered_df["trendline_bib_max"].values[0],
            filtered_df["trendline_bib_min"].values[0],
            selected_type,
            "odometer",
            "soh_bib",
        )
        st.plotly_chart(fig)


if st.button("show graph oem"):
    df = load_excel_data("Courbes de tendance", "Courbes OS")
    df_sheet = pd.DataFrame(columns=df[0, :8], data=df[1:, :8])
    df_sheet["Modèle"] = df_sheet["Modèle"].apply(lambda x: x.lower())
    df_model = df_sheet[
        (df_sheet["Modèle"] == selected_model) & (df_sheet["Type"] == selected_type)
    ].copy()
    df_model["SoH"] = (
        df_model["SoH"].apply(lambda x: x.replace("%", "").strip()).astype(float) / 100
    )
    df_model["Odomètre (km)"] = (
        df_model["Odomètre (km)"].apply(lambda x: x.replace(",", "")).astype(float)
    )
    fig = show_trendline(
        df_model,
        filtered_df["trendline_oem"].values[0],
        filtered_df["trendline_oem_max"].values[0],
        filtered_df["trendline_oem_min"].values[0],
        selected_type,
        "Odomètre (km)",
        "SoH_OEM",
    )
    st.plotly_chart(fig)
