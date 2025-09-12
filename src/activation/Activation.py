import streamlit as st
import asyncio
import pandas as pd
from activation.fleet_info import read_fleet_info as fleet_info
from activation.config.config import REASON_MAPPING

# --- Charger les données ---
async def load_df():
    df = await fleet_info(owner_filter=None)
    return df

df = asyncio.run(load_df())

# --- UI Streamlit ---
st.set_page_config(page_title="Fleet Info", layout="wide")
st.title("🚗 Fleet Information Dashboard")
st.markdown("### Filtrer par Owner et VIN")

# --- Filtres dynamiques ---
selected_owner = st.multiselect(
    "Choisir un ou plusieurs owners :", 
    options=df["owner"].dropna().unique().tolist()
)

if selected_owner:
    vins_options = df[df["owner"].isin(selected_owner)]["vin"].dropna().unique().tolist()
else:
    vins_options = df["vin"].dropna().unique().tolist()

selected_vin = st.multiselect(
    "Choisir un ou plusieurs VINs :", 
    options=vins_options
)

# Appliquer les filtres combinés
filtered_df = df.copy()
if selected_owner:
    filtered_df = filtered_df[filtered_df["owner"].isin(selected_owner)]
if selected_vin:
    filtered_df = filtered_df[filtered_df["vin"].isin(selected_vin)]

# Colonnes à afficher
columns_to_show = ["vin", "oem", "activation", "eligibility", "real_activation", "activation_error", "api_detail"]
filtered_df = filtered_df[columns_to_show]

filtered_df["interpretation"] = filtered_df.apply(
    lambda row: REASON_MAPPING.get(row["oem"], {}).get(row["activation_error"], ""),
    axis=1
)


# --- Transformer les booléens en ticks/croix ---
def bool_to_tick(val):
    if val in [True, "TRUE", "true"]:
        return "    ✅  "
    elif val in [False, "FALSE", "false"]:
        return "    ❌  "
    return ""  # pour les autres valeurs (ex: activation_error)

for col in ["activation", "eligibility", "real_activation"]:
    filtered_df[col] = filtered_df[col].apply(bool_to_tick)

# --- Affichage ---
st.markdown("### Résultats filtrés")
st.dataframe(filtered_df, use_container_width=True)

st.info(f"{len(filtered_df)} véhicules affichés")


