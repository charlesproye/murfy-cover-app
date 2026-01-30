#!/usr/bin/env python3
"""
Application Streamlit pour générer des cartes de présence géographique.
Lancement : streamlit run app.py
"""

import io
import shutil
import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import matplotlib.image as mpimg
import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st
from matplotlib.colors import ListedColormap
from mpl_toolkits.axes_grid1.inset_locator import inset_axes

# Configuration de la page
st.set_page_config(
    page_title="Générateur de cartes",
    page_icon="🗺️",
    layout="wide",
)

st.title("🗺️ Générateur de cartes de présence")
st.markdown("Créez des cartes personnalisées par départements ou codes postaux")

# Sidebar pour la configuration
with st.sidebar:
    st.header("⚙️ Configuration")

    # Upload du shapefile
    st.subheader("1. Shapefile")
    st.info(
        "💡 Vous pouvez uploader soit un ZIP, soit tous les fichiers individuels (.shp, .shx, .dbf, etc.)"
    )

    upload_method = st.radio(
        "Méthode d'upload", ["ZIP (recommandé)", "Fichiers individuels"]
    )

    if upload_method == "ZIP (recommandé)":
        uploaded_zip = st.file_uploader(
            "Téléchargez le ZIP du shapefile",
            type=["zip"],
            help="Archive ZIP contenant tous les fichiers du shapefile",
        )
        uploaded_files = None
    else:
        uploaded_files = st.file_uploader(
            "Téléchargez TOUS les fichiers du shapefile",
            type=["shp", "shx", "dbf", "prj", "cpg"],
            accept_multiple_files=True,
            help="Uploadez au minimum : .shp, .shx, .dbf",
        )
        uploaded_zip = None

    # Mode de sélection
    st.subheader("2. Mode")
    mode = st.radio(
        "Sélectionnez le mode d'affichage",
        ["Départements", "Codes postaux"],
        help="Départements = niveau département, Codes postaux = niveau code postal",
    )

    # Configuration des colonnes
    st.subheader("3. Colonnes du shapefile")
    if mode == "Départements":
        column_name = st.text_input("Nom de la colonne département", value="DEP")
    else:
        column_name = st.text_input("Nom de la colonne code postal", value="ID")

    # Couleurs
    st.subheader("4. Couleurs")
    col1, col2 = st.columns(2)
    with col1:
        color_active = st.color_picker("Zones actives", value="#2ca25f")
    with col2:
        color_inactive = st.color_picker("Autres zones", value="#e0e0e0")

    # Overlay
    st.subheader("5. Logo (optionnel)")
    overlay_file = st.file_uploader(
        "Téléchargez un logo",
        type=["png", "jpg", "jpeg"],
        help="Image affichée en haut à droite",
    )

# Zone principale
if uploaded_zip is None and uploaded_files is None:
    st.info("👈 Commencez par télécharger un shapefile dans la barre latérale")
    st.stop()

# Chargement du shapefile
try:
    # Création d'un dossier temporaire
    temp_dir = tempfile.mkdtemp()

    if uploaded_zip:
        # Extraction du ZIP
        with zipfile.ZipFile(uploaded_zip, "r") as zip_ref:
            zip_ref.extractall(temp_dir)

        # Recherche du fichier .shp
        shp_files = list(Path(temp_dir).rglob("*.shp"))

        if not shp_files:
            st.error("❌ Aucun fichier .shp trouvé dans le ZIP")
            st.stop()

        shp_path = shp_files[0]
        st.success(f"✅ ZIP extrait : {shp_path.name}")

    else:
        # Sauvegarde des fichiers individuels
        if uploaded_files is None or not any(
            f.name.endswith(".shp") for f in uploaded_files
        ):
            st.error("❌ Le fichier .shp est obligatoire !")
            st.stop()

        # Type narrowing: à partir d'ici uploaded_files n'est plus None
        assert uploaded_files is not None

        required_exts = {".shp", ".shx", ".dbf"}
        uploaded_exts = {Path(f.name).suffix.lower() for f in uploaded_files}
        missing = required_exts - uploaded_exts

        if missing:
            st.error(f"❌ Fichiers manquants : {', '.join(missing)}")
            st.stop()

        # Sauvegarde de tous les fichiers
        for uploaded_file in uploaded_files:
            file_path = Path(temp_dir) / uploaded_file.name
            with open(file_path, "wb") as f:
                f.write(uploaded_file.getbuffer())

            if uploaded_file.name.endswith(".shp"):
                shp_path = file_path

        st.success(f"✅ Fichiers uploadés : {len(uploaded_files)}")

    # Chargement du GeoDataFrame
    gdf = gpd.read_file(shp_path)
    st.success(f"✅ Shapefile chargé : {len(gdf)} entités")

except Exception as e:
    st.error(f"❌ Erreur lors du chargement du shapefile : {e}")
    st.stop()

# Vérification de la colonne
if column_name not in gdf.columns:
    st.error(f"❌ La colonne '{column_name}' n'existe pas dans le shapefile")
    st.info(f"Colonnes disponibles : {', '.join(gdf.columns.tolist())}")
    st.stop()

# Conversion en string
gdf[column_name] = gdf[column_name].astype(str)

# Normalisation des codes postaux : padding avec des zéros à gauche pour avoir 5 chiffres
if mode == "Codes postaux":
    gdf[column_name] = gdf[column_name].str.zfill(5)

# Récupération des valeurs uniques
unique_values = sorted(gdf[column_name].unique())

# Interface de sélection
st.header("🎯 Sélection des zones")

# Option CSV
st.subheader("📄 Import depuis CSV (optionnel)")
csv_file = st.file_uploader(
    "Téléchargez un fichier CSV contenant les codes postaux/départements",
    type=["csv"],
    help="Le CSV doit contenir une colonne avec les codes postaux ou départements à sélectionner",
)

csv_zones = []
if csv_file is not None:
    try:
        df_csv = pd.read_csv(csv_file)
        st.success(f"✅ CSV chargé : {len(df_csv)} lignes")

        # Sélection de la colonne
        csv_column = st.selectbox(
            "Sélectionnez la colonne contenant les codes",
            options=df_csv.columns.tolist(),
            index=df_csv.columns.tolist().index("CP") if "CP" in df_csv.columns else 0,
            help="Choisissez la colonne qui contient les codes postaux ou départements",
        )

        # Extraction des valeurs
        csv_zones = df_csv[csv_column].dropna().astype(str).unique().tolist()

        # Normalisation des codes postaux si en mode "Codes postaux"
        if mode == "Codes postaux":
            csv_zones = [str(z).zfill(5) for z in csv_zones]

        # Filtrage des valeurs qui existent dans le shapefile
        csv_zones_valid = [z for z in csv_zones if z in unique_values]
        csv_zones_not_found = [z for z in csv_zones if z not in unique_values]

        st.info(
            f"📊 {len(csv_zones_valid)} zones trouvées dans le shapefile"
            + (
                f" ({len(csv_zones_not_found)} codes non trouvés)"
                if len(csv_zones_not_found) > 0
                else ""
            )
        )

        # Affichage des zones non trouvées
        if csv_zones_not_found:
            with st.expander(
                f"⚠️ Voir les {len(csv_zones_not_found)} codes non trouvés"
            ):
                st.warning("Ces codes ne correspondent à aucune zone du shapefile :")
                # Afficher en colonnes pour économiser l'espace
                cols = st.columns(3)
                for idx, zone in enumerate(csv_zones_not_found):
                    cols[idx % 3].write(f"• {zone}")

        # Bouton pour appliquer la sélection CSV
        if st.button("✅ Appliquer la sélection du CSV"):
            st.session_state["csv_selection"] = csv_zones_valid
            st.rerun()

    except Exception as e:
        st.error(f"❌ Erreur lors de la lecture du CSV : {e}")

st.markdown("---")

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader(f"Zones disponibles ({len(unique_values)})")

    # Recherche
    search = st.text_input("🔍 Rechercher", placeholder="Tapez pour filtrer...")

    # Filtrage
    if search:
        filtered_values = [v for v in unique_values if search.lower() in v.lower()]
    else:
        filtered_values = unique_values

    # Déterminer la sélection par défaut
    if st.session_state.get("csv_selection"):
        default_selection = st.session_state["csv_selection"]
    else:
        default_selection = (
            filtered_values[:5] if len(filtered_values) >= 5 else filtered_values
        )

    # Sélection multiple
    selected_zones = st.multiselect(
        f"Sélectionnez les {mode.lower()}",
        options=filtered_values,
        default=default_selection,
        help="Utilisez Ctrl/Cmd + clic pour sélectionner plusieurs zones",
    )

with col2:
    st.subheader("📊 Résumé")
    st.metric("Zones sélectionnées", len(selected_zones))
    st.metric("Total zones", len(unique_values))

    if selected_zones:
        coverage = (len(selected_zones) / len(unique_values)) * 100
        st.metric("Couverture", f"{coverage:.1f}%")

# Boutons de sélection rapide
st.subheader("⚡ Sélection rapide")
col1, col2, col3, col4 = st.columns(4)

with col1:
    if st.button("Tout sélectionner"):
        selected_zones = unique_values

with col2:
    if st.button("Tout désélectionner"):
        selected_zones = []

with col3:
    if st.button("Inverser la sélection"):
        selected_zones = [v for v in unique_values if v not in selected_zones]


# Génération de la carte
if not selected_zones:
    st.warning("⚠️ Veuillez sélectionner au moins une zone")
    st.stop()

st.header("🗺️ Carte générée")

# Ajout de la colonne de présence
gdf["present"] = gdf[column_name].isin(selected_zones)

# Statistiques
nb_zones_found = gdf["present"].sum()
st.info(
    f"🎯 {nb_zones_found} zones trouvées dans le shapefile sur {len(selected_zones)} sélectionnées"
)

# Affichage des zones non trouvées
if nb_zones_found < len(selected_zones):
    zones_found_set = set(gdf[gdf["present"]][column_name].unique())
    zones_not_found = [z for z in selected_zones if z not in zones_found_set]

    with st.expander(f"⚠️ Voir les {len(zones_not_found)} codes non trouvés"):
        st.warning(
            "Ces codes sélectionnés ne correspondent à aucune zone du shapefile :"
        )
        # Afficher en colonnes pour économiser l'espace
        cols = st.columns(4)
        for idx, zone in enumerate(zones_not_found):
            cols[idx % 4].write(f"• {zone}")

# Configuration du style
cmap = ListedColormap([color_inactive, color_active])

# Génération de la carte
fig, ax = plt.subplots(1, 1, figsize=(12, 12))

gdf.plot(
    column="present",
    categorical=True,
    cmap=cmap,
    linewidth=0.3,
    edgecolor="white",
    ax=ax,
    legend=False,
)

ax.axis("off")

# Ajout de l'overlay si fourni
if overlay_file is not None:
    overlay_img = mpimg.imread(overlay_file)

    axins = inset_axes(
        ax,
        width="18%",
        height="18%",
        loc="upper right",
        borderpad=1,
    )

    axins.imshow(overlay_img)
    axins.axis("off")

# Affichage dans Streamlit
st.pyplot(fig)

# Téléchargement
st.header("💾 Téléchargement")

col1, col2 = st.columns(2)

with col1:
    # Export PNG
    buf_png = io.BytesIO()
    fig.savefig(buf_png, format="png", dpi=600, bbox_inches="tight", facecolor="white")
    buf_png.seek(0)

    st.download_button(
        label="📥 Télécharger PNG (600 DPI)",
        data=buf_png,
        file_name=f"carte_{mode.lower().replace(' ', '_')}.png",
        mime="image/png",
    )

with col2:
    # Export SVG
    buf_svg = io.BytesIO()
    fig.savefig(buf_svg, format="svg", bbox_inches="tight")
    buf_svg.seek(0)

    st.download_button(
        label="📥 Télécharger SVG (vectoriel)",
        data=buf_svg,
        file_name=f"carte_{mode.lower().replace(' ', '_')}.svg",
        mime="image/svg+xml",
    )

# Nettoyage
plt.close(fig)
shutil.rmtree(temp_dir, ignore_errors=True)

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: gray;'>
    🗺️ Générateur de cartes • Made with Streamlit
    </div>
    """,
    unsafe_allow_html=True,
)
