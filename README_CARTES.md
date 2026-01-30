# 🗺️ Générateur de Cartes de Présence

Générateur de cartes géographiques personnalisables pour visualiser la présence par départements ou codes postaux.

## 📋 Fonctionnalités

- **Deux modes d'affichage** : départements ou codes postaux
- **Import depuis CSV** : sélectionnez automatiquement les zones depuis un fichier CSV
- **Interface Streamlit** : application web interactive pour générer vos cartes
- **CLI** : script en ligne de commande pour automatiser la génération
- **Export multiple** : PNG haute résolution (600 DPI) et SVG vectoriel
- **Personnalisation** : couleurs personnalisables, logo optionnel
- **Support shapefile** : upload ZIP ou fichiers individuels

## 🚀 Installation

### Prérequis

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) (gestionnaire de paquets)

### Installation des dépendances

```bash
# Installation avec uv
uv sync --locked
```

## 💻 Utilisation

### Interface Streamlit (Recommandé)

Lancez l'application web interactive :

```bash
uv run streamlit run src/app.py
```

Puis :
1. Uploadez votre shapefile (ZIP ou fichiers individuels)
2. Choisissez le mode (départements ou codes postaux)
3. **Option CSV** : Importez un CSV avec vos codes postaux/départements
4. Ou sélectionnez manuellement les zones dans l'interface
5. Personnalisez les couleurs et ajoutez un logo
6. Téléchargez vos cartes en PNG ou SVG

### Script en ligne de commande

```bash
# Génération avec sélection manuelle
uv run python src/map_generator.py \
  -s data/communes.shp \
  -m postal \
  -z "38000,38100,69001"

# Génération depuis un fichier CSV
uv run python src/map_generator.py \
  -s data/communes.shp \
  -m postal \
  -c example_codes.csv \
  --csv-column CP

# Avec toutes les options
uv run python src/map_generator.py \
  -s data/communes.shp \
  -m dept \
  -c departements.csv \
  --csv-column DEP \
  -o logo.png \
  -d output \
  --color-active "#2ca25f" \
  --color-inactive "#e0e0e0"
```

### Options du script CLI

| Option | Raccourci | Description |
|--------|-----------|-------------|
| `--shapefile` | `-s` | Chemin vers le shapefile (.shp) |
| `--mode` | `-m` | Mode : `postal` ou `dept` |
| `--zones` | `-z` | Zones séparées par virgules (ex: "59,62,38") |
| `--csv` | `-c` | Fichier CSV contenant les zones |
| `--csv-column` | | Nom de la colonne dans le CSV (défaut: "CP") |
| `--overlay` | `-o` | Image à superposer (logo) |
| `--output-dir` | `-d` | Dossier de sortie (défaut: "output") |
| `--color-active` | | Couleur zones actives (défaut: "#2ca25f") |
| `--color-inactive` | | Couleur autres zones (défaut: "#e0e0e0") |
| `--dept-col` | | Colonne département dans shapefile (défaut: "DEP") |
| `--postal-col` | | Colonne code postal dans shapefile (défaut: "CODE_POST") |

## 📁 Format du fichier CSV

Le fichier CSV doit contenir au minimum une colonne avec les codes postaux ou départements :

### Exemple simple (codes postaux)

```csv
CP
59000
59100
62000
38000
69001
```

### Exemple avec plusieurs colonnes

```csv
Ville,CP,Region
Lille,59000,Hauts-de-France
Grenoble,38000,Auvergne-Rhône-Alpes
Lyon,69001,Auvergne-Rhône-Alpes
```

**Note** : Seule la colonne spécifiée (par défaut "CP") sera utilisée pour la sélection des zones.

## 📂 Structure du projet

```
.
├── src/
│   ├── app.py              # Application Streamlit
│   └── map_generator.py    # Script CLI
├── output/                 # Cartes générées (créé automatiquement)
├── example_codes.csv       # Exemple de fichier CSV
├── pyproject.toml          # Configuration du projet
└── README.md              # Ce fichier
```

## 🎨 Exemples d'utilisation

### Carte des départements du Nord et du Rhône-Alpes

```bash
uv run python src/map_generator.py \
  -s data/departements.shp \
  -m dept \
  -z "59,62,01,07,26,38,42,69,73,74" \
  -o logo.png
```

### Carte des codes postaux depuis CSV

```bash
uv run python src/map_generator.py \
  -s data/communes.shp \
  -m postal \
  -c codes_postaux.csv \
  --csv-column CP
```

### Personnalisation complète

```bash
uv run python src/map_generator.py \
  -s data/communes.shp \
  -m postal \
  -c zones_intervention.csv \
  --csv-column CodePostal \
  -o logo_entreprise.png \
  --color-active "#FF5733" \
  --color-inactive "#F0F0F0" \
  -d cartes_personnalisees
```

## 🔧 Personnalisation avancée

### Couleurs personnalisées

Vous pouvez utiliser n'importe quelle couleur au format hexadécimal :

- `#2ca25f` : Vert (défaut pour zones actives)
- `#e0e0e0` : Gris clair (défaut pour zones inactives)
- `#FF5733` : Rouge-orange
- `#3498db` : Bleu
- `#9b59b6` : Violet

### Logo / Overlay

L'image sera affichée en haut à droite de la carte. Formats supportés : PNG, JPG, JPEG.
Taille recommandée : 200x200 pixels avec fond transparent (PNG).

## 📊 Sources de données

### Shapefiles France

Vous pouvez obtenir les shapefiles France depuis :

- [IGN / Adminexpress](https://geoservices.ign.fr/adminexpress)
- [data.gouv.fr](https://www.data.gouv.fr/)
- [Natural Earth](https://www.naturalearthdata.com/)

## 🐛 Dépannage

### Erreur : "Colonne introuvable"

Vérifiez le nom des colonnes de votre shapefile :

```bash
# Dans l'application Streamlit, les colonnes disponibles sont affichées
# En CLI, l'erreur vous indiquera les colonnes disponibles
```

Puis utilisez les options `--dept-col` ou `--postal-col` pour spécifier la bonne colonne.

### Erreur CSV : "Colonne CP introuvable"

Votre CSV n'a pas de colonne nommée "CP". Utilisez `--csv-column` pour spécifier le bon nom :

```bash
--csv-column "Code_Postal"
```

### Aucune zone trouvée

Vérifiez que :
1. Les codes dans votre CSV correspondent exactement à ceux du shapefile
2. Le format est identique (ex: "59" vs "59000")
3. Pas d'espaces avant/après les codes

## 📝 License

Projet développé par Bib Batteries

## 🤝 Contribution

Pour toute question ou suggestion, contactez l'équipe de développement.
