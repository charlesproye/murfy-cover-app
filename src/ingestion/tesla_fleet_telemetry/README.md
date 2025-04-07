### Transform:

`tramsform` contains all the modules, for valorizing the data (cleaning, segmentation, ...).
See [transform readme](src/transform/readme.md).

# Tesla Fleet Telemetry

Ce module gère l'ingestion des données de télémétrie de la flotte Tesla via Kafka et leur stockage dans S3.

## Architecture

Le système est composé de trois composants principaux :

1. **Kafka Consumer** : Consomme les messages de télémétrie en temps réel
2. **Buffer Manager** : Gère le stockage temporaire des messages en mémoire
3. **S3 Handler** : Gère le stockage et la compression des données dans S3

### Flux de données

```
Kafka → Buffer (Mémoire) → S3 Temp → S3 Compressed
```

## Installation

### Prérequis

- Python 3.8+
- Conda
- Accès à un broker Kafka
- Accès à un bucket S3

### Configuration de l'environnement

1. Créer l'environnement conda :

```bash
conda create -n data_ev_transform python=3.8
conda activate data_ev_transform
```

2. Installer les dépendances :

```bash
conda install -c conda-forge aiokafka aioboto3 python-dotenv
```

3. Configuration des variables d'environnement (.env) :

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=your-kafka-server:9092
KAFKA_TOPIC=tesla_V
KAFKA_GROUP_ID=tesla-fleet-telemetry-consumer

# S3
S3_ENDPOINT=your-s3-endpoint
S3_REGION=your-region
S3_BUCKET=your-bucket
S3_KEY=your-access-key
S3_SECRET=your-secret-key
```

## Utilisation

### Démarrage du service

```bash
./start_tesla_fleet_telemetry.sh [options]
```

Options disponibles :

- `--verbose` : Active les logs détaillés
- `--auto-offset-reset=latest|earliest` : Point de départ de la consommation
- `--buffer-size=N` : Taille max du buffer (défaut: 100)
- `--buffer-flush-interval=N` : Intervalle de flush en secondes (défaut: 10)
- `--compression-interval=N` : Intervalle de compression en secondes (défaut: 300)

### Meilleures pratiques

#### 1. Gestion des Offsets Kafka

- Utilisation de `enable.auto.commit=true` avec un intervalle de 5 secondes
- Sauvegarde des offsets dans Kafka plutôt qu'en local
- Point de reprise automatique après redémarrage

#### 2. Gestion du Buffer

- Taille de buffer recommandée : 1000 messages
- Intervalle de flush recommandé : 30 secondes
- Mécanisme de backpressure pour éviter la surcharge mémoire

#### 3. Stockage S3

- Structure des données :
  ```
  s3://bucket/
    └── response/
        └── tesla/
            └── VIN/
                ├── temp/           # Données brutes temporaires
                └── compressed/     # Données compressées par heure
  ```
- Compression toutes les 5 minutes
- Rétention des données temp : 24 heures
- Format de fichier : JSON compressé en GZIP

#### 4. Performance

- Utilisation de pools de workers pour la compression
- Limitation de la mémoire par véhicule
- Gestion des erreurs avec retry pattern

#### 5. Monitoring

- Métriques clés :
  - Messages par seconde
  - Latence de traitement
  - Taille des buffers
  - Taux de compression
  - Erreurs S3/Kafka

## Dimensionnement

Pour un système traitant 10,000 véhicules avec 10 points de données par minute :

- Débit : ~100,000 messages/minute
- Volume : ~144 millions messages/jour
- Stockage : ~50-100 Go/jour (non compressé)

Recommandations :

- Buffer size : 1000 messages/véhicule
- Flush interval : 30 secondes
- Workers : 50 max
- Compression interval : 5 minutes

## Troubleshooting

### Problèmes courants

1. **Perte de connexion Kafka**

   - Vérifier la connectivité réseau
   - Vérifier les logs pour les erreurs de timeout
   - Vérifier le statut du broker

2. **Erreurs S3**

   - Vérifier les credentials
   - Vérifier les quotas et limites
   - Vérifier l'espace disponible

3. **Problèmes de performance**
   - Ajuster la taille du buffer
   - Vérifier la charge CPU/mémoire
   - Ajuster le nombre de workers

### Logs

Les logs sont configurés avec différents niveaux :

- INFO : Opérations normales
- WARNING : Problèmes non critiques
- ERROR : Erreurs nécessitant une intervention
- DEBUG : Information détaillée (avec --verbose)

## Maintenance

### Tâches quotidiennes

- Vérifier les logs d'erreur
- Monitorer l'utilisation des ressources
- Vérifier la compression des données

### Tâches hebdomadaires

- Analyser les métriques de performance
- Vérifier la rétention des données
- Nettoyer les données temporaires obsolètes

## Support

Pour toute question ou problème :

1. Consulter les logs avec `--verbose`
2. Vérifier la configuration
3. Contacter l'équipe de support

