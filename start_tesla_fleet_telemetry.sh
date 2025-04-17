#!/bin/bash

# Fonction d'aide
show_help() {
    cat << EOF
Tesla Fleet Telemetry - Script de démarrage
==========================================

Description:
-----------
Ce script démarre le service d'ingestion des données de télémétrie Tesla.
Il gère la réception des données via Kafka et leur stockage dans S3.

Utilisation:
-----------
./start_tesla_fleet_telemetry.sh [options]

Options:
--------
--help                          Affiche cette aide
--verbose                       Active les logs détaillés
--auto-offset-reset=<value>     Position de départ dans Kafka ('latest' ou 'earliest')
--buffer-size=<value>           Taille du buffer par véhicule (défaut: 3000)
--buffer-flush-interval=<value> Intervalle de flush en secondes (défaut: 30)
--compress-now                  Compresse immédiatement les données et quitte

Exemples:
--------
1. Démarrage standard avec les derniers messages:
   ./start_tesla_fleet_telemetry.sh --auto-offset-reset=latest

2. Démarrage depuis le début avec logs détaillés:
   ./start_tesla_fleet_telemetry.sh --auto-offset-reset=earliest --verbose

3. Configuration personnalisée des buffers:
   ./start_tesla_fleet_telemetry.sh --buffer-size=2000 --buffer-flush-interval=60

4. Compression immédiate des données:
   ./start_tesla_fleet_telemetry.sh --compress-now

Stockage des données:
-------------------
1. Les données brutes sont stockées temporairement dans S3:
   s3://bucket/temp/{VIN}/data.json

2. Chaque nuit à minuit (UTC), les données sont:
   - Compressées au format Parquet
   - Organisées par date: s3://bucket/compressed/YYYY/MM/DD/{VIN}.parquet
   - Les fichiers temporaires sont nettoyés

Notes:
-----
- Le script nécessite l'environnement conda 'data_ev_transform'
- Les variables d'environnement doivent être configurées (voir .env)
- La compression nocturne s'effectue à minuit UTC
EOF
}

# Vérifier si l'aide est demandée
if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    show_help
    exit 0
fi

# Charger les variables d'environnement depuis .env si disponible
if [ -f .env ]; then
    echo "Chargement des variables d'environnement depuis .env"
    source .env
fi

# Configuration par défaut
export PYTHONOPTIMIZE=1
export PYTHONUNBUFFERED=1

# Variables d'environnement pour Kafka
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-51.159.175.138:9092}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-tesla_V}"
export KAFKA_GROUP_ID="tesla-fleet-telemetry-consumer-$RANDOM"

# Vérification des variables S3 requises
if [ -z "$S3_ENDPOINT" ] || [ -z "$S3_KEY" ] || [ -z "$S3_SECRET" ] || [ -z "$S3_BUCKET" ]; then
    echo "AVERTISSEMENT: Variables S3 manquantes. Vérifiez que S3_ENDPOINT, S3_KEY, S3_SECRET et S3_BUCKET sont définies."
fi

# Augmenter les limites système
ulimit -n 65535 2>/dev/null || true
ulimit -s unlimited 2>/dev/null || true

echo "=== Configuration Tesla Fleet Telemetry ==="
echo "Kafka Bootstrap Servers: $KAFKA_BOOTSTRAP_SERVERS"
echo "Kafka Topic: $KAFKA_TOPIC"
echo "Kafka Group ID: $KAFKA_GROUP_ID"
echo "Auto Offset Reset: ${AUTO_OFFSET_RESET:-latest}"
echo "Buffer Size: ${BUFFER_SIZE:-3000}"
echo "Buffer Flush Interval: ${BUFFER_FLUSH_INTERVAL:-30}"

echo "Démarrage du module tesla-fleet-telemetry..."
python -m src.ingestion.tesla_fleet_telemetry.main \
    --bootstrap-servers "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$KAFKA_TOPIC" \
    --group-id "$KAFKA_GROUP_ID" \
    "$@"    
