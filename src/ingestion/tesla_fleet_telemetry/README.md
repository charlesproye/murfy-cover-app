# Tesla Fleet Telemetry

Module d'ingestion temps réel pour les flux de télémétrie Tesla. Il consomme un topic Kafka,
met les messages en tampon par VIN, puis pousse des lots JSON sur S3.

## Exécution locale

```bash
uv run python -m src.ingestion.tesla_fleet_telemetry.main [options]
```

Options utiles :

- `--topic` / `TESLA_FLEET_KAFKA_TOPIC` (défaut `tesla_V`)
- `--group-id` / `TESLA_FLEET_KAFKA_GROUP_ID`
- `--bootstrap-servers` / `TESLA_FLEET_KAFKA_BOOTSTRAP_SERVERS`
- `--auto-offset-reset` (`latest` par défaut)
- `--buffer-flush-interval` (secondes, 30 par défaut)
- `--verbose`

Seules les variables S3 (`S3_ENDPOINT`, `S3_BUCKET`, `S3_KEY`, `S3_SECRET`, `S3_REGION`) sont
obligatoires côté environnement.

## Fonctionnement

1. `KafkaConsumer` lit les messages et les passe à `process_message`.
2. `add_to_buffer` groupe par VIN, applique le backpressure, et déclenche `save_data_to_s3`
   lorsque la taille/latence dépasse les bornes (`MAX_BUFFER_SIZE` ou `FLUSH_INTERVAL_SECONDS`).
3. Les documents sont écrits sur `response/tesla-fleet-telemetry/{VIN}/temp/`.
4. La tâche `cleanup_inactive_vins` libère les buffers de VIN inactifs.

## Maintenance

- Logs principaux : `tesla-fleet-telemetry`, `kafka-consumer`, `s3-handler`.
- Arrêter proprement le service laisse `graceful_shutdown()` vider les buffers et
  fermer l'AsyncS3 client.
- Pour la production, configurez les secrets via Kubernetes secrets `tesla-secret` et `s3-secret`.
