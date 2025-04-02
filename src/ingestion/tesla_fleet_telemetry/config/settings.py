import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class Settings:
    """
    Configuration pour le module tesla-fleet-telemetry.
    Contient tous les paramètres nécessaires pour l'exécution du programme.
    """
    # Kafka settings
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_group_id: str
    
    # S3 settings
    s3_endpoint: str
    s3_region: str
    s3_key: str
    s3_secret: str
    s3_bucket: str
    
    # Application settings
    flush_interval_seconds: int = 300  # 5 minutes
    flush_batch_size: int = 100
    
    # Path settings
    base_s3_path: str = "response/tesla-fleet-telemetry"
    
    # Compression settings
    compression_enabled: bool = True
    compression_retention_days: int = 30


def get_settings() -> Settings:
    """
    Récupère la configuration à partir des variables d'environnement.
    
    Returns:
        Settings: La configuration pour le module tesla-fleet-telemetry.
    """
    # Valeurs par défaut et validation
    kafka_bootstrap_servers = os.getenv("TESLA_FLEET_KAFKA_BOOTSTRAP_SERVERS", "51.159.175.138:9092")
    kafka_topic = os.getenv("TESLA_FLEET_KAFKA_TOPIC", "tesla_V")
    kafka_group_id = os.getenv("TESLA_FLEET_KAFKA_GROUP_ID", "tesla-fleet-telemetry-consumer")
    
    s3_endpoint = os.getenv("S3_ENDPOINT")
    s3_region = os.getenv("S3_REGION", "fr-par")
    s3_key = os.getenv("S3_KEY")
    s3_secret = os.getenv("S3_SECRET")
    s3_bucket = os.getenv("S3_BUCKET")
    
    # Validation des paramètres obligatoires
    required_params = {
        "S3_ENDPOINT": s3_endpoint,
        "S3_KEY": s3_key,
        "S3_SECRET": s3_secret,
        "S3_BUCKET": s3_bucket
    }
    
    missing_params = [param for param, value in required_params.items() if not value]
    if missing_params:
        raise ValueError(f"Paramètres manquants: {', '.join(missing_params)}")
    
    # Paramètres additionnels
    base_s3_path = os.getenv("TESLA_FLEET_S3_BASE_PATH", "response/tesla-fleet-telemetry")
    flush_interval_seconds = int(os.getenv("TESLA_FLEET_FLUSH_INTERVAL_SECONDS", "300"))
    flush_batch_size = int(os.getenv("TESLA_FLEET_FLUSH_BATCH_SIZE", "100"))
    compression_enabled = os.getenv("TESLA_FLEET_COMPRESSION_ENABLED", "1") == "1"
    compression_retention_days = int(os.getenv("TESLA_FLEET_COMPRESSION_RETENTION_DAYS", "30"))
    
    return Settings(
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        kafka_topic=kafka_topic,
        kafka_group_id=kafka_group_id,
        s3_endpoint=s3_endpoint,
        s3_region=s3_region,
        s3_key=s3_key,
        s3_secret=s3_secret,
        s3_bucket=s3_bucket,
        base_s3_path=base_s3_path,
        flush_interval_seconds=flush_interval_seconds,
        flush_batch_size=flush_batch_size,
        compression_enabled=compression_enabled,
        compression_retention_days=compression_retention_days
    ) 
