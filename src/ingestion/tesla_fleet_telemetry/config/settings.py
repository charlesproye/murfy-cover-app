import os
from dataclasses import dataclass


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

    # Path settings
    base_s3_path: str = "response/tesla-fleet-telemetry"


def get_settings() -> Settings:
    """
    Récupère la configuration à partir des variables d'environnement.

    Returns:
        Settings: La configuration pour le module tesla-fleet-telemetry.
    """
    # Valeurs par défaut et validation
    kafka_bootstrap_servers = os.getenv(
        "TESLA_FLEET_KAFKA_BOOTSTRAP_SERVERS", "51.159.175.138:9092"
    )
    kafka_topic = os.getenv("TESLA_FLEET_KAFKA_TOPIC", "tesla_V")
    kafka_group_id = os.getenv(
        "TESLA_FLEET_KAFKA_GROUP_ID", "tesla-fleet-telemetry-consumer"
    )

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
        "S3_BUCKET": s3_bucket,
    }

    missing_params = [param for param, value in required_params.items() if not value]
    if missing_params:
        raise ValueError(f"Paramètres manquants: {', '.join(missing_params)}")

    # Validate HTTPS endpoint for security compliance (ISO27001)
    if not s3_endpoint.startswith("https://"):
        raise ValueError(
            f"S3_ENDPOINT must use HTTPS protocol to ensure encryption in transit. "
            f"Got: {s3_endpoint}. Please update to use https://"
        )

    # Paramètres additionnels
    base_s3_path = os.getenv(
        "TESLA_FLEET_S3_BASE_PATH", "response/tesla-fleet-telemetry"
    )
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
    )
