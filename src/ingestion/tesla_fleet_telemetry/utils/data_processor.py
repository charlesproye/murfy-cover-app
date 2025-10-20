import json
import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger("data-processor")


def process_telemetry_data(raw_data: dict[str, Any]) -> dict[str, Any] | None:
    """
    Traite les données brutes de télémétrie Tesla pour les normaliser et les préparer
    pour le stockage dans S3.

    Args:
        raw_data: Données brutes de télémétrie au format JSON

    Returns:
        Dict contenant les données de télémétrie traitées, ou None si les données sont invalides
    """
    try:
        # Vérification des champs obligatoires
        if not all(key in raw_data for key in ["vin", "createdAt", "data"]):
            logger.warning(
                "Données de télémétrie incomplètes: manque vin, createdAt ou data"
            )
            return None

        # Extraction des champs principaux
        vin = raw_data.get("vin")
        created_at = raw_data.get("createdAt")
        data_items = raw_data.get("data", [])
        is_resend = raw_data.get("isResend", False)

        # Validation du VIN
        if not vin or not isinstance(vin, str) or len(vin) < 5:
            logger.warning(f"VIN invalide: {vin}")
            return None

        # Validation de la date
        try:
            if isinstance(created_at, str):
                # Format ISO (2025-03-19T13:16:03.209653476Z)
                created_at_dt = datetime.fromisoformat(
                    created_at.replace("Z", "+00:00")
                )
                readable_date = created_at_dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                logger.warning(f"Format de date non pris en charge: {created_at}")
                readable_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError) as e:
            logger.warning(f"Erreur de parsing de la date {created_at}: {e!s}")
            readable_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Extraction des paires clé-valeur des données
        metrics = {}

        if not data_items or not isinstance(data_items, list):
            logger.warning(
                f"Données de télémétrie vides ou format invalide pour le VIN {vin}"
            )
            return None

        for item in data_items:
            if not isinstance(item, dict):
                continue

            key = item.get("key")
            value_container = item.get("value", {})

            if not key or not value_container:
                continue

            # Extraction de la valeur selon son type
            if "stringValue" in value_container:
                value = value_container["stringValue"]
            elif "numberValue" in value_container:
                value = value_container["numberValue"]
            elif "boolValue" in value_container:
                value = value_container["boolValue"]
            elif value_container.get("invalid"):
                value = None
            else:
                # Type de valeur inconnu
                continue

            metrics[key] = value

        # Si aucune métrique n'a été extraite, on ignore ce message
        if not metrics:
            logger.warning(f"Aucune métrique valide extraite pour le VIN {vin}")
            return None

        # Construction du document final
        processed_data = {
            "vin": vin,
            "created_at": created_at,
            "readable_date": readable_date,
            "metrics": metrics,
            "is_resend": is_resend,
            "processed_at": datetime.now().isoformat(),
            "data_source": "tesla-fleet-telemetry",
        }

        return processed_data

    except Exception as e:
        logger.error(f"Erreur de traitement des données: {e!s}")
        logger.debug(f"Données brutes: {json.dumps(raw_data)[:200]}...")
        return None


def extract_value_from_variant(value_container: dict[str, Any]) -> Any:
    """
    Extrait la valeur d'un conteneur de valeur au format variant.

    Args:
        value_container: Dictionnaire contenant la valeur avec son type

    Returns:
        La valeur extraite
    """
    if "stringValue" in value_container:
        return value_container["stringValue"]
    elif "numberValue" in value_container:
        return value_container["numberValue"]
    elif "boolValue" in value_container:
        return value_container["boolValue"]
    elif value_container.get("invalid"):
        return None
    else:
        return None

