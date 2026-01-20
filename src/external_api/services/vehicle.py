"""Service for accessing vehicle data"""

import logging

from fastapi import HTTPException, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# Importer les modèles de base de données nécessaires
from external_api.schemas.vehicle import DynamicVehicleData, StaticVehicleData

logger = logging.getLogger(__name__)


async def get_static_vehicle_data(db: AsyncSession, vin: str) -> StaticVehicleData:
    """
    Récupère les données statiques d'un véhicule par son VIN.

    Args:
        db: Session de base de données asynchrone
        vin: Numéro d'identification du véhicule

    Returns:
        Données statiques du véhicule
    """
    logger.info(f"Récupération des données statiques pour le VIN {vin}")

    try:
        # Utiliser une requête SQL directe basée sur les tables définies dans le modèle
        query = text("""
        SELECT
            v.vin,
            v.start_date,
            m.make_name,
            vm.model_name,
            vm.version,
            vm.type,
            b.battery_chemistry,
            b.capacity,
            b.net_capacity,
            vm.autonomy,
            vm.warranty_date,
            vm.warranty_km,
            vm.source
        FROM vehicle v
        JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
        LEFT JOIN battery b ON vm.battery_id = b.id
        LEFT JOIN make m ON vm.make_id = m.id
        LEFT JOIN oem o ON vm.oem_id = o.id
        WHERE v.vin = :vin
        """)

        result = await db.execute(query, {"vin": vin})
        record = result.fetchone()

        if not record:
            logger.warning(f"Aucun véhicule trouvé pour le VIN {vin}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Véhicule non trouvé"
            )

        # Créer le dictionnaire de données statiques en utilisant exactement les noms des colonnes de la base
        data = {
            "vin": record.vin,
            "start_date": record.start_date,
            "make_name": record.make_name,
            "model_name": record.model_name,
            "version": record.version,
            "type": record.type,
            "battery_chemistry": record.battery_chemistry,
            "capacity": float(record.capacity) if record.capacity is not None else None,
            "net_capacity": float(record.net_capacity)
            if record.net_capacity is not None
            else None,
            "autonomy": record.autonomy,
            "warranty_date": record.warranty_date,
            "warranty_km": float(record.warranty_km)
            if record.warranty_km is not None
            else None,
            "source": record.source,
        }

        return StaticVehicleData(**data)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Erreur lors de la récupération des données statiques pour le VIN {vin}: {e!s}"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la récupération des données du véhicule",
        )


async def get_dynamic_vehicle_data(db: AsyncSession, vin: str) -> DynamicVehicleData:
    """
    Récupère les données dynamiques d'un véhicule par son VIN.

    Args:
        db: Session de base de données asynchrone
        vin: Numéro d'identification du véhicule

    Returns:
        Données dynamiques du véhicule
    """
    logger.info(f"Récupération des données dynamiques pour le VIN {vin}")

    try:
        # D'abord, vérifier si le véhicule existe et récupérer son ID
        vehicle_query = text("""
        SELECT id, region_id FROM vehicle WHERE vin = :vin
        """)

        vehicle_result = await db.execute(vehicle_query, {"vin": vin})
        vehicle_record = vehicle_result.fetchone()

        if not vehicle_record:
            logger.warning(f"Aucun véhicule trouvé pour le VIN {vin}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Véhicule non trouvé"
            )

        vehicle_id = vehicle_record.id
        region_id = vehicle_record.region_id

        # Récupérer les données de région
        region_query = text("""
        SELECT region_name
        FROM region
        WHERE id = :region_id
        """)

        region_result = await db.execute(region_query, {"region_id": region_id})
        region_record = region_result.fetchone()
        region_name = region_record.region_name if region_record else None

        # Récupérer les dernières données du véhicule
        data_query = text("""
        SELECT
            odometer,
            region,
            speed,
            location,
            soh_bib,
            cycles,
            consumption,
            soh_comparison,
            timestamp,
            level_1,
            level_2,
            level_3,
            soh_oem
        FROM vehicle_data
        WHERE vehicle_id = :vehicle_id
        ORDER BY timestamp DESC
        LIMIT 1
        """)

        data_result = await db.execute(data_query, {"vehicle_id": vehicle_id})
        vehicle_data = data_result.fetchone()

        if not vehicle_data:
            logger.warning(f"Aucune donnée trouvée pour le véhicule {vin}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Données du véhicule non disponibles",
            )

        # Créer le dictionnaire de données dynamiques
        data = {
            "vin": vin,
            "odometer": float(vehicle_data.odometer)
            if vehicle_data.odometer is not None
            else None,
            "region": region_name or vehicle_data.region,
            "speed": float(vehicle_data.speed)
            if vehicle_data.speed is not None
            else None,
            "location": vehicle_data.location,
            "soh": float(vehicle_data.soh_bib)
            if vehicle_data.soh_bib is not None
            else None,
            "cycles": float(vehicle_data.cycles)
            if vehicle_data.cycles is not None
            else None,
            "consumption": float(vehicle_data.consumption)
            if vehicle_data.consumption is not None
            else None,
            "soh_comparison": float(vehicle_data.soh_comparison)
            if vehicle_data.soh_comparison is not None
            else None,
            "timestamp": vehicle_data.timestamp,
            "level_1": float(vehicle_data.level_1)
            if vehicle_data.level_1 is not None
            else None,
            "level_2": float(vehicle_data.level_2)
            if vehicle_data.level_2 is not None
            else None,
            "level_3": float(vehicle_data.level_3)
            if vehicle_data.level_3 is not None
            else None,
            "soh_oem": float(vehicle_data.soh_oem)
            if vehicle_data.soh_oem is not None
            else None,
        }

        return DynamicVehicleData(**data)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Erreur lors de la récupération des données dynamiques pour le VIN {vin}: {e!s}"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la récupération des données du véhicule",
        )


async def check_vehicle_eligibility(db: AsyncSession, vin: str) -> dict:
    """
    Vérifie si un véhicule existe, s'il est éligible et s'il est déjà activé.

    Args:
        db: Session de base de données asynchrone
        vin: Numéro d'identification du véhicule

    Returns:
        Dictionnaire contenant les informations sur l'existence, l'éligibilité et l'activation du véhicule
    """
    logger.info(f"Vérification de l'éligibilité pour le VIN {vin}")

    try:
        # Vérifier si le véhicule existe et récupérer son statut d'éligibilité et d'activation
        query = text("""
        SELECT
            v.id,
            v.is_eligible,
            v.activation_status
        FROM vehicle v
        WHERE v.vin = :vin
        """)

        result = await db.execute(query, {"vin": vin})
        record = result.fetchone()

        if not record:
            # Le véhicule n'existe pas dans la base de données
            return {"exists": False, "is_eligible": False, "is_activated": False}

        # Le véhicule existe, vérifier son éligibilité et son statut d'activation
        return {
            "exists": True,
            "is_eligible": bool(record.is_eligible),
            "is_activated": bool(record.activation_status),
        }

    except Exception as e:
        logger.error(
            f"Erreur lors de la vérification de l'éligibilité pour le VIN {vin}: {e!s}"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la vérification de l'éligibilité du véhicule",
        )
