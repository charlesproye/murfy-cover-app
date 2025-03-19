import os
import json
import logging
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Set
import random
from botocore.client import Config
from botocore.exceptions import ClientError

import aioboto3
import boto3

from src.ingestion.tesla_fleet_telemetry.config.settings import get_settings

logger = logging.getLogger("s3-handler")

# Variables globales pour le cache des clients S3
_s3_async_client = None
_s3_sync_client = None
# Cache des fichiers déjà compressés pour éviter de retraiter les mêmes données
_compressed_files_cache: Dict[str, Set[str]] = {}


async def get_s3_async_client():
    """
    Retourne un client S3 asynchrone, en le réutilisant s'il existe déjà.
    """
    global _s3_async_client
    
    if _s3_async_client is not None:
        return _s3_async_client
    
    settings = get_settings()
    session = aioboto3.Session()
    
    _s3_async_client = await session.client(
        's3',
        region_name=settings.s3_region,
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_key,
        aws_secret_access_key=settings.s3_secret,
        config=Config(
            signature_version='s3v4',
            s3={'addressing_style': 'path'},
            retries={'max_attempts': 3}
        )
    ).__aenter__()
    
    return _s3_async_client


def get_s3_sync_client():
    """
    Retourne un client S3 synchrone, en le réutilisant s'il existe déjà.
    """
    global _s3_sync_client
    
    if _s3_sync_client is not None:
        return _s3_sync_client
    
    settings = get_settings()
    
    _s3_sync_client = boto3.client(
        's3',
        region_name=settings.s3_region,
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_key,
        aws_secret_access_key=settings.s3_secret,
        config=Config(
            signature_version='s3v4',
            s3={'addressing_style': 'path'},
            retries={'max_attempts': 3}
        )
    )
    
    return _s3_sync_client


async def save_data_to_s3(data: List[Dict[str, Any]], vin: str) -> bool:
    """
    Sauvegarde des données de télémétrie dans le dossier temp de S3.
    
    Optimisé pour minimiser les écritures S3 en regroupant les données par heure.
    
    Args:
        data: Liste de données de télémétrie à sauvegarder
        vin: VIN du véhicule
        
    Returns:
        bool: True si sauvegarde réussie, False sinon
    """
    if not data or not vin:
        logger.warning("Aucune donnée à sauvegarder ou VIN manquant")
        return False
    
    settings = get_settings()
    s3_client = await get_s3_async_client()
    bucket_name = settings.s3_bucket
    
    # Organiser les données par heure pour regrouper les écritures
    data_by_hour = {}
    for item in data:
        readable_date = item.get('readable_date', '')
        if readable_date:
            try:
                # Extraire l'heure au format YYYY-MM-DD_HH
                hour_str = datetime.strptime(readable_date, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d_%H")
                if hour_str not in data_by_hour:
                    data_by_hour[hour_str] = []
                data_by_hour[hour_str].append(item)
            except Exception:
                # Fallback si le format n'est pas celui attendu
                current_hour = datetime.now().strftime("%Y-%m-%d_%H")
                if current_hour not in data_by_hour:
                    data_by_hour[current_hour] = []
                data_by_hour[current_hour].append(item)
        else:
            # Utiliser l'heure actuelle si pas de date lisible
            current_hour = datetime.now().strftime("%Y-%m-%d_%H")
            if current_hour not in data_by_hour:
                data_by_hour[current_hour] = []
            data_by_hour[current_hour].append(item)
    
    # Écrire un fichier par heure pour réduire le nombre d'opérations S3
    success = True
    save_tasks = []
    
    for hour_str, hour_data in data_by_hour.items():
        key = f"{settings.base_s3_path}/{vin}/temp/{hour_str}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.json"
        save_tasks.append(save_object_to_s3(s3_client, bucket_name, key, hour_data))
    
    # Exécuter les tâches en parallèle
    if save_tasks:
        results = await asyncio.gather(*save_tasks, return_exceptions=True)
        success = all(result == True for result in results if not isinstance(result, Exception))
        
        if not success:
            errors = [str(result) for result in results if isinstance(result, Exception)]
            logger.error(f"Erreurs lors de la sauvegarde des données: {errors}")
    
    return success


async def save_object_to_s3(s3_client, bucket_name: str, key: str, data: Any) -> bool:
    """
    Sauvegarde un objet dans S3 avec retry en cas d'échec.
    
    Args:
        s3_client: Client S3 aioboto3
        bucket_name: Nom du bucket S3
        key: Clé S3 (chemin du fichier)
        data: Données à sauvegarder
        
    Returns:
        bool: True si sauvegarde réussie, False sinon
    """
    try:
        await s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json.dumps(data),
            ContentType='application/json'
        )
        logger.debug(f"Données sauvegardées avec succès dans {key}")
        return True
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde des données dans S3: {str(e)}")
        return False


async def compress_data() -> bool:
    """
    Compresse les données temporaires de tous les véhicules pour optimiser le stockage.
    Les fichiers temp sont regroupés par date et sauvegardés dans un seul fichier,
    puis les fichiers temporaires sont supprimés.
    
    Returns:
        bool: True si compression réussie, False sinon
    """
    logger.info("Démarrage de la compression des données")
    
    settings = get_settings()
    s3_client = await get_s3_async_client()
    bucket_name = settings.s3_bucket
    base_path = settings.base_s3_path
    
    try:
        # Récupérer la liste des préfixes des véhicules
        response = await s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=f"{base_path}/",
            Delimiter="/"
        )
        
        if 'CommonPrefixes' not in response:
            logger.info("Aucun véhicule trouvé à compresser")
            return True
            
        # Créer les tâches de compression pour chaque véhicule
        compression_tasks = []
        for prefix in response.get('CommonPrefixes', []):
            vehicle_prefix = prefix.get('Prefix', '')
            if vehicle_prefix:
                vin = vehicle_prefix.split('/')[-2]
                task = asyncio.create_task(compress_vehicle_data(s3_client, bucket_name, vin))
                compression_tasks.append(task)
        
        if not compression_tasks:
            logger.info("Aucune tâche de compression créée")
            return True
            
        # Utiliser un semaphore pour limiter la concurrence
        semaphore = asyncio.Semaphore(50)  # Max 50 tâches en parallèle
        
        async def compress_with_semaphore(task):
            async with semaphore:
                return await task
                
        # Exécuter les tâches de compression
        results = await asyncio.gather(
            *[compress_with_semaphore(task) for task in compression_tasks],
            return_exceptions=True
        )
        
        # Vérifier s'il y a eu des erreurs
        success_count = sum(1 for r in results if r is True)
        error_count = sum(1 for r in results if isinstance(r, Exception))
        
        logger.info(f"Compression terminée: {success_count} véhicules traités avec succès, {error_count} erreurs")
        
        if error_count > 0:
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Erreur de compression du véhicule {i}: {str(result)}")
        
        # Nettoyer le cache des fichiers compressés une fois par jour
        if datetime.now().hour == 4:  # À 4h du matin
            _compressed_files_cache.clear()
            logger.info("Cache des fichiers compressés nettoyé")
        
        return error_count == 0
        
    except Exception as e:
        logger.error(f"Erreur lors de la compression des données: {str(e)}")
        return False


async def compress_vehicle_data(s3_client, bucket_name: str, vin: str) -> bool:
    """
    Compresse les données temporaires d'un véhicule spécifique.
    
    Args:
        s3_client: Client S3 aioboto3
        bucket_name: Nom du bucket S3
        vin: VIN du véhicule
        
    Returns:
        bool: True si compression réussie, False sinon
    """
    global _compressed_files_cache
    
    settings = get_settings()
    temp_folder = f"{settings.base_s3_path}/{vin}/temp/"
    
    try:
        # Lister les fichiers temporaires
        response = await s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=temp_folder
        )
        
        if 'Contents' not in response or not response['Contents']:
            logger.debug(f"Aucun fichier temporaire à compresser pour le véhicule {vin}")
            return True
            
        # Initialiser le cache pour ce VIN s'il n'existe pas
        if vin not in _compressed_files_cache:
            _compressed_files_cache[vin] = set()
        
        # Filtrer les fichiers déjà traités
        new_files = []
        for obj in response.get('Contents', []):
            file_key = obj['Key']
            if file_key not in _compressed_files_cache[vin]:
                new_files.append(obj)
        
        if not new_files:
            logger.debug(f"Pas de nouveaux fichiers temporaires à compresser pour le véhicule {vin}")
            return True
        
        logger.info(f"Compression de {len(new_files)} nouveaux fichiers pour le véhicule {vin}")
        
        # Organiser les données par date
        data_by_date = {}
        files_to_delete = []
        
        for obj in new_files:
            try:
                file_key = obj['Key']
                file_response = await s3_client.get_object(Bucket=bucket_name, Key=file_key)
                file_content = await file_response['Body'].read()
                
                try:
                    file_data = json.loads(file_content)
                except json.JSONDecodeError:
                    logger.error(f"JSON corrompu dans le fichier {file_key}, suppression...")
                    files_to_delete.append(file_key)
                    continue
                
                # S'assurer que file_data est une liste
                if not isinstance(file_data, list):
                    if isinstance(file_data, dict):
                        file_data = [file_data]
                    else:
                        logger.error(f"Format de données invalide dans {file_key}, suppression...")
                        files_to_delete.append(file_key)
                        continue
                
                # Traiter chaque élément dans le fichier
                for item in file_data:
                    if not isinstance(item, dict):
                        continue
                        
                    readable_date = item.get('readable_date')
                    if not readable_date:
                        logger.warning(f"Date lisible manquante dans {file_key}, élément ignoré")
                        continue
                        
                    # Extraire la date (YYYY-MM-DD)
                    file_date = readable_date.split()[0]
                    
                    if file_date not in data_by_date:
                        data_by_date[file_date] = []
                    
                    data_by_date[file_date].append(item)
                
                # Ajouter le fichier à la liste de suppression et marquer comme traité
                files_to_delete.append(file_key)
                _compressed_files_cache[vin].add(file_key)
                
            except Exception as e:
                logger.error(f"Erreur lors du traitement du fichier {obj.get('Key')}: {str(e)}")
                files_to_delete.append(obj['Key'])
                _compressed_files_cache[vin].add(obj['Key'])  # Marquer comme traité pour éviter des erreurs répétées
                continue
        
        # Si aucune donnée valide n'a été trouvée
        if not data_by_date:
            logger.warning(f"Aucune donnée valide à compresser pour le véhicule {vin}")
            
            # Nettoyer les fichiers temporaires invalides
            if files_to_delete:
                delete_tasks = [delete_with_retry(s3_client, bucket_name, key) for key in files_to_delete]
                await asyncio.gather(*delete_tasks)
                
            return True
            
        # Sauvegarder les données compressées par date
        save_tasks = []
        
        for date, items in data_by_date.items():
            if items:
                # Si un fichier existe déjà pour cette date, fusionner les données
                target_key = f"{settings.base_s3_path}/{vin}/{date}.json"
                
                try:
                    # Vérifier si le fichier existe déjà
                    try:
                        existing_response = await s3_client.get_object(Bucket=bucket_name, Key=target_key)
                        existing_content = await existing_response['Body'].read()
                        existing_data = json.loads(existing_content)
                        
                        # Fusionner avec les données existantes
                        if isinstance(existing_data, list):
                            items = existing_data + items
                    except ClientError as e:
                        # Le fichier n'existe pas, c'est normal
                        if e.response['Error']['Code'] != 'NoSuchKey':
                            raise
                
                    # Sauvegarder les données fusionnées
                    save_tasks.append(
                        save_with_retry(s3_client, bucket_name, target_key, items)
                    )
                    
                except Exception as e:
                    logger.error(f"Erreur lors de la fusion des données pour {vin}/{date}: {str(e)}")
        
        # Supprimer les fichiers temporaires
        delete_tasks = [delete_with_retry(s3_client, bucket_name, key) for key in files_to_delete]
        
        # Exécuter toutes les tâches
        await asyncio.gather(*save_tasks, *delete_tasks)
        
        logger.info(f"Compression réussie pour le véhicule {vin}: {len(files_to_delete)} fichiers compressés en {len(data_by_date)} dates")
        return True
        
    except Exception as e:
        logger.error(f"Erreur lors de la compression des données du véhicule {vin}: {str(e)}")
        return False


async def save_with_retry(s3_client, bucket_name: str, key: str, data: Any, max_retries: int = 3) -> bool:
    """
    Sauvegarde des données dans S3 avec retry en cas d'échec.
    
    Args:
        s3_client: Client S3 aioboto3
        bucket_name: Nom du bucket S3
        key: Clé S3 (chemin du fichier)
        data: Données à sauvegarder
        max_retries: Nombre maximum de tentatives
        
    Returns:
        bool: True si sauvegarde réussie, False sinon
    """
    for attempt in range(max_retries):
        try:
            await s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=json.dumps(data),
                ContentType='application/json'
            )
            return True
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Échec de la sauvegarde dans S3 après {max_retries} tentatives. Clé: {key}, Erreur: {str(e)}")
                raise
            await asyncio.sleep(random.uniform(0.1, 0.5) * (attempt + 1))
    
    return False


async def delete_with_retry(s3_client, bucket_name: str, key: str, max_retries: int = 3) -> bool:
    """
    Supprime un fichier S3 avec retry en cas d'échec.
    
    Args:
        s3_client: Client S3 aioboto3
        bucket_name: Nom du bucket S3
        key: Clé S3 (chemin du fichier)
        max_retries: Nombre maximum de tentatives
        
    Returns:
        bool: True si suppression réussie, False sinon
    """
    for attempt in range(max_retries):
        try:
            await s3_client.delete_object(Bucket=bucket_name, Key=key)
            return True
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Échec de la suppression dans S3 après {max_retries} tentatives. Clé: {key}, Erreur: {str(e)}")
                raise
            await asyncio.sleep(random.uniform(0.1, 0.5) * (attempt + 1))
    
    return False


async def cleanup_old_data(retention_days: int = 30) -> bool:
    """
    Nettoie les anciennes données au-delà de la période de rétention.
    
    Args:
        retention_days: Nombre de jours de rétention des données
        
    Returns:
        bool: True si nettoyage réussi, False sinon
    """
    logger.info(f"Démarrage du nettoyage des données plus anciennes que {retention_days} jours")
    
    settings = get_settings()
    s3_client = await get_s3_async_client()
    bucket_name = settings.s3_bucket
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    
    try:
        # Liste tous les préfixes de véhicules
        response = await s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=f"{settings.base_s3_path}/",
            Delimiter="/"
        )
        
        if 'CommonPrefixes' not in response:
            logger.info("Aucun véhicule trouvé pour le nettoyage")
            return True
            
        # Créer les tâches de nettoyage pour chaque véhicule
        cleanup_tasks = []
        for prefix in response.get('CommonPrefixes', []):
            vehicle_prefix = prefix.get('Prefix', '')
            if vehicle_prefix:
                vin = vehicle_prefix.split('/')[-2]
                task = asyncio.create_task(cleanup_vehicle_data(s3_client, bucket_name, vin, cutoff_date))
                cleanup_tasks.append(task)
        
        if not cleanup_tasks:
            logger.info("Aucune tâche de nettoyage créée")
            return True
            
        # Exécuter les tâches de nettoyage
        results = await asyncio.gather(*cleanup_tasks, return_exceptions=True)
        
        # Vérifier s'il y a eu des erreurs
        success_count = sum(1 for r in results if r is True)
        error_count = sum(1 for r in results if isinstance(r, Exception))
        
        logger.info(f"Nettoyage terminé: {success_count} véhicules traités avec succès, {error_count} erreurs")
        
        return error_count == 0
        
    except Exception as e:
        logger.error(f"Erreur lors du nettoyage des données: {str(e)}")
        return False


async def cleanup_vehicle_data(s3_client, bucket_name: str, vin: str, cutoff_date: datetime) -> bool:
    """
    Nettoie les anciennes données d'un véhicule spécifique.
    
    Args:
        s3_client: Client S3 aioboto3
        bucket_name: Nom du bucket S3
        vin: VIN du véhicule
        cutoff_date: Date limite de conservation
        
    Returns:
        bool: True si nettoyage réussi, False sinon
    """
    settings = get_settings()
    vehicle_prefix = f"{settings.base_s3_path}/{vin}/"
    
    try:
        # Lister tous les fichiers du véhicule (hors temp)
        response = await s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=vehicle_prefix
        )
        
        if 'Contents' not in response or not response['Contents']:
            return True
            
        files_to_delete = []
        
        for obj in response['Contents']:
            key = obj['Key']
            
            # Ignorer les fichiers temp
            if "/temp/" in key:
                continue
                
            # Extraire la date du nom de fichier (YYYY-MM-DD.json)
            try:
                filename = key.split('/')[-1]
                if not filename.endswith('.json'):
                    continue
                    
                date_str = filename[:-5]  # Enlever le .json
                file_date = datetime.strptime(date_str, "%Y-%m-%d")
                
                if file_date < cutoff_date:
                    files_to_delete.append(key)
                    
            except (ValueError, IndexError) as e:
                logger.warning(f"Impossible de parser la date du fichier {key}: {str(e)}")
                continue
        
        if not files_to_delete:
            logger.debug(f"Aucun fichier à supprimer pour le véhicule {vin}")
            return True
            
        # Supprimer les fichiers par lots pour éviter de surcharger S3
        batch_size = 100
        for i in range(0, len(files_to_delete), batch_size):
            batch = files_to_delete[i:i+batch_size]
            delete_tasks = [delete_with_retry(s3_client, bucket_name, key) for key in batch]
            await asyncio.gather(*delete_tasks)
            
        logger.info(f"Nettoyage réussi pour le véhicule {vin}: {len(files_to_delete)} fichiers supprimés")
        return True
        
    except Exception as e:
        logger.error(f"Erreur lors du nettoyage des données du véhicule {vin}: {str(e)}")
        return False 
