import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional, List
from itertools import islice

import boto3
import msgspec
from botocore.client import Config
from botocore.client import ClientError
from botocore.credentials import threading
from ingestion.mobilisights.schema import CarState, MergedCarState


class MobilisightsCompresser:
    __logger: logging.Logger
    __s3 = boto3.client("s3")
    __vehicles: dict[str, set[str]]

    threaded: bool = False
    max_workers: Optional[int] = 8
    batch_size: int = 50  # Taille du batch pour le traitement des VINs
    
    def __init__(
        self, s3, bucket, threaded: bool = False, max_workers: Optional[int] = 8, batch_size: int = 50
    ) -> None:
        self.__logger = logging.getLogger("COMPRESSER")
        self.__vehicles = {}
        
        # Configuration S3 optimisée
        config = Config(
            max_pool_connections=10,  # On reste sur 10 connexions max
            retries=dict(max_attempts=3),
            read_timeout=30,
            connect_timeout=30
        )
        
        if s3:
            self.__s3 = s3
        else:
            self.__s3 = boto3.client("s3", config=config)
            
        self.__bucket = bucket
        self.__connection_semaphore = asyncio.Semaphore(10)  # Contrôle des connexions
        self.threaded = threaded
        self.max_workers = max_workers
        self.batch_size = batch_size
        
        self.list_objects()

    def list_objects(self):
        self.__logger.info("Starting to list bucket objects")
        paginator = self.__s3.get_paginator("list_objects_v2")
        self.__vehicles = {}
        
        # Première passe : lister les VINs
        self.__logger.info("Listing VINs...")
        vins = set()
        prefix = "response/stellantis/"
        delimiter = '/'
        
        for page in paginator.paginate(Bucket=self.__bucket, Prefix=prefix, Delimiter=delimiter):
            if 'CommonPrefixes' in page:
                for prefix_obj in page['CommonPrefixes']:
                    vin = prefix_obj['Prefix'].split('/')[2]
                    if len(vin) == 17:
                        vins.add(vin)
        
        self.__logger.info(f"Found {len(vins)} VINs")

        # Traiter les VINs par lots pour éviter la surcharge mémoire
        batch_size = 5  # Traiter 5 VINs à la fois
        for i in range(0, len(vins), batch_size):
            vin_batch = list(vins)[i:i + batch_size]
            
            for vin in vin_batch:
                temp_files = set()
                temp_prefix = f"response/stellantis/{vin}/temp/"
                
                # Utiliser la pagination pour les fichiers temp
                file_count = 0
                for page in paginator.paginate(Bucket=self.__bucket, Prefix=temp_prefix, PaginationConfig={'PageSize': 1000}):
                    if 'Contents' not in page:
                        continue
                        
                    # Ajouter les fichiers par lots
                    new_files = {obj['Key'] for obj in page['Contents']}
                    temp_files.update(new_files)
                    file_count += len(new_files)
                    
                    # Log de progression tous les 5000 fichiers
                    if file_count % 5000 == 0:
                        self.__logger.info(f"VIN {vin}: processed {file_count} temp files so far...")
                
                if temp_files:
                    self.__vehicles[vin] = temp_files
                    self.__logger.info(f"Completed VIN {vin}: found {len(temp_files)} temp files")
            
            # Log de progression du batch
            self.__logger.info(f"Processed VINs {i+1}-{min(i+batch_size, len(vins))} of {len(vins)}")
            
        self.__logger.info(f"Listed all vehicles: {len(self.__vehicles)} VINs with temp data")


    async def __process_temp_data_async(
        self, s3_key: str, acc: list[CarState], acc_lock: asyncio.Lock
    ):
        async with self.__connection_semaphore:  # Limite les connexions simultanées
            try:
                loop = asyncio.get_event_loop()
                # Lecture et suppression en une seule connexion
                get_response = await loop.run_in_executor(
                    None, 
                    lambda: self.__s3.get_object(Bucket=self.__bucket, Key=s3_key)
                )
                content = get_response["Body"].read().decode("utf-8")
                parsed = msgspec.json.decode(content, type=CarState)
                
                async with acc_lock:
                    acc.append(parsed)
                
                # Suppression immédiate après lecture
                await loop.run_in_executor(
                    None,
                    lambda: self.__s3.delete_object(Bucket=self.__bucket, Key=s3_key)
                )
                
            except Exception as e:
                self.__logger.error(f"Error processing {s3_key}: {str(e)}")

    async def __process_vin_batch_async(self, vin: str, batch: list[str], temp_car_states: list, lock: asyncio.Lock):
        tasks = [
            self.__process_temp_data_async(s3_key, temp_car_states, lock)
            for s3_key in batch
        ]
        await asyncio.gather(*tasks)

    async def __process_vin_async(self, vin, temp_data):
        today = datetime.today().date()
        self.__logger.info(f"Compressing data for VIN {vin} ({len(temp_data)} files)")
        lock = asyncio.Lock()
        temp_car_states = []

        # Traitement par lots optimisé
        batch_size = 25  # Taille de lot optimale pour 10 connexions
        temp_data_list = list(temp_data)
        batches = [temp_data_list[i:i + batch_size] for i in range(0, len(temp_data_list), batch_size)]
        
        for i, batch in enumerate(batches):
            await self.__process_vin_batch_async(vin, batch, temp_car_states, lock)
            processed = min((i + 1) * batch_size, len(temp_data))
            self.__logger.info(f"VIN {vin}: Processed {processed}/{len(temp_data)} files")

        if not temp_car_states:
            self.__logger.warning(f"No data to compress for VIN {vin}")
            return

        try:
            # Compression et upload optimisés
            merged = MergedCarState.from_list(temp_car_states)
            encoded = msgspec.json.encode(merged)
            
            async with self.__connection_semaphore:
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self.__s3.put_object(
                        Bucket=self.__bucket,
                        Key=f"response/stellantis/{vin}/{today}.json",
                        Body=encoded,
                    )
                )
            self.__logger.info(f"Successfully compressed and uploaded data for VIN {vin}")
            
        except Exception as e:
            self.__logger.error(f"Error compressing data for VIN {vin}: {str(e)}")

    async def compress_async(self):
        concurrent_vins = 3  # On peut garder 3 VINs en parallèle car on contrôle les connexions
        items = list(self.__vehicles.items())
        
        for i in range(0, len(items), concurrent_vins):
            batch = items[i:i + concurrent_vins]
            tasks = [self.__process_vin_async(vin, temp_data) for vin, temp_data in batch]
            await asyncio.gather(*tasks)
            
            processed = min(i + concurrent_vins, len(items))
            self.__logger.info(f"Global progress: {processed}/{len(items)} VINs processed")

    def compress(self):
        asyncio.run(self.compress_async())

    def run(self):
        self.__logger.info("Starting Stellantis data compression")
        self.compress()
        self.__logger.info("Compressed Stellantis vehicle data")
