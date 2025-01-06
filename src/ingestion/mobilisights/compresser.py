import asyncio
import logging
from datetime import datetime
from typing import Optional, List, Set

import aioboto3
import boto3
import msgspec
from botocore.client import Config
from botocore.credentials import threading
from ingestion.mobilisights.schema import CarState, MergedCarState


class MobilisightsCompresser:
    __logger: logging.Logger
    __s3: boto3.client
    __bucket: str
    __vehicles: dict[str, set[str]]
    __shutdown_requested: threading.Event

    def __init__(
        self, s3=None, bucket=None, threaded: bool = False, 
        max_workers: Optional[int] = 8, batch_size: int = 50
    ) -> None:
        self.__logger = logging.getLogger("COMPRESSER")
        self.__vehicles = {}
        self.__shutdown_requested = threading.Event()
        
        config = Config(
            max_pool_connections=10,
            retries=dict(max_attempts=3),
            read_timeout=30,
            connect_timeout=30
        )
        
        self.__s3 = s3 if s3 else boto3.client("s3", config=config)
        self.__bucket = bucket
        self.threaded = threaded
        self.max_workers = max_workers
        self.batch_size = batch_size

    def shutdown(self):
        self.__logger.info("Shutting down compresser")
        self.__shutdown_requested.set()

    async def list_and_process(self):
        self.__logger.info("Starting to list and process bucket objects")
        paginator = self.__s3.get_paginator("list_objects_v2")
        
        # Liste des VINs avec pagination
        vins = set()
        prefix = "response/stellantis/"
        processing_tasks = []
        
        for page in paginator.paginate(
            Bucket=self.__bucket, 
            Prefix=prefix,
            Delimiter='/',
            PaginationConfig={'PageSize': 1000}
        ):
            if 'CommonPrefixes' in page:
                for prefix_obj in page['CommonPrefixes']:
                    vin = prefix_obj['Prefix'].split('/')[2]
                    if len(vin) == 17:
                        vins.add(vin)
                        # Démarrer le traitement des fichiers temp pour ce VIN immédiatement
                        task = asyncio.create_task(self.__list_and_process_vin(vin))
                        processing_tasks.append(task)
        
        self.__logger.info(f"Found {len(vins)} VINs, waiting for processing to complete")
        await asyncio.gather(*processing_tasks)

    async def list_and_process(self):
        self.__logger.info("Starting to list and process bucket objects")
        paginator = self.__s3.get_paginator("list_objects_v2")
        
        # Liste des VINs avec pagination
        vins = set()
        prefix = "response/stellantis/"
        processing_tasks = []
        semaphore = asyncio.Semaphore(20)  # Limite à 20 VINs traités simultanément
        
        async def process_vin_complete(vin: str):
            async with semaphore:
                session = aioboto3.Session()
                async with session.client(
                    "s3",
                    region_name=self.__s3.meta.region_name,
                    aws_access_key_id=self.__s3._request_signer._credentials.access_key,
                    aws_secret_access_key=self.__s3._request_signer._credentials.secret_key,
                    endpoint_url=self.__s3.meta.endpoint_url if hasattr(self.__s3.meta, 'endpoint_url') else None
                ) as s3:
                    try:
                        temp_files = set()
                        temp_prefix = f"response/stellantis/{vin}/temp/"
                        
                        # Liste les fichiers temp pour ce VIN
                        for page in paginator.paginate(
                            Bucket=self.__bucket,
                            Prefix=temp_prefix,
                            PaginationConfig={'PageSize': 3000}
                        ):
                            if 'Contents' in page:
                                temp_files.update(obj['Key'] for obj in page['Contents'])
                        
                        if temp_files:
                            self.__logger.info(f"Found {len(temp_files)} temp files for VIN {vin}")
                            
                            # Grouper les fichiers par datetimeSending
                            files_by_date = {}
                            
                            # Traiter tous les fichiers pour les grouper par date
                            for temp_file in temp_files:
                                try:
                                    response = await s3.get_object(Bucket=self.__bucket, Key=temp_file)
                                    async with response['Body'] as stream:
                                        content = await stream.read()
                                        car_state = msgspec.json.decode(content, type=CarState)
                                        
                                    if car_state.datetimeSending:
                                        sending_date = car_state.datetimeSending.date()
                                        if sending_date not in files_by_date:
                                            files_by_date[sending_date] = []
                                        files_by_date[sending_date].append((temp_file, car_state))
                                except Exception as e:
                                    self.__logger.error(f"Error processing temp file {temp_file}: {e}")
                                    continue
                            
                            # Traiter chaque groupe de date séparément
                            for sending_date, file_data in files_by_date.items():
                                if self.__shutdown_requested.is_set():
                                    return
                                    
                                try:
                                    temp_files = [f[0] for f in file_data]
                                    car_states = [f[1] for f in file_data]
                                    
                                    self.__logger.info(f"Processing {len(car_states)} states for VIN {vin} date {sending_date}")
                                    
                                    # Créer le fichier compressé pour cette date
                                    merged = MergedCarState.from_list(car_states)
                                    if merged:
                                        compressed_key = f"response/stellantis/{vin}/{sending_date}.json"
                                        encoded = msgspec.json.encode(merged)
                                        
                                        # Sauvegarder le fichier compressé
                                        await s3.put_object(
                                            Bucket=self.__bucket,
                                            Key=compressed_key,
                                            Body=encoded
                                        )
                                        
                                        # Supprimer les fichiers temp de cette date
                                        chunk_size = 50
                                        for i in range(0, len(temp_files), chunk_size):
                                            if self.__shutdown_requested.is_set():
                                                return
                                            chunk = temp_files[i:i + chunk_size]
                                            delete_tasks = [
                                                s3.delete_object(Bucket=self.__bucket, Key=temp_key)
                                                for temp_key in chunk
                                            ]
                                            await asyncio.gather(*delete_tasks)
                                            
                                        self.__logger.info(f"Processed and cleaned up {len(temp_files)} files for VIN {vin} date {sending_date}")
                                        
                                except Exception as e:
                                    self.__logger.error(f"Error processing date group {sending_date} for VIN {vin}: {e}")
                                    continue
                    except Exception as e:
                        self.__logger.error(f"Error processing VIN {vin}: {e}")
        
        # Lister tous les VINs
        for page in paginator.paginate(
            Bucket=self.__bucket, 
            Prefix=prefix,
            Delimiter='/',
            PaginationConfig={'PageSize': 3000}
        ):
            if self.__shutdown_requested.is_set():
                break
                
            if 'CommonPrefixes' in page:
                for prefix_obj in page['CommonPrefixes']:
                    vin = prefix_obj['Prefix'].split('/')[2]
                    if len(vin) == 17:
                        vins.add(vin)
                        # Démarrer le traitement complet immédiatement
                        task = asyncio.create_task(process_vin_complete(vin))
                        processing_tasks.append(task)
        
        self.__logger.info(f"Found {len(vins)} VINs, processing in parallel")
        await asyncio.gather(*processing_tasks)

    async def __process_batch_async(self, batch: List[str], vin: str):
        session = aioboto3.Session()
        async with session.client(
            "s3",
            region_name=self.__s3.meta.region_name,
            aws_access_key_id=self.__s3._request_signer._credentials.access_key,
            aws_secret_access_key=self.__s3._request_signer._credentials.secret_key,
            endpoint_url=self.__s3.meta.endpoint_url if hasattr(self.__s3.meta, 'endpoint_url') else None
        ) as s3:
            successful_processes = []
            merged_states = []

            # Traiter les fichiers par petits groupes pour éviter trop de tâches simultanées
            chunk_size = 50
            for i in range(0, len(batch), chunk_size):
                chunk = batch[i:i + chunk_size]
                tasks = []
                
                for s3_key in chunk:
                    if self.__shutdown_requested.is_set():
                        return [], None
                    tasks.append(self.__process_one_async(s3, s3_key))
                
                # Attendre que toutes les tâches du chunk soient terminées
                try:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for s3_key, result in zip(chunk, results):
                        if isinstance(result, Exception):
                            self.__logger.error(f"Error processing {s3_key}: {result}")
                        elif result:
                            successful_processes.append(s3_key)
                            merged_states.append(result)
                except Exception as e:
                    self.__logger.error(f"Error processing chunk for VIN {vin}: {e}")

            return successful_processes, merged_states

    async def __process_one_async(self, s3, s3_key: str):
        try:
            response = await s3.get_object(Bucket=self.__bucket, Key=s3_key)
            async with response['Body'] as stream:
                content = await stream.read()
                content = content.decode('utf-8')
            
            return msgspec.json.decode(content, type=CarState)
        except Exception as e:
            self.__logger.error(f"Error in process_one_async for {s3_key}: {e}")
            return None

    async def __save_compressed_data(self, successful_processes: List[str], merged: MergedCarState, vin: str):
        session = aioboto3.Session()
        async with session.client(
            "s3",
            region_name=self.__s3.meta.region_name,
            aws_access_key_id=self.__s3._request_signer._credentials.access_key,
            aws_secret_access_key=self.__s3._request_signer._credentials.secret_key,
            endpoint_url=self.__s3.meta.endpoint_url if hasattr(self.__s3.meta, 'endpoint_url') else None
        ) as s3:
            try:
                sending_date = merged.datetimeSending.date() if merged.datetimeSending else datetime.today().date()
                encoded = msgspec.json.encode(merged)
                compressed_key = f"response/stellantis/{vin}/{sending_date}.json"
                
                await s3.put_object(
                    Bucket=self.__bucket,
                    Key=compressed_key,
                    Body=encoded
                )

                # Supprimer les fichiers par petits groupes
                chunk_size = 50
                for i in range(0, len(successful_processes), chunk_size):
                    chunk = successful_processes[i:i + chunk_size]
                    delete_tasks = [
                        s3.delete_object(Bucket=self.__bucket, Key=temp_key)
                        for temp_key in chunk
                    ]
                    await asyncio.gather(*delete_tasks)
                    self.__logger.info(f"Deleted {len(chunk)} files for VIN {vin}")
                
            except Exception as e:
                self.__logger.error(f"Error saving compressed data for VIN {vin}: {e}")

    async def __process_vin(self, vin: str, temp_data: Set[str]):
        if self.__shutdown_requested.is_set():
            return

        self.__logger.info(f"Processing VIN {vin} with {len(temp_data)} files")
        
        # Traitement par lots
        batch_size = 3000
        temp_data_list = list(temp_data)
        batches = [temp_data_list[i:i + batch_size] 
                  for i in range(0, len(temp_data_list), batch_size)]
        
        self.__logger.info(f"Split data into {len(batches)} batches of size {batch_size}")
        
        # Lancer tous les batches en parallèle immédiatement
        batch_tasks = []
        for batch_index, batch in enumerate(batches):
            if self.__shutdown_requested.is_set():
                break
            
            task = asyncio.create_task(self.__process_batch_async(batch, vin))
            batch_tasks.append((batch_index, task))
        
        # Traiter les résultats au fur et à mesure qu'ils arrivent
        all_car_states = []
        all_successful_processes = []
        
        for batch_index, task in batch_tasks:
            if self.__shutdown_requested.is_set():
                break
                
            successful_processes, car_states = await task
            if car_states:
                all_car_states.extend(car_states)
                all_successful_processes.extend(successful_processes)
            
            self.__logger.info(f"Processed batch {batch_index + 1}/{len(batches)} for VIN {vin}")

        if all_car_states:
            merged = MergedCarState.from_list(all_car_states)
            await self.__save_compressed_data(all_successful_processes, merged, vin)
            self.__logger.info(f"Successfully compressed data for VIN {vin}")

    async def compress_async(self):
        concurrent_vins = 20  # Nombre de VINs traités en parallèle
        items = list(self.__vehicles.items())
        
        for i in range(0, len(items), concurrent_vins):
            if self.__shutdown_requested.is_set():
                break
                
            batch = items[i:i + concurrent_vins]
            tasks = [self.__process_vin(vin, temp_data) for vin, temp_data in batch]
            await asyncio.gather(*tasks)
            
            processed = min(i + concurrent_vins, len(items))
            self.__logger.info(f"Global progress: {processed}/{len(items)} VINs processed")

    def compress(self):
        asyncio.run(self.compress_async())

    def run(self):
        try:
            self.__logger.info("Starting Stellantis data compression")
            asyncio.run(self.list_and_process())
            self.__logger.info("Compressed Stellantis vehicle data")
        except KeyboardInterrupt:
            self.__logger.info("Received shutdown signal")
            self.__shutdown_requested.set()
        except Exception as e:
            self.__logger.error(f"Error during compression: {e}")
            raise
