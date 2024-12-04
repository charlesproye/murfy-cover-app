import asyncio
import aioboto3
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from itertools import islice
import multiprocessing as mp
from datetime import datetime, timedelta
import time
import logging
from typing import Optional
import msgspec
from .schema import BMWInfo, BMWMergedInfo
from multithreading import MergedInfoWrapper

class BMWCompresser:
    def __init__(
        self, 
        s3, 
        bucket, 
        threaded: bool = True,
        max_workers: Optional[int] = None,
        batch_size: int = 50
    ) -> None:
        self.__logger = logging.getLogger("COMPRESSER")
        self.__s3 = s3
        self.__bucket = bucket
        self.threaded = threaded
        self.batch_size = batch_size
        self.max_workers = max_workers or mp.cpu_count()
        
        self.__session = aioboto3.Session()
        self.__s3_keys_by_vin = {}
        self.__shutdown_requested = asyncio.Event()

    def _get_date_from_filename(self, filename: str) -> str:
        """Extract date from timestamp filename"""
        try:
            timestamp = int(filename.split('/')[-1].split('.')[0])
            return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
        except Exception as e:
            self.__logger.error(f"Error parsing date from filename {filename}: {e}")
            return None

    def _group_files_by_date(self, temp_files: set[str]) -> dict[str, set[str]]:
        """Group files by their date"""
        files_by_date = {}
        for file in temp_files:
            date = self._get_date_from_filename(file)
            if date:
                if date not in files_by_date:
                    files_by_date[date] = set()
                files_by_date[date].add(file)
        return files_by_date

    async def list_objects(self):
        async with self.__session.client(
            "s3",
            region_name=self.__s3.meta.region_name,
            aws_access_key_id=self.__s3._request_signer._credentials.access_key,
            aws_secret_access_key=self.__s3._request_signer._credentials.secret_key,
            endpoint_url=self.__s3.meta.endpoint_url if hasattr(self.__s3.meta, 'endpoint_url') else None
        ) as s3:
            paginator = s3.get_paginator("list_objects_v2")
            brand_name = "BMW"
            s3_keys = set()
            
            async for page in paginator.paginate(
                Bucket=self.__bucket, 
                Prefix=f"response/{brand_name}"
            ):
                if 'Contents' in page:
                    s3_keys.update(obj["Key"] for obj in page["Contents"])
            
            # Process VINs in parallel
            vins = set(
                filter(lambda v: len(v) == 17, map(lambda v: v.split("/")[2], s3_keys))
            )
            
            self.__s3_keys_by_vin[brand_name] = {
                vin: set(filter(
                    lambda e: e.startswith(f"response/{brand_name}/{vin}/temp/"),
                    s3_keys
                ))
                for vin in vins
        }

    async def __process_batch(self, vin: str, batch: list[str], date: str):
        async with self.__session.client(
            "s3",
            region_name=self.__s3.meta.region_name,
            aws_access_key_id=self.__s3._request_signer._credentials.access_key,
            aws_secret_access_key=self.__s3._request_signer._credentials.secret_key,
            endpoint_url=self.__s3.meta.endpoint_url if hasattr(self.__s3.meta, 'endpoint_url') else None
        ) as s3:
            merged = MergedInfoWrapper[BMWInfo, BMWMergedInfo](BMWMergedInfo)
            
            # Fetch all objects in batch concurrently
            tasks = [
                self.__fetch_and_process(s3, s3_key, merged)
                for s3_key in batch
            ]
            await asyncio.gather(*tasks)
            
            # Upload merged batch
            if merged.info is not None:
                bmw_info = merged.info.to_bmw_info()
                encoded = msgspec.json.encode(bmw_info)
                
                # Upload to date-specific file
                await s3.put_object(
                    Bucket=self.__bucket,
                    Key=f"response/BMW/{vin}/{date}.json",
                    Body=encoded,
                )
                
                # Delete processed files in batch
                delete_objects = {'Objects': [{'Key': key} for key in batch]}
                await s3.delete_objects(Bucket=self.__bucket, Delete=delete_objects)
                
                self.__logger.info(f"Processed and merged {len(batch)} files for VIN {vin} on date {date}")

    async def __fetch_and_process(self, s3, s3_key: str, merged: MergedInfoWrapper):
        try:
            response = await s3.get_object(Bucket=self.__bucket, Key=s3_key)
            async with response['Body'] as stream:
                content = await stream.read()
            
            parsed = msgspec.json.decode(content, type=BMWInfo)
            
            if merged.info is None:
                merged.set_info(parsed)
            else:
                merged.merge(parsed)
                
        except Exception as e:
            self.__logger.error(f"Error processing {s3_key}: {e}")

    async def process_vin(self, vin: str, temp_data: set[str]):
        if not temp_data:
            return

        # Group files by date
        files_by_date = self._group_files_by_date(temp_data)
        
        # Process each date separately
        for date, date_files in files_by_date.items():
            if self.__shutdown_requested.is_set():
                return

            # Process in batches for each date
            batches = [
                list(islice(date_files, i, i + self.batch_size))
                for i in range(0, len(date_files), self.batch_size)
            ]
            
            tasks = [
                self.__process_batch(vin, batch, date)
                for batch in batches
            ]
            await asyncio.gather(*tasks)
            
            self.__logger.info(f"Completed processing for VIN {vin} on date {date}")

    async def run(self):
        self.__logger.info("Starting BMW data compression")
        await self.list_objects()
        
        if self.__shutdown_requested.is_set():
            return
            
        # Créer des tâches asyncio pour chaque VIN
        tasks = []
        for vin, temp_data in self.__s3_keys_by_vin["BMW"].items():
            task = asyncio.create_task(self.process_vin(vin, temp_data))
            tasks.append(task)
        
        await asyncio.gather(*tasks)
        self.__logger.info("Completed BMW data compression")

    def start(self):
        asyncio.run(self.run())

    def shutdown(self):
        self.__shutdown_requested.set()
