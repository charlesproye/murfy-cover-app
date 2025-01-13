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
            brand_name = "bmw"
            s3_keys = set()
            
            async for page in paginator.paginate(
                Bucket=self.__bucket, 
                Prefix=f"response/{brand_name.lower()}"
            ):
                if 'Contents' in page:
                    s3_keys.update(obj["Key"] for obj in page["Contents"])
            
            # Process VINs in parallel
            vins = set(
                filter(lambda v: len(v) == 17, map(lambda v: v.split("/")[2], s3_keys))
            )
            
            self.__s3_keys_by_vin[brand_name.lower()] = {
                vin: set(filter(
                    lambda e: e.startswith(f"response/{brand_name.lower()}/{vin}/temp/"),
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
                    Key=f"response/bmw/{vin}/{date}.json",
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

        try:
            # Group files by date
            files_by_date = self._group_files_by_date(temp_data)
            
            # Process each date separately
            for date, date_files in files_by_date.items():
                if self.__shutdown_requested.is_set():
                    return

                # Reduce batch size if there are too many files
                adjusted_batch_size = min(self.batch_size, 20)  # Limit max batch size
                
                # Process in smaller batches for each date
                batches = [
                    list(islice(date_files, i, i + adjusted_batch_size))
                    for i in range(0, len(date_files), adjusted_batch_size)
                ]
                
                # Process batches sequentially instead of all at once
                for batch in batches:
                    if self.__shutdown_requested.is_set():
                        return
                        
                    try:
                        await self.__process_batch(vin, batch, date)
                        # Add small delay between batches to prevent overload
                        await asyncio.sleep(0.1)
                    except Exception as e:
                        self.__logger.error(f"Failed processing batch for VIN {vin} on date {date}: {e}")
                        continue  # Continue with next batch even if one fails
                
                self.__logger.info(f"Completed processing for VIN {vin} on date {date}")
                
        except Exception as e:
            self.__logger.error(f"Error in process_vin for {vin}: {e}")

    async def run(self):
        self.__logger.info("Starting bmw data compression")
        try:
            await self.list_objects()
            
            if self.__shutdown_requested.is_set():
                return
                
            # Process multiple VINs concurrently, but limit concurrency
            max_concurrent_vins = 5  # Ajuster selon les besoins
            tasks = []
            
            for vin, temp_data in self.__s3_keys_by_vin["bmw"].items():
                if self.__shutdown_requested.is_set():
                    break
                    
                tasks.append(self.process_vin(vin, temp_data))
                
                # When we reach max_concurrent_vins, wait for them to complete
                if len(tasks) >= max_concurrent_vins:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    tasks = []
            
            # Process any remaining tasks
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
                    
            self.__logger.info("Completed bmw data compression")
        except Exception as e:
            self.__logger.error(f"Error in compression run: {e}")
            raise

    def start(self):
        asyncio.run(self.run())

    def shutdown(self):
        self.__shutdown_requested.set()
