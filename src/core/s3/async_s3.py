import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import Annotated, AsyncGenerator, Iterable, TypeVar

import aioboto3
from botocore.exceptions import ClientError

from .settings import S3Settings, get_s3_settings

T = TypeVar("T")


class AsyncS3:
    def __init__(self, env: str = "prod", max_concurrency: int = 200):
        settings = get_s3_settings(env)
        self._settings = settings
        self.session = aioboto3.Session(
            aws_access_key_id=settings.S3_KEY,
            aws_secret_access_key=settings.S3_SECRET,
            region_name=settings.S3_REGION,
        )
        self.bucket = settings.S3_BUCKET
        self.max_concurrency = max_concurrency
        self._sem = asyncio.Semaphore(max_concurrency)
        self.logger = logging.getLogger("AsyncS3")

    @asynccontextmanager
    async def _client(self):
        async with self.session.client(
            "s3",
            region_name=self._settings.S3_REGION,
            endpoint_url=self._settings.S3_ENDPOINT,
            aws_access_key_id=self._settings.S3_KEY,
            aws_secret_access_key=self._settings.S3_SECRET,
        ) as client:
            yield client

    async def list_content(self, path: str = "") -> tuple[list[str], list[str]]:
        """Returns (folders, files)"""
        folders = set()
        files = []
        async with self._client() as client:
            paginator = client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(
                Bucket=self.bucket, Prefix=path, Delimiter="/"
            ):
                # On ne protège que le traitement des objets/pages
                async with self._sem:
                    for cp in page.get("CommonPrefixes", []):
                        folders.add(cp.get("Prefix"))
                    for content in page.get("Contents", []):
                        key = content["Key"]
                        if key != path:
                            files.append(key)
        return sorted(folders), sorted(files)

    async def get_file(self, path: str) -> bytes | None:
        async with self._sem:
            async with self._client() as client:  # type: ignore
                try:
                    response = await client.get_object(Bucket=self.bucket, Key=path)
                    async with response["Body"] as stream:
                        return await stream.read()
                except ClientError as e:
                    if e.response["Error"]["Code"] == "NoSuchKey":
                        return None
                    raise

    async def get_files(self, paths: Iterable[str]) -> dict[str, bytes]:
        paths_list = list(paths)
        self.logger.info(
            f"GETTING FILES for {len(paths_list)} files, first 5: {paths_list[:5]}"
        )

        start_time = time.time()
        results = await self._get_files_optimized(paths_list)
        elapsed = time.time() - start_time

        self.logger.info(
            f"Downloaded {len(results)} files in {elapsed:.2f}s ({len(results) / elapsed:.1f} files/s)"
        )
        return results

    async def _get_files_optimized(self, paths: list[str]) -> dict[str, bytes]:
        """Optimized download using single client with proper concurrency control"""
        results: dict[str, bytes] = {}

        async with self._client() as client:

            async def download_single(key: str):
                async with self._sem:
                    try:
                        response = await client.get_object(Bucket=self.bucket, Key=key)
                        async with response["Body"] as stream:
                            data = await stream.read()
                            results[key] = data
                    except ClientError as e:
                        if e.response["Error"]["Code"] != "NoSuchKey":
                            self.logger.error(f"Failed to download {key}: {e}")

            # Process all downloads concurrently with semaphore limiting
            await asyncio.gather(
                *(download_single(path) for path in paths), return_exceptions=True
            )

        return results

    async def upload_file(self, path: str, file: bytes) -> None:
        async with self._client() as client:  # type: ignore
            await client.put_object(Bucket=self.bucket, Key=path, Body=file)

    async def delete_file(self, path: str) -> bool:
        async with self._sem:
            async with self._client() as client:  # type: ignore
                try:
                    await client.delete_object(Bucket=self.bucket, Key=path)
                    return True
                except ClientError as e:
                    if e.response["Error"]["Code"] == "NoSuchKey":
                        return False
                    raise

    async def download_folder(self, folder_path: str) -> dict[str, bytes]:
        _, files = await self.list_content(folder_path)
        self.logger.info(f"{len(files) = }: {files[:10] = }")
        return await self.get_files(files)

    async def download_folder_in_batches(
        self, folder_path: str, batch_size: int = 1000
    ) -> AsyncGenerator[dict[str, bytes], None]:
        """
        Download folder in batches. The batch_size now refers to how many files
        to yield at once, not a concurrency limit (which is controlled by max_concurrency).
        """
        _, files = await self.list_content(folder_path)
        self.logger.info(f"Found {len(files)} files to download from {folder_path}")

        # For smaller datasets, just download everything at once
        if len(files) <= self.max_concurrency:
            yield await self.get_files(files)
            return

        # For larger datasets, process in chunks to avoid memory issues
        for i in range(0, len(files), batch_size):
            batch = files[i : i + batch_size]
            yield await self.get_files(batch)

    async def delete_folder(self, prefix: str) -> int:
        deleted_count = 0
        async with self._sem:
            async with self._client() as client:  # type: ignore
                paginator = client.get_paginator("list_objects_v2")
                async for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                    contents = page.get("Contents", [])
                    if not contents:
                        continue

                    # Batch delete (doc says max 1000 at a time)
                    for i in range(0, len(contents), 1000):
                        batch = contents[i : i + 1000]
                        keys = [{"Key": obj["Key"]} for obj in batch]

                        await client.delete_objects(
                            Bucket=self.bucket, Delete={"Objects": keys, "Quiet": True}
                        )
                        deleted_count += len(keys)

            return deleted_count


def get_async_s3() -> AsyncS3:
    return AsyncS3()

