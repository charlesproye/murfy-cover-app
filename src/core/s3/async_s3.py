import asyncio
from contextlib import asynccontextmanager
from typing import Annotated, Iterable
import aioboto3
from botocore.exceptions import ClientError
from fastapi import Depends
from .settings import S3Settings
import logging

class AsyncS3:
    def __init__(self, settings: S3Settings | None = None, max_concurrency: int = 200):
        settings = settings or S3Settings()
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
        async with self._sem:
            async with self._client() as client:  # type: ignore
                paginator = client.get_paginator("list_objects_v2")
                async for page in paginator.paginate(
                    Bucket=self.bucket, Prefix=path, Delimiter="/"
                ):
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

    async def get_files(self, paths:Iterable[str]) ->dict[str,bytes]:
        paths_list = list(paths)
        print(f"GETTING FILES for path {paths_list[:5]}")
        batch_size:int = self.max_concurrency
        results: dict[str, bytes] = {}
        for batch_index in range(0,len(paths_list), batch_size):
            self.logger.debug(f"({batch_index}-{batch_index+batch_size}):Started")
            print(f"({batch_index}-{batch_index+batch_size}):Started")
            batch = paths_list[batch_index:batch_index+batch_size]
            results.update(await self._get_files_no_limit(batch))
            self.logger.debug(f"({batch_index}-{batch_index+batch_size}):Finished")
            print(f"({batch_index}-{batch_index+batch_size}):Finished")
        return results
    
    async def _get_files_no_limit(self, paths: Iterable[str]) -> dict[str,bytes]:
        results: dict[str, bytes] = {}

        async def download(key: str):
            async with self._sem:
                data = await self.get_file(key)
                if data is not None:
                    results[key] = data
        await asyncio.gather(*(download(f) for f in paths))
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
        print(f"{len(files) = }: {files[:10] = }")
        return await self.get_files(files)
        
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

AsyncS3Dep = Annotated[AsyncS3,Depends(get_async_s3)]
