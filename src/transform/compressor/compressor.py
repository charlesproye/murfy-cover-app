import asyncio
import gc
import io
import multiprocessing as mp
import os
from abc import ABC, abstractmethod
from datetime import datetime

from botocore.exceptions import ClientError
from rich.progress import track

from core.s3.async_s3 import AsyncS3
from core.s3.s3_utils import S3Service

COMPRESSION_N_PROCESSES = int(os.getenv("COMPRESSION_N_PROCESSES", 1))


def _process_vin_chunk_worker(args):
    """Standalone worker function for multiprocessing that can be pickled."""
    chunk, brand_prefix, compressor_class = args

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    compressor = compressor_class(brand_prefix)

    try:
        loop.run_until_complete(
            _process_chunk_async_worker(chunk, brand_prefix, compressor)
        )
    finally:
        loop.close()


async def _process_chunk_async_worker(chunk, brand_prefix, compressor: "Compressor"):
    """Async worker function to process a chunk of VIN paths."""
    for _, vin_path in enumerate(chunk):
        print(f"DOWNLOAD VIN: {vin_path}")
        if brand_prefix in [
            "stellantis",
            "ford",
            "kia",
            "renault",
            "volvo-cars",
            "mercedes-benz",
        ]:
            await compressor._compress_temp_vin_data_buffer(vin_path)
        else:
            await compressor._compress_temp_vin_data(vin_path)


class Compressor(ABC):
    """Compressor class that could be extended or modified easily to be used for all car brand compression
    COMPRESSION_N_PROCESSES (default 1): Number of processes to use for compression. Set to -1 to use all available processes.
    """

    def __init__(self, s3: AsyncS3 | None = None) -> None:
        self._s3 = s3 or AsyncS3()
        self._s3_dev = AsyncS3(env="dev")
        self._sync_s3 = S3Service()

    @property
    @abstractmethod
    def brand_prefix(self) -> str:
        pass

    async def run(
        self,
        chunk_size: int | None = None,
        num_processes: int = COMPRESSION_N_PROCESSES,
    ):
        # async listing fails sometimes, so we use the sync version
        vins_folders, _ = self._sync_s3.list_content(
            self._s3.bucket, f"response/{self.brand_prefix}/"
        )

        folders_files = []
        for vin_path in track(vins_folders, description="Temp files listing..."):
            folders_files.append(
                self._sync_s3.list_content(self._s3.bucket, f"{vin_path}temp/")
            )

        vins_with_temps: list[str] = []
        for i, (_, files) in enumerate(folders_files):
            if len(files) > 0:
                vins_with_temps.append(vins_folders[i])

        if num_processes == -1:
            num_processes = mp.cpu_count()
        if chunk_size is None:
            chunk_size = max(1, len(vins_with_temps) // num_processes)

        # Split VINs into chunks
        chunks = self._chunk_list(vins_with_temps, chunk_size)

        print(
            f"Processing {len(vins_with_temps)} VINs in {len(chunks)} chunks using {num_processes} processes"
        )

        # Process chunks in parallel
        worker_args = [(chunk, self.brand_prefix, self.__class__) for chunk in chunks]
        with mp.Pool(processes=num_processes) as pool:
            pool.map(_process_vin_chunk_worker, worker_args)

    async def _compress_temp_vin_data(
        self, vin_folder_path: str, max_retries: int = 3, retry_delay: int = 2
    ):
        attempt = 0
        while attempt < max_retries:  # Random client error failure
            try:
                new_files = await self._s3.download_folder(f"{vin_folder_path}temp/")
                break  # Success, exit retry loop
            except ClientError as e:
                attempt += 1
                print(f"[WARN] S3 download failed (attempt {attempt}): {e}")
                if attempt >= max_retries:
                    print(f"[ERROR] Giving up on {vin_folder_path}")
                    return
                await asyncio.sleep(retry_delay)

        encoded_data = self._temp_data_to_daily_file(new_files)

        await self._s3.upload_file(
            path=f"{vin_folder_path}{self._filename()}", file=encoded_data
        )

        await self._s3_dev.upload_file(
            path=f"{vin_folder_path}{self._filename()}", file=encoded_data
        )

        await self._s3.delete_folder(f"{vin_folder_path}temp/")

        # Emptying memory
        del new_files
        del encoded_data
        gc.collect()

    async def _compress_temp_vin_data_buffer(
        self,
        vin_folder_path: str,
        max_retries: int = 3,
        retry_delay: int = 2,
        upload: bool = True,
    ):
        attempt = 0
        first_item = True

        while attempt < max_retries:
            try:
                buf = io.BytesIO()
                buf.write(b"[")
                async for batch in self._s3.download_folder_in_batches(
                    f"{vin_folder_path}temp/", batch_size=4000
                ):
                    merged_data = self._temp_data_to_daily_file(batch)

                    if merged_data:
                        # Emptying batch memory
                        del batch
                        gc.collect()
                        if not first_item:
                            buf.write(b",")
                        buf.write(merged_data)
                        first_item = False
                    else:
                        pass
                buf.write(b"]")
                break
            except ClientError as e:
                attempt += 1
                print(f"[WARN] S3 download failed (attempt {attempt}): {e}")
                if attempt >= max_retries:
                    print(f"[ERROR] Giving up on {vin_folder_path}")
                    return
                await asyncio.sleep(retry_delay)

        if buf.getvalue() != b"[]":
            buf.seek(0)
            if upload:
                await self._s3.upload_file(
                    path=f"{vin_folder_path}{self._filename()}", file=buf.getvalue()
                )
                await self._s3_dev.upload_file(
                    path=f"{vin_folder_path}{self._filename()}", file=buf.getvalue()
                )
            else:
                return buf.getvalue()
        if upload:
            await self._s3.delete_folder(f"{vin_folder_path}temp/")

        buf.close()
        gc.collect()

    @abstractmethod
    def _temp_data_to_daily_file(self, new_files: dict[str, bytes]) -> bytes:
        pass

    def _filename(self):
        return f"{datetime.now().strftime('%Y-%m-%d')}.json"

    def _chunk_list(self, lst: list, chunk_size: int) -> list[list]:
        """Split a list into chunks of specified size."""
        return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]

