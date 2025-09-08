import asyncio

import msgspec
from transform.compressor.compressor import Compressor
from core.s3.async_s3 import AsyncS3
from botocore.exceptions import ClientError
import gc


class VolkswagenCompressor(Compressor):
    def __init__(self, make='volkswagen') -> None:
        super().__init__()
        self.make = make
        self._s3 = AsyncS3()

    @property
    def brand_prefix(self) -> str:
        return self.make

    def _temp_data_to_daily_file(self, new_files: dict[str, bytes]) -> bytes:
        data = []
        for file in new_files.values():
            decoded = msgspec.json.decode(file)
            json = msgspec.json.decode(decoded.encode())
            data.append(json)
        return msgspec.json.encode({"data": data})


    @classmethod
    async def compress(cls, make):
        """Compress data for any make"""
        asyncio.get_event_loop().set_debug(False)
        compressor = cls(make)
        await compressor.run()

    async def _compress_temp_vin_data(self, vin_folder_path: str, max_retries: int = 3, retry_delay: int = 2):
        attempt = 0
        while attempt < max_retries: # Random client error failure
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
        
        await self._s3.delete_folder(f"{vin_folder_path}temp/")

        # Emptying memory
        del new_files
        del encoded_data
        gc.collect()

