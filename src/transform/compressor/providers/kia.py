import asyncio

import msgspec

from core.s3.async_s3 import AsyncS3
from transform.compressor.compressor import Compressor


class KiaCompressor(Compressor):
    def __init__(self, make="kia") -> None:
        super().__init__()
        self.make = make
        self._s3 = AsyncS3()

    @property
    def brand_prefix(self) -> str:
        return self.make

    def _temp_data_to_daily_file(self, new_files: dict[str, bytes]) -> bytes:
        all_items = []
        for file in new_files.values():
            decoded = msgspec.json.decode(file)
            if isinstance(decoded, list):
                all_items.extend(decoded)
            else:
                all_items.append(decoded)
        return msgspec.json.encode(all_items)

    async def _compress_temp_vin_data_buffer(
        self,
        vin_folder_path: str,
        max_retries: int = 3,
        retry_delay: int = 2,
        upload: bool = True,
    ):
        attempt = 0

        while attempt < max_retries:
            all_items: list = []

            async for batch in self._s3.download_folder_in_batches(
                f"{vin_folder_path}temp/", batch_size=4000
            ):
                merged_data = self._temp_data_to_daily_file(batch)
                if merged_data:
                    decoded_batch = msgspec.json.decode(merged_data)
                    all_items.extend(decoded_batch)
                    del batch

            break

        if all_items:
            final_bytes = msgspec.json.encode(all_items)
            if upload:
                await self._s3.upload_file(
                    path=f"{vin_folder_path}{self._filename()}", file=final_bytes
                )
                await self._s3_dev.upload_file(
                    path=f"{vin_folder_path}{self._filename()}", file=final_bytes
                )
                # Limit the batch size because we sometime get ReadTimeoutError from Scaleway
                await self._s3.delete_folder(f"{vin_folder_path}temp/", batch_size=500)
            else:
                return final_bytes

    @classmethod
    async def compress(cls, make):
        """Compress data for any make"""
        asyncio.get_event_loop().set_debug(False)
        compressor = cls(make)
        await compressor.run()
