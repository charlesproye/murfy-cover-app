import asyncio
import gc
import io
from abc import ABC, abstractmethod
from datetime import datetime

from botocore.exceptions import ClientError

from core.s3.async_s3 import AsyncS3


class Compressor(ABC):
    """Compressor class that could be extended or modified easily to be used for all car brand compression"""

    def __init__(self, s3: AsyncS3 | None = None) -> None:
        self._s3 = s3 or AsyncS3()
        self._s3_dev = AsyncS3(env="dev")

    @property
    @abstractmethod
    def brand_prefix(self) -> str:
        pass

    async def run(self):
        vins_folders, _ = await self._s3.list_content(f"response/{self.brand_prefix}/")
        r = await asyncio.gather(
            *(self._s3.list_content(f"{vin_path}temp/") for vin_path in vins_folders)
        )
        vins_with_temps: list[str] = []
        for i, (_, files) in enumerate(r):
            if len(files) > 0:
                vins_with_temps.append(vins_folders[i])
        for i, vin_path in enumerate(vins_with_temps):
            print(f"DOWNLOAD VIN: {vin_path}")
            print(f"{(i / len(vins_with_temps) * 100):.2f}%")
            if self.brand_prefix in [
                "stellantis",
                "ford",
                "kia",
                "renault",
                "volvo-cars",
                "mercedes-benz",
            ]:
                await self._compress_temp_vin_data_buffer(vin_path)
            else:
                await self._compress_temp_vin_data(vin_path)

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

