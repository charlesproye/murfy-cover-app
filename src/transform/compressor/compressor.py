import asyncio

from core.s3.async_s3 import AsyncS3
from datetime import datetime
from abc import ABC, abstractmethod
from botocore.exceptions import ClientError
import gc

class Compressor(ABC):
    """Compressor class that could be extended or modified easily to be used for all car brand compression"""

    def __init__(self, s3: AsyncS3 | None = None) -> None:
        self._s3 = s3 or AsyncS3()

    @property
    @abstractmethod
    def brand_prefix(self)->str:
        pass
    
    async def run(self):
        vins_folders, _ = await self._s3.list_content(f"response/{self.brand_prefix}/")
        r = await asyncio.gather(*(self._s3.list_content(f"{vin_path}temp/") for vin_path in vins_folders))
        vins_with_temps:list[str] = []
        for i,(_, files) in enumerate(r): 
            if len(files)>0:
                vins_with_temps.append(vins_folders[i])
        for i, vin_path in enumerate(vins_with_temps):
            print(f"DOWNLOAD VIN: {vin_path}")
            print(f"{(i/len(vins_with_temps) * 100):.2f}%")
            await self._compress_temp_vin_data(vin_path)
            gc.collect()

    async def _compress_temp_vin_data(self, vin_folder_path: str, max_retries: int = 3, retry_delay: int = 2):
        attempt = 0
        while attempt < max_retries:
            try:
                new_files = await self._s3.download_folder(f"{vin_folder_path}temp/")
                break
            except ClientError as e:
                attempt += 1
                print(f"[WARN] S3 download failed (attempt {attempt}): {e}")
                if attempt >= max_retries:
                    print(f"[ERROR] Giving up on {vin_folder_path}")
                    return
                await asyncio.sleep(retry_delay)

        encoded_data = self._temp_data_to_daily_file(new_files)

        await self._s3.upload_file(
            path=f"{vin_folder_path}{self._filename()}",
            file=encoded_data
        )
        
        await self._s3.delete_folder(f"{vin_folder_path}temp/")

        # 🔥 libérer la mémoire immédiatement
        del new_files
        del encoded_data
        gc.collect()
    
    @abstractmethod
    def _temp_data_to_daily_file(self, new_files:dict[str,bytes])->bytes:
        pass


    def _filename(self):
        return f"{datetime.now().strftime('%Y-%m-%d')}.json"

