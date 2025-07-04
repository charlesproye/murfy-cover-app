import asyncio
from datetime import datetime
from src.core.s3.async_s3 import AsyncS3
import pandas as pd
import pyarrow.parquet as pq
import io
from abc import ABC, abstractmethod

class ResponseToRaw(ABC):
    """Converter class that could be extended or modified easily to be used for all car brand conversion from response to raw. Only the  _get_files_to_add method have to be implemented to return the list of new data as dict. Each element of the list should be a dict representing one datapoint and the dict must contain at least the key 'date' and 'vin'"""

    def __init__(self, brand_prefix: str, s3: AsyncS3 | None = None) -> None:
        self._s3 = s3 or AsyncS3()
        self.brand_prefix = brand_prefix


    async def convert(self):
        ts = await self._get_ts()
        ts_last_date: datetime = ts["date"].max().to_pydatetime()
        new_files = await self._get_files_to_add(ts_last_date)
        new_df = pd.DataFrame(new_files)
        extended_ts = pd.concat([ts, new_df])
        await self._save_ts(extended_ts)

    async def _get_ts(self) -> pd.DataFrame:
        ts_bytes = await self._s3.get_file(
            f"raw_ts/{self.brand_prefix}/time_series/raw_tss.parquet"
        )
        if ts_bytes is None:
            return pd.DataFrame()
        buffer = io.BytesIO(ts_bytes)
        table = pq.read_table(buffer)
        return table.to_pandas()

    async def _save_ts(self, df: pd.DataFrame):
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow")
        parquet_bytes = buffer.getvalue()
        await self._s3.upload_file(
            f"raw_ts/{self.brand_prefix}/time_series/raw_tss.parquet", parquet_bytes
        )
    
    @abstractmethod
    async def _get_files_to_add(self, last_date: datetime) -> list[dict]:
        pass

    async def _paths_to_download(self, last_date: datetime) -> list[str]:
        vins, _ = await self._s3.list_content(f"response/{self.brand_prefix}")
        vins_data = await asyncio.gather(
            *(
                self._s3.list_content(f"response/{self.brand_prefix}/{vin}/")
                for vin in vins
            )
        )
        path_to_dl: list[str] = []
        for _, vin_daily_files in vins_data:
            path_to_dl.extend(
                path
                for path in vin_daily_files
                if datetime.strptime(path.split("/")[-1], "%Y-%m-%d.json") > last_date
            )
        return path_to_dl

