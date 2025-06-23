import asyncio
from datetime import datetime
from src.core.s3.async_s3 import AsyncS3
import pandas as pd
import pyarrow.parquet as pq
import io
from abc import ABC, abstractmethod

class ResponseToRaw(ABC):
    """Converter class that could be extended or modified easily to be used for all car brand conversion from response to raw. Only the  _get_files_to_add method have to be implemented to return the list of new data as dict. Each element of the list should be a dict representing one datapoint and the dict must contain at least the key 'date' and 'vin'"""

    def __init__(self, s3: AsyncS3 | None = None, max_file:int = 200) -> None:
        self._s3 = s3 or AsyncS3()
        self.max_file = max_file

    @property
    @abstractmethod
    def brand_prefix(self) -> str:
        pass

    async def convert(self):
        ts = await self._get_ts()
        if len(ts) == 0:
            ts_last_date = datetime(year=0,month=0,day=0)
        else:
            ts_last_date: datetime = ts["date"].max().to_pydatetime()
        new_files = await self._get_files_to_add(ts_last_date)
        new_df = pd.DataFrame(new_files)
        extended_ts = pd.concat([ts, new_df])
        extended_ts['date'] = pd.to_datetime(extended_ts['date'], utc=True)
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
    
    async def _get_files_to_add(self, last_date: datetime) -> list[dict]:
        path_to_download = await self._paths_to_download(last_date)
        sorted_path = sorted(
            path_to_download, 
            key=lambda x:datetime.strptime(x.split("/")[-1], "%Y-%m-%d.json")
            )[:self.max_file]
        new_datas = await self._s3.get_files(sorted_path)
        json_data:list[dict] = []
        for path, data in new_datas.items():
            values = self.build_dict_value_from_path_data(  path,data)
            json_data.extend(values)
        return json_data
    
    @abstractmethod
    def build_dict_value_from_path_data(self, path:str, data:bytes)->list[dict]:
        pass

    async def _paths_to_download(self, last_date: datetime) -> list[str]:
        vins, _ = await self._s3.list_content(f"response/{self.brand_prefix}/")
        vins_data = await asyncio.gather(
            *(
                self._s3.list_content(vin)
                for vin in vins
            )
        )
        path_to_dl: list[str] = []
        date = last_date.replace(tzinfo=None)
        for _, vin_daily_files in vins_data:
            path_to_dl.extend(
                path
                for path in vin_daily_files
                if datetime.strptime(path.split("/")[-1], "%Y-%m-%d.json") > date
            )
        return path_to_dl
 
