import asyncio
from datetime import datetime


import msgspec
from src.core.response_to_raw import ResponseToRaw


class BMWResponseToRaw(ResponseToRaw):
    @property
    def brand_prefix(self) -> str:
        return 'bmw'

    def build_dict_value_from_path_data(self, path:str, data:bytes)->list[dict]:
        decoded = msgspec.json.decode(data)
        values = decoded['data']
        values = [value for value in values if value['value']!=None and value['date_of_value']!=None]
        for value in values:
            value['vin'] = path.split('/')[-2]
            value['date'] = datetime.fromisoformat(value['date_of_value']).replace(tzinfo=None)
        return [value for value in values if value['value']!=None]


if __name__ == "__main__":
    RESPONSE_TO_RAW = BMWResponseToRaw()
    asyncio.run(RESPONSE_TO_RAW.convert())

