import asyncio


import msgspec
from src.core.response_to_raw import ResponseToRaw


class VWResponseToRaw(ResponseToRaw):

    @property
    def brand_prefix(self) -> str:
        return 'volkswagen'

    def build_dict_value_from_path_data(self, path:str, data:bytes)->list[dict]: # TODO; verify
        decoded = msgspec.json.decode(data)
        return decoded

if __name__ == "__main__":
    RESPONSE_TO_RAW = VWResponseToRaw()
    asyncio.run(RESPONSE_TO_RAW.convert())

