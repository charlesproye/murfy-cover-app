from .factory import decode_factory


def decode_vehicle_info(content: bytes, brand: str):
    decoder = decode_factory(brand)
    return decoder(content)

