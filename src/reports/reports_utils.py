from db_models import Asset
from external_api.services.s3 import get_make_image_url, get_model_image_url


def get_image_url(
    vehicle_image_asset: Asset | None, make_image_asset: Asset | None
) -> str | None:
    if vehicle_image_asset and vehicle_image_asset.public_url:
        return vehicle_image_asset.public_url
    elif vehicle_image_asset and vehicle_image_asset.name:
        return get_model_image_url(vehicle_image_asset.name)
    elif make_image_asset and make_image_asset.public_url:
        return make_image_asset.public_url
    elif make_image_asset and make_image_asset.name:
        return get_make_image_url(make_image_asset.name)

    return None
