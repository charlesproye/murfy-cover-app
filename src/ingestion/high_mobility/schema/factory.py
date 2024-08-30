import functools
from dataclasses import dataclass
from importlib import import_module, resources
from typing import Optional, Type

import msgspec


class BrandNotRegisteredException(Exception):
    def __init__(self, brand: str) -> None:
        self.message = f"The brand {brand} has not been registered"
        super().__init__(self.message)


class MergedClassNotRegisteredException(Exception):
    def __init__(self, brand: str) -> None:
        self.message = f"The merged class for the brand {brand} has not been registered"
        super().__init__(self.message)


@dataclass
class BrandInfo:
    cls: Type
    rate_limit: int


@dataclass
class Brand:
    info_class: Type
    merged_info_class: Type
    rate_limit: int


# Dictionary with information about all registered plugins
_INFO_CLASSES: dict[str, BrandInfo] = {}
_MERGED_INFO_CLASSES: dict[str, Type] = {}


def to_api_name(brand: str) -> str:
    normalized_brand_name = brand.replace("_", "-")
    return normalized_brand_name


def to_module_name(brand: str) -> str:
    normalized_brand_name = brand.replace("-", "_")
    return normalized_brand_name


def register_brand(_cls: Optional[Type] = None, *, rate_limit: int):
    """Decorator for registering a new brand"""

    def decorate_class(cls: Type):
        brand = cls.__module__.rsplit(".")[-1]
        normalized_brand_name = to_api_name(brand)
        _INFO_CLASSES[normalized_brand_name] = BrandInfo(cls, rate_limit)
        return cls

    if _cls is None:
        return decorate_class
    else:
        return decorate_class(_cls)


def register_merged(cls: Type) -> Type:
    brand = cls.__module__.rsplit(".")[-1]
    normalized_brand_name = to_api_name(brand)
    if not _INFO_CLASSES.get(normalized_brand_name):
        raise BrandNotRegisteredException(brand)
    _MERGED_INFO_CLASSES[normalized_brand_name] = cls
    return cls


def get_info_class(brand: str) -> BrandInfo:
    import_module(f"{__package__}.brands.{brand}")
    return _INFO_CLASSES[brand]


def get_merged_info_class(brand: str) -> Type:
    import_module(f"{__package__}.brands.{brand}")
    return _MERGED_INFO_CLASSES[brand]


def make_all_brands() -> dict[str, Brand]:
    res: dict[str, Brand] = {}
    files = resources.contents(f"{__package__}.brands")
    brands = [f[:-3] for f in files if f.endswith(".py") and f[0] != "_"]
    for brand in brands:
        import_module(f"{__package__}.brands.{brand}")
        normalized_brand_name = to_api_name(brand)
        if not _INFO_CLASSES.get(normalized_brand_name):
            raise BrandNotRegisteredException(brand)
        if not _MERGED_INFO_CLASSES.get(normalized_brand_name):
            raise MergedClassNotRegisteredException(brand)
        res[normalized_brand_name] = Brand(
            info_class=_INFO_CLASSES[normalized_brand_name].cls,
            merged_info_class=_MERGED_INFO_CLASSES[normalized_brand_name],
            rate_limit=_INFO_CLASSES[normalized_brand_name].rate_limit,
        )
    print(res)
    return res


def decode_factory(brand: str):
    normalized_brand_name = to_module_name(brand)
    import_module(f"{__package__}.brands.{normalized_brand_name}")
    return functools.partial(msgspec.json.decode, type=_INFO_CLASSES[brand].cls)

