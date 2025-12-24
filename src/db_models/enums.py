import enum


class LanguageEnum(str, enum.Enum):
    FR = "FR"
    EN = "EN"


class AssetTypeEnum(str, enum.Enum):
    car_images = "car_images"
    make_images = "make_images"
