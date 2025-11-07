from enum import Enum


class TeslaRegions(str, Enum):
    """Tesla Fleet API regions with their corresponding base URLs.

    https://developer.tesla.com/docs/fleet-api/getting-started/regions-countries#base-urls-by-region
    """

    NORTH_AMERICA = "NORTH_AMERICA"
    EUROPE = "EUROPE"
    CHINA = "CHINA"


FLEET_URLS: dict[TeslaRegions, str] = {
    TeslaRegions.NORTH_AMERICA: "https://fleet-api.prd.na.vn.cloud.tesla.com",
    TeslaRegions.EUROPE: "https://fleet-api.prd.eu.vn.cloud.tesla.com",
    TeslaRegions.CHINA: "https://fleet-api.prd.cn.vn.cloud.tesla.cn",
}
