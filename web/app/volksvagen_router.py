import logging

from fastapi import APIRouter, Depends, Request

from .response_storage import ResponseStorageDep
from .schemas import (
    ChargingRemainingTime,
    ChargingState,
    CruisingRange,
    DashboardErrorWarning,
    EnergyLevel,
    Location,
    Maintenance,
    Trip,
)

# # TODO:
# - remove useless logs
# - enforce security over endpoint when we have confirmation vw call them with the good api key


def authenticate(
    request: Request,
):
    api_key = request.headers.get("api-key")
    logging.info(f"{api_key = }")
    # if api_key is None:
    #     raise HTTPException(
    #         status_code=status.HTTP_401_UNAUTHORIZED,
    #         detail="api-key header missing",
    #     )
    return api_key


volkswagen_router = APIRouter(
    prefix="",
    dependencies=[Depends(authenticate)],
    tags=["Volkswagen"],
)


@volkswagen_router.post("/trips")
async def post_trips(
    trips: list[Trip],
    storage_service: ResponseStorageDep,
):
    logging.info(f"CONTENT = {trips}")
    await storage_service.store_basemodels_with_vin("volkswagen", trips)
    return


@volkswagen_router.post("/maintenances")
async def post_maintenances(
    storage_service: ResponseStorageDep,
    maintenances: list[Maintenance],
):
    logging.info(f"CONTENT = {maintenances}")
    await storage_service.store_basemodels_with_vin("volkswagen", maintenances)
    return {}


@volkswagen_router.post("/locations")
async def post_locations(
    locations: list[Location],
    storage_service: ResponseStorageDep,
):
    logging.info(f"CONTENT = {locations}")
    await storage_service.store_basemodels_with_vin("volkswagen", locations)
    return {}


@volkswagen_router.post("/cruising-ranges")
async def post_cruising_ranges(
    cruising_ranges: list[CruisingRange],
    storage_service: ResponseStorageDep,
):
    logging.info(f"CONTENT = {cruising_ranges}")
    await storage_service.store_basemodels_with_vin("volkswagen", cruising_ranges)
    return {}


@volkswagen_router.post("/dashboard-error-warnings")
async def post_dashboard_error_warnings(
    dashboard_error_warnings: list[DashboardErrorWarning],
    storage_service: ResponseStorageDep,
):
    logging.info(f"CONTENT = {dashboard_error_warnings}")
    await storage_service.store_basemodels_with_vin(
        "volkswagen", dashboard_error_warnings
    )
    return {}


@volkswagen_router.post("/energy-levels")
async def post_energy_levels(
    energy_levels: list[EnergyLevel],
    storage_service: ResponseStorageDep,
):
    logging.info(f"CONTENT = {energy_levels}")
    await storage_service.store_basemodels_with_vin("volkswagen", energy_levels)
    return {}


@volkswagen_router.post("/charging-states")
async def post_charging_states(
    charging_states: list[ChargingState],
    storage_service: ResponseStorageDep,
):
    logging.info(f"CONTENT = {charging_states}")
    await storage_service.store_basemodels_with_vin("volkswagen", charging_states)
    return {}


@volkswagen_router.post("/charging-remaining-times")
async def post_charging_remaining_times(
    charging_remaining_times: list[ChargingRemainingTime],
    storage_service: ResponseStorageDep,
):
    logging.info(f"CONTENT = {charging_remaining_times}")
    await storage_service.store_basemodels_with_vin(
        "volkswagen", charging_remaining_times
    )
    return {}


# Test route
@volkswagen_router.post("/")
async def test_post():
    return

