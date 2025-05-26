from pprint import pprint
from fastapi import APIRouter, Depends, HTTPException, Request, status

from core.logging_utils import LoggerDep
from .schemas import (
    Trip,
    Maintenance,
    Location,
    CruisingRange,
    EnergyLevel,
    DashboardErrorWarning,
    ChargingState,
    ChargingRemainingTime,
)
from .response_storage import ResponseStorageDep


def authenticate(request: Request, logger: LoggerDep):
    logger.debug(f"GETTING AUTH")
    api_key = request.headers.get("api-key")
    logger.debug(f"{api_key = }")
    # if api_key is None:
    #     raise HTTPException(
    #         status_code=status.HTTP_401_UNAUTHORIZED,
    #         detail="api-key header missing",
    #     )
    return api_key


volkswagen_router = APIRouter(
    prefix="", dependencies=[Depends(authenticate)], tags=["Volkswagen"]
)


# TODO finish each end point to store data in response storage object
@volkswagen_router.post("/trips")
async def post_trips(
    trips: list[Trip],
    storage_service: ResponseStorageDep,
    logger: LoggerDep,
):
    logger.debug("/trips CALLED")
    logger.debug(f"CONTENT = {trips}")
    storage_service.store_basemodels_with_vin(trips)
    return


@volkswagen_router.post("/maintenances")
async def post_maintenances(
    storage_service: ResponseStorageDep,
    maintenances: list[Maintenance],
    logger: LoggerDep,
):
    logger.debug("/maintenances CALLED")
    logger.debug(f"CONTENT = {maintenances}")
    storage_service.store_basemodels_with_vin(maintenances)
    return {}


@volkswagen_router.post("/locations")
async def post_locations(
    locations: list[Location],
    storage_service: ResponseStorageDep,
    logger: LoggerDep,
):
    logger.debug("/locations CALLED")
    logger.debug(f"CONTENT = {locations}")
    storage_service.store_basemodels_with_vin(locations)
    return {}


@volkswagen_router.post("/cruising-ranges")
async def post_cruising_ranges(
    cruising_ranges: list[CruisingRange],
    storage_service: ResponseStorageDep,
    logger: LoggerDep,
):
    logger.debug("/cruising-ranges CALLED")
    logger.debug(f"CONTENT = {cruising_ranges}")
    storage_service.store_basemodels_with_vin(cruising_ranges)
    return {}


@volkswagen_router.post("/dashboard-error-warnings")
async def post_dashboard_error_warnings(
    dashboard_error_warnings: list[DashboardErrorWarning],
    storage_service: ResponseStorageDep,
    logger: LoggerDep,
):
    logger.debug("/dashboard-error-warnings CALLED")
    logger.debug(f"CONTENT = {dashboard_error_warnings}")
    storage_service.store_basemodels_with_vin(dashboard_error_warnings)
    return {}


@volkswagen_router.post("/energy-levels")
async def post_energy_levels(
    energy_levels: list[EnergyLevel],
    storage_service: ResponseStorageDep,
    logger: LoggerDep,
):
    logger.debug("/energy-levels CALLED")
    logger.debug(f"CONTENT = {energy_levels}")
    storage_service.store_basemodels_with_vin(energy_levels)
    return {}


@volkswagen_router.post("/charging-states")
async def post_charging_states(
    charging_states: list[ChargingState],
    storage_service: ResponseStorageDep,
    logger: LoggerDep,
):
    logger.debug("/charging-states CALLED")
    logger.debug(f"CONTENT = {charging_states}")
    storage_service.store_basemodels_with_vin(charging_states)
    return {}


@volkswagen_router.post("/charging-remaining-times")
async def post_charging_remaining_times(
    charging_remaining_times: list[ChargingRemainingTime],
    storage_service: ResponseStorageDep,
    logger: LoggerDep,
):
    logger.debug("/charging-remaining-times CALLED")
    logger.debug(f"CONTENT = {charging_remaining_times}")
    storage_service.store_basemodels_with_vin(charging_remaining_times)
    return {}


# Test route
@volkswagen_router.post("/")
async def test_post(
    request: Request,
    logger: LoggerDep,
):
    logger.debug("HEADERS")
    pprint(request.headers.__dict__)
    logger.debug("BODY")
    pprint(await request.body())
    return

