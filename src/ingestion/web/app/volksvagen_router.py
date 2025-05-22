from pprint import pprint
from fastapi import APIRouter, Depends, HTTPException, Request, status
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


def authenticate(request: Request):
    print(f"GETTING AUTH")
    api_key = request.headers.get("api-key")
    print(f"{api_key = }")
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
async def post_trips(trips: list[Trip]):
    print("/trips CALLED")
    print(f"CONTENT = {trips}")
    return


@volkswagen_router.post("/maintenances")
async def post_maintenances(maintenances: list[Maintenance]):
    print("/maintenances CALLED")
    print(f"CONTENT = {maintenances}")
    return {}


@volkswagen_router.post("/locations")
async def post_locations(locations: list[Location]):
    print("/locations CALLED")
    print(f"CONTENT = {locations}")
    return {}


@volkswagen_router.post("/cruising-ranges")
async def post_cruising_ranges(cruising_ranges: list[CruisingRange]):
    print("/cruising-ranges CALLED")
    print(f"CONTENT = {cruising_ranges}")
    return {}


@volkswagen_router.post("/dashboard-error-warnings")
async def post_dashboard_error_warnings(
    dashboard_error_warnings: list[DashboardErrorWarning],
):
    print("/dashboard-error-warnings CALLED")
    print(f"CONTENT = {dashboard_error_warnings}")
    return {}


@volkswagen_router.post("/energy-levels")
async def post_energy_levels(energy_levels: list[EnergyLevel]):
    print("/energy-levels CALLED")
    print(f"CONTENT = {energy_levels}")
    return {}


@volkswagen_router.post("/charging-states")
async def post_charging_states(charging_states: list[ChargingState]):
    print("/charging-states CALLED")
    print(f"CONTENT = {charging_states}")
    return {}


@volkswagen_router.post("/charging-remaining-times")
async def post_charging_remaining_times(
    charging_remaining_times: list[ChargingRemainingTime],
):
    print("/charging-remaining-times CALLED")
    print(f"CONTENT = {charging_remaining_times}")
    return {}


# Test route
@volkswagen_router.post("/")
async def test_post(request: Request):
    print("HEADERS")
    pprint(request.headers.__dict__)
    print("BODY")
    pprint(await request.body())
    return

