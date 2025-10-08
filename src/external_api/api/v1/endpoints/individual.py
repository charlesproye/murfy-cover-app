from fastapi import APIRouter, Depends, Path, Query

from external_api.core.security import get_current_user_with_fleet_id
from external_api.db.session import get_db
from external_api.schemas.individual import IndividualCrud

router = APIRouter()


def get_user_dependency(fleet_id: str):
    async def inner(fleet_id=fleet_id):
        return await get_current_user_with_fleet_id(fleet_id=fleet_id)

    return inner


@router.get("/vehicles/pinned/{fleet_id}", include_in_schema=False)
async def get_all_pinned_vehicles(
    db=Depends(get_db),
    fleet_id: str = Path(..., description="The fleet id"),
    page: int = Query(1, description="The page number"),
    limit: int = Query(50, description="The number of items per page"),
):
    response = await IndividualCrud().get_all_pinned_vehicles(fleet_id, page, limit, db)
    return response


@router.get("/vehicles/fast-charge/{fleet_id}", include_in_schema=False)
async def get_fast_charge(
    db=Depends(get_db),
    fleet_id: str = Path(..., description="The fleet id"),
    page: int = Query(1, description="The page number"),
    limit: int = Query(50, description="The number of items per page"),
):
    response = await IndividualCrud().get_all_fast_charge(fleet_id, page, limit, db)
    return response


@router.get("/vehicles/consumption/{fleet_id}", include_in_schema=False)
async def get_consumption(
    db=Depends(get_db),
    fleet_id: str = Path(..., description="The fleet id"),
    page: int = Query(1, description="The page number"),
    limit: int = Query(50, description="The number of items per page"),
):
    response = await IndividualCrud().get_all_consumption(fleet_id, page, limit, db)
    return response

