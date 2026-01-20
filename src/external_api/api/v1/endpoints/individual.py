from fastapi import APIRouter, Depends, Path, Query

from core.sql_utils import get_async_db
from external_api.core.cookie_auth import (
    get_current_user_from_cookie,
    get_user_with_fleets,
)
from external_api.schemas.user import GetCurrentUser
from external_api.services.individual import get_all_pinned_vehicles

router = APIRouter()


@router.get("/favorite_table/{fleet_id}", include_in_schema=False)
async def pinned_vehicles(
    db=Depends(get_async_db),
    fleet_id: str = Path(..., description="The fleet id"),
    page: int = Query(1, description="The page number"),
    limit: int = Query(50, description="The number of items per page"),
    user: GetCurrentUser = Depends(get_current_user_from_cookie(get_user_with_fleets)),
):
    fleet_ids = [fleet_id] if fleet_id != "all" else [fleet.id for fleet in user.fleets]
    response = await get_all_pinned_vehicles(fleet_ids, page, limit, db)

    return response
