from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from db_models import Company, Fleet, Role, User, UserFleet
from external_api.core.cookie_auth import create_tokens
from tests.factories import (
    CompanyFactory,
    FleetFactory,
    RoleFactory,
    UserFactory,
    UserFleetFactory,
)


@dataclass
class AuthContext:
    user: User
    fleet: Fleet
    role: Role
    company: Company
    access_token: str
    user_fleet: UserFleet


async def create_authenticated_user_with_fleet(db_session: AsyncSession) -> AuthContext:
    """
    Helper to create a fully setup user with fleet and auth token.
    Explicitly created to avoid implicit fixtures.
    """
    company = await CompanyFactory.create_async(session=db_session)
    role = await RoleFactory.create_async(session=db_session)
    user = await UserFactory.create_async(
        session=db_session,
        company_id=company.id,
        role_id=role.id,
    )

    fleet = await FleetFactory.create_async(
        session=db_session,
        company_id=company.id,
    )

    user_fleet = await UserFleetFactory.create_async(
        session=db_session,
        user_id=user.id,
        fleet_id=fleet.id,
        role_id=role.id,
    )

    # Create authentication token
    token_data = {
        "sub": user.email,
        "user_id": str(user.id),
        "company_id": str(user.company_id),
    }
    tokens = create_tokens(token_data)
    access_token = tokens["access_token"]

    return AuthContext(
        user=user,
        fleet=fleet,
        role=role,
        company=company,
        access_token=access_token,
        user_fleet=user_fleet,
    )
