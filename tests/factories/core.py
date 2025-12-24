"""Factories for core business models: Company, User, Fleet, Role."""

from typing import Any
from uuid import uuid4

import bcrypt
from polyfactory import Use

from db_models.company import Company, Make, Oem
from db_models.fleet import Fleet, UserFleet
from db_models.user import Role, User
from tests.factories.base import BaseAsyncFactory


class CompanyFactory(BaseAsyncFactory[Company]):
    __model__ = Company

    name = Use(lambda: f"Test Company {uuid4().hex[:8]}")
    description = "A test company for integration tests"


class RoleFactory(BaseAsyncFactory[Role]):
    __model__ = Role

    role_name = "admin"


class UserFactory(BaseAsyncFactory[User]):
    __model__ = User

    first_name = "Test"
    last_name = "User"
    email = Use(lambda: f"user-{uuid4().hex[:8]}@example.com")
    phone = "+33123456789"
    is_active = True
    last_connection = None

    @classmethod
    def with_password(
        cls, password: str = "testpassword123", **kwargs
    ) -> dict[str, Any]:
        """
        Helper to create a user with a hashed password.

        Usage:
            user = await UserFactory.create_async(
                session=db_session,
                company_id=company.id,
                role_id=role.id,
                **UserFactory.with_password("mypassword")
            )
        """
        hashed = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode(
            "utf-8"
        )

        return {
            "password": hashed,
            "plain_password": password,
            **kwargs,
        }

    @classmethod
    async def create_async(cls, **kwargs):
        """Override to add plain_password attribute after creation."""
        # Extract plain_password parameter if provided
        plain_password = kwargs.pop("plain_password", "testpassword123")

        # If password not provided, use with_password helper
        if "password" not in kwargs:
            kwargs.update(cls.with_password(plain_password))
            # Remove plain_password from kwargs since it's already extracted
            # and the User model doesn't have this field
            kwargs.pop("plain_password", None)

        instance = await super().create_async(**kwargs)

        # Attach plain password for testing (not in DB)
        instance.plain_password = plain_password

        return instance


class FleetFactory(BaseAsyncFactory[Fleet]):
    __model__ = Fleet

    fleet_name = Use(lambda: f"Test Fleet {uuid4().hex[:8]}")


class UserFleetFactory(BaseAsyncFactory[UserFleet]):
    __model__ = UserFleet

    # user_id, fleet_id, and role_id must be provided


class OemFactory(BaseAsyncFactory[Oem]):
    __model__ = Oem

    oem_name = "tesla"
    description = "Tesla Motors"
    trendline = None
    trendline_min = None
    trendline_max = None


class MakeFactory(BaseAsyncFactory[Make]):
    __model__ = Make

    # oem_id must be provided
    make_name = "tesla"
    description = "Tesla vehicles"


__all__ = [
    "CompanyFactory",
    "FleetFactory",
    "MakeFactory",
    "OemFactory",
    "RoleFactory",
    "UserFactory",
    "UserFleetFactory",
]
