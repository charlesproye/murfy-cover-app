"""Base factory classes with async SQLAlchemy persistence for polyfactory."""

from abc import ABC
from typing import Any, TypeVar

from polyfactory.factories.sqlalchemy_factory import SQLAlchemyFactory
from polyfactory.persistence import AsyncPersistenceProtocol
from sqlalchemy.ext.asyncio import AsyncSession

T = TypeVar("T")


class SQLAlchemyAsyncPersistence(AsyncPersistenceProtocol[T]):
    """Async persistence handler for SQLAlchemy."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def save(self, data: T) -> T:
        """Save instance to database and return it."""
        self.session.add(data)
        await self.session.flush()
        await self.session.refresh(data)
        return data

    async def save_many(self, data: list[T]) -> list[T]:
        """Save multiple instances to database and return them."""
        for instance in data:
            self.session.add(instance)
        await self.session.flush()
        for instance in data:
            await self.session.refresh(instance)
        return data


class BaseAsyncFactory(SQLAlchemyFactory[T], ABC):
    """
    Base factory with async persistence for all model types.

    Supports models using BaseUUID, BaseUUIDCreatedAt, and BaseUUIDModel.
    """

    __set_relationships__ = False  # We'll handle relationships manually
    __set_foreign_keys__ = False  # We'll provide FK values explicitly
    __is_base_factory__ = True  # Tell polyfactory this is an abstract base

    @classmethod
    async def create_async(cls, **kwargs: Any) -> T:
        """
        Create and persist a model instance asynchronously.

        Args:
            session: AsyncSession instance (required in kwargs)
            **kwargs: Model field values

        Usage:
            company = await CompanyFactory.create_async(
                session=db_session,
                name="Custom Name"
            )
        """
        session: AsyncSession = kwargs.pop("session")
        cls.__async_persistence__ = SQLAlchemyAsyncPersistence(session)
        return await super().create_async(**kwargs)

    @classmethod
    async def create_batch_async(cls, size: int, **kwargs: Any) -> list[T]:
        """
        Create and persist multiple instances asynchronously.

        Args:
            size: Number of instances to create
            session: AsyncSession instance (required in kwargs)
            **kwargs: Model field values

        Usage:
            companies = await CompanyFactory.create_batch_async(
                size=5,
                session=db_session
            )
        """
        session: AsyncSession = kwargs.pop("session")
        cls.__async_persistence__ = SQLAlchemyAsyncPersistence(session)
        return await super().create_batch_async(size=size, **kwargs)


__all__ = [
    "BaseAsyncFactory",
    "SQLAlchemyAsyncPersistence",
]
