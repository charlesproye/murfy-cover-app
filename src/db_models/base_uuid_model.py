import uuid

from sqlalchemy import DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, declared_attr, mapped_column
from sqlalchemy.sql import func

Base_ = declarative_base()
ListTables = []


class Base(Base_):
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # test if the class is abstract
        if not cls.__abstract__ or "__abstract__" not in cls.__dict__:
            ListTables.append(cls)

    @declared_attr
    def __tablename__(cls) -> str:
        return cls.__name__

    @declared_attr
    def __table_args__(cls):
        doc = cls.__doc__
        table_args = cls.__dict__.get("table_args", ())
        if doc is None:
            doc = ""
        while (
            doc.startswith("\n")
            or doc.startswith(" ")
            or doc.endswith("\n")
            or doc.endswith(" ")
        ):
            doc = doc.strip(" ").strip("\n")
            doc = doc.strip(" ").strip("\n")
        if len(table_args) == 0:
            table_args = ({"comment": doc},)
        elif isinstance(table_args[-1], dict):
            table_args[-1]["comment"] = doc
        else:
            table_args = (*table_args, {"comment": doc})
        return table_args

    __abstract__ = True


class BaseUUIDModel(Base):
    @declared_attr
    def __tablename__(cls) -> str:
        return cls.__name__

    __abstract__ = True
    id = mapped_column(
        UUID,
        default=lambda: uuid.uuid4(),
        primary_key=True,
        index=False,
        nullable=False,
        comment="Unique identifier of the row",
    )
    updated_at = mapped_column(
        DateTime,
        nullable=True,
        comment="Date of the last update to this row",
        server_default=func.now(),
    )

    created_at = mapped_column(
        DateTime,
        nullable=True,
        server_default=func.now(),
        comment="Date of creation of this row",
    )


class BaseUUID(Base):
    @declared_attr
    def __tablename__(cls) -> str:
        return cls.__name__

    __abstract__ = True
    id = mapped_column(
        UUID,
        default=lambda: uuid.uuid4(),
        primary_key=True,
        index=False,
        nullable=False,
        comment="Unique identifier of the row",
    )

