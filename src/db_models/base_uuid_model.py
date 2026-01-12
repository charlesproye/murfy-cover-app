import uuid

from sqlalchemy import BigInteger, DateTime, event
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, declared_attr, mapped_column
from sqlalchemy.sql import DDL, func, text

# ------------------------------------------------------------------------------
# Base SQLAlchemy
# ------------------------------------------------------------------------------

Base_ = declarative_base()
ListTables = []


class Base(Base_):
    __abstract__ = True

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not cls.__dict__.get("__abstract__", False):
            ListTables.append(cls)

    @declared_attr
    def __tablename__(cls) -> str:
        return cls.__name__

    @declared_attr
    def __table_args__(cls):
        doc = cls.__doc__ or ""
        table_args = cls.__dict__.get("table_args", ())

        doc = doc.strip().strip("\n")

        if not table_args:
            return ({"comment": doc},)

        if isinstance(table_args[-1], dict):
            table_args[-1]["comment"] = doc
            return table_args

        return (*table_args, {"comment": doc})


class BaseUUID(Base):
    __abstract__ = True

    id = mapped_column(
        UUID,
        default=uuid.uuid4,
        primary_key=True,
        nullable=False,
        server_default=text("gen_random_uuid()"),
        comment="Unique identifier of the row",
    )


class BaseCreatedAt:
    __abstract__ = True

    created_at = mapped_column(
        DateTime,
        nullable=True,
        server_default=func.now(),
        comment="Date of creation of this row",
    )


class BaseUpdatedAt:
    __abstract__ = True

    updated_at = mapped_column(
        DateTime,
        nullable=True,
        server_default=func.now(),
        comment="Date of the last update to this row",
    )

    @classmethod
    def __declare_last__(cls):
        if cls.__dict__.get("__abstract__", False):
            return

        table = cls.__table__  # type: ignore[attr-defined]
        table_name = table.name
        table_schema = table.schema or "public"

        if table_schema != "public":
            full_table_name = f'"{table_schema}"."{table_name}"'
            trigger_name = f"update_{table_schema}_{table_name}_updated_at"
        else:
            full_table_name = f'"{table_name}"'
            trigger_name = f"update_{table_name}_updated_at"

        trigger_name = trigger_name.replace("-", "_").replace(".", "_")[:63]

        trigger_ddl = DDL(f"""
            DROP TRIGGER IF EXISTS "{trigger_name}" ON {full_table_name};
            CREATE TRIGGER "{trigger_name}"
            BEFORE UPDATE ON {full_table_name}
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
        """)

        event.listen(table, "after_create", trigger_ddl)


class BaseUUIDModel(BaseUUID, BaseCreatedAt, BaseUpdatedAt):
    __abstract__ = True


class BaseUUIDCreatedAt(BaseUUID, BaseCreatedAt):
    __abstract__ = True


class BaseAutoIncrementModel(Base, BaseCreatedAt, BaseUpdatedAt):
    __abstract__ = True

    id = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
        nullable=False,
        comment="Auto-incrementing identifier of the row",
    )
