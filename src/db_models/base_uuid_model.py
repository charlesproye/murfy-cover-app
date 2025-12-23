import uuid

from sqlalchemy import DateTime, event
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, declared_attr, mapped_column
from sqlalchemy.sql import DDL, func, text

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
        while doc.startswith(("\n", " ")) or doc.endswith(("\n", " ")):
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
        server_default=text("gen_random_uuid()"),
    )


class BaseUUIDCreatedAt(Base):
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
        server_default=text("gen_random_uuid()"),
    )
    created_at = mapped_column(
        DateTime,
        nullable=True,
        server_default=func.now(),
        comment="Date of creation of this row",
    )


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
        server_default=text("gen_random_uuid()"),
    )
    created_at = mapped_column(
        DateTime,
        nullable=True,
        server_default=func.now(),
        comment="Date of creation of this row",
    )
    updated_at = mapped_column(
        DateTime,
        nullable=True,
        server_default=func.now(),
        comment="Date of the last update to this row",
    )

    @classmethod
    def __declare_last__(cls):
        """Create trigger for automatically updating updated_at on UPDATE."""
        # Only create trigger for non-abstract classes
        if not cls.__dict__.get("__abstract__", False):
            # Get table name and schema from the table metadata
            table_name = cls.__table__.name
            table_schema = cls.__table__.schema or "public"

            # Build the full table identifier with proper quoting
            if table_schema != "public":
                full_table_name = f'"{table_schema}"."{table_name}"'
                trigger_name = f"update_{table_schema}_{table_name}_updated_at"
            else:
                full_table_name = f'"{table_name}"'
                trigger_name = f"update_{table_name}_updated_at"

            # Sanitize trigger name (PostgreSQL limit is 63 chars)
            trigger_name = trigger_name.replace("-", "_").replace(".", "_")
            if len(trigger_name) > 63:
                trigger_name = trigger_name[:63]

            trigger_ddl = DDL(f"""
                DROP TRIGGER IF EXISTS "{trigger_name}" ON {full_table_name};
                CREATE TRIGGER "{trigger_name}"
                BEFORE UPDATE ON {full_table_name}
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column();
            """)
            event.listen(cls.__table__, "after_create", trigger_ddl)
