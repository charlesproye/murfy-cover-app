import asyncio
from logging.config import fileConfig

from alembic import context, operations
from sqlalchemy.ext.asyncio import create_async_engine

from db_models.base_uuid_model import Base
from db_models.core.config import Settings

settings = Settings()
url = settings.ASYNC_DB_DATA_EV_URI
# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)


target_metadata = Base.metadata


def process_revision_directives(context, revision, directives):
    """Modify the MigrationScript directives to create schema as required."""
    script = directives[0]
    
    # Filter out table comment operations
    # compare_table_comment=False does not work with this version of alembic
    def filter_comment_ops(ops_list):
        filtered = []
        for op in ops_list:
            # Check if this is a ModifyTableOps that contains comment operations
            if hasattr(op, 'ops'):
                # Recursively filter nested operations
                filtered_nested = filter_comment_ops(op.ops)
                if filtered_nested:
                    op.ops = filtered_nested
                    filtered.append(op)
            # Filter out table comment operations by checking the operation type name
            elif hasattr(op, '__class__'):
                op_class_name = op.__class__.__name__
                # Only filter out table comment operations, keep column comments
                if 'TableComment' not in op_class_name:
                    filtered.append(op)
            else:
                filtered.append(op)
        return filtered
    
    script.upgrade_ops.ops = filter_comment_ops(script.upgrade_ops.ops)
    script.downgrade_ops.ops = filter_comment_ops(script.downgrade_ops.ops)
    
    tables = target_metadata.tables.values()
    if tables:
        t = list(tables)[-1]
        if t.schema is not None and script.upgrade_ops.ops:
            script.upgrade_ops.ops.insert(
                0,
                operations.ops.ExecuteSQLOp(f"CREATE SCHEMA IF NOT EXISTS {t.schema}"),
            )
            script.downgrade_ops.ops.append(
                operations.ops.ExecuteSQLOp(
                    f"DROP SCHEMA IF EXISTS {t.schema} RESTRICT"
                )
            )


def run_migrations_offline():
    """Run migrations in 'offline' mode.
    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.
    Calls to context.execute() here emit the given string to the
    script output.
    """
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
        compare_server_default=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection):
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_schemas=True,
        process_revision_directives=process_revision_directives,
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online():
    """Run migrations in 'online' mode.
    In this scenario we need to create an Engine
    and associate a connection with the context.
    """
    connectable = create_async_engine(url, echo=False, future=True)

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())

