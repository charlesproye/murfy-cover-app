"""Alembic environment for the data-engineering database."""

from datetime import UTC, datetime
from logging.config import fileConfig

from alembic import context, operations
from sqlalchemy import create_engine, pool

from db_models.core.config import Settings
from db_models.data_eng.base import DataEngBase

# Alembic Config object
config = context.config
config_file = config.config_file_name
if config_file:
    fileConfig(config_file)

settings = Settings()
url = settings.DB_DATA_ENG_URI

if not url:
    raise RuntimeError(
        "DB_DATA_ENG_URI is not configured. "
        "Set DB_DATA_ENG_* environment variables to run data-engineering migrations."
    )

target_metadata = DataEngBase.metadata


def process_revision_directives(context, revision, directives):
    """Align revision id generation with the main DB (timestamp-based)."""
    script = directives[0]
    script.rev_id = datetime.now(UTC).strftime("%Y%m%d%H%M")

    # Drop empty revisions generated when no diffs are present.
    if script.upgrade_ops.is_empty():
        directives[:] = []
        return

    # Filter out table comment operations (match main env behavior)
    def filter_comment_ops(ops_list):
        filtered = []
        for op in ops_list:
            if hasattr(op, "ops"):
                filtered_nested = filter_comment_ops(op.ops)
                if filtered_nested:
                    op.ops = filtered_nested
                    filtered.append(op)
            elif hasattr(op, "__class__"):
                if "TableComment" not in op.__class__.__name__:
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
    """Run migrations in 'offline' mode."""
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
        compare_server_default=True,
        dialect_opts={"paramstyle": "named"},
        process_revision_directives=process_revision_directives,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode."""
    connectable = create_engine(url, poolclass=pool.NullPool, future=True)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
            render_as_batch=False,
            include_schemas=True,
            process_revision_directives=process_revision_directives,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
