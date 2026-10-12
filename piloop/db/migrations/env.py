from alembic import context
from sqlalchemy import engine_from_config, pool

from piloop.db.models import Base
from piloop.db.triggers.fts import is_fts_object

config = context.config
target_metadata = Base.metadata


def include_name(name: str | None, type_: str, _parent_names: object) -> bool:
    """Hide the FTS5 indexes from autogenerate.

    Virtual tables and their shadow tables are not models. Without this filter, ``alembic check`` at head would want to
    drop them, and ``revision --autogenerate`` would write that drop.
    """
    return not (type_ == 'table' and name is not None and is_fts_object(name))


def run_migrations_online() -> None:
    connectable = engine_from_config(config.get_section(config.config_ini_section, {}), poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata, render_as_batch=True, include_name=include_name
        )
        with context.begin_transaction():
            context.run_migrations()
    connectable.dispose()


run_migrations_online()
