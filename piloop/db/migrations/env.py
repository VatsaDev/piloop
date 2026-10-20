from alembic import context
from sqlalchemy import engine_from_config, event, pool

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
    """Run every pending revision in one transaction, so an upgrade that fails or is killed changes nothing.

    pysqlite commits before each DDL statement by itself, so the driver's own transaction handling is turned off and
    the transaction is begun by hand.
    """
    connectable = engine_from_config(config.get_section(config.config_ini_section, {}), poolclass=pool.NullPool)
    event.listen(connectable, 'connect', lambda dbapi, _: setattr(dbapi, 'isolation_level', None))
    event.listen(connectable, 'begin', lambda connection: connection.exec_driver_sql('BEGIN'))
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=True,
            include_name=include_name,
            transactional_ddl=True,
        )
        with context.begin_transaction():
            context.run_migrations()
    connectable.dispose()


run_migrations_online()
