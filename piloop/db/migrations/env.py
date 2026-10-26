from alembic import context
from sqlalchemy import Connection, engine_from_config, event, pool

from piloop.db.models import Base
from piloop.db.schema import CORE, schemas
from piloop.db.session import CONNECTION_ATTRIBUTE, SCHEMA_ATTRIBUTE, version_table
from piloop.db.triggers.fts import is_fts_object

config = context.config
SCHEMA: str = config.attributes.get(SCHEMA_ATTRIBUTE) or (
    CORE if config.config_ini_section == 'alembic' else config.config_ini_section
)
"""The schema whose chain runs, by name: ``migrate`` names it, and ``alembic --name <name>`` (``alembic.ini``) for a
new revision. The section ``[alembic]`` is core's."""
OWNED = frozenset(table.__tablename__ for table in schemas()[SCHEMA].tables)
target_metadata = Base.metadata


def include_name(name: str | None, type_: str, _parent_names: object) -> bool:
    """Hide the FTS5 indexes from autogenerate.

    Virtual tables and their shadow tables are not models. Without this filter, ``alembic check`` at head would want to
    drop them, and ``revision --autogenerate`` would write that drop.
    """
    return not (type_ == 'table' and name is not None and is_fts_object(name))


def include_object(_object: object, name: str | None, type_: str, _reflected: bool, _compare_to: object) -> bool:
    """Compare only the tables of ``SCHEMA``: another schema's tables and version table are not its to change."""
    return type_ != 'table' or name in OWNED


def configure(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        render_as_batch=True,
        include_name=include_name,
        include_object=include_object,
        version_table=version_table(SCHEMA),
        transactional_ddl=True,
    )


def run_migrations_online() -> None:
    """Run every pending revision of ``SCHEMA`` in one transaction, so an upgrade that fails or is killed changes nothing.

    ``migrate`` passes its connection, in the transaction that runs every chain. Else, as for a new revision,
    this opens one. pysqlite commits before each DDL statement by itself, so the driver's own transaction handling is
    turned off and the transaction is begun by hand.
    """
    shared: Connection | None = config.attributes.get(CONNECTION_ATTRIBUTE)
    if shared is not None:
        configure(shared)
        context.run_migrations()
        return
    connectable = engine_from_config(config.get_section(config.config_ini_section, {}), poolclass=pool.NullPool)
    event.listen(connectable, 'connect', lambda dbapi, _: setattr(dbapi, 'isolation_level', None))
    event.listen(connectable, 'begin', lambda connection: connection.exec_driver_sql('BEGIN'))
    with connectable.connect() as connection:
        configure(connection)
        with context.begin_transaction():
            context.run_migrations()
    connectable.dispose()


run_migrations_online()
