"""Which tables are core's and which are a plugin's.

A plugin declares its own tables in a ``Schema`` (``SCHEMAS``), and every other table is core's. A plugin's tables reference its own tables, or a core table's ``id``, which core never renames or drops. Core's tables
reference only core's. So an uninstalled plugin leaves tables that nothing else needs.

Inspiration: Django's apps, each with its own models and migrations.
"""

import re
from dataclasses import dataclass
from pathlib import Path

from pydantic import TypeAdapter
from sqlalchemy import Column, Table

from piloop import deprecated
from piloop.db.models import (
    AgentSession,
    Base,
    Campaign,
    Comment,
    Event,
    FirstTransfer,
    Lease,
    LeaseWorker,
    Limit,
    Migration,
    Secret,
    Token,
    WorkerInstance,
)
from piloop.db.triggers import Trigger, on
from piloop.plugins.host import PluginError, Slot

CORE = 'core'
"""The name of piloop's own schema. No plugin declares a schema by this name."""
NAME = re.compile(r'[a-z][a-z0-9]*(_[a-z0-9]+)*(/[a-z][a-z0-9]*(_[a-z0-9]+)*)?')
"""A plugin's schema name: the provider that has the tables, such as ``memory/local``, or a name of its own."""


@dataclass(frozen=True)
class Schema:
    """What core or a plugin adds to the database: its tables, its migrations and its triggers."""

    tables: tuple[type[Base], ...]
    """Subclasses of ``piloop.db.models.Base``: one registry, so that a foreign key to a core table resolves."""
    versions: Path
    """The directory of its Alembic revisions: its own chain, ``0001`` first, in its own version table."""
    triggers: tuple[Trigger, ...] = ()
    """The triggers on its tables. Its revisions create the persistent ones (``piloop.db.triggers.recreate``), and
    every connection creates the TEMP ones."""


CORE_TABLES = (
    Campaign,
    AgentSession,
    Event,
    Comment,
    Lease,
    LeaseWorker,
    WorkerInstance,
    FirstTransfer,
    Limit,
    Secret,
    Token,
    Migration,
)
CORE_SCHEMA = Schema(
    tables=CORE_TABLES, versions=Path(__file__).parent / 'migrations' / 'versions', triggers=on(CORE_TABLES)
)


def _misnamed(name: str, _schema: Schema) -> str | None:
    if name == CORE:
        return f"{name!r} is piloop's own schema"
    if not NAME.fullmatch(name):
        return f'{name!r} is no schema name: lowercase words joined by _, and at most one /, such as memory/local'
    return None


SCHEMAS: Slot[Schema] = Slot('schemas', TypeAdapter(Schema), 'schema', _misnamed)
"""The schema of each plugin that has tables, by its name. The daemon checks them when it starts."""

CORE_REFERENCES = frozenset({('agent_sessions', 'run_id')})
"""Core's references to a plugin's table, as (table, column)."""
deprecated.until('0.4.0', 'agent_sessions.subject replaces agent_sessions.run_id: delete CORE_REFERENCES')


def schemas() -> dict[str, Schema]:
    """Core's schema, then each plugin's, by name, in the order of ``SCHEMAS``.

    Raises:
        PluginError: A schema does not load, two schemas have one table, or a foreign key crosses schemas other than
            to a core table's ``id``.
    """
    found = {CORE: CORE_SCHEMA, **SCHEMAS.loaded()}
    holder: dict[str, str] = {}
    for name, schema in found.items():
        for table in schema.tables:
            if (other := holder.get(table.__tablename__)) is not None:
                raise PluginError(f'the schema {name!r} has the table {table.__tablename__!r}, which {other!r} has')
            holder[table.__tablename__] = name
        if strays := sorted(t.name for t in schema.triggers if holder.get(t.table) != name):
            raise PluginError(f'the schema {name!r} has triggers on tables that are not its own: {", ".join(strays)}')
    for name, schema in found.items():
        for table in schema.tables:
            for key in table.__table__.foreign_keys:
                source = f'{table.__tablename__}.{key.parent.name}'
                if (why := _crossing(name, source, holder.get(key.column.table.name), key.column)) is not None:
                    raise PluginError(f'the schema {name!r}: {source} {why}')
    return found


def _crossing(name: str, source: str, to: str | None, target: Column[object]) -> str | None:
    """Why the reference of ``name`` from ``source`` to ``target``, a column of ``to``, crosses schemas, or None."""
    referenced = f'{target.table.name}.{target.name}'
    match to:
        case None:
            return f'references {referenced}, which no schema has'
        case _ if to == name:
            return None
        case 'core' if target.name == 'id':
            return None
        case _ if name == CORE and tuple(source.split('.')) in CORE_REFERENCES:
            return None
        case _ if name == CORE:
            return f"references {referenced} of {to!r}: core's tables reference only core's"
        case _:
            return f"references {referenced} of {to!r}: a plugin's table references its own, or the id of a core table"


def tables() -> list[Table]:
    """Every schema's tables, each after the tables that it references."""
    owned = {table.__tablename__ for schema in schemas().values() for table in schema.tables}
    return [t for t in Base.metadata.sorted_tables if t.name in owned]
