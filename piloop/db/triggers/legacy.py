"""Today's triggers in the names of an older schema, for the migrations that run before a renaming of it.

The migrations create the persistent triggers that the package renders today (``recreate``). Revision 0016 renamed
``projects`` to ``campaigns``, with its ``_id`` columns, and revision 0018 renamed ``workers`` to ``agent_sessions``
and the worker role to the implementer. A migration before a renaming runs on a schema that still has the old names,
so ``rendered`` gives a trigger its old name and its old identifiers there. Only identifiers and the role names that a
trigger lists change: any other string literal keeps its text.
"""

import re
from dataclasses import dataclass, field

from alembic.operations import Operations
from sqlalchemy import text

from piloop import deprecated
from piloop.db.triggers.ddl import Trigger

deprecated.until('0.3.0', 'old migrations need the old trigger names: squash 0001-0018 at a 0.2.0 floor, delete legacy')


@dataclass(frozen=True)
class Stage:
    """One renaming of the schema: the old table that shows a schema from before it, and what it renamed."""

    revision: str
    """The revision that renamed the schema."""
    table: str
    """The table of the old schema that the renaming renamed: a schema that has it is older than the renaming."""
    names: dict[str, str]
    """Each identifier of the new schema, with the one that it had before."""
    words: dict[str, str]
    """The words of the names of triggers and indexes, new then old."""
    literals: dict[str, str] = field(default_factory=dict)
    """The string literals of the triggers, new then old: the role names that a trigger lists."""


STAGES = (
    Stage('0018', 'workers', {'agent_sessions': 'workers'}, {'agent_session': 'worker'}, {"'implementer'": "'worker'"}),
    Stage('0016', 'projects', {'campaigns': 'projects', 'campaign_id': 'project_id'}, {'campaign': 'project'}),
)
"""The renamings, newest first: a trigger of today takes the names before each in turn."""
TOKEN = re.compile(r"""'(?:[^']|'')*'|--[^\n]*|/\*.*?(?:\*/|$)|"(?:[^"]|"")*"|`[^`]*`|\[[^\]]*\]|\w+""", re.DOTALL)
"""A string literal, a comment, a quoted identifier or a bare word: the SQLite tokens that can hold a name."""


def identifiers(sql: str, names: dict[str, str]) -> str:
    """Return ``sql`` with each identifier that ``names`` lists renamed, bare or quoted, in any case."""

    def one(token: re.Match[str]) -> str:
        word = token.group()
        quoted = word[0] in '"`['
        new = names.get((word[1:-1] if quoted else word).lower())
        if new is None:  # also every string literal and comment: it is matched whole, so it is no name
            return word
        return f'{word[0]}{new}{word[-1]}' if quoted else new

    return TOKEN.sub(one, sql)


def old_name(name: str, stages: tuple[Stage, ...] = STAGES) -> str:
    """Return the name that a trigger, an index or a table had before ``stages``."""
    for stage in stages:
        for new, old in stage.words.items():
            name = re.sub(rf'(?<![a-z]){new}', old, name)
    return name


def stages(op: Operations) -> tuple[Stage, ...]:
    """Return the renamings that the schema that ``op`` migrates is older than: those whose old table it has."""
    query = text("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = :name")
    return tuple(s for s in STAGES if op.get_bind().execute(query, {'name': s.table}).scalar() is not None)


def rendered(trigger: Trigger, old: tuple[Stage, ...]) -> tuple[str, str, str | None, str]:
    """Return the name, the table, the required table and the SQL of ``trigger``, in the names before ``old``."""
    if not old:
        return trigger.name, trigger.table, trigger.requires, trigger.sql()
    name, sql, table = old_name(trigger.name, old), trigger.sql(), trigger.table
    sql = sql.replace(trigger.name, name, 1)
    for stage in old:
        sql = identifiers(sql, stage.names)
        for new, was in stage.literals.items():
            sql = sql.replace(new, was)
        table = stage.names.get(table, table)
    requires = None if trigger.requires is None else old_name(trigger.requires, old)
    return name, table, requires, sql
