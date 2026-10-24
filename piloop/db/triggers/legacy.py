"""Today's triggers in the names of an older schema, for the migrations that run before a renaming of it.

The migrations create the persistent triggers that the package renders today (``recreate``). Revision 0016 renamed
``projects`` to ``campaigns``, with its ``_id`` columns, and revision 0018 renamed ``workers`` to ``agent_sessions``
and the worker role to the implementer. Revision 0028 generated ``events.kind`` from the payload, so an event trigger
before it inserts the kind itself. A migration before such a change runs on a schema that still has the old form, so
``rendered`` gives a trigger its old name, its old identifiers and its old text there. Only identifiers, the role names
that a trigger lists and the event insert change: any other string literal keeps its text. Revision 0040 took the
built-in roles out of the role trigger, so a migration before it gets them back.
"""

import re
from dataclasses import dataclass, field

from alembic.operations import Operations
from sqlalchemy import text

from piloop import deprecated
from piloop.db.triggers.ddl import Trigger

deprecated.until('0.4.0', 'old migrations need the old trigger names: squash 0001-0018 at a 0.2.0 floor, delete legacy')


def has_table(name: str) -> str:
    """A query that finds the table ``name``."""
    return f"SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '{name}'"


@dataclass(frozen=True)
class Stage:
    """One change of the schema: the query that finds a schema from before it, and what it changed."""

    revision: str
    """The revision that changed the schema."""
    older: str
    """A query that returns a row on a schema from before the change, such as one that finds a table it renamed."""
    names: dict[str, str] = field(default_factory=dict)
    """Each identifier of the new schema, with the one that it had before."""
    words: dict[str, str] = field(default_factory=dict)
    """The words of the names of triggers and indexes, new then old."""
    literals: dict[str, str] = field(default_factory=dict)
    """The text of the triggers, new then old: the role names that a trigger lists, the insert of an event."""


EVENT_INSERT = (
    "INSERT INTO events (at, actor, target_kind, target_id, payload)\n  VALUES (strftime('%Y-%m-%d %H:%M:%f', 'now'),"
    " NEW.author, 'comment', NEW.id, json_object('kind', 'comment.' || NEW.kind, "
)
KNOWN_ROLE = "\nWHEN NEW.campaign_id IS NOT NULL AND (SELECT json_type(config, '$.roles.' || json_quote(NEW.kind))"
KNOWN_ROLE_ERROR = "a role of the campaign: a key of roles in its config'"
STAGES = (
    Stage(
        '0040',
        "SELECT 1 FROM alembic_version WHERE version_num < '0040'",
        literals={
            KNOWN_ROLE: "\nWHEN NEW.kind NOT IN ('implementer', 'watchdog', 'research', 'scout') AND (SELECT"
            " json_type(config, '$.roles.' || json_quote(NEW.kind))",
            KNOWN_ROLE_ERROR: 'a role of the campaign: implementer, watchdog, research, scout or a [roles.<name>] table'
            " of its piloop.toml'",
        },
    ),
    Stage(
        '0028',
        "SELECT 1 FROM pragma_table_xinfo('events') WHERE name = 'kind' AND hidden = 0",
        literals={
            EVENT_INSERT: 'INSERT INTO events (kind, actor, target_kind, target_id, payload)\n'
            "  VALUES ('comment.escalation', NEW.author, 'comment', NEW.id, json_object("
        },
    ),
    Stage(
        '0018',
        has_table('workers'),
        {'agent_sessions': 'workers'},
        {'agent_session': 'worker'},
        {"'implementer'": "'worker'"},
    ),
    Stage(
        '0016', has_table('projects'), {'campaigns': 'projects', 'campaign_id': 'project_id'}, {'campaign': 'project'}
    ),
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
    """Return the changes that the schema that ``op`` migrates is older than: those whose ``older`` finds a row."""
    return tuple(s for s in STAGES if op.get_bind().execute(text(s.older)).scalar() is not None)


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
