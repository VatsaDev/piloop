"""Today's triggers in the names of the schema before revision 0016, for the migrations that run before it.

The migrations create the persistent triggers that the package renders today (``recreate``). Revision 0016 renamed
``projects`` to ``campaigns``, with its ``_id`` columns. A migration before it
runs on a schema that still has the old names, so ``rendered`` gives a trigger its old name and its old identifiers
there. Only identifiers change: a string literal keeps its text.
"""

import re

from alembic.operations import Operations
from sqlalchemy import text

from piloop.db.triggers.ddl import Trigger

OLD_NAMES = {'campaigns': 'projects', 'campaign_id': 'project_id'}
"""Each name of revision 0016, with the name that it had before."""
WORDS = {'campaign': 'project'}
"""The words that the names of triggers and indexes had before revision 0016."""
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


def old_name(name: str) -> str:
    """Return the name that a trigger or a table had before revision 0016."""
    for new, old in WORDS.items():
        name = re.sub(rf'(?<![a-z]){new}', old, name)
    return name


def before_renames(op: Operations) -> bool:
    """Return whether the schema that ``op`` migrates is older than revision 0016: it has ``projects``."""
    query = text("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'projects'")
    return op.get_bind().execute(query).scalar() is not None


def rendered(trigger: Trigger, *, old: bool) -> tuple[str, str, str | None, str]:
    """Return the name, the table, the required table and the SQL of ``trigger``, in the old names when ``old``."""
    if not old:
        return trigger.name, trigger.table, trigger.requires, trigger.sql()
    sql = identifiers(trigger.sql().replace(trigger.name, old_name(trigger.name), 1), OLD_NAMES)
    requires = None if trigger.requires is None else old_name(trigger.requires)
    return old_name(trigger.name), OLD_NAMES.get(trigger.table, trigger.table), requires, sql
