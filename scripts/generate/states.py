"""Generate ``docs/snippets/generated/states.mdx``: the values of the state enums."""

from enum import StrEnum

from sqlalchemy import Enum

from piloop.db.models import AgentSessionState, Origin, RunEnd, RunStatus
from piloop.db.schema import tables
from piloop.ids import Cause, IdeaStatus
from piloop.plugins.verdict_rule.contract import Outcome
from scripts.generate import SNIPPETS, header

PATH = SNIPPETS / 'states.mdx'
STATE_ENUMS: tuple[type[StrEnum], ...] = (IdeaStatus, RunStatus, RunEnd, AgentSessionState, Outcome, Origin, Cause)


def generate() -> str:
    """Return a table of the state enums: the columns that store each one, and every value."""
    rows = ['| Enum | Column | Values |', '|---|---|---|']
    for enum in STATE_ENUMS:
        columns = [
            f'`{table.name}.{column.name}`'
            for table in tables()
            for column in table.columns
            if isinstance(column.type, Enum) and column.type.enum_class is enum
        ]
        values = ', '.join(f'`{member.value}`' for member in enum)
        rows.append(f'| `{enum.__name__}` | {", ".join(columns)} | {values} |')
    return header('the enums in `piloop/db/models.py`') + '\n' + '\n'.join(rows) + '\n'
