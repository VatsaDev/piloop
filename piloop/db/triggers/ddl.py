"""One trigger type and one renderer. Every trigger in the package is a ``Trigger``, rendered by ``Trigger.sql``."""

from dataclasses import dataclass
from typing import Literal

Event = Literal['BEFORE INSERT', 'AFTER INSERT', 'AFTER UPDATE', 'AFTER DELETE']
"""When a trigger fires. ``Trigger.of`` narrows ``AFTER UPDATE`` to ``AFTER UPDATE OF <columns>``."""


@dataclass(frozen=True)
class Trigger:
    """A SQLite trigger as data. ``sql()`` renders the one ``CREATE TRIGGER`` form that the package uses.

    Args:
        name: ``<table>_<what>``. The migrations fix the persistent names. ``sqlite_temp_master`` shows the TEMP ones.
        table: The table the trigger is on.
        event: See ``Event``.
        body: The statements between ``BEGIN`` and ``END``, without the final semicolon. Separate statements with a
            semicolon and a newline. The renderer indents each line.
        of: Columns for ``AFTER UPDATE OF``, so that the trigger fires only when a statement names one of them.
        when: The ``WHEN`` expression, or None to fire on every event.
        temp: A trigger of one connection (``CREATE TEMP TRIGGER``). Only these can read ``temp.session_actor``.
        requires: A table that must exist for the body to be valid, for example the index that a sync trigger writes.
            ``recreate`` skips the trigger when that table does not exist at its point of the migration chain. None
            when there is no such table.
    """

    name: str
    table: str
    event: Event
    body: str
    of: tuple[str, ...] = ()
    when: str | None = None
    temp: bool = False
    requires: str | None = None

    def __post_init__(self) -> None:
        if self.of and self.event != 'AFTER UPDATE':
            raise ValueError(f'{self.name}: OF {", ".join(self.of)} needs AFTER UPDATE, not {self.event}')

    def sql(self) -> str:
        """The ``CREATE TRIGGER`` statement, ending with ``;``. It is one statement, so ``sqlite3`` can run it alone."""
        temp = 'TEMP ' if self.temp else ''
        of = f' OF {", ".join(self.of)}' if self.of else ''
        lines = [f'CREATE {temp}TRIGGER {self.name} {self.event}{of} ON {self.table}']
        if self.when is not None:
            lines.append(f'WHEN {self.when}')
        lines.append('BEGIN')
        lines.extend(f'  {line}' for line in f'{self.body};'.splitlines())
        lines.append('END;')
        return '\n'.join(lines)
