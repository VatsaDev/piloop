from datetime import datetime, timedelta

from pydantic import field_validator

from piloop.db.sql import sql_error
from piloop.plugins.trigger.contract import Fire, Pass, Trigger


def since(p: Pass) -> datetime:
    """Return when the role's latest row started, or when the campaign was registered if the role has no row yet."""
    return p.last.started_at if p.last is not None else p.campaign.created_at


SQL_PARAMETERS = ('campaign', 'since')


class SqlTrigger(Trigger):
    """A query on the piloop database returns at least one row.

    The query may use two parameters: ``:campaign``, the campaign's id, and ``:since``, when the role's latest row
    started (or when the campaign was registered), as UTC text (``2026-01-31 12:00:00.000000``). Compare ``:since`` with
    a ``created_at`` column to fire only on what is new. The query is checked when ``piloop.toml`` loads: it must be
    one statement that only reads.
    """

    when: str
    """The query, for example ``SELECT 1 FROM candidates WHERE campaign_id = :campaign AND verdict = 'keep' AND
    created_at > :since``."""

    @field_validator('when')
    @classmethod
    def _reads_only(cls, when: str) -> str:
        if (error := sql_error(when, SQL_PARAMETERS)) is not None:
            raise ValueError(f'the query does not run on the piloop database: {error}')
        return when

    def fire(self, p: Pass) -> Fire | None:
        params: dict[str, object] = {
            'campaign': p.campaign.id,
            'since': since(p).strftime('%Y-%m-%d %H:%M:%S.%f'),
        }
        row = p.s.connection().exec_driver_sql(self.when, params).first()
        return None if row is None else Fire(f'sql: {" ".join(self.when.split())}')

    def due_in(self, p: Pass) -> timedelta | None:
        """A query can read the time, but its answer cannot say when: pair it with ``cron`` for that."""
        return None
