"""An event can be on the whole home (``home#1``): a limit, a worker. A comment still cannot.

SQLite cannot change a CHECK, so the upgrade rebuilds ``events`` with ``home`` in ``targetkind``, and copies every row
with its id: the cursors of the readers stay valid. Its indexes come back as they were. The persistent
``comment.escalation`` trigger inserts into ``events``, so it goes before the swap and comes back after it, as in 0044.
``comments`` keeps its CHECK: no comment is on the home.

The downgrade deletes the events on the home, then rebuilds the table without ``home``.

Revision ID: 0047
Revises: 0046
"""

import re
from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

from piloop.db.triggers import install

revision: str = '0047'
down_revision: str | None = '0046'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
TARGET_KINDS = ('campaign', 'idea', 'run', 'candidate', 'measurement', 'agent_session', 'comment', 'instance')


def upgrade() -> None:
    _rebuild((*TARGET_KINDS, 'home'))


def downgrade() -> None:
    op.execute("DELETE FROM events WHERE target_kind = 'home'")
    _rebuild(TARGET_KINDS)


def _rebuild(kinds: Sequence[str]) -> None:
    """Copy ``events`` into a new table whose CHECK of ``target_kind`` takes ``kinds``, with its ids, and swap them."""
    bind = op.get_bind()
    sql = bind.execute(text("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'events'")).scalar_one()
    indexes = (
        bind.execute(
            text("SELECT sql FROM sqlite_master WHERE type = 'index' AND tbl_name = 'events' AND sql IS NOT NULL")
        )
        .scalars()
        .all()
    )
    stored = bind.execute(text("SELECT name FROM pragma_table_xinfo('events') WHERE hidden = 0")).scalars().all()
    columns = ', '.join(f'"{c}"' for c in stored)
    check = f'CHECK (target_kind IN ({", ".join(repr(k) for k in kinds)}))'
    new, found = re.subn(r'CHECK \(target_kind IN \([^)]*\)\)', check, sql)
    assert found == 1, f'events has no single CHECK of target_kind: {sql}'
    op.execute('DROP TRIGGER comments_escalation_event')
    op.execute(re.sub(r'^CREATE TABLE "?events"?', 'CREATE TABLE events_new', new))
    op.execute(f'INSERT INTO events_new ({columns}) SELECT {columns} FROM events')
    op.execute('DROP TABLE events')
    op.execute('ALTER TABLE events_new RENAME TO events')
    for index in indexes:
        op.execute(index)
    install(op, ['comments_escalation_event'])
