"""``agent_sessions.session_file``: an agent's harness session file is a blob in the home's CAS.

``usage.source`` named the file by its path on the machine that ran the agent. The upgrade puts each such file that is
still on this machine into the CAS (``migrate`` passes it), its whole lines as a chunked file of one part
(``piloop.store.chunks``), sets ``session_file``, and drops every path: one on another machine names nothing piloop
can read. The move is not a change of the row: ``agent_sessions_updated_at`` is off while it runs. The downgrade gives every usage a ``source`` again, which the older code requires: the address of its
file, or an empty one.

Revision ID: 0054
Revises: 0053
"""

from collections.abc import Sequence
from pathlib import Path

import sqlalchemy as sa
from alembic import op
from covalence import Value

from piloop.db.session import migration_cas
from piloop.db.triggers import install

revision: str = '0054'
down_revision: str | None = '0053'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column('agent_sessions', sa.Column('session_file', sa.String(), nullable=True))
    op.execute('DROP TRIGGER agent_sessions_updated_at')
    cas, con = migration_cas(), op.get_bind()
    found = con.execute(
        sa.text("SELECT id, json_extract(usage, '$.source') FROM agent_sessions WHERE usage IS NOT NULL ORDER BY id")
    ).all()
    for row, source in found:
        path = Path(source) if isinstance(source, str) and source else None
        if path is None or not path.is_file():  # another machine's, or gone
            continue
        data = path.read_bytes()
        manifest = Value({'chunks': [cas.put(data[: data.rfind(b'\n') + 1])]})  # frozen here: a chunked file
        cas.put(manifest.encode())
        con.execute(
            sa.text('UPDATE agent_sessions SET session_file = :cid WHERE id = :id'),
            {'cid': str(manifest.cid()), 'id': row},
        )
    op.execute("UPDATE agent_sessions SET usage = json_remove(usage, '$.source') WHERE usage IS NOT NULL")
    install(op, ['agent_sessions_updated_at'])


def downgrade() -> None:
    op.execute('DROP TRIGGER agent_sessions_updated_at')
    op.execute(
        "UPDATE agent_sessions SET usage = json_set(usage, '$.source', coalesce(session_file, ''))"
        " WHERE usage IS NOT NULL AND json_type(usage, '$.source') IS NULL"
    )
    install(op, ['agent_sessions_updated_at'])
    op.execute('ALTER TABLE agent_sessions DROP COLUMN session_file')
