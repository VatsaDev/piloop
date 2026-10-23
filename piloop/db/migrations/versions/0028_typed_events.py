"""Typed events: the payload holds the event's ``kind``, and ``events.kind`` is generated from it.

Each payload is an ``EventModel`` now. Every row gets its kind in ``$.kind``, and these shapes that 0.1.0, 0.2.0 and
main wrote change to the model's:

- ``<role>.started`` and ``<role>.finished`` on an agent session are ``agent_session.started`` and
  ``agent_session.finished``, with the ``role`` (the row's kind, else the prefix). A status that no process exit has
  (``idle``, ``blocked``, ``working``) is ``exited``.
- ``campaign.stop_requested``'s ``kill`` and the cancel requests' ``wrap_up`` are ``ending``: ``now`` or ``wrap_up``.
- ``run.finished`` has ``end``, from its status and error as in 0021, and no ``status``: the end says it.
- ``agent_session.stop_ended``'s ``error`` (0.2.0) is ``end``.
- ``anomaly.reported``'s ``key`` is ``subject`` and ``anomaly``, and its line is ``line``. An agent's subject is
  ``agent_session#<id>``, whatever role name an older key gave it.
- ``kind`` in a payload is another field: ``role`` in ``anomaly.startup`` (``worker`` is ``implementer``),
  ``link_kind`` in ``idea.linked`` and ``idea.unlinked``, ``comment_kind`` in ``comment.deleted``, and ``ended`` holds
  the whole payload of ``agent_session.job_ended``. A payload that still has a ``kind`` of its own after these stops
  the upgrade: its kind needs a rewrite here, or its ``kind`` would be lost.
- ``target_kind`` in the payload of a comment's event names the table of 0.2.0: ``project`` is ``campaign`` and
  ``worker`` is ``agent_session``.

A payload that is not a JSON object is ``{"invalid": <its text>}``. The table is rebuilt: a generated column cannot be
added in place of a stored one. The ids stay, so every reader's cursor still holds. The persistent escalation trigger
inserts the new shape.

The downgrade puts ``kind`` back in its column and takes it out of the payload. It gives back the shapes that the
code before reads: the role kinds, ``kill`` and ``wrap_up``, the anomaly ``key`` and the bare ``Ended``. The other
shapes stay as they are.

Revision ID: 0028
Revises: 0027
"""

from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

from piloop.db.triggers import install

revision: str = '0028'
down_revision: str | None = '0027'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
TARGET_KINDS = ('campaign', 'idea', 'run', 'candidate', 'measurement', 'agent_session', 'comment')
CHECK = f'CONSTRAINT targetkind CHECK (target_kind IN ({", ".join(repr(k) for k in TARGET_KINDS)}))'
COLUMNS = 'id, at, actor, target_kind, target_id'
NEW_TABLE = f"""CREATE TABLE events_new (
\tid INTEGER NOT NULL,
\tat DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
\tkind VARCHAR NOT NULL GENERATED ALWAYS AS (json_extract(payload, '$.kind')),
\tactor VARCHAR NOT NULL,
\ttarget_kind VARCHAR(16) NOT NULL,
\ttarget_id INTEGER NOT NULL,
\tpayload JSON NOT NULL,
\tPRIMARY KEY (id),
\t{CHECK}
)"""
OLD_TABLE = f"""CREATE TABLE events_new (
\tid INTEGER NOT NULL,
\tat DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
\tkind VARCHAR NOT NULL,
\tactor VARCHAR NOT NULL,
\ttarget_kind VARCHAR(16) NOT NULL,
\ttarget_id INTEGER NOT NULL,
\tpayload JSON DEFAULT '{{}}' NOT NULL,
\tPRIMARY KEY (id),
\t{CHECK}
)"""
INDEX = 'CREATE INDEX ix_events_target ON events (target_kind, target_id, id)'
OLD_ESCALATION = (
    "CREATE TRIGGER comments_escalation_event AFTER INSERT ON comments\nWHEN NEW.kind = 'escalation'\nBEGIN\n"
    '  INSERT INTO events (kind, actor, target_kind, target_id, payload)\n'
    "  VALUES ('comment.escalation', NEW.author, 'comment', NEW.id, json_object('target_kind', NEW.target_kind,"
    " 'target_id', NEW.target_id));\nEND"
)

SENTENCES = {
    'daemon stopped': 'daemon_stopped',
    'daemon exited while the run was in progress': 'stale',
    'campaign stopped': 'campaign_stopped',
    'its role is no longer in piloop.toml': 'role_removed',
    'no agent of the run started to work': 'not_started',
    'cancelled': 'cancelled',
    'worker lost': 'worker_lost',
}
"""The errors of the runs that 0.1.0 and 0.2.0 stopped, by the end that each one means (as 0021)."""
P = 'payload'
ERROR = f"json_extract({P}, '$.error')"
FROM_ERROR = ' '.join(f'WHEN {sentence!r} THEN {end!r}' for sentence, end in SENTENCES.items())
RUN_END = f"""CASE json_extract({P}, '$.status')
    WHEN 'succeeded' THEN 'finished'
    WHEN 'timed_out' THEN 'timed_out'
    ELSE CASE WHEN {ERROR} LIKE 'blocked on escalation%' THEN 'awaiting_answer'
        ELSE CASE {ERROR} {FROM_ERROR} ELSE 'failed' END END
END"""
ROLE = "substr(kind, 1, instr(kind, '.') - 1)"
WHAT = "substr(kind, instr(kind, '.') + 1)"
KEY = f"json_extract({P}, '$.key')"
REF = f"substr({KEY}, 1, instr({KEY}, ' ') - 1)"
REF_KIND = f"substr({REF}, 1, instr({REF}, '#') - 1)"
SUBJECT = (
    f'CASE WHEN {REF_KIND} IN ({", ".join(repr(k) for k in TARGET_KINDS)}) THEN {REF_KIND}'
    f" WHEN {REF_KIND} = 'project' THEN 'campaign' ELSE 'agent_session' END || substr({REF}, instr({REF}, '#'))"
)
TABLE_OF = f"""CASE json_extract({P}, '$.target_kind') WHEN 'project' THEN 'campaign' WHEN 'worker' THEN 'agent_session'
    ELSE json_extract({P}, '$.target_kind') END"""


def renamed(old: str, new: str) -> str:
    """The payload with its field ``old`` as ``new``."""
    return f"json_set(json_remove({P}, '$.{old}'), '$.{new}', json_extract({P}, '$.{old}'))"


REWRITES = (
    (
        f"""UPDATE events SET {P} = json_object('invalid', CAST({P} AS TEXT))
        WHERE NOT CASE WHEN json_valid({P}, 2) THEN json_type({P}) = 'object' ELSE 0 END"""
    ),
    (
        f"""UPDATE events SET
            {P} = json_set({P}, '$.role', coalesce((SELECT a.kind FROM agent_sessions a WHERE a.id = target_id), {ROLE})),
            kind = 'agent_session.' || {WHAT}
        WHERE target_kind = 'agent_session' AND {WHAT} IN ('started', 'finished')
            AND kind NOT IN ('agent_session.started', 'agent_session.finished')"""
    ),
    (
        f"""UPDATE events SET {P} = json_set({P}, '$.status', 'exited')
        WHERE kind = 'agent_session.finished' AND json_extract({P}, '$.status') IN ('idle', 'blocked', 'working')"""
    ),
    (
        f"""UPDATE events SET {P} = json_set(json_remove({P}, '$.kill'), '$.ending',
            CASE WHEN json_extract({P}, '$.kill') THEN 'now' ELSE 'wrap_up' END)
        WHERE kind = 'campaign.stop_requested' AND json_type({P}, '$.ending') IS NULL"""
    ),
    (
        f"""UPDATE events SET {P} = json_set(json_remove({P}, '$.wrap_up'), '$.ending',
            CASE WHEN json_extract({P}, '$.wrap_up') THEN 'wrap_up' ELSE 'now' END)
        WHERE kind IN ('run.cancel_requested', 'agent_session.cancel_requested')
            AND json_type({P}, '$.ending') IS NULL"""
    ),
    (
        f"""UPDATE events SET {P} = json_set({P}, '$.end', {RUN_END})
        WHERE kind = 'run.finished' AND json_type({P}, '$.end') IS NULL"""
    ),
    f"UPDATE events SET {P} = json_remove({P}, '$.status') WHERE kind = 'run.finished'",
    (
        f"""UPDATE events SET {P} = json_set(json_remove({P}, '$.error'), '$.end',
            CASE {ERROR} WHEN 'cancelled' THEN 'cancelled' ELSE 'campaign_stopped' END)
        WHERE kind = 'agent_session.stop_ended' AND json_type({P}, '$.end') IS NULL"""
    ),
    (
        f"""UPDATE events SET {P} = json_object('subject', {SUBJECT},
            'anomaly', substr({KEY}, instr({KEY}, ' ') + 1), 'line', json_extract({P}, '$.anomaly'))
        WHERE kind = 'anomaly.reported' AND json_type({P}, '$.key') = 'text'"""
    ),
    (
        f"""UPDATE events SET {P} = json_set({renamed('kind', 'role')}, '$.role',
            CASE json_extract({P}, '$.kind') WHEN 'worker' THEN 'implementer' ELSE json_extract({P}, '$.kind') END)
        WHERE kind = 'anomaly.startup' AND json_type({P}, '$.kind') = 'text'"""
    ),
    (
        f"""UPDATE events SET {P} = {renamed('kind', 'link_kind')}
        WHERE kind IN ('idea.linked', 'idea.unlinked') AND json_type({P}, '$.kind') = 'text'"""
    ),
    (
        f"""UPDATE events SET {P} = {renamed('kind', 'comment_kind')}
        WHERE kind = 'comment.deleted' AND json_type({P}, '$.kind') = 'text'"""
    ),
    (
        f"""UPDATE events SET {P} = json_set({P}, '$.target_kind', {TABLE_OF})
        WHERE kind GLOB 'comment.*' AND json_extract({P}, '$.target_kind') IN ('project', 'worker')"""
    ),
    (
        f"""UPDATE events SET {P} = json_object('ended', json({P}))
        WHERE kind = 'agent_session.job_ended' AND json_type({P}, '$.ended') IS NULL"""
    ),
)
"""The shapes that a model of this revision has another field for, oldest first. Each reads the old kind column."""


DOWNGRADES = (
    (
        f"""UPDATE events_new SET kind = json_extract({P}, '$.role') || '.' || {WHAT}, {P} = json_remove({P}, '$.role')
        WHERE kind IN ('agent_session.started', 'agent_session.finished')"""
    ),
    (
        f"""UPDATE events_new SET {P} = json_set(json_remove({P}, '$.ending'), '$.kill',
            json(CASE json_extract({P}, '$.ending') WHEN 'now' THEN 'true' ELSE 'false' END))
        WHERE kind = 'campaign.stop_requested'"""
    ),
    (
        f"""UPDATE events_new SET {P} = json_set(json_remove({P}, '$.ending'), '$.wrap_up',
            json(CASE json_extract({P}, '$.ending') WHEN 'wrap_up' THEN 'true' ELSE 'false' END))
        WHERE kind IN ('run.cancel_requested', 'agent_session.cancel_requested')"""
    ),
    (
        f"""UPDATE events_new SET {P} = json_object('key', json_extract({P}, '$.subject') || ' '
            || json_extract({P}, '$.anomaly'), 'anomaly', json_extract({P}, '$.line'))
        WHERE kind = 'anomaly.reported'"""
    ),
    f"UPDATE events_new SET {P} = json_extract({P}, '$.ended') WHERE kind = 'agent_session.job_ended'",
)
"""The shapes that the code before this revision reads, on the rebuilt table."""


def upgrade() -> None:
    op.execute('DROP TRIGGER comments_escalation_event')
    for rewrite in REWRITES:
        op.execute(rewrite)
    _refuse_own_kinds()
    op.execute(NEW_TABLE)
    op.execute(f"INSERT INTO events_new ({COLUMNS}, {P}) SELECT {COLUMNS}, json_set({P}, '$.kind', kind) FROM events")
    _swap()
    install(op, ['comments_escalation_event'])


def downgrade() -> None:
    op.execute('DROP TRIGGER comments_escalation_event')
    op.execute(OLD_TABLE)
    op.execute(
        f"INSERT INTO events_new ({COLUMNS}, kind, {P}) SELECT {COLUMNS}, kind, json_remove({P}, '$.kind') FROM events"
    )
    for rewrite in DOWNGRADES:
        op.execute(rewrite)
    _swap()
    op.execute(OLD_ESCALATION)


def _refuse_own_kinds() -> None:
    """Stop the upgrade when a payload still has a ``kind`` of its own: setting the event's kind would overwrite it."""
    query = f"SELECT DISTINCT kind FROM events WHERE json_type({P}, '$.kind') IS NOT NULL ORDER BY kind"
    kinds = list(op.get_bind().execute(text(query)).scalars())
    if kinds:
        raise RuntimeError(
            f'the payloads of these events have a field "kind" that no rewrite of revision {revision} renames:'
            f' {", ".join(kinds)}. The upgrade changed nothing: report these kinds as a bug of piloop.'
        )


def _swap() -> None:
    op.execute('DROP TABLE events')
    op.execute('ALTER TABLE events_new RENAME TO events')
    op.execute(INDEX)
