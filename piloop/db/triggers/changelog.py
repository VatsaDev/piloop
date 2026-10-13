"""The change log: the decisions that leave a record in ``events``, whoever makes them.

Every trigger here inserts one ``events`` row through ``event_insert``. The event kind is ``<table singular>.<column>``
for a change, ``<singular>.deleted`` for a tombstone and ``comment.<kind>`` for a comment. ``target_kind`` is the
table's singular, which is also a ``TargetKind`` value. Payloads have the old and new values, never a large body.
"""

from piloop.db.triggers.ddl import Trigger
from piloop.db.triggers.provenance import DAEMON_ACTOR, SESSION_ACTOR


def singular(table: str) -> str:
    """``ideas`` → ``idea``: the prefix of the event kind, and the ``TargetKind`` value. The table names are regular."""
    return table.removesuffix('s')


def event_insert(kind: str, actor: str, target_kind: str, target_id: str, payload: str) -> str:
    """The INSERT that every event trigger runs. Each argument is a SQL expression."""
    return (
        'INSERT INTO events (kind, actor, target_kind, target_id, payload)\n'
        f'VALUES ({kind}, {actor}, {target_kind}, {target_id}, {payload})'
    )


def changed(table: str, column: str, *extra: str) -> Trigger:
    """TEMP: log a change of ``column`` as a ``<singular>.<column>`` event.

    The payload has ``old`` and ``new``, and the ``extra`` columns of the new row. ``AFTER UPDATE OF`` fires only when
    a statement names the column, so the ``updated_at`` and ``updated_by`` triggers (which SET their own column) never
    fire it. The WHEN clause ignores a SET to the value already there.
    """
    pairs = ', '.join([f"'old', OLD.{column}", f"'new', NEW.{column}", *(f"'{c}', NEW.{c}" for c in extra)])
    return Trigger(
        f'{table}_{column}_event',
        table,
        'AFTER UPDATE',
        of=(column,),
        when=f'OLD.{column} IS NOT NEW.{column}',
        body=event_insert(
            f"'{singular(table)}.{column}'", SESSION_ACTOR, f"'{singular(table)}'", 'NEW.id', f'json_object({pairs})'
        ),
        temp=True,
    )


def tombstone(table: str, *columns: str) -> Trigger:
    """TEMP: log a deleted row as a ``<singular>.deleted`` event with ``columns`` of the old row."""
    pairs = ', '.join(f"'{c}', OLD.{c}" for c in columns)
    return Trigger(
        f'{table}_deleted_event',
        table,
        'AFTER DELETE',
        body=event_insert(
            f"'{singular(table)}.deleted'", SESSION_ACTOR, f"'{singular(table)}'", 'OLD.id', f'json_object({pairs})'
        ),
        temp=True,
    )


COMMENT_PAYLOAD = "json_object('target_kind', NEW.target_kind, 'target_id', NEW.target_id)"


def comment_event() -> Trigger:
    """TEMP: log every comment except an escalation as a ``comment.<kind>`` event, shaped like the escalation event."""
    return Trigger(
        'comments_inserted_event',
        'comments',
        'AFTER INSERT',
        when="NEW.kind != 'escalation'",
        body=event_insert("'comment.' || NEW.kind", 'NEW.author', "'comment'", 'NEW.id', COMMENT_PAYLOAD),
        temp=True,
    )


def escalation() -> Trigger:
    """Persistent: log ``comment.escalation`` whatever connection wrote the comment."""
    return Trigger(
        'comments_escalation_event',
        'comments',
        'AFTER INSERT',
        when="NEW.kind = 'escalation'",
        body=event_insert("'comment.escalation'", 'NEW.author', "'comment'", 'NEW.id', COMMENT_PAYLOAD),
    )


# The one exception to `changed`: when the daemon writes a brief where there was none, the run starts (`run.started`),
# and that is not a steer. Anyone else who writes a brief, or the daemon replacing one, steers the run. The payload has
# the lengths only, because a brief has thousands of characters.
RUN_BRIEF = Trigger(
    'runs_brief_event',
    'runs',
    'AFTER UPDATE',
    of=('brief',),
    when=f"OLD.brief IS NOT NEW.brief AND NOT (OLD.brief IS NULL AND {SESSION_ACTOR} = '{DAEMON_ACTOR}')",
    body=event_insert(
        "'run.brief'",
        SESSION_ACTOR,
        "'run'",
        'NEW.id',
        "json_object('old_length', length(OLD.brief), 'new_length', length(NEW.brief))",
    ),
    temp=True,
)

CHANGES = (
    changed('ideas', 'status'),
    changed('ideas', 'priority'),
    changed('ideas', 'track'),
    changed('candidates', 'verdict', 'verdict_by', 'verdict_reason'),
    RUN_BRIEF,
    # stopped_reason is set in the same statement as the status, so a stop says why and a resume says null
    changed('projects', 'status', 'stopped_reason'),
    # an inbox item was acknowledged: the actor of the event is who did it, whatever acked_by says
    changed('comments', 'acked_at', 'acked_by'),
)
"""The decisions that are logged. To log another column, add it here and run ``make docs``."""
TOMBSTONES = (
    tombstone('ideas', 'title', 'status', 'hypothesis'),
    tombstone('comments', 'target_kind', 'target_id', 'author', 'kind', 'body'),
    tombstone('candidates', 'tree_sha', 'commit_sha', 'verdict'),
)
"""The deletes that are logged, with the key columns, so the feed can still say what was there."""
COMMENTS = (comment_event(),)
"""The forum (``comment.note``, ``comment.finding``, ``comment.decision``), which the daemon pushes to workers."""
