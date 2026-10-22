"""``runs.end``: how a run ended, as a value, where ``runs.error`` held it as one of a set of sentences.

Every finished run gets its end from its status and, for a failed run, from the sentence that the daemon wrote in
``error``. A failed run with any other error failed on its own: ``failed``. ``error`` keeps its text. Two CHECKs keep
``status`` a function of ``end``. ``ADD COLUMN`` adds the column, then a batch rebuild adds the CHECKs, which read
both columns, and ``recreate`` puts back the triggers that the rebuild drops. The mapping keeps ``updated_at``.

The downgrade drops the column. The errors still say how each run ended.

Revision ID: 0021
Revises: 0020
"""

from collections.abc import Sequence

from alembic import op

from piloop.db.triggers import recreate

revision: str = '0021'
down_revision: str | None = '0020'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
ENDS = (
    'finished',
    'failed',
    'timed_out',
    'awaiting_answer',
    'cancelled',
    'campaign_stopped',
    'daemon_stopped',
    'stale',
    'role_removed',
    'not_started',
    'worker_lost',
)
END = f'"end" VARCHAR(16) CONSTRAINT runend CHECK ("end" IN ({", ".join(repr(e) for e in ENDS)}))'
SENTENCES = {
    'daemon stopped': 'daemon_stopped',
    'daemon exited while the run was in progress': 'stale',
    'campaign stopped': 'campaign_stopped',
    'its role is no longer in piloop.toml': 'role_removed',
    'no agent of the run started to work': 'not_started',
    'cancelled': 'cancelled',
    'worker lost': 'worker_lost',
}
"""The errors of the runs that 0.1.0 and 0.2.0 stopped, by the end that each one means."""
FROM_ERROR = ' '.join(f'WHEN {sentence!r} THEN {end!r}' for sentence, end in SENTENCES.items())
ENDED = f"""UPDATE runs SET "end" = CASE
    WHEN status IN ('queued', 'running') THEN NULL
    WHEN status = 'succeeded' THEN 'finished'
    WHEN status = 'timed_out' THEN 'timed_out'
    WHEN error LIKE 'blocked on escalation%' THEN 'awaiting_answer'
    ELSE CASE error {FROM_ERROR} ELSE 'failed' END
END"""
SET_ONCE = ('runs.end is set once a run is finished', """("end" IS NULL) = (status IN ('queued', 'running'))""")
FOLLOWS = (
    'runs.status follows from runs.end',
    """"end" IS NULL OR status = CASE "end" WHEN 'finished' THEN 'succeeded' WHEN 'timed_out' THEN 'timed_out'"""
    " ELSE 'failed' END",
)


def upgrade() -> None:
    op.execute(f'ALTER TABLE runs ADD COLUMN {END}')
    op.execute('DROP TRIGGER runs_updated_at')  # the mapping is not an update of the runs: recreate puts it back
    op.execute(ENDED)
    with op.batch_alter_table('runs', schema=None, recreate='always') as batch_op:
        for name, check in (SET_ONCE, FOLLOWS):
            batch_op.create_check_constraint(name, check)
    recreate(op, 'runs')


def downgrade() -> None:
    with op.batch_alter_table('runs', schema=None, recreate='always') as batch_op:
        for name, _ in (SET_ONCE, FOLLOWS):
            batch_op.drop_constraint(name, type_='check')
        batch_op.drop_constraint('runend', type_='check')
        batch_op.drop_column('end')
    recreate(op, 'runs')
