"""The ``unmeasured`` verdict: a ``Piloop-Eval: skip`` commit is not a ``discard``.

The ``verdict`` CHECK of ``candidates`` admits ``unmeasured``. Batch mode rebuilds the table with its row ids and
drops its triggers. The daemon's skip verdicts (a ``discard`` with the skip reason, by the daemon) become
``unmeasured`` before ``recreate`` puts the triggers back, so the change log records no decision for them.

The downgrade turns every ``unmeasured`` back into a ``discard``, which is what the older code wrote.

Revision ID: 0003
Revises: 0002
"""

from collections.abc import Sequence

from alembic import op

from piloop.db.triggers import recreate

revision: str = '0003'
down_revision: str | None = '0002'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
BEFORE = ('pending', 'keep', 'interesting', 'discard', 'crash', 'reference')
AFTER = (*BEFORE, 'unmeasured')
SKIPPED = 'not evaluated: Piloop-Eval: skip'


def upgrade() -> None:
    _verdicts(AFTER)
    op.execute(
        "UPDATE candidates SET verdict = 'unmeasured'"
        f" WHERE verdict = 'discard' AND verdict_by = 'daemon' AND verdict_reason = '{SKIPPED}'"
    )
    recreate(op, 'candidates')


def downgrade() -> None:
    op.execute("UPDATE candidates SET verdict = 'discard' WHERE verdict = 'unmeasured'")
    _verdicts(BEFORE)
    recreate(op, 'candidates')


def _verdicts(verdicts: tuple[str, ...]) -> None:
    with op.batch_alter_table('candidates', schema=None, recreate='always') as batch_op:
        batch_op.drop_constraint('verdict', type_='check')
        batch_op.create_check_constraint('verdict', f'verdict IN ({", ".join(f"{v!r}" for v in verdicts)})')
