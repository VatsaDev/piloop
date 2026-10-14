"""The ``recovered`` verdict: the daemon's commit of what a worker left uncommitted is not judged.

The ``verdict`` CHECK of ``candidates`` admits ``recovered``. Batch mode rebuilds the table with its row ids and drops
its triggers, so ``recreate`` puts them back. Existing candidates keep their verdicts: the daemon judged its older
recovery commits by the rule, and those judgements stand.

The downgrade turns every ``recovered`` into an ``unmeasured``, the closest verdict the older code knows: nothing was
measured.

Revision ID: 0005
Revises: 0004
"""

from collections.abc import Sequence

from alembic import op

from piloop.db.triggers import recreate

revision: str = '0005'
down_revision: str | None = '0004'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
BEFORE = ('pending', 'keep', 'interesting', 'discard', 'crash', 'reference', 'unmeasured')
AFTER = (*BEFORE, 'recovered')


def upgrade() -> None:
    _verdicts(AFTER)
    recreate(op, 'candidates')


def downgrade() -> None:
    op.execute("UPDATE candidates SET verdict = 'unmeasured' WHERE verdict = 'recovered'")
    _verdicts(BEFORE)
    recreate(op, 'candidates')


def _verdicts(verdicts: tuple[str, ...]) -> None:
    with op.batch_alter_table('candidates', schema=None, recreate='always') as batch_op:
        batch_op.drop_constraint('verdict', type_='check')
        batch_op.create_check_constraint('verdict', f'verdict IN ({", ".join(f"{v!r}" for v in verdicts)})')
