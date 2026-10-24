"""A candidate's verdict is only a judgement: ``candidates.origin`` and ``candidates.cause`` hold the rest.

``verdict`` is NULL until the candidate is judged, and holds only ``keep``, ``interesting``, ``discard`` or ``crash``.
The states that it held move to ``origin``: ``reference`` stays ``reference``, ``unmeasured`` is ``skipped`` and
``recovered`` stays ``recovered``, each with no verdict. ``pending`` is no verdict. Each campaign's first candidate with
no run and no parent is the ``baseline``, and every other candidate is a ``run``'s. An ``interesting`` verdict whose
reason starts as a note's or a moved head's gets its ``cause``. The reasons keep their text.

The events change with the column:

- ``candidate.verdict``: an old or new value that is not a judgement is null. A change between two of them is a
  ``candidate.origin`` event, from the old origin to the new.
- ``candidate.judged`` of an ``unmeasured`` or ``recovered`` verdict was no judgement: it is a ``candidate.origin``
  event, from ``run``.
- ``candidate.deleted`` has the ``origin``, and its ``verdict`` is a judgement or null.

Two batch rebuilds change the table: the first lets ``verdict`` be NULL and adds the columns, the second adds the
CHECKs once the rows fit them. ``recreate`` puts back the triggers that the rebuilds drop. The mapping keeps
``updated_at``.

The downgrade gives each candidate without a verdict its old state back, and puts ``note: `` before the reason of a
note. ``candidate.deleted`` gets its old shape back. The other events keep their new shapes: an older piloop reads a
null verdict, and loads ``candidate.origin`` as an event that it does not know.

Revision ID: 0043
Revises: 0042
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from piloop.db.triggers import recreate

revision: str = '0043'
down_revision: str | None = '0042'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
OUTCOMES = ('keep', 'interesting', 'discard', 'crash')
ORIGINS = ('baseline', 'run', 'reference', 'recovered', 'skipped')
CAUSES = ('note', 'moved')
OLD = ('pending', *OUTCOMES, 'reference', 'unmeasured', 'recovered')
ORIGIN_OF = {'reference': 'reference', 'unmeasured': 'skipped', 'recovered': 'recovered'}
"""The origin of a candidate by the old verdict that was a state, not a judgement."""
NOTE = 'note: '
MOVED = 'keeps against its own bar, but the head moved on'


def _in(values: Sequence[str]) -> str:
    return ', '.join(repr(v) for v in values)


def _origin_of(verdict: str) -> str:
    """The origin of an old verdict, as SQL. A judgement or ``pending`` is a run's."""
    cases = ' '.join(f'WHEN {old!r} THEN {new!r}' for old, new in ORIGIN_OF.items())
    return f"CASE {verdict} {cases} ELSE 'run' END"


def _state_of(origin: str) -> str:
    """The old verdict of a candidate of ``origin`` that has no verdict, as SQL."""
    cases = ' '.join(f'WHEN {new!r} THEN {old!r}' for old, new in ORIGIN_OF.items())
    return f"CASE {origin} {cases} ELSE 'pending' END"


def _judgement(value: str) -> str:
    """``value`` if it is a judgement, else NULL, as SQL."""
    return f'CASE WHEN {value} IN ({_in(OUTCOMES)}) THEN {value} END'


BASELINE = """run_id IS NULL AND parent_id IS NULL
    AND id = (SELECT min(c.id) FROM candidates c WHERE c.campaign_id = candidates.campaign_id)"""
MAPPED = f"""UPDATE candidates SET
    origin = CASE WHEN verdict NOT IN ({_in(tuple(ORIGIN_OF))}) AND {BASELINE} THEN 'baseline' ELSE {_origin_of('verdict')} END,
    cause = CASE
        WHEN verdict = 'interesting' AND verdict_reason LIKE '{NOTE}%' THEN 'note'
        WHEN verdict = 'interesting' AND verdict_reason LIKE '{MOVED}%' THEN 'moved'
    END,
    verdict = {_judgement('verdict')}"""
JUDGED = ('candidates.verdict needs a judged origin', "verdict IS NULL OR origin IN ('baseline', 'run')")
INTERESTING = ('candidates.cause needs interesting', "cause IS NULL OR verdict = 'interesting'")

CHANGE = "json_extract(payload, '$.old')", "json_extract(payload, '$.new')"
STATES = f'{{value}} IN ({_in(tuple(ORIGIN_OF) + ("pending",))})'
"""An old value of a ``candidate.verdict`` event that is no judgement."""
EVENTS = (
    # a change between two states: an origin change
    f"""UPDATE events SET payload = json_object(
        'kind', 'candidate.origin', 'old', {_origin_of(CHANGE[0])}, 'new', {_origin_of(CHANGE[1])})
    WHERE kind = 'candidate.verdict' AND {STATES.format(value=CHANGE[0])} AND {STATES.format(value=CHANGE[1])}""",
    f"""UPDATE events SET payload = json_set(payload, '$.old', {_judgement(CHANGE[0])}, '$.new', {_judgement(CHANGE[1])})
    WHERE kind = 'candidate.verdict'""",
    f"""UPDATE events SET payload = json_object(
        'kind', 'candidate.origin', 'old', 'run', 'new', {_origin_of("json_extract(payload, '$.verdict')")})
    WHERE kind = 'candidate.judged' AND json_extract(payload, '$.verdict') NOT IN ({_in(OUTCOMES)})""",
    f"""UPDATE events SET payload = json_set(payload,
        '$.origin', {_origin_of("json_extract(payload, '$.verdict')")},
        '$.verdict', {_judgement("json_extract(payload, '$.verdict')")})
    WHERE kind = 'candidate.deleted'""",
)


def upgrade() -> None:
    op.execute('DROP TRIGGER candidates_updated_at')  # the mapping is not an update of the candidates
    with op.batch_alter_table('candidates', schema=None, recreate='always') as batch_op:
        batch_op.drop_constraint('verdict', type_='check')
        batch_op.alter_column('verdict', existing_type=sa.String(16), nullable=True, server_default=None)
        batch_op.add_column(sa.Column('origin', sa.String(16), nullable=True))
        batch_op.add_column(sa.Column('cause', sa.String(16), nullable=True))
    op.execute(MAPPED)
    with op.batch_alter_table('candidates', schema=None, recreate='always') as batch_op:
        batch_op.alter_column('origin', existing_type=sa.String(16), nullable=False, server_default='run')
        batch_op.create_check_constraint('outcome', f'verdict IN ({_in(OUTCOMES)})')
        batch_op.create_check_constraint('origin', f'origin IN ({_in(ORIGINS)})')
        batch_op.create_check_constraint('cause', f'cause IN ({_in(CAUSES)})')
        for name, check in (JUDGED, INTERESTING):
            batch_op.create_check_constraint(name, check)
    for rewrite in EVENTS:
        op.execute(rewrite)
    recreate(op, 'candidates')


def downgrade() -> None:
    op.execute(f"""UPDATE events SET payload = json_remove(json_set(payload,
        '$.verdict', coalesce(json_extract(payload, '$.verdict'), {_state_of("json_extract(payload, '$.origin')")})),
        '$.origin')
    WHERE kind = 'candidate.deleted'""")
    with op.batch_alter_table('candidates', schema=None, recreate='always') as batch_op:
        for name in ('outcome', 'origin', 'cause', JUDGED[0], INTERESTING[0]):
            batch_op.drop_constraint(name, type_='check')
    op.execute(f"""UPDATE candidates SET
        verdict = coalesce(verdict, {_state_of('origin')}),
        verdict_reason = CASE WHEN cause = 'note' AND verdict_reason NOT LIKE '{NOTE}%'
            THEN '{NOTE}' || coalesce(verdict_reason, '') ELSE verdict_reason END""")
    with op.batch_alter_table('candidates', schema=None, recreate='always') as batch_op:
        batch_op.drop_column('cause')
        batch_op.drop_column('origin')
        batch_op.alter_column('verdict', existing_type=sa.String(16), nullable=False, server_default='pending')
        batch_op.create_check_constraint('verdict', f'verdict IN ({_in(OLD)})')
    recreate(op, 'candidates')
