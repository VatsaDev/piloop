"""Lessons get their own table: ``lessons`` replaces the ``lesson`` comment kind and its first-line CHECKs.

A lesson was a comment whose first line had to match ``family: <f> | status: <s> | best: <effect> (<evaluator>)``,
and the briefs parsed that line back. Now the family, the status, the effect and its metric are columns. Each
``lesson`` comment becomes a row, in id order, so the newest lesson of a family stays the newest. The row keeps the
comment's body as it was, first line included, and its author and time; ``parse`` reads the columns from the first
line exactly as the briefs ranked it before. Then the comments go, and batch mode rebuilds ``comments`` without the
``lesson`` kind and its CHECKs (``recreate`` puts the triggers back). The ``journal`` and ``lessons`` triggers refuse
an ``INSERT OR REPLACE`` (``piloop.db.triggers.immutable``).

The downgrade turns each lesson back into a ``lesson`` comment. A body that does not open with the old first line gets
one, made from the columns.

Revision ID: 0007
Revises: 0006
"""

import re
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

import piloop.db.models
from piloop.db.triggers import recreate

revision: str = '0007'
down_revision: str | None = '0006'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
KINDS = ('note', 'finding', 'decision', 'escalation')
STATUSES = ('dead', 'promising', 'untested')
TEXT = ('family', 'body', 'created_by')
FIRST_LINE = 'substr(body, 1, instr(body || char(10), char(10)) - 1)'
LESSON_CHECKS = {
    'a lesson is on its project': "kind != 'lesson' OR target_kind = 'project'",
    'a lesson starts family: <family> | status: dead|promising|untested | best: <+N metric> (<evaluator/eval>)': (
        "kind != 'lesson' OR ("
        + ' OR '.join(f"{FIRST_LINE} GLOB 'family: ?* | status: {status} | best: ?*'" for status in STATUSES)
        + ')'
    ),
}
LESSON = re.compile(r'family: (?P<family>.+?) \| status: (?P<status>\w+) \| best: (?P<best>.*)')
NUMBER = r'[+-]?\d+(?:\.\d+)?%?'
EFFECT = re.compile(rf'(?P<number>{NUMBER})(?:\s+to\s+{NUMBER})?(?:\s+(?P<metric>[^\s(,;]+))?')


def parse(body: str) -> tuple[str, str, float | None, str | None]:
    """The family, status, effect and metric of a lesson comment, read from its first line.

    The CHECK guaranteed the shape. The effect is the first number after ``best:``, with its sign, and the metric is
    the word after it (after a range's second number). A number without a metric word, or ``none measured``, is no
    effect.
    """
    m = LESSON.match(body.strip().split('\n', 1)[0])
    if m is None:  # the CHECK refused any other first line
        raise ValueError(f'not a lesson: {body[:80]!r}')
    effect = EFFECT.match(m['best'])
    if effect is None or effect['metric'] is None:
        return m['family'], m['status'], None, None
    return m['family'], m['status'], float(effect['number'].rstrip('%')), effect['metric']


def upgrade() -> None:
    op.create_table(
        'lessons',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('project_id', sa.Integer(), nullable=False),
        sa.Column('family', sa.String(), nullable=False),
        sa.Column(
            'status',
            sa.Enum(*STATUSES, name='lessonstatus', native_enum=False, create_constraint=True, length=16),
            nullable=False,
        ),
        sa.Column('effect', sa.Double(), nullable=True),
        sa.Column('metric', sa.String(), nullable=True),
        sa.Column('body', sa.String(), nullable=False),
        sa.Column('created_by', sa.String(), nullable=False),
        sa.Column(
            'created_at', piloop.db.models.UTCDateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False
        ),
        sa.CheckConstraint('(effect IS NULL) = (metric IS NULL)', name='lessons.effect and lessons.metric go together'),
        sa.CheckConstraint("typeof(effect) IN ('integer', 'real', 'null')", name='lessons.effect must be a number'),
        *(sa.CheckConstraint(f"typeof({c}) IN ('text')", name=f'lessons.{c} must be a text value') for c in TEXT),
        sa.CheckConstraint("typeof(metric) IN ('text', 'null')", name='lessons.metric must be a text value'),
        sa.ForeignKeyConstraint(['project_id'], ['projects.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )
    with op.batch_alter_table('lessons', schema=None) as batch_op:
        batch_op.create_index('ix_lessons_project_family', ['project_id', 'family'], unique=False)
    con = op.get_bind()
    comments = con.execute(
        sa.text("SELECT id, target_id, author, body, created_at FROM comments WHERE kind = 'lesson' ORDER BY id")
    ).all()
    for _, project_id, author, body, created_at in comments:
        family, status, effect, metric = parse(body)
        con.execute(
            sa.text(
                'INSERT INTO lessons (project_id, family, status, effect, metric, body, created_by, created_at)'
                ' VALUES (:project_id, :family, :status, :effect, :metric, :body, :created_by, :created_at)'
            ),
            {
                'project_id': project_id,
                'family': family,
                'status': status,
                'effect': effect,
                'metric': metric,
                'body': body,
                'created_by': author,
                'created_at': created_at,
            },
        )
    # while its triggers still keep comments_fts in step
    op.execute("DELETE FROM comments WHERE kind = 'lesson'")
    with op.batch_alter_table('comments', schema=None, recreate='always') as batch_op:
        for name in LESSON_CHECKS:
            batch_op.drop_constraint(name, type_='check')
        batch_op.drop_constraint('commentkind', type_='check')
        batch_op.create_check_constraint('commentkind', _kinds(KINDS))
    recreate(op, 'comments')
    recreate(op, 'journal')
    recreate(op, 'lessons')


def downgrade() -> None:
    with op.batch_alter_table('comments', schema=None, recreate='always') as batch_op:
        batch_op.drop_constraint('commentkind', type_='check')
        batch_op.create_check_constraint('commentkind', _kinds((*KINDS, 'lesson')))
        for name, condition in LESSON_CHECKS.items():
            batch_op.create_check_constraint(name, condition)
    recreate(op, 'comments')
    con = op.get_bind()
    lessons = con.execute(
        sa.text(
            'SELECT project_id, family, status, effect, metric, body, created_by, created_at FROM lessons ORDER BY id'
        )
    ).all()
    for project_id, family, status, effect, metric, body, created_by, created_at in lessons:
        if LESSON.match(body) is None:
            best = 'none measured' if effect is None else f'{effect:+g} {metric}'
            body = f'family: {family} | status: {status} | best: {best}\n{body}'
        con.execute(
            sa.text(
                'INSERT INTO comments (target_kind, target_id, author, kind, body, created_at, updated_at)'
                " VALUES ('project', :project_id, :author, 'lesson', :body, :created_at, :created_at)"
            ),
            {'project_id': project_id, 'author': created_by, 'body': body, 'created_at': created_at},
        )
    op.execute('DROP TRIGGER journal_no_replace')
    op.drop_table('lessons')


def _kinds(kinds: tuple[str, ...]) -> str:
    return f'kind IN ({", ".join(f"{k!r}" for k in kinds)})'
