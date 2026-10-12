"""The initial schema: every table, index, full-text index and persistent trigger.

Revision ID: 0001
Revises:
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

import piloop.db.models
from piloop.db.triggers import PERSISTENT
from piloop.db.triggers.fts import FTS_COLUMNS, fts_table

revision: str = '0001'
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'comments',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column(
            'target_kind',
            sa.Enum(
                'project',
                'idea',
                'run',
                'candidate',
                'measurement',
                'worker',
                'comment',
                name='targetkind',
                native_enum=False,
                create_constraint=True,
                length=16,
            ),
            nullable=False,
        ),
        sa.Column('target_id', sa.Integer(), nullable=False),
        sa.Column('author', sa.String(), nullable=False),
        sa.Column(
            'kind',
            sa.Enum(
                'note',
                'finding',
                'decision',
                'escalation',
                name='commentkind',
                native_enum=False,
                create_constraint=True,
                length=16,
            ),
            server_default='note',
            nullable=False,
        ),
        sa.Column('body', sa.String(), nullable=False),
        sa.Column('priority', sa.Integer(), nullable=True),
        sa.Column('acked_at', piloop.db.models.UTCDateTime(), nullable=True),
        sa.Column('acked_by', sa.String(), nullable=True),
        sa.Column(
            'created_at', piloop.db.models.UTCDateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False
        ),
        sa.Column(
            'updated_at', piloop.db.models.UTCDateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False
        ),
        sa.Column('updated_by', sa.String(), nullable=True),
        sa.CheckConstraint("kind != 'escalation' OR priority IS NOT NULL", name='escalation_needs_priority'),
        sa.CheckConstraint("typeof(author) IN ('text')", name='comments.author must be a text value'),
        sa.CheckConstraint("typeof(body) IN ('text')", name='comments.body must be a text value'),
        sa.PrimaryKeyConstraint('id'),
    )
    with op.batch_alter_table('comments', schema=None) as batch_op:
        batch_op.create_index(
            'ix_comments_inbox',
            ['priority', 'id'],
            unique=False,
            sqlite_where=sa.text('priority IS NOT NULL AND acked_at IS NULL'),
        )
        batch_op.create_index('ix_comments_target', ['target_kind', 'target_id'], unique=False)

    op.create_table(
        'events',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('at', piloop.db.models.UTCDateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False),
        sa.Column('kind', sa.String(), nullable=False),
        sa.Column('actor', sa.String(), nullable=False),
        sa.Column(
            'target_kind',
            sa.Enum(
                'project',
                'idea',
                'run',
                'candidate',
                'measurement',
                'worker',
                'comment',
                name='targetkind',
                native_enum=False,
                create_constraint=True,
                length=16,
            ),
            nullable=False,
        ),
        sa.Column('target_id', sa.Integer(), nullable=False),
        sa.Column('payload', sa.JSON(), server_default=sa.text("'{}'"), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    with op.batch_alter_table('events', schema=None) as batch_op:
        batch_op.create_index('ix_events_target', ['target_kind', 'target_id', 'id'], unique=False)

    op.create_table(
        'projects',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('path', sa.String(), nullable=False),
        sa.Column('config', sa.JSON(), nullable=False),
        sa.Column('evaluator_id', sa.Integer(), nullable=True),
        sa.Column(
            'status',
            sa.Enum('active', 'stopped', name='projectstatus', native_enum=False, create_constraint=True, length=16),
            server_default='active',
            nullable=False,
        ),
        sa.Column('stopped_reason', sa.String(), nullable=True),
        sa.Column('spend_usd', sa.Double(), nullable=True),
        sa.Column('spend_error', sa.String(), nullable=True),
        sa.Column('spend_at', piloop.db.models.UTCDateTime(), nullable=True),
        sa.Column(
            'created_at', piloop.db.models.UTCDateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False
        ),
        sa.Column(
            'updated_at', piloop.db.models.UTCDateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False
        ),
        sa.Column('updated_by', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name'),
    )
    op.create_table(
        'evaluators',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('project_id', sa.Integer(), nullable=False),
        sa.Column('key', sa.String(), nullable=False),
        sa.Column('definition', sa.JSON(), nullable=False),
        sa.Column(
            'created_at', piloop.db.models.UTCDateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False
        ),
        sa.ForeignKeyConstraint(['project_id'], ['projects.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('project_id', 'key'),
    )
    op.create_table(
        'ideas',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('project_id', sa.Integer(), nullable=False),
        sa.Column('title', sa.String(), nullable=False),
        sa.Column('hypothesis', sa.String(), nullable=False),
        sa.Column('track', sa.String(), nullable=False),
        sa.Column('plan', sa.String(), nullable=True),
        sa.Column(
            'status',
            sa.Enum(
                'seed',
                'proposed',
                'ready',
                'scheduled',
                'done',
                'parked',
                'rejected',
                name='ideastatus',
                native_enum=False,
                create_constraint=True,
                length=16,
            ),
            server_default='ready',
            nullable=False,
        ),
        sa.Column('priority', sa.Integer(), server_default=sa.text('2'), nullable=False),
        sa.Column('harness', sa.String(), nullable=True),
        sa.Column('base_candidate_id', sa.Integer(), nullable=True),
        sa.Column('max_runs', sa.Integer(), server_default=sa.text('2'), nullable=False),
        sa.Column('created_by', sa.String(), nullable=False),
        sa.Column('provenance', sa.String(), nullable=True),
        sa.Column('family', sa.String(), nullable=True),
        sa.Column(
            'created_at', piloop.db.models.UTCDateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False
        ),
        sa.Column(
            'updated_at', piloop.db.models.UTCDateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False
        ),
        sa.Column('updated_by', sa.String(), nullable=True),
        sa.CheckConstraint("typeof(created_by) IN ('text')", name='ideas.created_by must be a text value'),
        sa.CheckConstraint("typeof(family) IN ('text', 'null')", name='ideas.family must be a text value'),
        sa.CheckConstraint("typeof(harness) IN ('text', 'null')", name='ideas.harness must be a text value'),
        sa.CheckConstraint("typeof(hypothesis) IN ('text')", name='ideas.hypothesis must be a text value'),
        sa.CheckConstraint("typeof(plan) IN ('text', 'null')", name='ideas.plan must be a text value'),
        sa.CheckConstraint("typeof(provenance) IN ('text', 'null')", name='ideas.provenance must be a text value'),
        sa.CheckConstraint("typeof(title) IN ('text')", name='ideas.title must be a text value'),
        sa.CheckConstraint("typeof(track) IN ('text')", name='ideas.track must be a text value'),
        sa.ForeignKeyConstraint(['project_id'], ['projects.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('project_id', 'title'),
    )
    with op.batch_alter_table('ideas', schema=None) as batch_op:
        batch_op.create_index('ix_ideas_project_status_priority', ['project_id', 'status', 'priority'], unique=False)

    op.create_table(
        'techniques',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('project_id', sa.Integer(), nullable=False),
        sa.Column('title', sa.String(), nullable=False),
        sa.Column('mechanism', sa.String(), nullable=False),
        sa.Column('url', sa.String(), nullable=False),
        sa.Column('created_by', sa.String(), nullable=False),
        sa.Column(
            'created_at', piloop.db.models.UTCDateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False
        ),
        sa.CheckConstraint("typeof(created_by) IN ('text')", name='techniques.created_by must be a text value'),
        sa.CheckConstraint("typeof(mechanism) IN ('text')", name='techniques.mechanism must be a text value'),
        sa.CheckConstraint("typeof(title) IN ('text')", name='techniques.title must be a text value'),
        sa.CheckConstraint("typeof(url) IN ('text')", name='techniques.url must be a text value'),
        sa.ForeignKeyConstraint(['project_id'], ['projects.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('project_id', 'url'),
    )
    op.create_table(
        'idea_links',
        sa.Column('idea_id', sa.Integer(), nullable=False),
        sa.Column('target_id', sa.Integer(), nullable=False),
        sa.Column(
            'kind',
            sa.Enum('after', 'supersedes', name='linkkind', native_enum=False, create_constraint=True, length=16),
            nullable=False,
        ),
        sa.CheckConstraint('idea_id != target_id', name='idea_links_not_self'),
        sa.ForeignKeyConstraint(['idea_id'], ['ideas.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['target_id'], ['ideas.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('idea_id', 'target_id', 'kind'),
    )
    with op.batch_alter_table('idea_links', schema=None) as batch_op:
        batch_op.create_index('ix_idea_links_target', ['target_id', 'kind'], unique=False)

    op.create_table(
        'runs',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('idea_id', sa.Integer(), nullable=False),
        sa.Column('attempt', sa.Integer(), nullable=False),
        sa.Column('base_candidate_id', sa.Integer(), nullable=False),
        sa.Column('branch', sa.Boolean(), server_default=sa.text('0'), nullable=False),
        sa.Column('harness', sa.String(), nullable=False),
        sa.Column('model', sa.String(), nullable=False),
        sa.Column(
            'status',
            sa.Enum(
                'queued',
                'running',
                'succeeded',
                'failed',
                'timed_out',
                name='runstatus',
                native_enum=False,
                create_constraint=True,
                length=16,
            ),
            server_default='queued',
            nullable=False,
        ),
        sa.Column('brief', sa.String(), nullable=True),
        sa.Column('started_at', piloop.db.models.UTCDateTime(), nullable=True),
        sa.Column('finished_at', piloop.db.models.UTCDateTime(), nullable=True),
        sa.Column('error', sa.String(), nullable=True),
        sa.Column('log_dir', sa.String(), nullable=True),
        sa.Column(
            'created_at', piloop.db.models.UTCDateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False
        ),
        sa.Column(
            'updated_at', piloop.db.models.UTCDateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False
        ),
        sa.Column('updated_by', sa.String(), nullable=True),
        sa.ForeignKeyConstraint(['idea_id'], ['ideas.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('idea_id', 'attempt'),
    )
    with op.batch_alter_table('runs', schema=None) as batch_op:
        batch_op.create_index('ix_runs_status', ['status'], unique=False)

    op.create_table(
        'candidates',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('project_id', sa.Integer(), nullable=False),
        sa.Column('run_id', sa.Integer(), nullable=True),
        sa.Column('parent_id', sa.Integer(), nullable=True),
        sa.Column('tree_sha', sa.String(), nullable=False),
        sa.Column('commit_sha', sa.String(), nullable=False),
        sa.Column('summary', sa.String(), nullable=True),
        sa.Column(
            'verdict',
            sa.Enum(
                'pending',
                'keep',
                'interesting',
                'discard',
                'crash',
                'reference',
                name='verdict',
                native_enum=False,
                create_constraint=True,
                length=16,
            ),
            server_default='pending',
            nullable=False,
        ),
        sa.Column('verdict_by', sa.String(), nullable=True),
        sa.Column('verdict_reason', sa.String(), nullable=True),
        sa.Column(
            'created_at', piloop.db.models.UTCDateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False
        ),
        sa.Column(
            'updated_at', piloop.db.models.UTCDateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False
        ),
        sa.Column('updated_by', sa.String(), nullable=True),
        sa.CheckConstraint("typeof(summary) IN ('text', 'null')", name='candidates.summary must be a text value'),
        sa.CheckConstraint("typeof(verdict_by) IN ('text', 'null')", name='candidates.verdict_by must be a text value'),
        sa.CheckConstraint(
            "typeof(verdict_reason) IN ('text', 'null')", name='candidates.verdict_reason must be a text value'
        ),
        sa.ForeignKeyConstraint(
            ['parent_id'],
            ['candidates.id'],
        ),
        sa.ForeignKeyConstraint(['project_id'], ['projects.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['run_id'], ['runs.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('project_id', 'tree_sha'),
    )
    with op.batch_alter_table('candidates', schema=None) as batch_op:
        batch_op.create_index('ix_candidates_parent', ['parent_id'], unique=False)
        batch_op.create_index('ix_candidates_project_verdict', ['project_id', 'verdict'], unique=False)
        batch_op.create_index('ix_candidates_run', ['run_id'], unique=False)

    op.create_table(
        'workers',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column(
            'kind',
            sa.Enum(
                'worker',
                'watchdog',
                'research',
                name='workerkind',
                native_enum=False,
                create_constraint=True,
                length=16,
            ),
            nullable=False,
        ),
        sa.Column('project_id', sa.Integer(), nullable=True),
        sa.Column('run_id', sa.Integer(), nullable=True),
        sa.Column('harness', sa.String(), nullable=True),
        sa.Column('model', sa.String(), nullable=True),
        sa.Column('runtime', sa.String(), nullable=True),
        sa.Column('handle', sa.JSON(), server_default=sa.text("'{}'"), nullable=False),
        sa.Column(
            'state',
            sa.Enum(
                'requested',
                'starting',
                'running',
                'exited',
                'killed',
                name='workerstate',
                native_enum=False,
                create_constraint=True,
                length=16,
            ),
            server_default='requested',
            nullable=False,
        ),
        sa.Column('reason', sa.String(), nullable=True),
        sa.Column('log_dir', sa.String(), nullable=True),
        sa.Column(
            'started_at', piloop.db.models.UTCDateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False
        ),
        sa.Column('ended_at', piloop.db.models.UTCDateTime(), nullable=True),
        sa.Column('error', sa.String(), nullable=True),
        sa.Column('session_id', sa.String(), nullable=True),
        sa.Column('checkpoint_id', sa.String(), nullable=True),
        sa.Column('usage', sa.JSON(), nullable=True),
        sa.Column(
            'updated_at', piloop.db.models.UTCDateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False
        ),
        sa.Column('updated_by', sa.String(), nullable=True),
        sa.CheckConstraint("typeof(harness) IN ('text', 'null')", name='workers.harness must be a text value'),
        sa.CheckConstraint("typeof(model) IN ('text', 'null')", name='workers.model must be a text value'),
        sa.CheckConstraint("typeof(reason) IN ('text', 'null')", name='workers.reason must be a text value'),
        sa.ForeignKeyConstraint(['project_id'], ['projects.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['run_id'], ['runs.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
    )
    with op.batch_alter_table('workers', schema=None) as batch_op:
        batch_op.create_index('ix_workers_project_kind_state', ['project_id', 'kind', 'state'], unique=False)
        batch_op.create_index('ix_workers_run', ['run_id'], unique=False)

    op.create_table(
        'measurements',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('candidate_id', sa.Integer(), nullable=False),
        sa.Column('evaluator_id', sa.Integer(), nullable=False),
        sa.Column('metrics', sa.JSON(), server_default=sa.text("'{}'"), nullable=False),
        sa.Column('error', sa.String(), nullable=True),
        sa.Column('seconds', sa.Double(), nullable=True),
        sa.Column(
            'created_at', piloop.db.models.UTCDateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False
        ),
        sa.ForeignKeyConstraint(['candidate_id'], ['candidates.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(
            ['evaluator_id'],
            ['evaluators.id'],
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    with op.batch_alter_table('measurements', schema=None) as batch_op:
        batch_op.create_index('ix_measurements_candidate_evaluator', ['candidate_id', 'evaluator_id'], unique=False)
    for table in FTS_COLUMNS:
        op.execute(fts_table(table))
    for trigger in PERSISTENT:
        op.execute(trigger.sql())


def downgrade() -> None:
    for table in FTS_COLUMNS:
        op.execute(f'DROP TABLE {table}_fts')
    with op.batch_alter_table('measurements', schema=None) as batch_op:
        batch_op.drop_index('ix_measurements_candidate_evaluator')

    op.drop_table('measurements')
    with op.batch_alter_table('workers', schema=None) as batch_op:
        batch_op.drop_index('ix_workers_run')
        batch_op.drop_index('ix_workers_project_kind_state')

    op.drop_table('workers')
    with op.batch_alter_table('candidates', schema=None) as batch_op:
        batch_op.drop_index('ix_candidates_run')
        batch_op.drop_index('ix_candidates_project_verdict')
        batch_op.drop_index('ix_candidates_parent')

    op.drop_table('candidates')
    with op.batch_alter_table('runs', schema=None) as batch_op:
        batch_op.drop_index('ix_runs_status')

    op.drop_table('runs')
    with op.batch_alter_table('idea_links', schema=None) as batch_op:
        batch_op.drop_index('ix_idea_links_target')

    op.drop_table('idea_links')
    op.drop_table('techniques')
    with op.batch_alter_table('ideas', schema=None) as batch_op:
        batch_op.drop_index('ix_ideas_project_status_priority')

    op.drop_table('ideas')
    op.drop_table('evaluators')
    op.drop_table('projects')
    with op.batch_alter_table('events', schema=None) as batch_op:
        batch_op.drop_index('ix_events_target')

    op.drop_table('events')
    with op.batch_alter_table('comments', schema=None) as batch_op:
        batch_op.drop_index('ix_comments_target')
        batch_op.drop_index('ix_comments_inbox', sqlite_where=sa.text('priority IS NOT NULL AND acked_at IS NULL'))

    op.drop_table('comments')
