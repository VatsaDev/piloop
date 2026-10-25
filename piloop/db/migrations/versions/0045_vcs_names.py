"""What core stores about versions loses its git names.

``candidates.commit_sha`` is ``revision``, the provider's name for the version. So is the key of a revision in the
``candidate.deleted`` events (``commit_sha``) and the ``candidate.imported`` events (``commit``). An agent's job names
the run that its work is captured for (``$.capture``), not a ``$.commits`` with a summary: the provider writes the
summary. The work that a job brought back is ``work``, not ``commits``, in each stored report: the
``agent_session.job_ended`` events and the reports that a fence refused (``lease.stale_report``).

Revision ID: 0045
Revises: 0044
"""

from collections.abc import Sequence

from alembic import op

revision: str = '0045'
down_revision: str | None = '0044'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

UNCOMMITTED = 'piloop: uncommitted implementer changes'
"""The summary that a job carried before: the ``git`` provider's subject of the commit of unsaved work."""
KEYS = (
    ('agent_session.job_ended', '$.ended.commits', '$.ended.work'),
    ('lease.stale_report', '$.report.commits', '$.report.work'),
    ('candidate.deleted', '$.commit_sha', '$.revision'),
    ('candidate.imported', '$.commit', '$.revision'),
)
"""Each event kind with a renamed key: the key before, then after."""


def upgrade() -> None:
    op.alter_column('candidates', 'commit_sha', new_column_name='revision')
    op.execute(
        "UPDATE leases SET job = json_set(json_remove(job, '$.commits'), '$.capture', json_extract(job, '$.commits.run'))"
        " WHERE json_type(job, '$.commits') IS NOT NULL"
    )
    for kind, old, new in KEYS:
        _rename(kind, old, new)


def downgrade() -> None:
    for kind, old, new in KEYS:
        _rename(kind, new, old)
    op.execute(
        "UPDATE leases SET job = json_set(json_remove(job, '$.capture'), '$.commits',"
        f" CASE WHEN json_type(job, '$.capture') = 'integer' THEN json_object('summary', '{UNCOMMITTED}', 'run',"
        " json_extract(job, '$.capture')) END) WHERE json_type(job, '$.capture') IS NOT NULL"
    )
    op.alter_column('candidates', 'revision', new_column_name='commit_sha')


def _rename(kind: str, old: str, new: str) -> None:
    op.execute(
        f"UPDATE events SET payload = json_set(json_remove(payload, '{old}'), '{new}', json_extract(payload, '{old}'))"
        f" WHERE kind = '{kind}' AND json_type(payload, '{old}') IS NOT NULL"
    )
