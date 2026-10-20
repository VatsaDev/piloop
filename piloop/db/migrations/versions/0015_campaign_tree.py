"""``projects.type`` and ``projects.parent_id``: each campaign has a type, and the campaigns of a home form a tree.

Every existing project becomes a root ``optimization`` campaign, which is what it was. The type is a name, checked
against the installed campaign types when it is set, so the column has no fixed list. Two triggers refuse a cycle of
parents.

``ADD COLUMN`` keeps the table: a rebuild of ``projects`` breaks the ``workers_kind`` trigger, which reads it. SQLite
adds a column with a ``REFERENCES`` clause only when its default is NULL, as ``parent_id``'s is.

Revision ID: 0015
Revises: 0014
"""

from collections.abc import Sequence

from alembic import op

revision: str = '0015'
down_revision: str | None = '0014'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
TYPE = "type VARCHAR DEFAULT 'optimization' NOT NULL"
PARENT = 'parent_id INTEGER REFERENCES projects (id)'
TRIGGERS = (
    """CREATE TRIGGER projects_parent_insert AFTER INSERT ON projects
WHEN NEW.parent_id IS NOT NULL AND NEW.id IN (WITH RECURSIVE up(id) AS (SELECT NEW.parent_id UNION SELECT p.parent_id FROM projects p JOIN up ON p.id = up.id WHERE p.parent_id IS NOT NULL) SELECT id FROM up)
BEGIN
  SELECT RAISE(ABORT, 'projects.parent_id: a project cannot be under itself or under one of its descendants');
END;""",
    """CREATE TRIGGER projects_parent_update AFTER UPDATE OF parent_id ON projects
WHEN NEW.parent_id IS NOT NULL AND NEW.id IN (WITH RECURSIVE up(id) AS (SELECT NEW.parent_id UNION SELECT p.parent_id FROM projects p JOIN up ON p.id = up.id WHERE p.parent_id IS NOT NULL) SELECT id FROM up)
BEGIN
  SELECT RAISE(ABORT, 'projects.parent_id: a project cannot be under itself or under one of its descendants');
END;""",
)


def upgrade() -> None:
    op.execute(f'ALTER TABLE projects ADD COLUMN {TYPE}')
    op.execute(f'ALTER TABLE projects ADD COLUMN {PARENT}')
    for trigger in TRIGGERS:
        op.execute(trigger)


def downgrade() -> None:
    # the triggers first: SQLite refuses to drop a column that a trigger names
    op.execute('DROP TRIGGER projects_parent_insert')
    op.execute('DROP TRIGGER projects_parent_update')
    op.execute('ALTER TABLE projects DROP COLUMN parent_id')
    op.execute('ALTER TABLE projects DROP COLUMN type')
