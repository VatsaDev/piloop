"""A project is a campaign: the one renaming of the schema.

- ``projects`` is ``campaigns``, and every ``project_id`` is ``campaign_id``. SQLite's ``RENAME`` keeps every row and
  rewrites the foreign keys.
- ``comments.target_kind`` and ``events.target_kind`` say ``campaign`` where they said ``project``, and every
  ``project.*`` event is a ``campaign.*`` event. A CHECK lists the target kinds, and SQLite changes a CHECK only by
  rebuilding the table, so ``comments``, ``events`` and ``campaigns`` (whose CHECKs are named after the table) are
  rebuilt. The payloads keep their text: an old payload may have ``project`` keys.
- The indexes and triggers named after a renamed table or column are made again under the new names. Every trigger is
  dropped first and made again last, so no rebuild breaks one that reads another table.
- The stored copy of each ``piloop.toml`` (``campaigns.config``) gets the new names in its queries, a role's ``takes``
  and the ``when`` of its ``sql`` triggers, where the parameter ``project`` (``:project``, ``@project`` or
  ``$project``) is ``campaign``. Only identifiers and parameters are renamed, bare or quoted: string literals and
  comments keep their text. A role's ``workdir = "project"`` is ``"repo"``. The downgrade renames back the new names
  that a query uses: it is exact unless a query used one of the new names for something else, such as an alias, before
  this revision.
- The directories of the campaigns stay under ``$PILOOP_HOME/projects``.

Revision ID: 0016
Revises: 0015
"""

import json
import re
from collections.abc import Callable, Sequence

from alembic import op
from sqlalchemy import text

revision: str = '0016'
down_revision: str | None = '0015'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
OLD_TABLES = {
    'projects': "CREATE TABLE projects (\n\tid INTEGER NOT NULL, \n\tname VARCHAR NOT NULL, \n\tpath VARCHAR NOT NULL, \n\tconfig JSON NOT NULL, \n\tevaluator_id INTEGER, \n\tstatus VARCHAR(16) DEFAULT 'active' NOT NULL, \n\tstopped_reason VARCHAR, \n\tspend_usd DOUBLE, \n\tspend_error VARCHAR, \n\tspend_at DATETIME, \n\tcreated_at DATETIME DEFAULT (CURRENT_TIMESTAMP) NOT NULL, \n\tupdated_at DATETIME DEFAULT (CURRENT_TIMESTAMP) NOT NULL, \n\tupdated_by VARCHAR, repo VARCHAR, paused VARCHAR(16) CONSTRAINT pause CHECK (paused IN ('launches', 'all')), paused_reason VARCHAR CONSTRAINT \"projects.paused and projects.paused_reason go together\" CHECK ((paused IS NULL) = (paused_reason IS NULL)) CONSTRAINT \"projects.paused_reason must be a text value\" CHECK (typeof(paused_reason) IN ('text', 'null')), type VARCHAR DEFAULT 'optimization' NOT NULL, parent_id INTEGER REFERENCES projects (id), \n\tPRIMARY KEY (id), \n\tUNIQUE (name), \n\tCONSTRAINT projectstatus CHECK (status IN ('active', 'stopped'))\n)",
    'comments': "CREATE TABLE \"comments\" (\n\tid INTEGER NOT NULL, \n\ttarget_kind VARCHAR(16) NOT NULL, \n\ttarget_id INTEGER NOT NULL, \n\tauthor VARCHAR NOT NULL, \n\tkind VARCHAR(16) DEFAULT 'note' NOT NULL, \n\tbody VARCHAR NOT NULL, \n\tpriority INTEGER, \n\tacked_at DATETIME, \n\tacked_by VARCHAR, \n\tcreated_at DATETIME DEFAULT (CURRENT_TIMESTAMP) NOT NULL, \n\tupdated_at DATETIME DEFAULT (CURRENT_TIMESTAMP) NOT NULL, \n\tupdated_by VARCHAR, \n\tPRIMARY KEY (id), \n\tCONSTRAINT \"comments.body must be a text value\" CHECK (typeof(body) IN ('text')), \n\tCONSTRAINT escalation_needs_priority CHECK (kind != 'escalation' OR priority IS NOT NULL), \n\tCONSTRAINT \"comments.author must be a text value\" CHECK (typeof(author) IN ('text')), \n\tCONSTRAINT targetkind CHECK (target_kind IN ('project', 'idea', 'run', 'candidate', 'measurement', 'worker', 'comment')), \n\tCONSTRAINT commentkind CHECK (kind IN ('note', 'finding', 'decision', 'escalation'))\n)",
    'events': "CREATE TABLE events (\n\tid INTEGER NOT NULL, \n\tat DATETIME DEFAULT (CURRENT_TIMESTAMP) NOT NULL, \n\tkind VARCHAR NOT NULL, \n\tactor VARCHAR NOT NULL, \n\ttarget_kind VARCHAR(16) NOT NULL, \n\ttarget_id INTEGER NOT NULL, \n\tpayload JSON DEFAULT '{}' NOT NULL, \n\tPRIMARY KEY (id), \n\tCONSTRAINT targetkind CHECK (target_kind IN ('project', 'idea', 'run', 'candidate', 'measurement', 'worker', 'comment'))\n)",
}
NEW_TABLES = {
    'campaigns': "CREATE TABLE campaigns (\n\tid INTEGER NOT NULL,\n\tname VARCHAR NOT NULL,\n\tpath VARCHAR NOT NULL,\n\tconfig JSON NOT NULL,\n\tevaluator_id INTEGER,\n\tstatus VARCHAR(16) DEFAULT 'active' NOT NULL,\n\tstopped_reason VARCHAR,\n\tpaused VARCHAR(16),\n\tpaused_reason VARCHAR,\n\tspend_usd DOUBLE,\n\tspend_error VARCHAR,\n\tspend_at DATETIME,\n\trepo VARCHAR,\n\ttype VARCHAR DEFAULT 'optimization' NOT NULL,\n\tparent_id INTEGER,\n\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,\n\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,\n\tupdated_by VARCHAR,\n\tPRIMARY KEY (id),\n\tCONSTRAINT \"campaigns.paused and campaigns.paused_reason go together\" CHECK ((paused IS NULL) = (paused_reason IS NULL)),\n\tCONSTRAINT \"campaigns.paused_reason must be a text value\" CHECK (typeof(paused_reason) IN ('text', 'null')),\n\tUNIQUE (name),\n\tCONSTRAINT campaignstatus CHECK (status IN ('active', 'stopped')),\n\tCONSTRAINT pause CHECK (paused IN ('launches', 'all')),\n\tFOREIGN KEY(parent_id) REFERENCES campaigns (id)\n)",
    'comments': "CREATE TABLE comments (\n\tid INTEGER NOT NULL,\n\ttarget_kind VARCHAR(16) NOT NULL,\n\ttarget_id INTEGER NOT NULL,\n\tauthor VARCHAR NOT NULL,\n\tkind VARCHAR(16) DEFAULT 'note' NOT NULL,\n\tbody VARCHAR NOT NULL,\n\tpriority INTEGER,\n\tacked_at DATETIME,\n\tacked_by VARCHAR,\n\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,\n\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,\n\tupdated_by VARCHAR,\n\tPRIMARY KEY (id),\n\tCONSTRAINT escalation_needs_priority CHECK (kind != 'escalation' OR priority IS NOT NULL),\n\tCONSTRAINT \"comments.author must be a text value\" CHECK (typeof(author) IN ('text')),\n\tCONSTRAINT \"comments.body must be a text value\" CHECK (typeof(body) IN ('text')),\n\tCONSTRAINT targetkind CHECK (target_kind IN ('campaign', 'idea', 'run', 'candidate', 'measurement', 'worker', 'comment')),\n\tCONSTRAINT commentkind CHECK (kind IN ('note', 'finding', 'decision', 'escalation'))\n)",
    'events': "CREATE TABLE events (\n\tid INTEGER NOT NULL,\n\tat DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,\n\tkind VARCHAR NOT NULL,\n\tactor VARCHAR NOT NULL,\n\ttarget_kind VARCHAR(16) NOT NULL,\n\ttarget_id INTEGER NOT NULL,\n\tpayload JSON DEFAULT '{}' NOT NULL,\n\tPRIMARY KEY (id),\n\tCONSTRAINT targetkind CHECK (target_kind IN ('campaign', 'idea', 'run', 'candidate', 'measurement', 'worker', 'comment'))\n)",
}
OLD_COLUMNS = {
    'projects': [
        'id',
        'name',
        'path',
        'config',
        'evaluator_id',
        'status',
        'stopped_reason',
        'spend_usd',
        'spend_error',
        'spend_at',
        'created_at',
        'updated_at',
        'updated_by',
        'repo',
        'paused',
        'paused_reason',
        'type',
        'parent_id',
    ],
    'comments': [
        'id',
        'target_kind',
        'target_id',
        'author',
        'kind',
        'body',
        'priority',
        'acked_at',
        'acked_by',
        'created_at',
        'updated_at',
        'updated_by',
    ],
    'events': ['id', 'at', 'kind', 'actor', 'target_kind', 'target_id', 'payload'],
}
OLD_INDEXES = {
    'ix_archives_project_idea': 'CREATE INDEX ix_archives_project_idea ON archives (project_id, idea_id)',
    'ix_candidates_project_verdict': 'CREATE INDEX ix_candidates_project_verdict ON candidates (project_id, verdict)',
    'ix_comments_inbox': 'CREATE INDEX ix_comments_inbox ON comments (priority, id) WHERE priority IS NOT NULL AND acked_at IS NULL',
    'ix_comments_target': 'CREATE INDEX ix_comments_target ON comments (target_kind, target_id)',
    'ix_events_target': 'CREATE INDEX ix_events_target ON events (target_kind, target_id, id)',
    'ix_ideas_project_status_priority': 'CREATE INDEX ix_ideas_project_status_priority ON ideas (project_id, status, priority)',
    'ix_lessons_project_family': 'CREATE INDEX ix_lessons_project_family ON lessons (project_id, family)',
    'ix_workers_project_kind_state': 'CREATE INDEX ix_workers_project_kind_state ON workers (project_id, kind, state)',
}
NEW_INDEXES = {
    'ix_archives_campaign_idea': 'CREATE INDEX ix_archives_campaign_idea ON archives (campaign_id, idea_id)',
    'ix_candidates_campaign_verdict': 'CREATE INDEX ix_candidates_campaign_verdict ON candidates (campaign_id, verdict)',
    'ix_comments_inbox': 'CREATE INDEX ix_comments_inbox ON comments (priority, id) WHERE priority IS NOT NULL AND acked_at IS NULL',
    'ix_comments_target': 'CREATE INDEX ix_comments_target ON comments (target_kind, target_id)',
    'ix_events_target': 'CREATE INDEX ix_events_target ON events (target_kind, target_id, id)',
    'ix_ideas_campaign_status_priority': 'CREATE INDEX ix_ideas_campaign_status_priority ON ideas (campaign_id, status, priority)',
    'ix_lessons_campaign_family': 'CREATE INDEX ix_lessons_campaign_family ON lessons (campaign_id, family)',
    'ix_workers_campaign_kind_state': 'CREATE INDEX ix_workers_campaign_kind_state ON workers (campaign_id, kind, state)',
}
OLD_TRIGGERS = {
    'candidates_updated_at': "CREATE TRIGGER candidates_updated_at AFTER UPDATE ON candidates\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE candidates SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND",
    'comments_escalation_event': "CREATE TRIGGER comments_escalation_event AFTER INSERT ON comments\nWHEN NEW.kind = 'escalation'\nBEGIN\n  INSERT INTO events (kind, actor, target_kind, target_id, payload)\n  VALUES ('comment.escalation', NEW.author, 'comment', NEW.id, json_object('target_kind', NEW.target_kind, 'target_id', NEW.target_id));\nEND",
    'comments_fts_delete': "CREATE TRIGGER comments_fts_delete AFTER DELETE ON comments\nBEGIN\n  INSERT INTO comments_fts (comments_fts, rowid, body) VALUES ('delete', OLD.id, OLD.body);\nEND",
    'comments_fts_insert': 'CREATE TRIGGER comments_fts_insert AFTER INSERT ON comments\nBEGIN\n  INSERT INTO comments_fts (rowid, body) VALUES (NEW.id, NEW.body);\nEND',
    'comments_fts_update': "CREATE TRIGGER comments_fts_update AFTER UPDATE OF id, body ON comments\nBEGIN\n  INSERT INTO comments_fts (comments_fts, rowid, body) VALUES ('delete', OLD.id, OLD.body);\n  INSERT INTO comments_fts (rowid, body) VALUES (NEW.id, NEW.body);\nEND",
    'comments_updated_at': "CREATE TRIGGER comments_updated_at AFTER UPDATE ON comments\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE comments SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND",
    'documents_fts_delete': "CREATE TRIGGER documents_fts_delete AFTER DELETE ON documents\nBEGIN\n  INSERT INTO documents_fts (documents_fts, rowid, title, body) VALUES ('delete', OLD.id, OLD.title, OLD.body);\nEND",
    'documents_fts_insert': 'CREATE TRIGGER documents_fts_insert AFTER INSERT ON documents\nBEGIN\n  INSERT INTO documents_fts (rowid, title, body) VALUES (NEW.id, NEW.title, NEW.body);\nEND',
    'documents_fts_update': "CREATE TRIGGER documents_fts_update AFTER UPDATE OF id, title, body ON documents\nBEGIN\n  INSERT INTO documents_fts (documents_fts, rowid, title, body) VALUES ('delete', OLD.id, OLD.title, OLD.body);\n  INSERT INTO documents_fts (rowid, title, body) VALUES (NEW.id, NEW.title, NEW.body);\nEND",
    'documents_updated_at': "CREATE TRIGGER documents_updated_at AFTER UPDATE ON documents\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE documents SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND",
    'idea_links_project': "CREATE TRIGGER idea_links_project BEFORE INSERT ON idea_links\nWHEN (SELECT project_id FROM ideas WHERE id = NEW.idea_id) IS NOT (SELECT project_id FROM ideas WHERE id = NEW.target_id)\nBEGIN\n  SELECT RAISE(ABORT, 'idea_links: idea_id and target_id must be ideas of the same project');\nEND",
    'ideas_fts_delete': "CREATE TRIGGER ideas_fts_delete AFTER DELETE ON ideas\nBEGIN\n  INSERT INTO ideas_fts (ideas_fts, rowid, title, hypothesis, plan) VALUES ('delete', OLD.id, OLD.title, OLD.hypothesis, OLD.plan);\nEND",
    'ideas_fts_insert': 'CREATE TRIGGER ideas_fts_insert AFTER INSERT ON ideas\nBEGIN\n  INSERT INTO ideas_fts (rowid, title, hypothesis, plan) VALUES (NEW.id, NEW.title, NEW.hypothesis, NEW.plan);\nEND',
    'ideas_fts_update': "CREATE TRIGGER ideas_fts_update AFTER UPDATE OF id, title, hypothesis, plan ON ideas\nBEGIN\n  INSERT INTO ideas_fts (ideas_fts, rowid, title, hypothesis, plan) VALUES ('delete', OLD.id, OLD.title, OLD.hypothesis, OLD.plan);\n  INSERT INTO ideas_fts (rowid, title, hypothesis, plan) VALUES (NEW.id, NEW.title, NEW.hypothesis, NEW.plan);\nEND",
    'ideas_updated_at': "CREATE TRIGGER ideas_updated_at AFTER UPDATE ON ideas\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE ideas SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND",
    'journal_no_replace': "CREATE TRIGGER journal_no_replace BEFORE INSERT ON journal\nWHEN EXISTS (SELECT 1 FROM journal WHERE id = NEW.id)\nBEGIN\n  SELECT RAISE(ABORT, 'journal rows are never replaced: insert a new row');\nEND",
    'lessons_no_replace': "CREATE TRIGGER lessons_no_replace BEFORE INSERT ON lessons\nWHEN EXISTS (SELECT 1 FROM lessons WHERE id = NEW.id)\nBEGIN\n  SELECT RAISE(ABORT, 'lessons rows are never replaced: insert a new row');\nEND",
    'projects_parent_insert': "CREATE TRIGGER projects_parent_insert AFTER INSERT ON projects\nWHEN NEW.parent_id IS NOT NULL AND NEW.id IN (WITH RECURSIVE up(id) AS (SELECT NEW.parent_id UNION SELECT p.parent_id FROM projects p JOIN up ON p.id = up.id WHERE p.parent_id IS NOT NULL) SELECT id FROM up)\nBEGIN\n  SELECT RAISE(ABORT, 'projects.parent_id: a project cannot be under itself or under one of its descendants');\nEND",
    'projects_parent_update': "CREATE TRIGGER projects_parent_update AFTER UPDATE OF parent_id ON projects\nWHEN NEW.parent_id IS NOT NULL AND NEW.id IN (WITH RECURSIVE up(id) AS (SELECT NEW.parent_id UNION SELECT p.parent_id FROM projects p JOIN up ON p.id = up.id WHERE p.parent_id IS NOT NULL) SELECT id FROM up)\nBEGIN\n  SELECT RAISE(ABORT, 'projects.parent_id: a project cannot be under itself or under one of its descendants');\nEND",
    'projects_updated_at': "CREATE TRIGGER projects_updated_at AFTER UPDATE ON projects\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE projects SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND",
    'runs_updated_at': "CREATE TRIGGER runs_updated_at AFTER UPDATE ON runs\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE runs SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND",
    'workers_kind': "CREATE TRIGGER workers_kind BEFORE INSERT ON workers\nWHEN NEW.kind NOT IN ('worker', 'watchdog', 'research', 'scout') AND (SELECT json_type(config, '$.roles.' || json_quote(NEW.kind)) FROM projects WHERE id = NEW.project_id) IS NULL\nBEGIN\n  SELECT RAISE(ABORT, 'workers.kind must be a role of the project: worker, watchdog, research, scout or a [roles.<name>] table of its piloop.toml');\nEND",
    'workers_updated_at': "CREATE TRIGGER workers_updated_at AFTER UPDATE ON workers\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE workers SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND",
}
NEW_TRIGGERS = {
    'campaigns_updated_at': "CREATE TRIGGER campaigns_updated_at AFTER UPDATE ON campaigns\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE campaigns SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND;",
    'ideas_updated_at': "CREATE TRIGGER ideas_updated_at AFTER UPDATE ON ideas\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE ideas SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND;",
    'runs_updated_at': "CREATE TRIGGER runs_updated_at AFTER UPDATE ON runs\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE runs SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND;",
    'candidates_updated_at': "CREATE TRIGGER candidates_updated_at AFTER UPDATE ON candidates\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE candidates SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND;",
    'comments_updated_at': "CREATE TRIGGER comments_updated_at AFTER UPDATE ON comments\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE comments SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND;",
    'workers_updated_at': "CREATE TRIGGER workers_updated_at AFTER UPDATE ON workers\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE workers SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND;",
    'documents_updated_at': "CREATE TRIGGER documents_updated_at AFTER UPDATE ON documents\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE documents SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND;",
    'comments_escalation_event': "CREATE TRIGGER comments_escalation_event AFTER INSERT ON comments\nWHEN NEW.kind = 'escalation'\nBEGIN\n  INSERT INTO events (kind, actor, target_kind, target_id, payload)\n  VALUES ('comment.escalation', NEW.author, 'comment', NEW.id, json_object('target_kind', NEW.target_kind, 'target_id', NEW.target_id));\nEND;",
    'idea_links_campaign': "CREATE TRIGGER idea_links_campaign BEFORE INSERT ON idea_links\nWHEN (SELECT campaign_id FROM ideas WHERE id = NEW.idea_id) IS NOT (SELECT campaign_id FROM ideas WHERE id = NEW.target_id)\nBEGIN\n  SELECT RAISE(ABORT, 'idea_links: idea_id and target_id must be ideas of the same campaign');\nEND;",
    'workers_kind': "CREATE TRIGGER workers_kind BEFORE INSERT ON workers\nWHEN NEW.kind NOT IN ('worker', 'watchdog', 'research', 'scout') AND (SELECT json_type(config, '$.roles.' || json_quote(NEW.kind)) FROM campaigns WHERE id = NEW.campaign_id) IS NULL\nBEGIN\n  SELECT RAISE(ABORT, 'workers.kind must be a role of the campaign: worker, watchdog, research, scout or a [roles.<name>] table of its piloop.toml');\nEND;",
    'campaigns_parent_insert': "CREATE TRIGGER campaigns_parent_insert AFTER INSERT ON campaigns\nWHEN NEW.parent_id IS NOT NULL AND NEW.id IN (WITH RECURSIVE up(id) AS (SELECT NEW.parent_id UNION SELECT p.parent_id FROM campaigns p JOIN up ON p.id = up.id WHERE p.parent_id IS NOT NULL) SELECT id FROM up)\nBEGIN\n  SELECT RAISE(ABORT, 'campaigns.parent_id: a campaign cannot be under itself or under one of its descendants');\nEND;",
    'campaigns_parent_update': "CREATE TRIGGER campaigns_parent_update AFTER UPDATE OF parent_id ON campaigns\nWHEN NEW.parent_id IS NOT NULL AND NEW.id IN (WITH RECURSIVE up(id) AS (SELECT NEW.parent_id UNION SELECT p.parent_id FROM campaigns p JOIN up ON p.id = up.id WHERE p.parent_id IS NOT NULL) SELECT id FROM up)\nBEGIN\n  SELECT RAISE(ABORT, 'campaigns.parent_id: a campaign cannot be under itself or under one of its descendants');\nEND;",
    'journal_no_replace': "CREATE TRIGGER journal_no_replace BEFORE INSERT ON journal\nWHEN EXISTS (SELECT 1 FROM journal WHERE id = NEW.id)\nBEGIN\n  SELECT RAISE(ABORT, 'journal rows are never replaced: insert a new row');\nEND;",
    'lessons_no_replace': "CREATE TRIGGER lessons_no_replace BEFORE INSERT ON lessons\nWHEN EXISTS (SELECT 1 FROM lessons WHERE id = NEW.id)\nBEGIN\n  SELECT RAISE(ABORT, 'lessons rows are never replaced: insert a new row');\nEND;",
    'ideas_fts_insert': 'CREATE TRIGGER ideas_fts_insert AFTER INSERT ON ideas\nBEGIN\n  INSERT INTO ideas_fts (rowid, title, hypothesis, plan) VALUES (NEW.id, NEW.title, NEW.hypothesis, NEW.plan);\nEND;',
    'ideas_fts_delete': "CREATE TRIGGER ideas_fts_delete AFTER DELETE ON ideas\nBEGIN\n  INSERT INTO ideas_fts (ideas_fts, rowid, title, hypothesis, plan) VALUES ('delete', OLD.id, OLD.title, OLD.hypothesis, OLD.plan);\nEND;",
    'ideas_fts_update': "CREATE TRIGGER ideas_fts_update AFTER UPDATE OF id, title, hypothesis, plan ON ideas\nBEGIN\n  INSERT INTO ideas_fts (ideas_fts, rowid, title, hypothesis, plan) VALUES ('delete', OLD.id, OLD.title, OLD.hypothesis, OLD.plan);\n  INSERT INTO ideas_fts (rowid, title, hypothesis, plan) VALUES (NEW.id, NEW.title, NEW.hypothesis, NEW.plan);\nEND;",
    'comments_fts_insert': 'CREATE TRIGGER comments_fts_insert AFTER INSERT ON comments\nBEGIN\n  INSERT INTO comments_fts (rowid, body) VALUES (NEW.id, NEW.body);\nEND;',
    'comments_fts_delete': "CREATE TRIGGER comments_fts_delete AFTER DELETE ON comments\nBEGIN\n  INSERT INTO comments_fts (comments_fts, rowid, body) VALUES ('delete', OLD.id, OLD.body);\nEND;",
    'comments_fts_update': "CREATE TRIGGER comments_fts_update AFTER UPDATE OF id, body ON comments\nBEGIN\n  INSERT INTO comments_fts (comments_fts, rowid, body) VALUES ('delete', OLD.id, OLD.body);\n  INSERT INTO comments_fts (rowid, body) VALUES (NEW.id, NEW.body);\nEND;",
    'documents_fts_insert': 'CREATE TRIGGER documents_fts_insert AFTER INSERT ON documents\nBEGIN\n  INSERT INTO documents_fts (rowid, title, body) VALUES (NEW.id, NEW.title, NEW.body);\nEND;',
    'documents_fts_delete': "CREATE TRIGGER documents_fts_delete AFTER DELETE ON documents\nBEGIN\n  INSERT INTO documents_fts (documents_fts, rowid, title, body) VALUES ('delete', OLD.id, OLD.title, OLD.body);\nEND;",
    'documents_fts_update': "CREATE TRIGGER documents_fts_update AFTER UPDATE OF id, title, body ON documents\nBEGIN\n  INSERT INTO documents_fts (documents_fts, rowid, title, body) VALUES ('delete', OLD.id, OLD.title, OLD.body);\n  INSERT INTO documents_fts (rowid, title, body) VALUES (NEW.id, NEW.title, NEW.body);\nEND;",
}
RENAMED_COLUMNS = [
    ('archives', 'project_id'),
    ('candidates', 'project_id'),
    ('documents', 'project_id'),
    ('evaluators', 'project_id'),
    ('ideas', 'project_id'),
    ('lessons', 'project_id'),
    ('techniques', 'project_id'),
    ('workers', 'project_id'),
]
TABLES = {'projects': 'campaigns'}
COLUMNS = {'project_id': 'campaign_id'}
NAMES = {**TABLES, **COLUMNS}
PARAMETERS = {'project': 'campaign'}
"""The parameter of an ``sql`` trigger, after ``:``, ``@`` or ``$``."""
"""Each old name, with its new one: in the schema, and in the queries of the stored configs."""
WORKDIRS = {'project': 'repo'}
TOKEN = re.compile(
    r"""'(?:[^']|'')*'|--[^\n]*|/\*.*?(?:\*/|$)|"(?:[^"]|"")*"|`[^`]*`|\[[^\]]*\]|[:@$]\w+|\w+""", re.DOTALL
)
"""A string literal, a comment, a quoted identifier, a parameter or a bare word: the tokens that can hold a name."""


def upgrade() -> None:
    _drop('TRIGGER', OLD_TRIGGERS)
    _drop('INDEX', OLD_INDEXES)
    for old, new in TABLES.items():
        op.execute(f'ALTER TABLE {old} RENAME TO {new}')
    for table, column in RENAMED_COLUMNS:
        op.execute(f'ALTER TABLE {table} RENAME COLUMN {column} TO {COLUMNS[column]}')
    renamed = {v: k for k, v in COLUMNS.items()}
    _rebuild('campaigns', NEW_TABLES['campaigns'], lambda c: renamed.get(c, c))
    kinds = _values('project', 'campaign')
    _rebuild('comments', NEW_TABLES['comments'], kinds)
    _rebuild('events', NEW_TABLES['events'], kinds)
    _create(NEW_INDEXES)
    _configs(NAMES, PARAMETERS, WORKDIRS)
    _create(NEW_TRIGGERS)


def downgrade() -> None:
    _drop('TRIGGER', NEW_TRIGGERS)
    _drop('INDEX', NEW_INDEXES)
    for old, new in TABLES.items():
        op.execute(f'ALTER TABLE {new} RENAME TO {old}')
    for table, column in RENAMED_COLUMNS:
        op.execute(f'ALTER TABLE {_old(table)} RENAME COLUMN {COLUMNS[column]} TO {column}')
    _rebuild('projects', OLD_TABLES['projects'], lambda c: COLUMNS.get(c, c))
    kinds = _values('campaign', 'project')
    _rebuild('comments', OLD_TABLES['comments'], kinds)
    _rebuild('events', OLD_TABLES['events'], kinds)
    _create(OLD_INDEXES)
    _configs(*({new: old for old, new in pairs.items()} for pairs in (NAMES, PARAMETERS, WORKDIRS)))
    _create(OLD_TRIGGERS)


def _old(table: str) -> str:
    return {new: old for old, new in TABLES.items()}.get(table, table)


def _drop(kind: str, names: dict[str, str]) -> None:
    for name in names:
        op.execute(f'DROP {kind} {name}')


def _create(statements: dict[str, str]) -> None:
    for sql in statements.values():
        op.execute(sql)


def _values(old: str, new: str) -> Callable[[str], str]:
    """The expressions that copy a column of ``comments`` or ``events``, with ``old`` as ``new`` in the target kinds."""

    def source(column: str) -> str:
        if column == 'target_kind':
            return f"CASE target_kind WHEN '{old}' THEN '{new}' ELSE target_kind END"
        if column == 'kind':
            return f"CASE WHEN kind LIKE '{old}.%' THEN '{new}.' || substr(kind, {len(old) + 2}) ELSE kind END"
        return column

    return source


def _rebuild(table: str, ddl: str, source: Callable[[str], str]) -> None:
    """Make ``table`` again from ``ddl``, copying every row: each column of ``ddl`` from the expression ``source`` gives."""
    temporary = f'_rebuilt_{table}'
    op.execute(re.sub(r'^CREATE TABLE "?\w+"?', f'CREATE TABLE {temporary}', ddl, count=1))
    bind = op.get_bind()
    names = [row[1] for row in bind.execute(text(f'PRAGMA table_info({temporary})'))]
    op.execute(f'INSERT INTO {temporary} ({", ".join(names)}) SELECT {", ".join(map(source, names))} FROM {table}')
    op.execute(f'DROP TABLE {table}')
    op.execute(f'ALTER TABLE {temporary} RENAME TO {table}')


def _configs(names: dict[str, str], parameters: dict[str, str], workdirs: dict[str, str]) -> None:
    """Rename the identifiers and the parameters in the queries of every stored config, and its roles' workdirs."""

    def one(token: re.Match[str]) -> str:
        word = token.group()
        if word[0] in ':@$':
            return word[0] + parameters.get(word[1:].lower(), word[1:])
        quoted = word[0] in '"`['
        new = names.get((word[1:-1] if quoted else word).lower())
        if new is None:  # also every string literal and comment: it is matched whole, so it is no name
            return word
        return f'{word[0]}{new}{word[-1]}' if quoted else new

    bind = op.get_bind()
    table = 'campaigns' if 'projects' in names else 'projects'
    for row_id, stored in bind.execute(text(f'SELECT id, config FROM {table}')).all():
        config = json.loads(stored)
        for role in (config.get('roles') or {}).values():
            if role.get('takes') is not None:
                role['takes'] = TOKEN.sub(one, role['takes'])
            if role.get('workdir') in workdirs:
                role['workdir'] = workdirs[role['workdir']]
            for trigger in role.get('triggers') or ():
                if trigger.get('kind') == 'sql':
                    trigger['when'] = TOKEN.sub(one, trigger['when'])
        bind.execute(text(f'UPDATE {table} SET config = :c WHERE id = :id'), {'c': json.dumps(config), 'id': row_id})
