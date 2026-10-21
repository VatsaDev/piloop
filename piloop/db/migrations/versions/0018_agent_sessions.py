"""The implementer role and agent sessions: the second renaming of the schema.

- ``workers`` is ``agent_sessions``: one row per harness session, in any role. The role ``worker`` is ``implementer``:
  in ``agent_sessions.kind``, in ``runs.role``, in the stored configs (``roles.worker`` is ``roles.implementer``) and
  in every signature that a column holds (``worker#12`` is ``implementer#12``).
- ``comments.target_kind`` and ``events.target_kind`` say ``agent_session`` where they said ``worker``. A session's
  own event is ``agent_session.*`` (``worker.wrap_up`` is ``agent_session.wrap_up``), and a role's event takes the
  new role name (``worker.started`` is ``implementer.started``). A signature in a payload takes the new role name,
  and a payload's ``worker`` key is ``agent_session``. The bodies keep their text.
- SQLite changes a CHECK or a default only by rebuilding the table, so ``comments`` and ``events`` (whose CHECKs list
  the target kinds), ``agent_sessions`` (whose CHECKs are named after the table) and ``runs`` (whose ``role``
  defaults to the role) are rebuilt.
- Every trigger is dropped first and made again last; the indexes named after the table are made again.
- The queries of the stored configs (a role's ``takes``, the ``when`` of its ``sql`` triggers) get the new table name.
  Only identifiers are renamed: string literals and comments keep their text, so ``kind = 'worker'`` in a query
  finds nothing until it is edited.

Revision ID: 0018
Revises: 0017
"""

import json
import re
from collections.abc import Callable, Sequence

from alembic import op
from sqlalchemy import text

revision: str = '0018'
down_revision: str | None = '0017'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
OLD_TABLES = {
    'workers': "CREATE TABLE \"workers\" (\n\tid INTEGER NOT NULL, \n\tkind VARCHAR NOT NULL, \n\tcampaign_id INTEGER, \n\trun_id INTEGER, \n\tharness VARCHAR, \n\tmodel VARCHAR, \n\truntime VARCHAR, \n\thandle JSON DEFAULT '{}' NOT NULL, \n\tstate VARCHAR(16) DEFAULT 'requested' NOT NULL, \n\treason VARCHAR, \n\tlog_dir VARCHAR, \n\tstarted_at DATETIME DEFAULT (CURRENT_TIMESTAMP) NOT NULL, \n\tended_at DATETIME, \n\terror VARCHAR, \n\tsession_id VARCHAR, \n\tcheckpoint_id VARCHAR, \n\tusage JSON, \n\tupdated_at DATETIME DEFAULT (CURRENT_TIMESTAMP) NOT NULL, \n\tupdated_by VARCHAR, \n\tPRIMARY KEY (id), \n\tCONSTRAINT \"workers.model must be a text value\" CHECK (typeof(model) IN ('text', 'null')), \n\tCONSTRAINT \"workers.reason must be a text value\" CHECK (typeof(reason) IN ('text', 'null')), \n\tCONSTRAINT workerstate CHECK (state IN ('requested', 'starting', 'running', 'exited', 'killed')), \n\tCONSTRAINT \"workers.harness must be a text value\" CHECK (typeof(harness) IN ('text', 'null')), \n\tCONSTRAINT \"workers.kind must be a text value\" CHECK (typeof(kind) IN ('text')), \n\tFOREIGN KEY(run_id) REFERENCES runs (id) ON DELETE SET NULL, \n\tFOREIGN KEY(campaign_id) REFERENCES \"campaigns\" (id) ON DELETE CASCADE\n)",
    'runs': "CREATE TABLE \"runs\" (\n\tid INTEGER NOT NULL, \n\tidea_id INTEGER NOT NULL, \n\tattempt INTEGER NOT NULL, \n\tbase_candidate_id INTEGER NOT NULL, \n\tbranch BOOLEAN DEFAULT 0 NOT NULL, \n\tharness VARCHAR NOT NULL, \n\tmodel VARCHAR NOT NULL, \n\tstatus VARCHAR(16) DEFAULT 'queued' NOT NULL, \n\tbrief VARCHAR, \n\tstarted_at DATETIME, \n\tfinished_at DATETIME, \n\terror VARCHAR, \n\tlog_dir VARCHAR, \n\tcreated_at DATETIME DEFAULT (CURRENT_TIMESTAMP) NOT NULL, \n\tupdated_at DATETIME DEFAULT (CURRENT_TIMESTAMP) NOT NULL, \n\tupdated_by VARCHAR, \n\trole VARCHAR DEFAULT 'worker' NOT NULL, \n\tscoring BOOLEAN DEFAULT 0 NOT NULL, \n\tPRIMARY KEY (id), \n\tCONSTRAINT runstatus CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'timed_out')), \n\tCONSTRAINT \"runs.scoring needs a finished status\" CHECK (scoring = 0 OR status IN ('succeeded', 'failed', 'timed_out')), \n\tFOREIGN KEY(idea_id) REFERENCES ideas (id) ON DELETE CASCADE, \n\tUNIQUE (idea_id, attempt)\n)",
    'comments': "CREATE TABLE \"comments\" (\n\tid INTEGER NOT NULL,\n\ttarget_kind VARCHAR(16) NOT NULL,\n\ttarget_id INTEGER NOT NULL,\n\tauthor VARCHAR NOT NULL,\n\tkind VARCHAR(16) DEFAULT 'note' NOT NULL,\n\tbody VARCHAR NOT NULL,\n\tpriority INTEGER,\n\tacked_at DATETIME,\n\tacked_by VARCHAR,\n\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,\n\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,\n\tupdated_by VARCHAR,\n\tPRIMARY KEY (id),\n\tCONSTRAINT escalation_needs_priority CHECK (kind != 'escalation' OR priority IS NOT NULL),\n\tCONSTRAINT \"comments.author must be a text value\" CHECK (typeof(author) IN ('text')),\n\tCONSTRAINT \"comments.body must be a text value\" CHECK (typeof(body) IN ('text')),\n\tCONSTRAINT targetkind CHECK (target_kind IN ('campaign', 'idea', 'run', 'candidate', 'measurement', 'worker', 'comment')),\n\tCONSTRAINT commentkind CHECK (kind IN ('note', 'finding', 'decision', 'escalation'))\n)",
    'events': "CREATE TABLE \"events\" (\n\tid INTEGER NOT NULL,\n\tat DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,\n\tkind VARCHAR NOT NULL,\n\tactor VARCHAR NOT NULL,\n\ttarget_kind VARCHAR(16) NOT NULL,\n\ttarget_id INTEGER NOT NULL,\n\tpayload JSON DEFAULT '{}' NOT NULL,\n\tPRIMARY KEY (id),\n\tCONSTRAINT targetkind CHECK (target_kind IN ('campaign', 'idea', 'run', 'candidate', 'measurement', 'worker', 'comment'))\n)",
}
NEW_TABLES = {
    'agent_sessions': "CREATE TABLE agent_sessions (\n\tid INTEGER NOT NULL,\n\tkind VARCHAR NOT NULL,\n\tcampaign_id INTEGER,\n\trun_id INTEGER,\n\tharness VARCHAR,\n\tmodel VARCHAR,\n\truntime VARCHAR,\n\thandle JSON DEFAULT '{}' NOT NULL,\n\tstate VARCHAR(16) DEFAULT 'requested' NOT NULL,\n\treason VARCHAR,\n\tlog_dir VARCHAR,\n\tstarted_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,\n\tended_at DATETIME,\n\terror VARCHAR,\n\tsession_id VARCHAR,\n\tcheckpoint_id VARCHAR,\n\tusage JSON,\n\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,\n\tupdated_by VARCHAR,\n\tPRIMARY KEY (id),\n\tCONSTRAINT \"agent_sessions.kind must be a text value\" CHECK (typeof(kind) IN ('text')),\n\tCONSTRAINT \"agent_sessions.reason must be a text value\" CHECK (typeof(reason) IN ('text', 'null')),\n\tCONSTRAINT \"agent_sessions.harness must be a text value\" CHECK (typeof(harness) IN ('text', 'null')),\n\tCONSTRAINT \"agent_sessions.model must be a text value\" CHECK (typeof(model) IN ('text', 'null')),\n\tFOREIGN KEY(campaign_id) REFERENCES campaigns (id) ON DELETE CASCADE,\n\tFOREIGN KEY(run_id) REFERENCES runs (id) ON DELETE SET NULL,\n\tCONSTRAINT agentsessionstate CHECK (state IN ('requested', 'starting', 'running', 'exited', 'killed'))\n)",
    'runs': "CREATE TABLE runs (\n\tid INTEGER NOT NULL,\n\tidea_id INTEGER NOT NULL,\n\trole VARCHAR DEFAULT 'implementer' NOT NULL,\n\tattempt INTEGER NOT NULL,\n\tbase_candidate_id INTEGER NOT NULL,\n\tbranch BOOLEAN DEFAULT 0 NOT NULL,\n\tharness VARCHAR NOT NULL,\n\tmodel VARCHAR NOT NULL,\n\tstatus VARCHAR(16) DEFAULT 'queued' NOT NULL,\n\tscoring BOOLEAN DEFAULT 0 NOT NULL,\n\tbrief VARCHAR,\n\tstarted_at DATETIME,\n\tfinished_at DATETIME,\n\terror VARCHAR,\n\tlog_dir VARCHAR,\n\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,\n\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,\n\tupdated_by VARCHAR,\n\tPRIMARY KEY (id),\n\tUNIQUE (idea_id, attempt),\n\tCONSTRAINT \"runs.scoring needs a finished status\" CHECK (scoring = 0 OR status IN ('succeeded', 'failed', 'timed_out')),\n\tFOREIGN KEY(idea_id) REFERENCES ideas (id) ON DELETE CASCADE,\n\tCONSTRAINT runstatus CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'timed_out'))\n)",
    'comments': "CREATE TABLE comments (\n\tid INTEGER NOT NULL,\n\ttarget_kind VARCHAR(16) NOT NULL,\n\ttarget_id INTEGER NOT NULL,\n\tauthor VARCHAR NOT NULL,\n\tkind VARCHAR(16) DEFAULT 'note' NOT NULL,\n\tbody VARCHAR NOT NULL,\n\tpriority INTEGER,\n\tacked_at DATETIME,\n\tacked_by VARCHAR,\n\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,\n\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,\n\tupdated_by VARCHAR,\n\tPRIMARY KEY (id),\n\tCONSTRAINT escalation_needs_priority CHECK (kind != 'escalation' OR priority IS NOT NULL),\n\tCONSTRAINT \"comments.author must be a text value\" CHECK (typeof(author) IN ('text')),\n\tCONSTRAINT \"comments.body must be a text value\" CHECK (typeof(body) IN ('text')),\n\tCONSTRAINT targetkind CHECK (target_kind IN ('campaign', 'idea', 'run', 'candidate', 'measurement', 'agent_session', 'comment')),\n\tCONSTRAINT commentkind CHECK (kind IN ('note', 'finding', 'decision', 'escalation'))\n)",
    'events': "CREATE TABLE events (\n\tid INTEGER NOT NULL,\n\tat DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,\n\tkind VARCHAR NOT NULL,\n\tactor VARCHAR NOT NULL,\n\ttarget_kind VARCHAR(16) NOT NULL,\n\ttarget_id INTEGER NOT NULL,\n\tpayload JSON DEFAULT '{}' NOT NULL,\n\tPRIMARY KEY (id),\n\tCONSTRAINT targetkind CHECK (target_kind IN ('campaign', 'idea', 'run', 'candidate', 'measurement', 'agent_session', 'comment'))\n)",
}
OLD_INDEXES = {
    'ix_comments_inbox': 'CREATE INDEX ix_comments_inbox ON comments (priority, id) WHERE priority IS NOT NULL AND acked_at IS NULL',
    'ix_comments_target': 'CREATE INDEX ix_comments_target ON comments (target_kind, target_id)',
    'ix_events_target': 'CREATE INDEX ix_events_target ON events (target_kind, target_id, id)',
    'ix_runs_status': 'CREATE INDEX ix_runs_status ON runs (status)',
    'ix_workers_campaign_kind_state': 'CREATE INDEX ix_workers_campaign_kind_state ON workers (campaign_id, kind, state)',
    'ix_workers_run': 'CREATE INDEX ix_workers_run ON workers (run_id)',
}
NEW_INDEXES = {
    'ix_agent_sessions_campaign_kind_state': 'CREATE INDEX ix_agent_sessions_campaign_kind_state ON agent_sessions (campaign_id, kind, state)',
    'ix_agent_sessions_run': 'CREATE INDEX ix_agent_sessions_run ON agent_sessions (run_id)',
    'ix_comments_inbox': 'CREATE INDEX ix_comments_inbox ON comments (priority, id) WHERE priority IS NOT NULL AND acked_at IS NULL',
    'ix_comments_target': 'CREATE INDEX ix_comments_target ON comments (target_kind, target_id)',
    'ix_events_target': 'CREATE INDEX ix_events_target ON events (target_kind, target_id, id)',
    'ix_runs_status': 'CREATE INDEX ix_runs_status ON runs (status)',
}
OLD_TRIGGERS = {
    'campaigns_parent_insert': "CREATE TRIGGER campaigns_parent_insert AFTER INSERT ON campaigns\nWHEN NEW.parent_id IS NOT NULL AND NEW.id IN (WITH RECURSIVE up(id) AS (SELECT NEW.parent_id UNION SELECT p.parent_id FROM campaigns p JOIN up ON p.id = up.id WHERE p.parent_id IS NOT NULL) SELECT id FROM up)\nBEGIN\n  SELECT RAISE(ABORT, 'campaigns.parent_id: a campaign cannot be under itself or under one of its descendants');\nEND",
    'campaigns_parent_update': "CREATE TRIGGER campaigns_parent_update AFTER UPDATE OF parent_id ON campaigns\nWHEN NEW.parent_id IS NOT NULL AND NEW.id IN (WITH RECURSIVE up(id) AS (SELECT NEW.parent_id UNION SELECT p.parent_id FROM campaigns p JOIN up ON p.id = up.id WHERE p.parent_id IS NOT NULL) SELECT id FROM up)\nBEGIN\n  SELECT RAISE(ABORT, 'campaigns.parent_id: a campaign cannot be under itself or under one of its descendants');\nEND",
    'campaigns_updated_at': "CREATE TRIGGER campaigns_updated_at AFTER UPDATE ON campaigns\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE campaigns SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND",
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
    'idea_links_campaign': "CREATE TRIGGER idea_links_campaign BEFORE INSERT ON idea_links\nWHEN (SELECT campaign_id FROM ideas WHERE id = NEW.idea_id) IS NOT (SELECT campaign_id FROM ideas WHERE id = NEW.target_id)\nBEGIN\n  SELECT RAISE(ABORT, 'idea_links: idea_id and target_id must be ideas of the same campaign');\nEND",
    'ideas_fts_delete': "CREATE TRIGGER ideas_fts_delete AFTER DELETE ON ideas\nBEGIN\n  INSERT INTO ideas_fts (ideas_fts, rowid, title, hypothesis, plan) VALUES ('delete', OLD.id, OLD.title, OLD.hypothesis, OLD.plan);\nEND",
    'ideas_fts_insert': 'CREATE TRIGGER ideas_fts_insert AFTER INSERT ON ideas\nBEGIN\n  INSERT INTO ideas_fts (rowid, title, hypothesis, plan) VALUES (NEW.id, NEW.title, NEW.hypothesis, NEW.plan);\nEND',
    'ideas_fts_update': "CREATE TRIGGER ideas_fts_update AFTER UPDATE OF id, title, hypothesis, plan ON ideas\nBEGIN\n  INSERT INTO ideas_fts (ideas_fts, rowid, title, hypothesis, plan) VALUES ('delete', OLD.id, OLD.title, OLD.hypothesis, OLD.plan);\n  INSERT INTO ideas_fts (rowid, title, hypothesis, plan) VALUES (NEW.id, NEW.title, NEW.hypothesis, NEW.plan);\nEND",
    'ideas_updated_at': "CREATE TRIGGER ideas_updated_at AFTER UPDATE ON ideas\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE ideas SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND",
    'journal_no_replace': "CREATE TRIGGER journal_no_replace BEFORE INSERT ON journal\nWHEN EXISTS (SELECT 1 FROM journal WHERE id = NEW.id)\nBEGIN\n  SELECT RAISE(ABORT, 'journal rows are never replaced: insert a new row');\nEND",
    'lessons_no_replace': "CREATE TRIGGER lessons_no_replace BEFORE INSERT ON lessons\nWHEN EXISTS (SELECT 1 FROM lessons WHERE id = NEW.id)\nBEGIN\n  SELECT RAISE(ABORT, 'lessons rows are never replaced: insert a new row');\nEND",
    'runs_updated_at': "CREATE TRIGGER runs_updated_at AFTER UPDATE ON runs\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE runs SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND",
    'workers_kind': "CREATE TRIGGER workers_kind BEFORE INSERT ON workers\nWHEN NEW.kind NOT IN ('worker', 'watchdog', 'research', 'scout') AND (SELECT json_type(config, '$.roles.' || json_quote(NEW.kind)) FROM campaigns WHERE id = NEW.campaign_id) IS NULL\nBEGIN\n  SELECT RAISE(ABORT, 'workers.kind must be a role of the campaign: worker, watchdog, research, scout or a [roles.<name>] table of its piloop.toml');\nEND",
    'workers_updated_at': "CREATE TRIGGER workers_updated_at AFTER UPDATE ON workers\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE workers SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND",
}
NEW_TRIGGERS = {
    'campaigns_updated_at': "CREATE TRIGGER campaigns_updated_at AFTER UPDATE ON campaigns\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE campaigns SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND;",
    'ideas_updated_at': "CREATE TRIGGER ideas_updated_at AFTER UPDATE ON ideas\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE ideas SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND;",
    'runs_updated_at': "CREATE TRIGGER runs_updated_at AFTER UPDATE ON runs\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE runs SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND;",
    'candidates_updated_at': "CREATE TRIGGER candidates_updated_at AFTER UPDATE ON candidates\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE candidates SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND;",
    'comments_updated_at': "CREATE TRIGGER comments_updated_at AFTER UPDATE ON comments\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE comments SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND;",
    'agent_sessions_updated_at': "CREATE TRIGGER agent_sessions_updated_at AFTER UPDATE ON agent_sessions\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE agent_sessions SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND;",
    'documents_updated_at': "CREATE TRIGGER documents_updated_at AFTER UPDATE ON documents\nWHEN NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by\nBEGIN\n  UPDATE documents SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id;\nEND;",
    'comments_escalation_event': "CREATE TRIGGER comments_escalation_event AFTER INSERT ON comments\nWHEN NEW.kind = 'escalation'\nBEGIN\n  INSERT INTO events (kind, actor, target_kind, target_id, payload)\n  VALUES ('comment.escalation', NEW.author, 'comment', NEW.id, json_object('target_kind', NEW.target_kind, 'target_id', NEW.target_id));\nEND;",
    'idea_links_campaign': "CREATE TRIGGER idea_links_campaign BEFORE INSERT ON idea_links\nWHEN (SELECT campaign_id FROM ideas WHERE id = NEW.idea_id) IS NOT (SELECT campaign_id FROM ideas WHERE id = NEW.target_id)\nBEGIN\n  SELECT RAISE(ABORT, 'idea_links: idea_id and target_id must be ideas of the same campaign');\nEND;",
    'agent_sessions_kind': "CREATE TRIGGER agent_sessions_kind BEFORE INSERT ON agent_sessions\nWHEN NEW.kind NOT IN ('implementer', 'watchdog', 'research', 'scout') AND (SELECT json_type(config, '$.roles.' || json_quote(NEW.kind)) FROM campaigns WHERE id = NEW.campaign_id) IS NULL\nBEGIN\n  SELECT RAISE(ABORT, 'agent_sessions.kind must be a role of the campaign: implementer, watchdog, research, scout or a [roles.<name>] table of its piloop.toml');\nEND;",
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
TABLES = {'workers': 'agent_sessions'}
ROLES = {'worker': 'implementer'}
KINDS = {'worker': 'agent_session'}
"""The target kind, and the prefix of the event kinds of a session."""
ROLE_EVENTS = ('started', 'finished')
"""The events that a session emits under its role's name (``<role>.started``): they take the new role name."""
ACTORS = (
    ('campaigns', 'updated_by'),
    ('ideas', 'created_by'),
    ('ideas', 'updated_by'),
    ('techniques', 'created_by'),
    ('agent_sessions', 'updated_by'),
    ('candidates', 'verdict_by'),
    ('candidates', 'updated_by'),
    ('documents', 'created_by'),
    ('documents', 'updated_by'),
    ('journal', 'author'),
    ('lessons', 'created_by'),
    ('runs', 'updated_by'),
    ('evaluations', 'cancelled_by'),
    ('comments', 'author'),
    ('comments', 'acked_by'),
    ('comments', 'updated_by'),
    ('events', 'actor'),
)
"""The columns that hold a signature, ``<role>#<id>``."""
TOKEN = re.compile(
    r"""'(?:[^']|'')*'|--[^\n]*|/\*.*?(?:\*/|$)|"(?:[^"]|"")*"|`[^`]*`|\[[^\]]*\]|[:@$]\w+|\w+""", re.DOTALL
)
"""A string literal, a comment, a quoted identifier, a parameter or a bare word: the tokens that can hold a name."""


def upgrade() -> None:
    _drop('TRIGGER', OLD_TRIGGERS)
    _drop('INDEX', OLD_INDEXES)
    op.execute('ALTER TABLE workers RENAME TO agent_sessions')
    _rebuild('agent_sessions', NEW_TABLES['agent_sessions'], _role('kind', ROLES))
    _rebuild('runs', NEW_TABLES['runs'], _role('role', ROLES))
    kinds = _values(KINDS, ROLES)
    _rebuild('comments', NEW_TABLES['comments'], kinds)
    _rebuild('events', NEW_TABLES['events'], kinds)
    _rows(ROLES)
    _create(NEW_INDEXES)
    _configs(TABLES, ROLES)
    _create(NEW_TRIGGERS)


def downgrade() -> None:
    back = {new: old for old, new in ROLES.items()}
    _drop('TRIGGER', NEW_TRIGGERS)
    _drop('INDEX', NEW_INDEXES)
    _configs({new: old for old, new in TABLES.items()}, back)
    _rows(back)
    op.execute('ALTER TABLE agent_sessions RENAME TO workers')
    _rebuild('workers', OLD_TABLES['workers'], _role('kind', back))
    _rebuild('runs', OLD_TABLES['runs'], _role('role', back))
    kinds = _values({new: old for old, new in KINDS.items()}, back)
    _rebuild('comments', OLD_TABLES['comments'], kinds)
    _rebuild('events', OLD_TABLES['events'], kinds)
    _create(OLD_INDEXES)
    _create(OLD_TRIGGERS)


def _drop(kind: str, names: dict[str, str]) -> None:
    for name in names:
        op.execute(f'DROP {kind} {name}')


def _create(statements: dict[str, str]) -> None:
    for sql in statements.values():
        op.execute(sql)


def _role(column: str, roles: dict[str, str]) -> Callable[[str], str]:
    """The expressions that copy a column, with each old role in ``column`` as the new one."""
    cases = ' '.join(f"WHEN '{old}' THEN '{new}'" for old, new in roles.items())
    return lambda c: f'CASE {c} {cases} ELSE {c} END' if c == column else c


def _values(kinds: dict[str, str], roles: dict[str, str]) -> Callable[[str], str]:
    """The expressions that copy a column of ``comments`` or ``events``: the new target kind, event kind and signatures.

    A session's own event (``worker.wrap_up``) takes the new target kind as its prefix, and a role's event
    (``worker.started``) the new role name. A signature in a payload (``"worker#12"``) takes the new role name too, and
    a payload's ``worker`` key (the session that ``run.relaunched`` relaunched) is its ``agent_session`` key.
    """
    ((old, new),) = kinds.items()
    ((was, role),) = roles.items()
    events = ', '.join(f"'{was}.{what}', '{new}.{what}'" for what in ROLE_EVENTS)

    def source(column: str) -> str:
        if column == 'target_kind':
            return f"CASE target_kind WHEN '{old}' THEN '{new}' ELSE target_kind END"
        if column == 'kind':
            return (
                f"CASE WHEN kind IN ({events}) THEN '{role}.' || substr(kind, instr(kind, '.') + 1) "
                f"WHEN kind LIKE '{old}.%' THEN '{new}.' || substr(kind, {len(old) + 2}) ELSE kind END"
            )
        if column == 'payload':
            signed = f"""replace(payload, '"{was}#', '"{role}#')"""
            key, new_key = f"'$.{old}'", f"'$.{new}'"
            return (
                f'CASE WHEN json_type(payload, {key}) IS NULL THEN {signed} ELSE '
                f'json_remove(json_set({signed}, {new_key}, json_extract(payload, {key})), {key}) END'
            )
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


def _rows(roles: dict[str, str]) -> None:
    """Give the signatures of the rows the new role name: every ``<role>#<id>``.

    It runs while the table is ``agent_sessions``, and while no trigger is installed, so no ``updated_at`` moves.
    """
    for old, new in roles.items():
        for name, column in ACTORS:
            op.execute(
                f"UPDATE {name} SET {column} = '{new}#' || substr({column}, {len(old) + 2}) WHERE {column} LIKE '{old}#%'"
            )


def _configs(names: dict[str, str], roles: dict[str, str]) -> None:
    """Rename the table in the queries of every stored config, and the role's table (``roles.<role>``)."""

    def one(token: re.Match[str]) -> str:
        word = token.group()
        quoted = word[0] in '"`['
        new = names.get((word[1:-1] if quoted else word).lower())
        if new is None:  # also every string literal and comment: it is matched whole, so it is no name
            return word
        return f'{word[0]}{new}{word[-1]}' if quoted else new

    bind = op.get_bind()
    for row_id, stored in bind.execute(text('SELECT id, config FROM campaigns')).all():
        config = json.loads(stored)
        table = config.get('roles') or {}
        for old, new in roles.items():
            if old in table and new not in table:
                table[new] = table.pop(old)
        for role in table.values():
            if role.get('takes') is not None:
                role['takes'] = TOKEN.sub(one, role['takes'])
            for trigger in role.get('triggers') or ():
                if trigger.get('kind') == 'sql':
                    trigger['when'] = TOKEN.sub(one, trigger['when'])
        bind.execute(text('UPDATE campaigns SET config = :c WHERE id = :id'), {'c': json.dumps(config), 'id': row_id})
