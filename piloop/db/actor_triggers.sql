-- WARNING: This file is auto-generated (uv run -m scripts.generate_all). Update the triggers in 'piloop/db/triggers/' if you want to change anything here.
-- Who changed each row, for this connection only. piloop.db.sql.install_actor runs this file, then inserts the
-- connection's actor into session_actor. A TEMP trigger can update main tables and read temp ones, so every UPDATE
-- through this connection sets updated_by, and the caller does not need to name itself. The WHEN clause stops the
-- trigger's own UPDATE from firing it again, and replaces any actor that a statement tries to set.
-- WITHOUT ROWID keeps the actor row out of last_insert_rowid, which clients report for the caller's statement.
-- agent_session is the agent session of an agent's connection, and NULL for every other actor.
CREATE TEMP TABLE session_actor (actor TEXT NOT NULL PRIMARY KEY, agent_session INTEGER) WITHOUT ROWID;
CREATE TEMP TRIGGER campaigns_updated_by AFTER UPDATE ON campaigns
WHEN NEW.updated_by IS NOT (SELECT actor FROM temp.session_actor)
BEGIN
  UPDATE campaigns SET updated_by = (SELECT actor FROM temp.session_actor) WHERE id = NEW.id;
END;
CREATE TEMP TRIGGER ideas_updated_by AFTER UPDATE ON ideas
WHEN NEW.updated_by IS NOT (SELECT actor FROM temp.session_actor)
BEGIN
  UPDATE ideas SET updated_by = (SELECT actor FROM temp.session_actor) WHERE id = NEW.id;
END;
CREATE TEMP TRIGGER runs_updated_by AFTER UPDATE ON runs
WHEN NEW.updated_by IS NOT (SELECT actor FROM temp.session_actor)
BEGIN
  UPDATE runs SET updated_by = (SELECT actor FROM temp.session_actor) WHERE id = NEW.id;
END;
CREATE TEMP TRIGGER candidates_updated_by AFTER UPDATE ON candidates
WHEN NEW.updated_by IS NOT (SELECT actor FROM temp.session_actor)
BEGIN
  UPDATE candidates SET updated_by = (SELECT actor FROM temp.session_actor) WHERE id = NEW.id;
END;
CREATE TEMP TRIGGER comments_updated_by AFTER UPDATE ON comments
WHEN NEW.updated_by IS NOT (SELECT actor FROM temp.session_actor)
BEGIN
  UPDATE comments SET updated_by = (SELECT actor FROM temp.session_actor) WHERE id = NEW.id;
END;
CREATE TEMP TRIGGER agent_sessions_updated_by AFTER UPDATE ON agent_sessions
WHEN NEW.updated_by IS NOT (SELECT actor FROM temp.session_actor)
BEGIN
  UPDATE agent_sessions SET updated_by = (SELECT actor FROM temp.session_actor) WHERE id = NEW.id;
END;
CREATE TEMP TRIGGER documents_updated_by AFTER UPDATE ON documents
WHEN NEW.updated_by IS NOT (SELECT actor FROM temp.session_actor)
BEGIN
  UPDATE documents SET updated_by = (SELECT actor FROM temp.session_actor) WHERE id = NEW.id;
END;

-- The change log: decisions leave a record in the append-only events table, signed by the same actor. Every client
-- loads this file, so a raw UPDATE from the CLI or an implementer is logged like an ORM write of the daemon.
-- `AFTER UPDATE OF <col>` fires only when a statement names that column, so the updated_at and updated_by triggers
-- above (which SET their own column) never fire these. The WHEN clause ignores a SET to the value already there.
-- Event kinds are `<table singular>.<column>`. Payloads have the old and new value, never a large body.
CREATE TEMP TRIGGER ideas_status_event AFTER UPDATE OF status ON ideas
WHEN OLD.status IS NOT NEW.status
BEGIN
  INSERT INTO events (at, actor, target_kind, target_id, payload)
  VALUES (strftime('%Y-%m-%d %H:%M:%f', 'now'), (SELECT actor FROM temp.session_actor), 'idea', NEW.id, json_object('kind', 'idea.status', 'old', OLD.status, 'new', NEW.status));
END;
CREATE TEMP TRIGGER ideas_priority_event AFTER UPDATE OF priority ON ideas
WHEN OLD.priority IS NOT NEW.priority
BEGIN
  INSERT INTO events (at, actor, target_kind, target_id, payload)
  VALUES (strftime('%Y-%m-%d %H:%M:%f', 'now'), (SELECT actor FROM temp.session_actor), 'idea', NEW.id, json_object('kind', 'idea.priority', 'old', OLD.priority, 'new', NEW.priority));
END;
CREATE TEMP TRIGGER ideas_track_event AFTER UPDATE OF track ON ideas
WHEN OLD.track IS NOT NEW.track
BEGIN
  INSERT INTO events (at, actor, target_kind, target_id, payload)
  VALUES (strftime('%Y-%m-%d %H:%M:%f', 'now'), (SELECT actor FROM temp.session_actor), 'idea', NEW.id, json_object('kind', 'idea.track', 'old', OLD.track, 'new', NEW.track));
END;
CREATE TEMP TRIGGER ideas_base_candidate_id_event AFTER UPDATE OF base_candidate_id ON ideas
WHEN OLD.base_candidate_id IS NOT NEW.base_candidate_id
BEGIN
  INSERT INTO events (at, actor, target_kind, target_id, payload)
  VALUES (strftime('%Y-%m-%d %H:%M:%f', 'now'), (SELECT actor FROM temp.session_actor), 'idea', NEW.id, json_object('kind', 'idea.base_candidate_id', 'old', OLD.base_candidate_id, 'new', NEW.base_candidate_id));
END;
CREATE TEMP TRIGGER idea_links_linked_event AFTER INSERT ON idea_links
BEGIN
  INSERT INTO events (at, actor, target_kind, target_id, payload)
  VALUES (strftime('%Y-%m-%d %H:%M:%f', 'now'), (SELECT actor FROM temp.session_actor), 'idea', NEW.idea_id, json_object('kind', 'idea.linked', 'target', NEW.target_id, 'link_kind', NEW.kind));
END;
CREATE TEMP TRIGGER idea_links_unlinked_event AFTER DELETE ON idea_links
BEGIN
  INSERT INTO events (at, actor, target_kind, target_id, payload)
  VALUES (strftime('%Y-%m-%d %H:%M:%f', 'now'), (SELECT actor FROM temp.session_actor), 'idea', OLD.idea_id, json_object('kind', 'idea.unlinked', 'target', OLD.target_id, 'link_kind', OLD.kind));
END;
CREATE TEMP TRIGGER candidates_verdict_event AFTER UPDATE OF verdict ON candidates
WHEN OLD.verdict IS NOT NEW.verdict
BEGIN
  INSERT INTO events (at, actor, target_kind, target_id, payload)
  VALUES (strftime('%Y-%m-%d %H:%M:%f', 'now'), (SELECT actor FROM temp.session_actor), 'candidate', NEW.id, json_object('kind', 'candidate.verdict', 'old', OLD.verdict, 'new', NEW.verdict, 'verdict_by', NEW.verdict_by, 'verdict_reason', NEW.verdict_reason));
END;
CREATE TEMP TRIGGER candidates_origin_event AFTER UPDATE OF origin ON candidates
WHEN OLD.origin IS NOT NEW.origin
BEGIN
  INSERT INTO events (at, actor, target_kind, target_id, payload)
  VALUES (strftime('%Y-%m-%d %H:%M:%f', 'now'), (SELECT actor FROM temp.session_actor), 'candidate', NEW.id, json_object('kind', 'candidate.origin', 'old', OLD.origin, 'new', NEW.origin));
END;
CREATE TEMP TRIGGER runs_brief_event AFTER UPDATE OF brief ON runs
WHEN OLD.brief IS NOT NEW.brief AND NOT (OLD.brief IS NULL AND (SELECT actor FROM temp.session_actor) = 'daemon')
BEGIN
  INSERT INTO events (at, actor, target_kind, target_id, payload)
  VALUES (strftime('%Y-%m-%d %H:%M:%f', 'now'), (SELECT actor FROM temp.session_actor), 'run', NEW.id, json_object('kind', 'run.brief', 'old_length', length(OLD.brief), 'new_length', length(NEW.brief)));
END;
CREATE TEMP TRIGGER campaigns_status_event AFTER UPDATE OF status ON campaigns
WHEN OLD.status IS NOT NEW.status
BEGIN
  INSERT INTO events (at, actor, target_kind, target_id, payload)
  VALUES (strftime('%Y-%m-%d %H:%M:%f', 'now'), (SELECT actor FROM temp.session_actor), 'campaign', NEW.id, json_object('kind', 'campaign.status', 'old', OLD.status, 'new', NEW.status, 'stopped_reason', NEW.stopped_reason));
END;
CREATE TEMP TRIGGER campaigns_type_event AFTER UPDATE OF type ON campaigns
WHEN OLD.type IS NOT NEW.type
BEGIN
  INSERT INTO events (at, actor, target_kind, target_id, payload)
  VALUES (strftime('%Y-%m-%d %H:%M:%f', 'now'), (SELECT actor FROM temp.session_actor), 'campaign', NEW.id, json_object('kind', 'campaign.type', 'old', OLD.type, 'new', NEW.type));
END;
CREATE TEMP TRIGGER campaigns_parent_id_event AFTER UPDATE OF parent_id ON campaigns
WHEN OLD.parent_id IS NOT NEW.parent_id
BEGIN
  INSERT INTO events (at, actor, target_kind, target_id, payload)
  VALUES (strftime('%Y-%m-%d %H:%M:%f', 'now'), (SELECT actor FROM temp.session_actor), 'campaign', NEW.id, json_object('kind', 'campaign.parent_id', 'old', OLD.parent_id, 'new', NEW.parent_id));
END;
CREATE TEMP TRIGGER campaigns_paused_event AFTER UPDATE OF paused ON campaigns
WHEN OLD.paused IS NOT NEW.paused
BEGIN
  INSERT INTO events (at, actor, target_kind, target_id, payload)
  VALUES (strftime('%Y-%m-%d %H:%M:%f', 'now'), (SELECT actor FROM temp.session_actor), 'campaign', NEW.id, json_object('kind', 'campaign.paused', 'old', OLD.paused, 'new', NEW.paused, 'paused_reason', NEW.paused_reason));
END;
CREATE TEMP TRIGGER comments_acked_at_event AFTER UPDATE OF acked_at ON comments
WHEN OLD.acked_at IS NOT NEW.acked_at
BEGIN
  INSERT INTO events (at, actor, target_kind, target_id, payload)
  VALUES (strftime('%Y-%m-%d %H:%M:%f', 'now'), (SELECT actor FROM temp.session_actor), 'comment', NEW.id, json_object('kind', 'comment.acked_at', 'old', OLD.acked_at, 'new', NEW.acked_at, 'acked_by', NEW.acked_by));
END;

-- Tombstones: the key columns of a deleted row, so the feed can still say what was there. A cascade (DELETE FROM
-- campaigns) fires them for every child row that it deletes.
CREATE TEMP TRIGGER ideas_deleted_event AFTER DELETE ON ideas
BEGIN
  INSERT INTO events (at, actor, target_kind, target_id, payload)
  VALUES (strftime('%Y-%m-%d %H:%M:%f', 'now'), (SELECT actor FROM temp.session_actor), 'idea', OLD.id, json_object('kind', 'idea.deleted', 'title', OLD.title, 'status', OLD.status, 'hypothesis', OLD.hypothesis));
END;
CREATE TEMP TRIGGER comments_deleted_event AFTER DELETE ON comments
BEGIN
  INSERT INTO events (at, actor, target_kind, target_id, payload)
  VALUES (strftime('%Y-%m-%d %H:%M:%f', 'now'), (SELECT actor FROM temp.session_actor), 'comment', OLD.id, json_object('kind', 'comment.deleted', 'target_kind', OLD.target_kind, 'target_id', OLD.target_id, 'author', OLD.author, 'comment_kind', OLD.kind, 'body', OLD.body));
END;
CREATE TEMP TRIGGER candidates_deleted_event AFTER DELETE ON candidates
BEGIN
  INSERT INTO events (at, actor, target_kind, target_id, payload)
  VALUES (strftime('%Y-%m-%d %H:%M:%f', 'now'), (SELECT actor FROM temp.session_actor), 'candidate', OLD.id, json_object('kind', 'candidate.deleted', 'tree_sha', OLD.tree_sha, 'commit_sha', OLD.commit_sha, 'origin', OLD.origin, 'verdict', OLD.verdict));
END;

-- Every comment is an event, with the same shape as the event of the persistent escalation trigger. That trigger
-- stays persistent, because `comment.escalation` must be logged whatever connection wrote the comment. The rest of
-- the forum (`comment.note`, `comment.finding`, `comment.decision`) is what the daemon pushes to a running implementer.
CREATE TEMP TRIGGER comments_inserted_event AFTER INSERT ON comments
WHEN NEW.kind != 'escalation'
BEGIN
  INSERT INTO events (at, actor, target_kind, target_id, payload)
  VALUES (strftime('%Y-%m-%d %H:%M:%f', 'now'), NEW.author, 'comment', NEW.id, json_object('kind', 'comment.' || NEW.kind, 'target_kind', NEW.target_kind, 'target_id', NEW.target_id));
END;

-- An agent whose session has ended leaves no finding. session_actor holds the agent session of an agent's connection,
-- and the daemon sets the row ending in a commit before it kills the agent.
CREATE TEMP TRIGGER comments_no_late_finding BEFORE INSERT ON comments
WHEN NEW.kind = 'finding' AND EXISTS (SELECT 1 FROM agent_sessions WHERE id = (SELECT agent_session FROM temp.session_actor) AND state IN ('ending', 'exited', 'killed'))
BEGIN
  SELECT RAISE(ABORT, 'the agent session of this connection has ended: its finding is refused');
END;

-- The feed is append-only: an event takes the next id, because a reader's cursor is an event id. Its columns have the
-- types that a reader reads. The authorizer of `piloop sql` denies an UPDATE or a DELETE of events.
CREATE TEMP TRIGGER events_checked BEFORE INSERT ON events
BEGIN
  SELECT RAISE(ABORT, 'events take the next id: leave id out') WHERE NEW.id != -1;
  SELECT RAISE(ABORT, 'events.actor is text') WHERE typeof(NEW.actor) != 'text';
  SELECT RAISE(ABORT, 'events.target_id is an integer') WHERE typeof(NEW.target_id) != 'integer';
  SELECT RAISE(ABORT, 'events.at is a time in text, like 2026-10-20 08:00:00') WHERE typeof(NEW.at) != 'text' OR julianday(NEW.at) IS NULL;
  SELECT RAISE(ABORT, 'events.payload is JSON text, not a blob') WHERE typeof(NEW.payload) = 'blob';
  SELECT RAISE(ABORT, 'events.payload is a JSON object with its kind in $.kind') WHERE json_type(NEW.payload, '$.kind') IS NOT 'text';
END;
CREATE TEMP TRIGGER events_next_id AFTER INSERT ON events
WHEN NEW.id < 1
BEGIN
  SELECT RAISE(ABORT, 'events take the next id: leave id out');
END;
