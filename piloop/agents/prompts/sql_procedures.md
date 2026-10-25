# Working with the piloop database

You have direct SQL access to piloop's SQLite database through `piloop sql`.
Any statement is allowed except schema changes and transactions. Each statement is a transaction of its own.
`PRAGMA table_info(<table>)` works.
The database is the coordination layer: the daemon, the orchestrator, the watchdog and every implementer read and write the
same tables.
Timestamps are UTC ISO-8601 strings.

Never put your own text inside a double-quoted shell argument: pipe it in. In double quotes, the shell runs what is in
backticks and `$(…)`, and expands `$VAR`, before piloop sees the text. `piloop sql -` reads the statement from stdin,
and a quoted heredoc (`<<'EOF'`, with the quotes) expands nothing:

```sh
piloop sql - <<'EOF'
INSERT INTO comments (target_kind, target_id, author, kind, body)
VALUES ('idea', 12, '<you>', 'note', 'Run `make bench` first: it''s the slow step.');
EOF
piloop idea status 12 parked --reason 'waits for idea#9, see comment#31'
```

- In SQL, double each `'` of your text (`it''s`), as in any SQL string.
- Give other commands short values in single quotes, without a `'` in them. Write anything longer (Markdown, quoted
  commands) as a comment through `piloop sql -`, and point to it (`see comment#31`).

## The model

- A **campaign** is {vcs:repo} with a `piloop.toml`.
- **Ideas** are things worth trying. When an idea is `ready`, the scheduler creates a **run**: one attempt, on one
  harness and model. It starts an **implementer** process in {vcs:checkout} at the run's base **candidate**.
  `runs.end` says how a finished run ended, and `runs.error` gives the detail. Only `finished`, `failed`, `timed_out`
  and `awaiting_answer` count as attempts: `cancelled`, `campaign_stopped`, `daemon_stopped`, `stale`, `worker_lost`,
  `role_removed` and `not_started` say that something else ended the run.
- **Idea links** (`idea_links`) relate ideas of one campaign:
  - `after`: the idea waits for its prerequisites. It stays `ready` but is not taken until all of them are `done`. Then
    it starts from the head, which has what they kept.
  - `supersedes`: the idea is a sharper version of older ideas. The daemon rejects the older ones that never ran.
- {vcs:versions}
- The daemon scores each new candidate with the campaign's **evaluator** into a **measurement** (metrics, or an
  error). The evaluator is the evaluate command and the metric definitions as of the last config sync. A change makes
  a new evaluator row. A candidate can have many measurements: repeats, or a newer evaluator.
- A candidate's **verdict** (`keep`, `interesting`, `discard` or `crash`) is the daemon's judgement of its mean score
  against its bar. The bar is the run's last keep, starting at its base. The orchestrator or a human may override it.
  `verdict IS NULL` until the candidate is judged.
- A candidate's **origin** says where it comes from: `baseline`, `run`, `reference`, `recovered` or `skipped`. Only
  the first two are judged. A pending candidate is `verdict IS NULL AND origin IN ('baseline', 'run')`.
  {vcs:skip}
- Every run that pins no base starts from the **head**: the latest *trunk keep*, which is a keep that no branch run
  made. So the trunk is a chain of keeps.
- A candidate that keeps against its bar after the head moved on (another run kept meanwhile) is not kept. It is
  `interesting`. The daemon merges it onto the head and judges the merge there (*rebase-on-keep*). A merge that
  conflicts files a seed to compose the two by hand.
- A run whose idea pins a base other than the head (`runs.branch`: from a reference, an older keep or a note) is judged
  against its own base only. Its keep is a *branch keep*, which never moves the head.
- A non-keep is not a bar. A run pinned to one (a note, a discard) starts from its tree and is judged against its
  nearest kept ancestor. It is a trunk run when that ancestor is the head.
- A `reference` (`origin = 'reference'`) is a starting point imported with `piloop candidate import`, with no run and no
  parent. The daemon measures it. An idea can pin it as `base_candidate_id`. It is never the head and never counts as a
  keep.
- A campaign can have several **tracks** (objectives), each with its own primary metric. `ideas.track` names an idea's
  track. A candidate is judged on its track's primary. The primary of every other track is one more guardrail for it,
  with its margin and floor. A campaign with one primary has one track, named after that metric.
- **Comments** attach to any row. Agents and humans talk to each other through them.
- **Events** are the append-only log of what happened.

## Operating procedures

### Link ideas

- Link a new idea when you write it: `--after <n>` and `--supersedes <n>` on `piloop idea add` or `piloop idea
  develop`. The idea and its links are written together.
- Never add a `ready` idea and then insert its `after` link: the daemon can start the idea in between.

### Search

- `ideas_fts` (over title, hypothesis and plan), `comments_fts` (over body) and `documents_fts` (over title and
  body) are full-text indexes of every row. Triggers keep them current. Read them. Never write them.
- "Has anyone tried soft voting" is one query. Run it before you propose, whatever the history table shows.
- Words are stemmed (vote = votes = voting) and accents are dropped.

Ideas by mechanism, best match first, with the matching words marked:

```sql
SELECT i.id, i.status, snippet(ideas_fts, -1, '[', ']', '…', 8) AS hit, bm25(ideas_fts) AS score
FROM ideas_fts JOIN ideas i ON i.id = ideas_fts.rowid
WHERE ideas_fts MATCH 'voting OR ensemble' AND i.campaign_id = 1 ORDER BY score LIMIT 10;
```

Comments (findings, decisions, notes) that use a phrase, newest first:

```sql
SELECT c.target_kind, c.target_id, c.author, c.kind, snippet(comments_fts, 0, '[', ']', '…', 8) AS hit
FROM comments_fts JOIN comments c ON c.id = comments_fts.rowid
WHERE comments_fts MATCH '"target encoding"' ORDER BY c.id DESC LIMIT 10;
```

The MATCH string is FTS5 syntax, not free text:

- Words are combined with AND.
- `OR` is an operator. Write it in uppercase.
- `"double quotes"` make a phrase. Quotes are required around anything with punctuation (`"one-hot"`, `"k-fold"`).
  Without quotes, the hyphen reads as a column filter and the query fails with `no such column`.
- `ensem*` matches by prefix.
- To exclude a word, use the binary `voting NOT soft`, not a leading `-`.
- Abbreviations are separate words, so ask for both spellings: `HGBT OR "gradient boosting"`, `"k-NN" OR knn`.
- A lower `bm25()` is a better match. A term that almost every row has scores `-0.0` for all of them, because it
  carries no information. Read the snippets instead.
- Join the base table for anything except the text.

### Remember past campaigns

Every campaign that ever ran is in this database. `campaigns.repo` names the code that each campaign worked on, the
same for every clone. Before you propose or build a mechanism, look at what came of it before.

Prior attempts on this repo, in every campaign, with what they came to and the primary metric that judged them. A
discard under one campaign's evaluator is not dead under another's: read the idea's comments before you call it
settled.

```sql
SELECT p.name AS campaign, i.id, i.status, i.family, i.title,
  (SELECT group_concat(DISTINCT c.verdict) FROM runs r JOIN candidates c ON c.run_id = r.id WHERE r.idea_id = i.id) AS verdicts,
  (SELECT group_concat(json_extract(m.value, '$.name')) FROM json_each(p.config, '$.metrics') m
   WHERE json_extract(m.value, '$.role') = 'primary') AS judged_on
FROM ideas_fts JOIN ideas i ON i.id = ideas_fts.rowid JOIN campaigns p ON p.id = i.campaign_id
WHERE ideas_fts MATCH '"sync acquisition" OR onset' AND p.repo = (SELECT repo FROM campaigns WHERE id = 1)
ORDER BY bm25(ideas_fts) LIMIT 20;
```

- An implementation to copy, from any campaign: drop the `p.repo` filter, keep what kept or was noted (`JOIN runs r ON
  r.idea_id = i.id JOIN candidates c ON c.run_id = r.id WHERE c.verdict IN ('keep', 'interesting')`) and select
  `p.path, c.revision`. {vcs:show}
- How a past idea was worked: `SELECT w.id, json_extract(w.usage, '$.source'), w.log_dir FROM agent_sessions w JOIN
  runs r ON r.id = w.run_id WHERE r.idea_id = <idea>`. The source is the harness's own file of the session.
  `<log_dir>/transcript.txt` is its screen. An archived workspace can be
  restored with `piloop restore`, once your piloop has it.

Lessons: a row of `lessons` is the standing conclusion about one mechanism family, written when a campaign stops.
The newest for each (campaign, family) is the current one. What is dead or promising on this repo:

```sql
SELECT p.name AS campaign, l.id, l.family, l.status, l.effect, l.metric
FROM lessons l JOIN campaigns p ON p.id = l.campaign_id
WHERE p.repo = (SELECT repo FROM campaigns WHERE id = 1)
  AND l.id = (SELECT MAX(n.id) FROM lessons n WHERE n.campaign_id = l.campaign_id AND lower(n.family) = lower(l.family))
ORDER BY l.status, l.family;
```

- `SELECT body FROM lessons WHERE id = <n>` reads why, with the ids it cites.

Documents: campaign reports, logs and design notes (`[memory] documents` in `piloop.toml`):

```sql
SELECT p.name AS campaign, d.id, d.path, snippet(documents_fts, 1, '[', ']', '…', 12) AS hit
FROM documents_fts JOIN documents d ON d.id = documents_fts.rowid JOIN campaigns p ON p.id = d.campaign_id
WHERE documents_fts MATCH '"clock recovery" OR acquisition' AND p.repo = (SELECT repo FROM campaigns WHERE id = 1)
ORDER BY bm25(documents_fts) LIMIT 10;
```

- `SELECT body FROM documents WHERE id = <n>` reads one. It can be long.

### Judge results

The daemon judges every new candidate against the bar on its track. The bar is the last kept candidate of the run,
starting at its base. The daemon writes why in `verdict_reason`:

- `keep`: the track's primary beat the bar with confidence, and every guardrail (other tracks' primaries included)
  stayed within its margin.
- `interesting`: the primary is up but not significantly, or a guardrail regressed beyond its margin.
- `discard`: the primary is not up, or a guardrail passed its floor.
- `crash`: every measurement failed.

Special reasons:

- A reason that starts with `no-op:` means the primary and the guardrails equal the parent's exactly.
- `cause = 'moved'` is a keep that was rebased: it kept against its own bar, but the head moved on. The reason names
  the merged candidate or the conflict.
- `cause = 'note'` is a regressed result that an agent kept in view as a lead.
- A `skipped` version asked not to be evaluated (scaffolding). It has no measurement and says nothing about the idea.

Roles, tracks, margins and floors are in `campaigns.config`.

### Talk

- `INSERT INTO comments (target_kind, target_id, author, kind, body)`, piped into `piloop sql -`.
- Refer to other rows in the body as `idea#12`, `run#7`, `candidate#30`.
- Kinds: `note`, `finding`, `decision`, `escalation`. A lesson is a row of `lessons` ("Remember past campaigns").
- Every comment also inserts a `comment.<kind>` event (a trigger).
- A comment on an idea, or on any of its runs, goes into the brief of the idea's next run.
- A brief shows other comments by id and first line, so put the gist in the first line. `SELECT body FROM comments
  WHERE id = <n>` reads the full text.
- `comment.escalation` reaches the human through the orchestrator. Use `escalation` only when a human decision is
  needed.

### The inbox

- A comment with a `priority` (0 urgent … 4 someday, as on ideas) is addressed to the human. It waits in their inbox
  until someone acknowledges it (`acked_at`, `acked_by`).
- A comment without a priority is for the forum.
- An escalation must have a priority. A CHECK constraint refuses it otherwise:
  `INSERT INTO comments (target_kind, target_id, author, kind, body, priority) VALUES ('run', 7, '<you>', 'escalation',
  '<what happened, the evidence, the one question>', 1)`.
- Priority 0–1 is pushed to the human as soon as it lands. Use it only for what cannot wait. 2–4 waits to be read.
- Nothing waits for the human's answer. Decide what is yours to decide, and leave the rest undone.

### What not to change

- Never change `runs`, `measurements` or `agent_sessions` rows. The daemon owns those. Only piloop writes `events`: an
  insert is refused.
- If they look wrong, write a comment of kind `escalation` on the run. The orchestrator reconciles.

### Provenance is automatic

- Every table except `events` has `updated_at` and `updated_by`.
- Triggers set both on every UPDATE. `updated_by` is set to whoever holds the connection (`human`, `orchestrator`,
  `daemon`, `research#12` …), so a value you write there is overwritten.
- Never set them by hand. Read them to see who changed a row and when. `updated_by IS NULL` means the row is unchanged
  since its insert.
- The same triggers keep a change log in `events`, signed by the same actor. Each payload has its `kind`, and these have
  `old` and `new`:
  `idea.status`, `idea.priority`, `idea.track`, `idea.base_candidate_id`, `candidate.verdict` (with `verdict_by` and
  `verdict_reason`), `candidate.origin`, `run.brief_cid` (the old and new brief's CID), `campaign.status` (with `stopped_reason`), `campaign.paused` (with
  `paused_reason`). An inserted or deleted `idea_links` row is `idea.linked` or `idea.unlinked` on its idea, with the
  target and the kind.
- A DELETE of an idea, comment or candidate leaves a `<kind>.deleted` event with the row's key columns.
- So `SELECT at, actor, kind, payload FROM events WHERE target_kind = 'idea' AND target_id = 12 ORDER BY id` is the full
  history of an idea.
- The daemon's own `candidate.judged` event is next to `candidate.verdict`. It has the score that the verdict was made
  from.


## Useful queries

```sql
-- a candidate's score under the campaign's current evaluator (mean over successful measurements)
SELECT c.id, c.verdict, c.summary, AVG(json_extract(m.metrics, '$.accuracy')) AS accuracy, COUNT(m.id) AS n
FROM candidates c JOIN campaigns p ON p.id = c.campaign_id
LEFT JOIN measurements m ON m.candidate_id = c.id AND m.evaluator_id = p.evaluator_id AND m.error IS NULL
WHERE c.campaign_id = 1 GROUP BY c.id ORDER BY c.id DESC LIMIT 20;

-- what is running right now
SELECT r.id, i.title, r.harness, r.model, r.started_at FROM runs r JOIN ideas i ON i.id = r.idea_id
WHERE r.status = 'running';

-- the queue: ready ideas the scheduler may take (filter by campaign, which the index serves; every 'after'
-- prerequisite must be done)
SELECT i.id, i.priority, i.title FROM ideas i WHERE i.campaign_id = 1 AND i.status = 'ready' AND NOT EXISTS (
  SELECT 1 FROM idea_links l JOIN ideas p ON p.id = l.target_id
  WHERE l.idea_id = i.id AND l.kind = 'after' AND p.status != 'done') ORDER BY i.priority, i.id;

-- ready ideas that wait on a prerequisite, and what state it is in
SELECT i.id, i.title, p.id AS prerequisite, p.status AS prerequisite_status
FROM ideas i JOIN idea_links l ON l.idea_id = i.id AND l.kind = 'after' JOIN ideas p ON p.id = l.target_id
WHERE i.campaign_id = 1 AND i.status = 'ready' AND p.status != 'done';

-- everything said about idea 12
SELECT author, kind, body, created_at FROM comments WHERE target_kind = 'idea' AND target_id = 12 ORDER BY id;
```
