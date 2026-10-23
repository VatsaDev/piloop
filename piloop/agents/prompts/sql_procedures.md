# Working with the piloop database

You have direct SQL access to piloop's SQLite database through `piloop sql`.
Any statement is allowed except schema changes and transactions. Every statement commits on its own.
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

- A **campaign** is a git repo with a `piloop.toml`.
- **Ideas** are things worth trying. When an idea is `ready`, the scheduler creates a **run**: one attempt, on one
  harness and model. It starts an **implementer** process in a fresh git worktree at the run's base **candidate**.
  `runs.end` says how a finished run ended, and `runs.error` gives the detail. Only `finished`, `failed`, `timed_out`
  and `awaiting_answer` count as attempts: `cancelled`, `campaign_stopped`, `daemon_stopped`, `stale`, `worker_lost`,
  `role_removed` and `not_started` say that something else ended the run.
- **Idea links** (`idea_links`) relate ideas of one campaign:
  - `after`: the idea waits for its prerequisites. It stays `ready` but is not taken until all of them are `done`. Then
    it starts from the head, which has what they kept.
  - `supersedes`: the idea is a sharper version of older ideas. The daemon rejects the older ones that never ran.
- The implementer commits. Every new commit becomes a candidate: a distinct state of the code, identified by its git tree.
  A commit that reverts to a known state *is* that candidate.
- The daemon scores each new candidate with the campaign's **evaluator** into a **measurement** (metrics, or an
  error). The evaluator is the evaluate command and the metric definitions as of the last config sync. A change makes
  a new evaluator row. A candidate can have many measurements: repeats, or a newer evaluator.
- A candidate's **verdict** (`keep`, `interesting`, `discard` or `crash`) is the daemon's judgement of its mean score
  against its bar. The bar is the run's last keep, starting at its base. Anyone may override a verdict. A commit
  marked `Piloop-Eval: skip` is `unmeasured`: no judgement at all. What an implementer left uncommitted when it stopped
  is `recovered`: committed by the daemon, never measured; if the run did not finish, a new run starts from it.
- Every run that pins no base starts from the **head**: the latest *trunk keep*, which is a keep that no branch run
  made. So the trunk is a chain of keeps.
- A candidate that keeps against its bar after the head moved on (another run kept meanwhile) is not kept. It is
  `interesting`. The daemon merges it onto the head and judges the merge there (*rebase-on-keep*). A merge that
  conflicts files a seed to compose the two by hand.
- A run whose idea pins a base other than the head (`runs.branch`: from a reference, an older keep or a note) is judged
  against its own base only. Its keep is a *branch keep*, which never moves the head.
- A non-keep is not a bar. A run pinned to one (a note, a discard) starts from its tree and is judged against its
  nearest kept ancestor. It is a trunk run when that ancestor is the head.
- A `reference` is not a judgement. It is a starting point imported with `piloop candidate import`, with no run and no
  parent. The daemon measures it. An idea can pin it as `base_candidate_id`. It is never the head and never counts as a
  keep.
- A campaign can have several **tracks** (objectives), each with its own primary metric. `ideas.track` names an idea's
  track. A candidate is judged on its track's primary. The primary of every other track is one more guardrail for it,
  with its margin and floor. A campaign with one primary has one track, named after that metric.
- **Comments** attach to any row. Agents and humans talk to each other through them.
- **Events** are the append-only log of what happened.

## Operating procedures

### Propose work

- A hunch is a *seed*. A defined experiment is an *idea*.
- File a seed, when your brief says that you may, with
  `piloop idea seed <campaign> '<hunch, one line>' --from '<pointers: ids, paths, numbers>'`.
  Add `--base <candidate>` when it is about that tree.
- A seed starts with status `seed`, and research develops it first. Research reads the existing work and checks for
  duplicates and earlier negative results. Then it either defines the seed or rejects it with a `note` that names the
  work that settled it.
- To define a seed: `piloop idea develop <id> --hypothesis … --evidence … --check … --metric … --effect …`. This makes
  it `ready`.
- An agent whose brief says so, the orchestrator and humans can also add defined ideas directly:
  `piloop idea add <campaign> '<title>' --hypothesis … --plan … --family <mechanism> --from '<the evidence, with ids>'`.
  The idea is `ready` ("schedule me"), or `proposed` with `--proposed` ("a human's approval first").
- Both commands sign `created_by` with your actor and store `--from` in `provenance`. The full commands are under
  Context.
- Priority 0 is urgent. 4 is someday.
- Titles are unique per campaign. A UNIQUE error means someone proposed it first. Read their idea and extend it with a
  `note` comment instead.
- What each source's ideas came to (keeps, not ideas) is in the overview's "Ideas by source" line.

### Link ideas

- Link a new idea when you write it: `--after <n>` and `--supersedes <n>` on `piloop idea add` or `piloop idea
  develop`. The idea and its links are written together.
  - Never add a `ready` idea and then insert its `after` link: the daemon can start the idea in between.
- `after` makes idea `new` wait until idea n is `done`.
  - Give one `--after` per prerequisite. All of them must be done.
  - Then `new` starts from the head like any other run. A prerequisite's branch keep is reached only by a pin.
  - Meanwhile, `new` stays `ready`, and `piloop status` shows it as `blocked by idea#n (<status>)`.
  - A prerequisite that ends `rejected` or `parked` blocks until someone parks the dependent idea or deletes the link:
    `piloop idea unlink <new> after <n> --reason '<why>'`.
- To link an idea that exists already: `piloop idea link <new> after <n> --reason '<why>'`. The link is written in one
  transaction, so the daemon never takes the idea without it. It is refused while the idea's run is open (the daemon
  took it already), and when it would close a cycle.
- `supersedes` says that `new` is a sharper version of n.
  - On its next tick, the daemon sets n to `rejected` if it never ran (no run yet, and still `seed`, `proposed` or
    `ready`), with a `note` on n that names `new`.
  - If n ran, the daemon leaves it alone.
- Both ideas must belong to the same campaign and must differ. A pair can have both kinds.

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
  `p.path, c.commit_sha`. Then `git -C <path> show <commit_sha>`.
- How a past idea was worked: `SELECT w.id, w.checkpoint_id, w.log_dir FROM agent_sessions w JOIN runs r ON r.id = w.run_id
  WHERE r.idea_id = <idea>`. `entire checkpoint explain <checkpoint_id>`, run in that campaign's repo
  (`campaigns.path`), summarises the session. `<log_dir>/transcript.txt` is its screen. An archived workspace can be
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
- To write one: `INSERT INTO lessons (campaign_id, family, status, effect, metric, body, created_by) VALUES (<p>,
  '<family>', 'dead', 0.002, '<metric>', '<what was tried, the evaluator that measured it, why it stopped, with ids>',
  '<you>')`. `status` is `dead`, `promising` or `untested`. `effect` is the best effect measured, in the units of
  `metric` and with its sign. Leave both NULL when none was measured. A lesson is never changed: a new conclusion is a
  new row.

Documents: campaign reports, logs and design notes (`[memory] documents` in `piloop.toml`, or inserted by an agent):

```sql
SELECT p.name AS campaign, d.id, d.path, snippet(documents_fts, 1, '[', ']', '…', 12) AS hit
FROM documents_fts JOIN documents d ON d.id = documents_fts.rowid JOIN campaigns p ON p.id = d.campaign_id
WHERE documents_fts MATCH '"clock recovery" OR acquisition' AND p.repo = (SELECT repo FROM campaigns WHERE id = 1)
ORDER BY bm25(documents_fts) LIMIT 10;
```

- `SELECT body FROM documents WHERE id = <n>` reads one. It can be long.
- To add one: `INSERT INTO documents (campaign_id, path, title, body, hash, created_by) SELECT <p>, '<path>',
  '<title>', body, blake3(body), '<you>' FROM (SELECT '<text>' AS body)`.
- Never add a document derived from validation or test data: nothing that agents can search may show the scored
  data.

### Steer

- A comment on an idea, or on any of its runs, goes into the brief of the idea's next run.
- A `note`, `finding` or `decision` on a running run, or on the idea of one, also reaches its implementer within a tick.
  The daemon prompts the implementer with one line:
  `[piloop] decision on idea#12 by human: <first line> — full text: SELECT body FROM comments WHERE id=<n>`.
- The daemon then records `comment.delivered` on the implementer. When it cannot reach the pane, it records
  `comment.undelivered` with the error. This happens for a headless subprocess agent. The comment is still in the
  next brief.
- Nothing is delivered twice, and a daemon restart loses nothing: those two events are the record.
- So put the instruction in the first line of the body, and the reasons below it.
- Escalations are never pushed to an implementer.
- `piloop prompt <agent_session_id> '<text>'` steers any running agent with a note on the agent itself, which it gets in
  full.

### Ask for an agent

- `INSERT INTO agent_sessions (kind, campaign_id, state) VALUES ('research', <p>, 'requested')`. The daemon starts it on its
  next tick.
- Kinds: the sweeping roles that the Context lists. A role that does runs needs a run, so leave its agents to the
  scheduler.
- Optional: `harness` and `model` pin the choice. `reason` says why.

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
- `not evaluated: Piloop-Eval: skip` is the reason of an `unmeasured` commit, one that asked not to be evaluated
  (scaffolding). It has no measurement and says nothing about the idea.
- A reason that starts with `recovered:` is a `recovered` candidate: what an implementer left uncommitted when it stopped.
  It has no measurement. If its run did not finish, the idea was requeued with it as the base.
- A reason that starts with `keeps against its own bar, but the head moved on` is a keep that was rebased. The rest of
  the reason names the merged candidate or the conflict.

Roles, tracks, margins and floors are in `campaigns.config`.

Override verdicts freely, and record why. A candidate can be `interesting` without winning:
`piloop candidate verdict <c> interesting --reason '<why>'`. It signs the verdict with your actor. A keep must build
on the head, unless its run is a branch run.
The daemon never overwrites a verdict that is not its own.

### Note a lead

- To keep a result that regressed but is worth starting from in view:
  `piloop candidate note <candidate> '<why>' --follow-up '<hunch>'`. `--follow-up` can repeat.
- The result is an `interesting` whose reason starts with `note:`, plus one seed per follow-up, pinned to it. It is not
  a keep and not a bar.
- An implementer's candidates exist only after its commits are scored. So an implementer ends the commit message with a
  `Piloop-Note: <why>` trailer instead, and files no follow-ups.

### Measure again

- A candidate's score is the mean of its successful measurements under the current evaluator.
- To estimate noise, ask an implementer, or run the evaluate command yourself, and record it:
  `piloop measurement add <c> --report <file>`, with `{"metrics": {"<name>": <value>}, "seconds": <s>}` in the file.
  It counts under the current evaluator (`campaigns.evaluator_id`), with the metric names of `piloop.toml`. A `crash`,
  `discard` or `interesting` that the daemon judged is judged again with every measurement. Never insert into `measurements` by hand: nothing judges it.

### Talk

- `INSERT INTO comments (target_kind, target_id, author, kind, body)`, piped into `piloop sql -`.
- Refer to other rows in the body as `idea#12`, `run#7`, `candidate#30`.
- Kinds: `note`, `finding`, `decision`, `escalation`. A lesson is a row of `lessons` ("Remember past campaigns").
- Every comment also inserts a `comment.<kind>` event (a trigger).
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
- What waits:
  `SELECT id, priority, kind, author, target_kind, target_id, body FROM comments WHERE priority IS NOT NULL AND
  acked_at IS NULL ORDER BY priority, id`.

### Change an idea

Each command takes `--reason '<why>'`, signs the change with your actor and records it in `events`.

- Park (for now) or reject (for good): `piloop idea status <id> parked` or `rejected`. Also `ready`, `proposed`, and
  `done`, which settles an idea without a run so that the ideas after it start. The reason becomes a `note` on the
  idea.
- Start it from another candidate: `piloop idea base <id> <candidate>`, or `piloop idea base <id> head` to start from
  the head when it runs.
- Move it to another track: `piloop idea track <id> <track>`. The track must be in `piloop.toml`.
- Link it: `piloop idea link` and `piloop idea unlink` ("Link ideas" above).

### Pause a campaign

- `piloop pause <campaign> --reason '<why>'` launches nothing more: no run, no sweep, no requested agent. Running work
  finishes and is scored. It raises no escalation. A sweep that runs already files its ideas as `proposed`.
- The daemon still measures what a new evaluator or a request needs. `--evals` holds that too.
- No watchdog starts for an anomaly while paused; it gets the anomaly after the resume. Paused time does not count
  toward `[stop] max_hours`.
- `piloop resume <campaign>` ends it. `campaigns.paused` and `paused_reason` show it, and `campaign.paused` events log it.
- Never pause with a `[spend]` command that reads a file: the `[spend]` hold escalates each time it holds, and it
  stops every measurement.

### Stop, cancel, or resume a stopped campaign

- When a `[stop]` condition in `piloop.toml` holds, or after `piloop campaign stop <campaign> [--kill]`, the daemon:
  - sets `campaigns.status='stopped'` (`stopped_reason` says which condition, or who stopped it);
  - drops the requests and queued runs pending at that moment (their ideas go back to `ready`);
  - tells each running implementer to commit and stop, and ends it 10 minutes later (`--kill`: at once); its commits are
    scored;
  - checks the `stopped` triggers: by default, they request one closing watchdog sweep;
  - starts and files nothing more.
- Stop a campaign with `piloop campaign stop`, not with an UPDATE of `campaigns.status`: an UPDATE gets no closing sweep.
- `piloop run cancel <run> [--wrap-up]` and `piloop agent cancel <agent-session> [--wrap-up]` end one run or one agent,
  with every process that it started. A cancelled run's idea is parked.
- To continue, raise or change the conditions in `piloop.toml` and run `piloop init <path>`.
- Or by hand: `UPDATE campaigns SET status='active', stopped_reason=NULL WHERE id=<p>`. With the conditions unchanged, it
  stops again on the next tick.

### Spend

- `agent_sessions.usage` holds the tokens an agent spent. It is written when the agent ends:
  `{"input", "output", "cache_read", "cache_write", "total", "turns", "source"}`.
  - `total` is the harness's own count of the session's tokens.
  - `input` is the uncached input for every harness; `cache_read` and `cache_write` are the cached input.
  - `turns` is the number of model responses.
  - `source` is the session file that was read.
- It is NULL while the agent runs, or when no file was found.
- `json_extract(usage, '$.total')` sums the spend per idea (agent_sessions → runs → ideas) or per campaign. The queries below do
  both.

### What not to change

- Never change `runs.status` or `agent_sessions` rows, except to insert a request. The daemon owns those. Only piloop
  writes `events`: an insert is refused.
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
  `verdict_reason`), `run.brief` (lengths only), `campaign.status` (with `stopped_reason`), `campaign.paused` (with
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

-- the head: the latest trunk keep (a keep no branch run made)
SELECT c.id, c.commit_sha, c.summary FROM candidates c LEFT JOIN runs r ON r.id = c.run_id
WHERE c.campaign_id = 1 AND c.verdict = 'keep' AND (r.id IS NULL OR NOT r.branch) ORDER BY c.id DESC LIMIT 1;

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

-- the kept lineage
SELECT id, parent_id, run_id, summary FROM candidates WHERE campaign_id = 1 AND verdict = 'keep' ORDER BY id;

-- tokens per idea: every implementer run of it, summed (NULL usage counts nothing)
SELECT i.id, i.title, SUM(json_extract(w.usage, '$.total')) AS tokens, COUNT(w.id) AS agents
FROM ideas i JOIN runs r ON r.idea_id = i.id JOIN agent_sessions w ON w.run_id = r.id
WHERE i.campaign_id = 1 GROUP BY i.id ORDER BY tokens DESC;

-- tokens per role, every agent of the campaign that ended (filter by campaign: that is what the index serves)
SELECT kind, SUM(json_extract(usage, '$.total')) AS tokens, COUNT(*) AS agents
FROM agent_sessions WHERE campaign_id = 1 AND ended_at IS NOT NULL GROUP BY kind;
```
