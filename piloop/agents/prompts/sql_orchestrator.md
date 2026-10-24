## The orchestrator's procedures

These are for the orchestrator and the human. The agents' briefs leave them out.

### Link ideas that exist

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

### Change an idea

Each command takes `--reason '<why>'`, signs the change with your actor and records it in `events`.

- Park (for now) or reject (for good): `piloop idea status <id> parked` or `rejected`. Also `ready`, `proposed`, and
  `done`, which settles an idea without a run so that the ideas after it start. The reason becomes a `note` on the
  idea.
- Start it from another candidate: `piloop idea base <id> <candidate>`, or `piloop idea base <id> head` to start from
  the head when it runs.
- Move it to another track: `piloop idea track <id> <track>`. The track must be in `piloop.toml`.

### Add a document

- `INSERT INTO documents (campaign_id, path, title, body, hash, created_by) SELECT <p>, '<path>', '<title>', body,
  blake3(body), '<you>' FROM (SELECT '<text>' AS body)`.
- Never add a document derived from validation or test data: nothing that agents can search may show the scored
  data.

### Steer

- A `note`, `finding` or `decision` on a running run, or on the idea of one, also reaches its implementer within a tick.
  The daemon prompts the implementer with one line:
  `[piloop] decision on idea#12 by human: <first line> — full text: SELECT body FROM comments WHERE id=<n>`.
- The daemon then records `comment.queued` on the implementer, and `comment.delivered` once the implementer read it.
  An implementer reads it only between two tool calls. When the prompt cannot reach the implementer, or the
  implementer ends before it read it, the daemon records `comment.undelivered` with the error. The comment is still in
  the next brief.
- Nothing is sent twice, and a daemon restart loses nothing: those events are the record.
- So put the instruction in the first line of the body, and the reasons below it.
- Escalations are never pushed to an implementer.
- `piloop prompt <agent_session_id> '<text>'` steers any running agent with a note on the agent itself, which it gets in
  full.

### Ask for an agent

- `INSERT INTO agent_sessions (kind, campaign_id, state) VALUES ('research', <p>, 'requested')`. The daemon starts it on its
  next tick.
- Kinds: the sweeping roles of `piloop.toml`. A role that does runs needs a run, so leave its agents to the
  scheduler.
- Optional: `harness` and `model` pin the choice. `reason` says why.

### Override a verdict

A candidate can be `interesting` without winning: `piloop candidate verdict <c> interesting --reason '<why>'`. It signs
the verdict with your actor. A keep must build on the head, unless its run is a branch run. The daemon never
overwrites a verdict that is not its own.

### Note a lead

- To keep a result that regressed but is worth starting from in view:
  `piloop candidate note <candidate> '<why>' --follow-up '<hunch>'`. `--follow-up` can repeat.
- The result is an `interesting` with `cause = 'note'`, plus one seed per follow-up, pinned to it. It is not a keep
  and not a bar.

### Measure again

- A candidate's score is the mean of its successful measurements under the current evaluator.
- To estimate noise, ask an implementer, or run the evaluate command yourself, and record it:
  `piloop measurement add <c> --report <file>`, with `{"metrics": {"<name>": <value>}, "seconds": <s>}` in the file.
  It counts under the current evaluator (`campaigns.evaluator_id`), with the metric names of `piloop.toml`. A `crash`,
  `discard` or `interesting` that the daemon judged is judged again with every measurement.
- Never insert into `measurements` by hand: nothing judges it.

### Pause a campaign

- `piloop pause <campaign> --reason '<why>'` launches nothing more: no run, no sweep, no requested agent. Running work
  finishes and is scored. It raises no escalation. A sweep that runs already files its ideas as `proposed`.
- The daemon still measures what a new evaluator or a request needs. `--evals` holds that too.
- No watchdog starts for an anomaly while paused; it gets the anomaly after the resume. Paused time does not count
  toward `max_hours`.
- `piloop resume <campaign>` ends it. `campaigns.paused` and `paused_reason` show it, and `campaign.paused` events log it.
- Never pause with a `[spend]` command that reads a file: the `[spend]` hold escalates each time it holds, and it
  stops every measurement.

### Stop, cancel, or resume a stopped campaign

- When a limit in `[plugins.stop_rule]` of `piloop.toml` is reached, or after `piloop campaign stop <campaign> [--kill]`, the daemon:
  - sets `campaigns.status='stopped'` (`stopped_reason` says which condition, or who stopped it);
  - drops the requests and queued runs pending at that moment (their ideas go back to `ready`);
  - tells each running implementer to commit and stop, and ends it 10 minutes later (`--kill`: at once); its commits are
    scored;
  - checks the `stopped` triggers once those commits are judged: by default, they request one closing watchdog sweep;
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

```sql
-- tokens per idea: every implementer run of it, summed (NULL usage counts nothing)
SELECT i.id, i.title, SUM(json_extract(w.usage, '$.total')) AS tokens, COUNT(w.id) AS agents
FROM ideas i JOIN runs r ON r.idea_id = i.id JOIN agent_sessions w ON w.run_id = r.id
WHERE i.campaign_id = 1 GROUP BY i.id ORDER BY tokens DESC;

-- tokens per role, every agent of the campaign that ended (filter by campaign: that is what the index serves)
SELECT kind, SUM(json_extract(usage, '$.total')) AS tokens, COUNT(*) AS agents
FROM agent_sessions WHERE campaign_id = 1 AND ended_at IS NOT NULL GROUP BY kind;
```
