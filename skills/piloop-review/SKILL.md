---
name: piloop-review
description: Review a piloop campaign during or after a run to check if the gains are real, if an agent gamed the evaluation, where time and tokens went, and which failures are piloop bugs, then turn the findings into bug reports, PRs and inbox items. Use when the human asks to review, audit, post-mortem or look back at a campaign, or asks if a result can be trusted.
---

# Review a piloop campaign

A review answers four questions, in this order:

1. **Is the result real?**
2. **Did an agent cheat?**
3. **What did it cost?**
4. **Who is responsible for each failure?**

Then it files fixes: bug reports, PRs and inbox items.

Rules for the whole review:

- Do not change the live system until the review ends. Post the inbox items only at the end (see Output).
- Open the database read-only, with `?mode=ro`. If that fails with "unable to open database file" while the daemon
  is stopped, use `?immutable=1` instead. Use `immutable=1` only when nothing writes the database.
- Never prompt a running agent.
- Never edit the working tree of a campaign repo.
- Never change config.
- **Verify before you claim.** Every number comes from a query, a diff or a transcript line that you can cite:
  `path:line`, `path @ timestamp`, or a database row id. An agent's own report is a claim to check, not evidence. This
  includes the orchestrator's final status.
- **Measured or inferred.** Tag each finding **M** (you ran the query or read the line) or **I** (you reasoned to it).
  Dollar estimates for killed jobs, idle slot-hours and "would have" counterfactuals are I. End the report with both
  lists.

## Where the data is

| What | Where |
|---|---|
| Coordination state | `sqlite3 'file:<PILOOP_HOME>/piloop.db?mode=ro'` (default `~/.piloop`); schema at https://piloop.autoresearch.inc/reference/schema (or `piloop schema`) |
| Daemon log | `<PILOOP_HOME>/serve.log` (OTEL spans on stderr) |
| Briefs the agents got | `<PILOOP_HOME>/runs/<run>/brief.md`, `<PILOOP_HOME>/agent_sessions/<agent-session>/brief.md` |
| Agent output | `<PILOOP_HOME>/agent_sessions/<agent-session>/transcript.txt`; `runs.log_dir` |
| Agent session file | `piloop cas get <agent_sessions.session_file>` prints it, for an agent on any machine |
| Orchestrator | the harness that ran `piloop-operate`; its writes are signed `orchestrator`. pi: `~/.pi/agent/sessions/--<cwd>--/`, subagents in `~/.pi/agent/pi-subagents/` and `run-history.jsonl`. Claude Code: `$CLAUDE_CONFIG_DIR/projects/<cwd, non-alphanumerics as ->/*.jsonl`, subagents under `<session>/subagents/`. Codex: `$CODEX_HOME/sessions/YYYY/MM/DD/rollout-*.jsonl` |
| What agents left in `./workspace/` | `<PILOOP_HOME>/projects/<id>/workspace`, once each run ends: a running agent writes its own copy, and conflicts are `agent_session.workspace_conflict` events. A settled idea's files (`subject.settled`), and what a stopped campaign left (except `NOTES.md` and `research/`), are archived (`archives`) and removed from the workspace later (`archives.pruned_at`): `piloop restore <campaign> [idea#<id>\|<path>]` writes them back |
| Candidates | the campaign repo: `refs/piloop/<campaign>/r<run>/<n>`, `git show <revision>` |
| Side effects outside piloop | `$CLAUDE_CONFIG_DIR/settings.json`, `$CODEX_HOME/config.toml`, `$CLAUDE_CONFIG_DIR/projects/*/memory/` |

Token counts:

- `input` (uncached), `cache_read` and `cache_write` mean the same for every harness, so they compare.
- `agent_sessions.usage.total` is the harness's own count. Claude Code: input + output + cache read + cache write.
- Codex records no price.

## Structure: fan out, then combine

A campaign is too large for one context. Act as an orchestrator over read-only subagents that run in parallel.

Give each subagent the same rubric:

- Answer the four questions.
- Cite evidence.
- Tag findings M or I.
- Write the full report to a file.
- Return 500 words or fewer.

The subagents:

1. **Database numbers.** Trajectory with the global best so far, funnel, run outcomes and wall-clock time, tokens and
   cost per role, escalations, goalpost changes, statistical checks. This subagent owns the numbers that the others
   cite.
2. **Implementers, one subagent per campaign.** Read the transcripts and diffs of the kept lineage. Run the integrity checks
   below.
3. **Orchestrator.** Its session: decisions it took for the human, briefs, status reports compared with the database,
   spend.
4. **Research and watchdog.** For each idea: where it came from and what it produced. For each seed: who filed it, if
   it was developed or rejected, what the developed ones produced. Sweeps that produced nothing. What the watchdog
   caught and missed.
5. **Evaluation design.** Is the scoring panel held out? How much does a single unit move the score? Could an agent
   read the labels?

Then combine the results yourself:

- a verdict first,
- a timeline,
- what worked (specific and fair) and what did not,
- an efficiency table,
- an attribution table,
- suggestions in priority order,
- open items for the human.

Before you trust the results:

- Check again every subagent number that the verdict depends on.
- Send deep-dive subagents on the two or three findings that matter most. Record their corrections.
- Read 20–30 traces yourself before you trust any failure category that a subagent reports.
- In each trace, note the *first* thing that went wrong. Later errors usually follow from it.

## SQL queries

`<p>` is the campaign id. `<m>` is the primary metric: `SELECT config->'$.metrics' FROM campaigns WHERE id = <p>`.

```sql
-- Trajectory: every measured candidate and the global best so far (MIN for a lower-is-better metric).
-- `keep` is relative to the bar of its run; this is the campaign-level view. Only the current evaluator's scores
-- count. References are left out, because an imported reference was never a keep.
WITH s AS (SELECT c.id, c.run_id, c.parent_id, c.verdict, c.created_at,
                  AVG(json_extract(m.metrics, '$.<m>')) AS score, COUNT(*) AS n
           FROM candidates c JOIN measurements m ON m.candidate_id = c.id
           WHERE c.campaign_id = <p> AND c.origin != 'reference' AND m.error IS NULL AND m.rung IS NULL
             AND m.evaluator_id = (SELECT evaluator_id FROM campaigns WHERE id = <p>)
           GROUP BY c.id)
SELECT *, MAX(score) OVER (ORDER BY created_at ROWS UNBOUNDED PRECEDING) AS best FROM s ORDER BY created_at;

-- Funnel
SELECT 'idea', status, COUNT(*) FROM ideas WHERE campaign_id = <p> GROUP BY status
UNION ALL SELECT 'run', r.status, COUNT(*) FROM runs r JOIN ideas i ON i.id = r.idea_id
          WHERE i.campaign_id = <p> GROUP BY r.status
UNION ALL SELECT 'candidate', coalesce(verdict, origin), COUNT(*) FROM candidates WHERE campaign_id = <p>
          GROUP BY coalesce(verdict, origin);

-- Keeps that break a guardrail (floor from the config or the brief)
SELECT c.id, c.verdict_by, json_extract(m.metrics, '$.<guard>') AS guard
FROM candidates c JOIN measurements m ON m.candidate_id = c.id
WHERE c.campaign_id = <p> AND c.verdict = 'keep' AND json_extract(m.metrics, '$.<guard>') < <floor>;

-- Selection noise: spread among the candidates of one run (SQLite has no STDEV)
SELECT run_id, COUNT(*) n, AVG(s) mean, SQRT(AVG(s*s) - AVG(s)*AVG(s)) sd, MIN(s), MAX(s)
FROM (SELECT c.run_id, json_extract(m.metrics, '$.<m>') s FROM candidates c
      JOIN measurements m ON m.candidate_id = c.id WHERE c.campaign_id = <p> AND m.error IS NULL AND m.rung IS NULL
        AND m.evaluator_id = (SELECT evaluator_id FROM campaigns WHERE id = <p>)) GROUP BY run_id HAVING n > 3;

-- Repeat measurements of one tree (double evaluation) and their cost
SELECT candidate_id, COUNT(*), SUM(seconds) FROM measurements m JOIN candidates c ON c.id = m.candidate_id
WHERE c.campaign_id = <p> GROUP BY candidate_id HAVING COUNT(*) > 1;

-- Tokens and wall-clock time per role / harness / model
SELECT kind, harness, model, COUNT(*) agents, SUM(usage->>'$.total') tokens, SUM(usage->>'$.output') output,
       ROUND(SUM(julianday(ended_at) - julianday(started_at)) * 1440) minutes,
       SUM(usage IS NULL) no_usage, SUM(state = 'killed') killed
FROM agent_sessions WHERE campaign_id = <p> GROUP BY kind, harness, model;

-- Runs left open after their implementer exited
SELECT r.id run, w.id implementer, w.ended_at, r.finished_at,
       ROUND((julianday(COALESCE(r.finished_at, 'now')) - julianday(w.ended_at)) * 1440) open_min
FROM runs r JOIN agent_sessions w ON w.run_id = r.id JOIN ideas i ON i.id = r.idea_id
WHERE i.campaign_id = <p> AND w.ended_at IS NOT NULL ORDER BY open_min DESC LIMIT 10;

-- Work after the last stop of the campaign (agents still running, measurements nobody used)
SELECT r.id, r.started_at, r.finished_at FROM runs r JOIN ideas i ON i.id = r.idea_id
WHERE i.campaign_id = <p> AND COALESCE(r.finished_at, 'now') > (SELECT MAX(at) FROM events WHERE kind = 'campaign.status'
      AND target_id = <p> AND payload->>'$.new' = 'stopped');

-- Escalations: who decided, how fast (limit to the campaign window; targets can be any kind of row).
-- A decision is on the same row as the escalation, or a reply to the escalation itself (`piloop watch`, inbox reply).
SELECT e.id, e.author, e.target_kind, e.target_id, e.created_at, d.author decided_by, d.created_at decided_at,
       substr(e.body, 1, 100)
FROM comments e LEFT JOIN comments d ON d.id = (SELECT MIN(id) FROM comments WHERE kind = 'decision' AND id > e.id
       AND ((target_kind = e.target_kind AND target_id = e.target_id)
            OR (target_kind = 'comment' AND target_id = e.id)))
WHERE e.kind = 'escalation' AND e.created_at BETWEEN '<start>' AND '<end>';

-- Lessons that the closing watchdog left, and what past campaigns on the same code tried (campaigns.repo)
SELECT id, created_at, family, status, effect, metric, body FROM lessons WHERE campaign_id = <p> ORDER BY id;
SELECT p.name, i.id, i.status, i.title FROM ideas_fts JOIN ideas i ON i.id = ideas_fts.rowid
JOIN campaigns p ON p.id = i.campaign_id
WHERE ideas_fts MATCH '<mechanism>' AND p.repo = (SELECT repo FROM campaigns WHERE id = <p>) ORDER BY bm25(ideas_fts);

-- The orchestrator's journal: what it decided and planned, in its own words
SELECT created_at, author, line FROM journal WHERE campaign_id = <p> ORDER BY id;

-- Goalposts: verdict overrides, status changes, evaluator versions
SELECT at, actor, kind, payload FROM events WHERE kind IN ('candidate.verdict', 'campaign.status')
  AND at BETWEEN '<start>' AND '<end>' AND actor != 'daemon';
SELECT id, key, created_at FROM evaluators WHERE campaign_id = <p>;
```

Orchestrator spend and cache churn:

- A pi session records the cost per turn. For Claude Code or Codex, sum the usage the same way that piloop reads an
  implementer's session into `agent_sessions.usage`.
- `jq -r 'select(.message.usage) | [.timestamp, .message.usage.cacheWrite, .message.usage.cost.total] | @tsv' <session>.jsonl`
- Sum the cost.
- Count the turns with a large `cacheWrite` after an idle gap. Each one is a full re-cache of the prompt.

## Integrity checks

Run all of them on every kept candidate. Cheating and honest overfitting both show here. Say which one it is.

- **Diffs of frozen files.**
  - For each keep: `git -C <repo> diff --stat <baseline_sha> <revision> -- <evaluate> <scorer> <references> <data>`.
  - Over `refs/piloop/`: `git log --all -- <frozen paths>`.
  - The evaluator key hashes the evaluate command, the `link` list, and the name and direction of each metric. It
    does not hash the contents of `evaluate.py` or of its data. An edited evaluator scores without any warning.
- **Reads of labels and references.**
  - Run `rg -l '<label file>|<references dir>|<per-unit ledger>'` over the session file of every agent. Read the hits.
  - In the diffs, look for record-id literals and lookup tables keyed on inputs:
    `git diff <base> <keep> | rg '<unit ids>'`.
  - Reading the per-unit correct flags of the scoring panel is oracle use, even without a training step.
- **Tuning against the evaluation.**
  - Count the evaluate calls per implementer transcript: `rg -c '<evaluate command>'`.
  - Look for grid searches, "best of N seeds", and thresholds set from a few named units.
  - Add the daemon's measurements. The total is the number of looks at the panel.
  - About 200 looks at a panel of 70 units is a screen, not a judge.
- **Concentration of the gain per unit.**
  - Use the per-unit results: the units file of the evaluate command, or its ledgers.
  - Find the share of the gain on the top 1–3 units, and how many units changed at all.
  - A large gain that changes only 2 units has an effective sample of 2.
- **Is the held-out set held out?**
  - Join its units against everything that any candidate trained on or was tuned against.
  - Report the trained and the untrained slices separately.
- **Report against measurement.**
  - Compare every number that an implementer, the research agent, the watchdog or the orchestrator reported with the
    `measurements` row it names.
  - Query again before you repeat any of them.
- **Side channels.**
  - New files under `~/.claude/projects/*/memory/` since the campaign started. They load into every later session in
    that repo.
  - Changes to the global config of a harness.
  - Repo trust prompts that an agent answered.

Statistics to apply:

- **Noise floor.** At n units and accuracy p, the standard error is √(p(1−p)/n). For 70 units at 80%, that is about
  ±4.8 points. Units from one cluster (one site, one source, one subject) are correlated: the effective n is the
  number of clusters.
- **Compare two candidates** with a paired sign test (exact McNemar test) on the units where they disagree. With 5 or
  fewer disagreements, no result is significant. With 10, you need a net gain of 8.
- **Keeping the best inflates the score by itself.** K candidates with the same true score produce a winner several
  points above the baseline. Compare the step of a new best with the spread among sibling candidates (query above).
- **Transfer is the proof.** For every promoted candidate, compute the gap Δ between the development set and the
  holdout. A step that moves the development set and not the holdout is overfitting, whatever the transcript says.

## Efficiency accounting

Make one table, measured where possible:

- orchestrator spend, and the share that is re-caching,
- the tokens and dollars of each role,
- the waste lines:
  - runs open after their implementer exited,
  - double evaluation (implementer and daemon),
  - runs after the stop,
  - research sweeps that produced nothing,
  - killed agents, and agents that never started,
  - polling loops,
  - agents blocked on credentials.

Judge spend against value. Research tokens that produced the kept ideas are not waste. Deriving the schema again in
every sweep is.

## Attribution

Give every failure one owner. Use the counterfactual: "if this were fixed, would the outcome change?"

| Owner | Typical failures |
|---|---|
| **model** | invented numbers, misread tool output, said one thing and did another, ignored the brief |
| **harness** (piloop / pi / Codex / Claude Code) | runs left open, wrong base, unenforced guardrails, messages sent into the wrong session, cache churn |
| **environment** | expired credentials, trust dialogs, quota, a remote job service down |
| **spec** (brief, eval design, orchestrator decisions) | a brief that bans training but not tuning, a panel that is not held out, an oracle left readable, a goalpost moved after seeing the result |

Separate harness faults and environment faults before you blame a model.

## Output

1. **Report.** Verdict, timeline, what worked and what did not, efficiency, attribution, suggestions, open items,
   measured and inferred lists. Use short sentences. Put the numbers first.
2. **Bug reports and PRs.** One for each harness fault.
   - Include the evidence (row ids, transcript lines) and a minimal reproduction where possible.
   - Follow the contribution rules of the repo you fix, for example one worktree per change and all checks green.
   - In the PR body, cite the review evidence and say what you verified and what you did not.
   - Do not merge.
3. **Inbox items for the human.** Decisions that only the human can make: goalposts, reverting an agent's config or
   memory writes, pausing a campaign whose evaluation is broken, keeps below a floor.
   - One item for each decision, with a recommendation and a priority.
   - Post each one as an `escalation` comment on the campaign, with a `priority`: pipe the INSERT into
     `piloop sql -` from a quoted heredoc (`<<'EOF'`), never in a double-quoted argument.

## Every review: check the surfacing policy of the inbox

At the end of every review, check what reached the human and what did not:

- escalations that the orchestrator decided alone,
- decisions that the human would have changed,
- items that reached the human but were noise.

Propose changes to what is surfaced, at what priority, and what the orchestrator may decide and log.
The review judges the surfacing policy along with the result.
