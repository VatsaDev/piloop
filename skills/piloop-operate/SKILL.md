---
name: piloop-operate
description: Operate a running piloop autoresearch campaign as its orchestrator, from any harness with a shell (Claude Code, Codex, pi, Gemini CLI, Cursor), by looping on piloop inbox wait to decide escalations, manage ideas, launch sweeps, steer workers and judge candidates. Use when the human asks you to orchestrate, watch, run or report on a campaign, when an escalation arrives, or when you take a decision for the human; do not use it in an agent that piloop launched (PILOOP_ACTOR is a worker, watchdog or research id).
---

# Operate a piloop campaign

The database is the contract. Everything below is a `piloop` command in your shell, SQL included.

- Run SQL with `piloop sql "<one statement>"`.
- Read `piloop schema` once before your first write. It gives the conventions and every table.
- Run every piloop command as `PILOOP_ACTOR=orchestrator piloop …`.
  - The rows you change, the items you acknowledge and their events are then signed `orchestrator`.
  - `piloop inbox wait` then skips what you wrote yourself.
- Limit your queries to the project in the current directory. `SELECT id, name FROM projects WHERE path = '<cwd>'`
  gives `<p>` and `<name>`.

## The loop

Loop on one blocking command. It costs no turns while nothing happens:

```bash
PILOOP_ACTOR=orchestrator piloop inbox wait [<name>] --timeout 1800   # first call: from now
PILOOP_ACTOR=orchestrator piloop inbox wait [<name>] --after <cursor> --timeout 1800   # every later call
```

It returns, one line each, as soon as something needs you:

- a new unacknowledged inbox item that someone else wrote,
- a project that the daemon stopped,
- an anomaly that the daemon gave to a watchdog,
- the daemon's verdict on a measured candidate.

Otherwise it returns `nothing new` at the timeout.
Its last line is the exact command to wait again. That command has the cursor (`--after <event id>`), so you miss
nothing that arrives between two waits.

Do not let your shell tool kill the wait:

- Claude Code: run it in the background (`run_in_background`). You are woken when it exits.
- Codex and pi: set the timeout parameter of the shell tool above `--timeout`.
- When the tool limit is lower than 30 minutes, pass a `--timeout` below the limit. Use a shorter wait, never a polling
  loop of your own.

Steps:

1. **Start.** Run `piloop status` and `piloop inbox`. Items that already wait are yours too: the wait reports only
   new items.
2. **Wait.** Run the command above.
3. **Read** each line.
   - An item: `piloop inbox show <id>` shows it with its thread.
   - A stop or an anomaly: `piloop status <name>`. The watchdog investigates anomalies and escalates what needs a
     decision. Do not repeat its work.
4. **Decide and record.** See "The inbox" below: escalations, every decision, the surfacing policy.
5. **Acknowledge** what you answered: `PILOOP_ACTOR=orchestrator piloop inbox ack <id>`. Leave unacknowledged what
   only the human can decide.
6. **Act.** Apply the decision: ideas, priorities, sweeps, `piloop.toml` followed by `piloop init .`.
   Never raise `[spend] limit` yourself. A `Spend limit reached` escalation belongs to the human.
7. **Report** when the human is present, at least every few waits or every hour. Then go back to step 2.
   - Give one short status: the head against the baseline (per track when there are several), what ran, what you
     decided. `piloop status <name>` has the numbers.
   - Include the keeps by source in every digest: the "ideas by source" table at the top of `piloop history <name>`.
     This shows whether research earns its sweeps.
   - When every project you watch has stopped, report the result and stop the loop.

Run the loop yourself, or give it to a background subagent so that the human's session stays free. Give the subagent
this skill and the project name. Tell it to report decisions and status back.

Run one orchestrator per campaign. Two loops answer the same escalation twice.

## Status

`piloop status [<name>]` shows the same overview that watchdog and research sweeps get in their brief.
Without a name, it uses the project in the current directory, else the only active project, else every project.
It shows:

- the head (the latest trunk keep) and its delta over the baseline,
- one line per track when there are several: its primary at the head, the distance to its floor, its trunk keeps, its
  share against what runs,
- the trajectory of trunk keeps and the progress against `[stop]`,
- open runs and the ready queue,
- the live agents with their last line,
- the last 8 candidates with their delta against their parent,
- the anomalies that the daemon detects,
- the inbox and the latest decisions,
- the last events with their ages.

Imported references are counted separately as starting points. They are never kept.

`piloop history [<name>]` prints the research pack: metrics, ideas by source, analysis artifacts, what was tried per
mechanism family, the waiting seeds, every idea, the kept lineage, the starting points, findings and digests.

`piloop watch` shows the same as `piloop status`, live, in the human's terminal.

If the daemon is down, `requested` rows collect in `workers` and nothing runs.
Check with `SELECT id, kind, reason FROM workers WHERE state = 'requested'`.
Start the daemon with `piloop serve --detach`.

## Ideas

### Add an idea

```bash
PILOOP_ACTOR=orchestrator piloop idea add <name> "<title>" --hypothesis "…" --plan "…" --family "<mechanism>" \
  --from "<what prompted it: comment#…, a digest, a loss table>" --priority 1
```

- `--base <candidate>` sets the start. `--after` and `--supersedes` link it to other ideas.
- Priority is 0 (urgent) to 4 (someday).
- Add `--proposed` when the human should look at it first.
- Your ideas carry context that research does not have: the digests, the loss analysis, the human's direction.
  Write them down as ideas as soon as you see them. Use `--from` to say where they came from.

### File a seed

A seed is a hunch that you have no time to define: a follow-up that a result suggests, two candidates worth
combining, new data.
Send it to research as soon as it comes up:

```bash
PILOOP_ACTOR=orchestrator piloop idea seed <name> "<hunch, one line>" --from "<pointers: candidate#…, comment#…, a path>"
```

- Add `--base <c>` when the hunch is about the tree of candidate `<c>`.
- The idea gets status `seed`. It shows in `piloop inbox` and `piloop watch`.
- It starts a research sweep, however full the backlog is.
- Research reads the existing work and checks for duplicates and earlier negative results. Then it does one of these:
  - It develops the seed into a defined idea with `piloop idea develop`: hypothesis, evidence, check and stop rule,
    metric, expected effect.
  - It rejects the seed with a `note` that names the evidence that settled it.
- Watchdogs file seeds the same way, and only seeds.
- Workers file no seeds. They put a hunch in their finding.
- You may develop or reject a seed yourself when you get to it first.

A link that the human pastes (a post, a repo, a paper) is a seed signed `human`, not signed by you:

```bash
PILOOP_ACTOR=human piloop idea seed <name> "<its mechanism, one line>" --from "<link>; targets <the measured loss>"
```

### Scout for outside techniques

- This needs `[roles.research.scout]` in `piloop.toml`, with `interval` set.
- Research then also runs scout sweeps. Each one searches X, GitHub and arXiv for techniques that address a measured
  loss.
- A scout sweep files at most `seeds` seeds, signed `scout#…`. The other techniques go to the `techniques` library.
- `piloop launch <name> research --scout` asks for a scout sweep now.
- Each scout sweep leaves a priority-3 `finding` that starts with its problem statement. Read it: a scout that
  misread the problem searched for the wrong thing.
- In your digests, report the `scout` row of "ideas by source" next to the `research` row.

### Point research at the evidence

When you or a worker write an analysis of where the head loses (against the reference, per slice):

1. Add its path to `[roles.research] context` in `piloop.toml`.
2. Run `piloop init .`.

The research pack of every sweep then lists it first.

### Reprioritise, park, reject

- Reprioritise: `UPDATE ideas SET priority = 0 WHERE id = …`.
- Park (for now): `UPDATE ideas SET status = 'parked' WHERE id = …`.
- Reject (never): `UPDATE ideas SET status = 'rejected' WHERE id = …`.
- Always say why in a `note` comment on the idea.

### Steer an idea

- A comment on the idea goes into its next brief.
- A `note` or `decision` comment on the idea, or on its *running* run, goes into that worker's pane. The daemon sends
  it on the next tick, first line first, so put the instruction in the first line.
- `comment.delivered` on the worker confirms that the comment arrived.
- `piloop prompt <worker> "<text>"` sends text directly, when no comment is worth keeping.

### Start from a tree outside the lineage

Use this for a candidate of another campaign, or a branch.

- Run `piloop candidate import <name> <commit-or-ref> --summary "…"`. It registers a `reference` candidate.
  Never `INSERT INTO candidates`.
- The daemon measures it.
- Start an idea from it with `piloop idea add <name> "<title>" --base <c>`.
- A candidate of another project is its `commit_sha`. If it is in another repo, fetch it into this repo first.
- A reference is never a keep, the head or a bar.

Branch runs:

- A run pinned to a reference, or to an older keep, is a branch run. It is judged against its own base.
- Its keeps form a branch and never move the head. The overview lists the latest keep of each lineage.
- To bring a branch keep onto the trunk, file an unpinned idea that merges it into the head.
- A note or another non-keep is not a bar. A run pinned to one starts from its tree and is judged against the nearest
  keep. When that keep is the head, it is a trunk run.
- So pin a follow-up to a lead. Do not tell its worker to reset onto the lead.
- One run in `[roles.worker] explore_every` (default 4) takes the most urgent ready idea pinned to a base other than
  the head, whatever its priority.

### Keep a failed result as a lead

```bash
piloop candidate note <c> "<why>" --follow-up "<hunch>"
```

- `--follow-up` is repeatable.
- It changes a measured `discard` or `interesting` candidate to `interesting` with the reason `note: <why>`.
- It files each follow-up as a seed pinned to the candidate. Research develops the seeds.
- It is not a keep and not a bar.
- Workers note a lead with a `Piloop-Note:` commit trailer, without follow-ups.

### Refill the backlog

- Run `piloop launch <name> research`.
- A sweep develops the seeds first. Then, while the backlog is short, it adds up to `max_ideas` ideas of its own.
- It leaves a `finding` on the project.
- Research sweeps also run by themselves while the backlog is below its target. They wait longer after sweeps that
  define nothing.

## The inbox: decide, record, keep moving

The human is often away and does not want to block work. Nothing waits on them.

- You decide what comes up and keep the campaign moving.
- Every decision you take for the human becomes an item in their inbox, so that they can review and change it later.

The inbox is every comment that has a `priority`.

- `piloop inbox` lists the unacknowledged items, most urgent first.
- `piloop inbox show <id>` shows one item with its thread.
- `piloop inbox ack <id>` (or `--up-to <id>`) marks items as seen.

The priority says how urgent an item is:

- **0–1:** now.
- **2–4:** can wait. Status counts them.
- **No priority:** the log.

### Escalations

An escalation arrives as a `comment#<id> escalation …` line from `piloop inbox wait`. It is already an inbox item: its
writer gave it a priority.

1. **Read** the thread and the target.
   - `piloop inbox show <id>`.
   - For a run: `SELECT status, error, log_dir FROM runs WHERE id = <t>`, and the end of `<log_dir>/transcript.txt`.
   - With `entire` installed: `SELECT checkpoint_id FROM workers WHERE run_id = <t>`, then
     `entire checkpoint explain <checkpoint_id>` for the agent's own session.
   - Verify before you build on a theory: the agent's own transcript, `ps`, a new query. Do not guess the mechanism.
2. **Decide.**
   - If the human is talking to you now, give them the one decision, with the evidence and your recommendation.
   - Otherwise decide yourself. Take the smallest reversible step that keeps work moving.
   - An irreversible step that can wait is not yours. Leave the escalation unacknowledged and keep everything else
     moving. If the writer gave it too low a priority, raise it: `UPDATE comments SET priority = 0 WHERE id = <id>`.
3. **Record** the decision on the same target, addressed to the human (use the template below).
   - Then acknowledge the escalation: `PILOOP_ACTOR=orchestrator piloop inbox ack <id>`. Your decision replaces it in
     the inbox.
   - If the human decided it with you, the decision is theirs. Record it without a priority, and acknowledge it as the
     human: `piloop inbox ack <id>`, with no `PILOOP_ACTOR`.
4. **Apply** it: un-park or reject the idea, change its priority, or fix `piloop.toml` and run `piloop init .` to read
   it again.

### Every decision you take for the human

Record every decision you take for the human, with or without an escalation: a guardrail, a merge, a global setting,
a spend, a row you edited by hand. Record it when you apply it. Never record it only in the chat.

```sql
INSERT INTO comments (target_kind, target_id, author, kind, priority, body) VALUES ('<k>', <t>, 'orchestrator', 'decision', <p>,
'[<category>, reversible|irreversible] <what you decided, one line>
Why: <the evidence, with ids>
Changed: <exactly what: rows, files, settings, commits>
Revert: <the SQL, command or edit that undoes it>
Re: comment#<the escalation, if one>')
```

- Target the row that the decision is about: the target of the escalation, or the project for a project-wide decision.
- Never move a goalpost after you see the result that the move would let in. If it really must move, the decision has
  priority 0 and the body says what you had seen.
- A revision by the human arrives as a `decision` comment on the same target. From `piloop watch`, it arrives as a
  `decision` by `human` that replies to the item (`on comment#<id>`). Apply it before anything else, then acknowledge
  it.

### Surfacing policy

The human reviews this table regularly. Follow it as written. When it fits a case badly, say so in a `finding` with a
priority. Do not move away from it.

| category | what | priority |
|---|---|---|
| goalpost | target, guardrail or floor, metric, evaluate or scorer, references, stop conditions | 0 |
| outside the campaign | global config (`~/.claude`, `~/.codex`, the shell), repo trust, a merge, a push to a default branch, deleting data | 0 irreversible, 1 reversible |
| spend | above the brief's budget / within it | 1 / 3 |
| scope | finalize, extend or pause a campaign; park or reject an idea a human proposed | 2 |
| process | requeue, reprioritise, a brief or prompt fix, a workaround for a piloop bug | 3 |
| routine | a duplicate escalation, a false alarm you verified | 4 |

Some findings the human should see: a piloop bug, a result that changes the plan, a doubt about the evaluation.
Post each one as a `finding` with a priority from the same table. Everything else stays in the forum, without a
priority.

## Agents

- Each agent runs as a tab in the workspace of the project in the `piloop` Herdr session. The ids are in the status.
- `piloop attach <worker_id>` focuses that tab. From a terminal outside that session, it also attaches to the session.
  It takes over the terminal, so it is for the human, not for your shell tool.
- `piloop prompt <worker_id> "…"` steers a running Herdr agent of any role.
- To request one more sweep, run `piloop launch <name> watchdog|research [--harness pi --model …] [--reason "…"]`.
  Or insert a request:
  `INSERT INTO workers (kind, project_id, state, reason) VALUES ('research', <p>, 'requested', '…')`.
  The daemon launches it on the next tick, within the `max_concurrent` of the role.
- When an agent's Herdr tab closes, the agent ends. The daemon records the outcome on its `workers` row.

## Read a candidate or a session

- The commit is in the repo:
  - `SELECT commit_sha, summary FROM candidates WHERE id = <c>`, then `git show <commit_sha>`.
  - `git diff <base_sha> <commit_sha>` compares it with its parent.
  - `git cherry-pick <commit_sha>` takes it.
  - The kept lineage in `piloop history` lists the commit and checkpoint of each candidate.
- Why it was made: run `entire checkpoint explain <id>` on the checkpoint of the worker session that made it. It shows
  intent, prompts, files touched and tokens.
  - The checkpoint id is in the kept lineage, or in `workers.checkpoint_id` of any agent, sweeps included.
  - `git show refs/entire/checkpoints/<id[-2:]>/<id>:0/full.jsonl` is the full transcript. It is long: read it only
    for a specific question.
- Every agent session is attached to Entire when the agent ends, if `entire` is installed
  (`curl -fsSL https://entire.io/install.sh | bash`). A session that a daemon stop cut short is not attached.

## Judge candidates

The daemon's verdict is mechanical:

- `keep`: the candidate's primary metric beat the bar with confidence, and the guardrails held.
- `interesting`: it gained, but without that confidence.
- `discard`: otherwise.
- `crash`: it failed to measure.

The bar:

- The bar is the run's last keep, or its base, on the track of its idea.
- With several tracks, the primary metric of every other track is one more guardrail.
- A keep against a bar that the head has passed is merged onto the head by the daemon, and judged there. Its reason
  starts with `keeps against its own bar, but the head moved on`.
- If that merge conflicts, the daemon files a seed `Compose candidate#N onto the head`.
- The floors and margins of a track change only through an edit to `piloop.toml`. That is a decision: record it.

Override the verdict when the number is not the full story:

```sql
UPDATE candidates SET verdict = 'interesting', verdict_by = 'orchestrator', verdict_reason = '…' WHERE id = …
```

- The daemon never overwrites a verdict that it did not set.
- A suspicious jump: add a measurement with another seed and look at the mean.

Rules:

- Never change `runs.status` or `events`. Never change `workers`, except to insert a request.
- If they look wrong, post a comment of kind `escalation` on the run. You, the orchestrator, reconcile it.

Find who changed what:

```sql
SELECT at, actor, kind, payload FROM events WHERE target_kind = '<k>' AND target_id = <t> ORDER BY id
```

- Status, priority and verdict changes carry `{old, new}` and the actor (`human`, `orchestrator`, `daemon`,
  `watchdog#7` …).
- Deletes leave `*.deleted` tombstones.
