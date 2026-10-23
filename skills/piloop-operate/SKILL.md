---
name: piloop-operate
description: Operate a running piloop autoresearch campaign as its orchestrator, from any harness with a shell (Claude Code, Codex, pi, Gemini CLI, Cursor), by looping on piloop inbox wait to decide escalations, manage ideas, launch sweeps, steer implementers and judge candidates. Use when the human asks you to orchestrate, watch, run or report on a campaign, when an escalation arrives, or when you take a decision for the human; do not use it in an agent that piloop launched (PILOOP_ACTOR is an implementer, watchdog, research or scout id).
---

# Operate a piloop campaign

The database is the contract. Everything below is a `piloop` command in your shell, SQL included.

- Run SQL with `piloop sql -`, the statement piped in from a quoted heredoc (`<<'EOF'`, with the quotes).
- Never put your own text inside a double-quoted shell argument: the shell runs what is in backticks and `$(…)`, and
  expands `$VAR`, before piloop sees it. Give other commands short values in single quotes, without a `'` in them,
  and write anything longer as a comment through `piloop sql -`.
- Read `piloop schema` once before your first write. It gives the conventions and every table.
- Run every piloop command as `PILOOP_ACTOR=orchestrator piloop …`. When the human runs a second install under
  another command, such as `piloop-canary` next to a release, use that command wherever this skill says `piloop`.
  - The rows you change, the items you acknowledge and their events are then signed `orchestrator`.
  - `piloop inbox wait` then skips what you wrote yourself.
- Limit your queries to the campaigns that you orchestrate: the campaign in the current directory
  (`SELECT id, name FROM campaigns WHERE path = '<cwd>'` gives `<p>` and `<name>`), or each campaign that the human names.
  For several campaigns, leave the name out of `piloop inbox wait`: it then waits for every campaign.

## The loop

Loop on one blocking command. It costs no turns while nothing happens:

```bash
PILOOP_ACTOR=orchestrator piloop inbox wait [<name>] --timeout 1800   # first call: from now
PILOOP_ACTOR=orchestrator piloop inbox wait [<name>] --after <cursor> --timeout 1800   # every later call
```

It returns, one line each, as soon as something needs you:

- a new unacknowledged inbox item that someone else wrote,
- a seed that someone else filed,
- a campaign that the daemon stopped,
- an anomaly that the daemon gave to a watchdog,
- the daemon's verdict on a measured candidate.

Otherwise it returns `nothing new` at the timeout.
Its last line, after `next: `, is the command to wait again, with the cursor (`--after <event-id>`), so you miss
nothing that arrives between two waits. Run it in the form above, with `PILOOP_ACTOR=orchestrator` and your
`--timeout`.

Do not let your shell tool kill the wait:

- Claude Code: run it in the background (`run_in_background`). You are woken when it exits.
- Codex and pi: set the timeout parameter of the shell tool above `--timeout`.
- When the tool limit is lower than 30 minutes, pass a `--timeout` below the limit. Use a shorter wait, never a polling
  loop of your own.

Steps:

1. **Start.** Run `PILOOP_ACTOR=orchestrator piloop wake` first. In a fixed budget (`--budget <tokens>`), it prints
   your journal (what earlier sessions decided, planned and found), the open inbox, the head of each active campaign
   and the current lessons. Then run `piloop status` and `piloop inbox`. Items that already wait are yours too: the
   wait reports only new items.
   Then run `piloop doctor`, and again once a day. Run the commands that it names exactly as it names them: a second
   install, such as `piloop-canary`, names its own command, so that you never restart or upgrade the other install.
   - `FAIL daemon: … runs older code than the installed …`: run its fix, the restart. The running agents keep going.
   - `cli: … is out`: a newer piloop is available. When the line says that `PILOOP_AUTO_UPGRADE` is on, run the
     upgrade that it names with `PILOOP_ACTOR=orchestrator`, and record it as a decision. Otherwise do not upgrade:
     file a p2 escalation on the campaign that asks the human, with the line and its changes link. An upgrade
     migrates the database one way, and the runs after it are scored by the new code.
   - Any other `FAIL`: fix it as its `fix:` line says, or escalate it.
2. **Wait.** Run the command above.
3. **Read** each line.
   - An item: `piloop inbox show <id>` shows it with its thread.
   - A stop or an anomaly: `piloop status <name>`. The watchdog investigates anomalies and escalates what needs a
     decision. Do not repeat its work.
4. **Decide and record.** See "The inbox" below: escalations, every decision, the surfacing policy.
5. **Acknowledge** what you answered: `PILOOP_ACTOR=orchestrator piloop inbox ack <id>`. Leave unacknowledged what
   only the human can decide.
6. **Act.** Apply the decision with a piloop command: `piloop idea status|link|base|track`, `piloop candidate
   verdict`, `piloop pause`, sweeps, or `piloop.toml` followed by `piloop init .`. Use `piloop sql` for a write only
   when no command does it.
   Never raise `[spend] limit` yourself. A `Spend limit reached` escalation belongs to the human.
7. **Journal** each decision, plan and result as you go, one line with its ids:
   `PILOOP_ACTOR=orchestrator piloop journal 'sync-r1: parked idea#412 (dup of #398); next: compose c#486+c#501'`.
   At most 280 bytes. A session that loses its context (a crash, a full window) starts again from `piloop wake`, so
   write what the next session needs, not what the database already says.
8. **Report** when the human is present, at least every few waits or every hour. Then go back to step 2.
   - Give one short status: the head against the baseline (per track when there are several), what ran, what you
     decided. `piloop status <name>` has the numbers.
   - Include the keeps by source in every digest: the "Ideas by source" line of `piloop status <name>`.
     This shows whether research earns its sweeps.
   - When every campaign you watch has stopped, report the result and stop the loop.

Run the loop yourself, or give it to a background subagent so that the human's session stays free. Give the subagent
this skill and the campaign name. Tell it to report decisions and status back.

Run one orchestrator per campaign. Two loops answer the same escalation twice.

## Status

`piloop status [<name>]` shows the same overview that watchdog and research sweeps get in their brief.
Without a name, it uses the campaign in the current directory, else the only active campaign, else every campaign.
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

`piloop history [<name>]` prints the research pack: metrics, analysis artifacts, what was tried per
mechanism family, the waiting seeds, every idea, the kept lineage, the starting points, findings and digests.

`piloop watch` shows the same as `piloop status`, live, in the human's terminal.

If the daemon is down, `requested` rows collect in `agent_sessions` and nothing runs.
Check with `SELECT id, kind, reason FROM agent_sessions WHERE state = 'requested'`.
Start the daemon with `piloop serve --detach`.

## Ideas

### Add an idea

```bash
PILOOP_ACTOR=orchestrator piloop idea add <name> '<title>' --hypothesis '…' --plan '…' --family '<mechanism>' \
  --from '<what prompted it: comment#…, a digest, a loss table>' --priority 1
```

- `--base <candidate>` sets the start. `--after` and `--supersedes` link it to other ideas, in the same write as the
  idea. Never add a `ready` idea and then insert its `after` link: the daemon can start it in between.
- Priority is 0 (urgent) to 4 (someday).
- Add `--proposed` when the human should look at it first.
- Your ideas carry context that research does not have: the digests, the loss analysis, the human's direction.
  Write them down as ideas as soon as you see them. Use `--from` to say where they came from.

### File a seed

A seed is a hunch that you have no time to define: a follow-up that a result suggests, two candidates worth
combining, new data.
Send it to research as soon as it comes up:

```bash
PILOOP_ACTOR=orchestrator piloop idea seed <name> '<hunch, one line>' --from '<pointers: candidate#…, comment#…, a path>'
```

- Add `--base <c>` when the hunch is about the tree of candidate `<c>`.
- The idea gets status `seed`. It shows in `piloop inbox` and `piloop watch`.
- It starts a research sweep, however full the backlog is.
- Research reads the existing work and checks for duplicates and earlier negative results. Then it does one of these:
  - It develops the seed into a defined idea with `piloop idea develop`: hypothesis, evidence, check and stop rule,
    metric, expected effect.
  - It rejects the seed with a `note` that names the evidence that settled it.
- Watchdogs file seeds the same way, and only seeds.
- Implementers file no seeds. They put a hunch in their finding.
- You may develop or reject a seed yourself when you get to it first.

A link that the human pastes (a post, a repo, a paper) is a seed signed `human`, not signed by you:

```bash
PILOOP_ACTOR=human piloop idea seed <name> '<its mechanism, one line>' --from '<link>; targets <the measured loss>'
```

### Scout for outside techniques

- This needs `[roles.scout]` in `piloop.toml`, with `interval` set.
- The scout then sweeps on that timer. Each sweep searches X, GitHub and arXiv for techniques that address a measured
  loss.
- A scout sweep files at most `seeds` seeds, signed `scout#…`. The other techniques go to the `techniques` library.
- `piloop launch <name> scout` asks for a scout sweep now.
- Each scout sweep leaves a priority-3 `finding` that starts with its problem statement. Read it: a scout that
  misread the problem searched for the wrong thing.
- In your digests, report the `scout` row of "ideas by source" next to the `research` row.

### Point research at the evidence

When you or an implementer write an analysis of where the head loses (against the reference, per slice):

1. Add its path to the top-level `context` of `piloop.toml`.
2. Run `piloop init .`.

The research pack of every sweep then lists it under the analysis artifacts, newest first, before the history.

### Change an idea

Use these commands, not SQL. Each one validates, takes `--reason '<why, with ids>'`, signs the change as
`orchestrator` and records an event, so your decision's `Revert:` line is one more command.

- Park (for now), reject (never), approve, or settle without a run:
  `piloop idea status <id> parked|rejected|ready|proposed|done --reason '…'`. The reason becomes a `note` on the idea.
  An idea whose run is open (queued, running or being scored) can only be parked or rejected: its run finishes. Set
  it ready again after the run, never during it: that would queue a second run.
- Link an idea that exists: `piloop idea link <id> after|supersedes <target> --reason '…'`. It refuses a link that
  closes a cycle, and an `after` link to an idea that the daemon took already. When a prerequisite ends without a keep, the
  daemon parks the ideas after it and escalates. To run one anyway, `piloop idea unlink` it, then set it ready.
- Start it from another candidate: `piloop idea base <id> <candidate> --reason '…'`, or `head` to unpin it.
- Move it to another track: `piloop idea track <id> <track> --reason '…'`.
- Reprioritize: `UPDATE ideas SET priority = 0 WHERE id = …`, with a `note` that says why.

### Pause a campaign

```bash
PILOOP_ACTOR=orchestrator piloop pause <name> --reason '<what the pause waits for>'
PILOOP_ACTOR=orchestrator piloop resume <name>
```

- Nothing launches: no run, no sweep, no requested agent. Research files no ideas, and a sweep that runs already files
  its ideas as `proposed`. Running work finishes and is scored.
- A pause raises no escalation. `piloop status` shows it with the reason.
- No agent launches, the incident watchdog included: an anomaly of the running work waits in `piloop status` until the
  resume. Watch the running work yourself during a long pause. Paused time does not count toward `[stop] max_hours`.
- The daemon still measures what a new evaluator or `piloop candidate remeasure` needs, so the pause ends on a measured
  head. `--evals` holds those measurements too.
- Never pause with a `[spend]` command that reads a file, nor with a gate idea and `after` links: the `[spend]` hold
  escalates p0 each time it holds and stops every measurement, and research files ideas without the link.
- Pausing a campaign is a scope decision for the human: record it (see "Every decision you take for the human").

### Steer an idea

- A comment on the idea goes into its next brief.
- With `runtime = "herdr"`, a `note`, `finding` or `decision` comment (any kind except an escalation) on the idea, or
  on its *running* run, goes into that implementer's pane on the next tick. The pane gets only the first line (up to 200
  characters) and a query for the full text, so put the instruction in the first line.
- `comment.delivered` on the implementer confirms that the comment arrived. `comment.undelivered` says why it did not.
- `piloop prompt <agent-session> '<text>'` writes a note on the agent itself, which it gets in full. It says whether
  the note arrived.
- A comment on the campaign reaches no implementer. When a decision changes the work of a running implementer, write it on its
  run, not on the campaign.
- An implementer asks for what it cannot do itself with an escalation. When its run ends before the answer, the run ends
  `awaiting_answer` and its idea is parked. Answer with a `decision` on the idea, then set it `ready`.

### Cancel a run, stop a campaign

- A run that became redundant: `piloop run cancel <run> --wrap-up`. The implementer is told to commit and stop, and ended
  10 minutes later, with every process it started. Its commits are scored and its idea is parked. Without `--wrap-up`,
  it is ended at once. `piloop agent cancel <agent-session> [--wrap-up]` does the same for an agent of any role, a sweep too.
- A campaign: `piloop campaign stop <name> --reason '<why>'`. The daemon stops it as a `[stop]` condition does: it
  drops queued work, winds the implementers down, scores their commits and launches the closing review. `--kill` ends the
  implementers at once. Never `UPDATE campaigns SET status`: that gets no closing review.
- Never `pkill` an agent: the cancel ends what it left running, in any session.

### Start from a tree outside the lineage

Use this for a candidate of another campaign, or a branch.

- Run `piloop candidate import <name> <commit-or-ref> --summary '…'`. It registers a `reference` candidate.
  Never `INSERT INTO candidates`.
- The daemon measures it.
- Start an idea from it with `piloop idea add <name> '<title>' --base <c>`.
- A candidate of another campaign is its `commit_sha`. If it is in another repo, fetch it into this repo first.
- A reference is never a keep and never the head. It is the bar only of a run that is pinned to it.

Branch runs:

- A run pinned to a reference, or to an older keep, is a branch run. It is judged against its own base.
- Its keeps form a branch and never move the head. The overview lists the latest keep of each lineage.
- To bring a branch keep onto the trunk, file an unpinned idea that merges it into the head.
- A noted candidate (`piloop candidate note`) or any other non-keep is not a bar. A run pinned to one starts from its
  tree and is judged against the nearest keep. When that keep is the head, it is a trunk run.
- A run pinned to a candidate that has the head's changes (a merge onto the head, or a compose that cherry-picked it)
  is a trunk run too: its keep can move the head.
- So pin a follow-up to a lead. Do not tell its implementer to reset onto the lead.
- One run in `[roles.implementer] explore_every` (default 4) takes the most urgent ready idea pinned to a base other than
  the head, whatever its priority.

### Keep a failed result as a lead

```bash
piloop candidate note <c> '<why>' --follow-up '<hunch>'
```

- `--follow-up` is repeatable.
- It changes a measured `discard` or `interesting` candidate to `interesting` with the reason `note: <why>`.
- It files each follow-up as a seed pinned to the candidate. Research develops the seeds.
- It is not a keep and not a bar.
- Implementers note a lead with a `Piloop-Note:` commit trailer, without follow-ups.

### Refill the backlog

- Run `piloop launch <name> research`.
- A sweep develops the seeds first. Then, while the backlog is short, it adds up to `max_ideas` ideas of its own.
- It leaves a `finding` on the campaign.
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
   - For a run: `SELECT status, end, error, log_dir FROM runs WHERE id = <t>`, and the end of `<log_dir>/transcript.txt`.
   - With `entire` installed: `SELECT checkpoint_id FROM agent_sessions WHERE run_id = <t>`, then
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

```bash
PILOOP_ACTOR=orchestrator piloop sql - <<'EOF'
INSERT INTO comments (target_kind, target_id, author, kind, priority, body) VALUES ('<k>', <t>, 'orchestrator', 'decision', <p>,
'[<category>, reversible|irreversible] <what you decided, one line>
Why: <the evidence, with ids>
Changed: <exactly what: rows, files, settings, commits>
Revert: <the SQL, command or edit that undoes it>
Re: comment#<the escalation, if one>')
EOF
```

- Target the row that the decision is about: the target of the escalation, or the campaign for a campaign-wide decision.
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
| process | requeue, reprioritize, a brief or prompt fix, a workaround for a piloop bug | 3 |
| routine | a duplicate escalation, a false alarm you verified | 4 |

Some findings the human should see: a piloop bug, a result that changes the plan, a doubt about the evaluation.
Post each one as a `finding` with a priority from the same table. Everything else stays in the forum (comments
without a priority), out of the inbox.

## Agents

- The ids of the agents are in the status.
- By default (`runtime = "subprocess"`), each agent is a background process. `piloop prompt <agent_session_id> '…'` queues a
  message for it: when its turn ends, piloop resumes its session with the message.
- With `runtime = "herdr"`, each agent runs as a tab in the workspace of the campaign in the `piloop` Herdr session:
  - `piloop attach <agent_session_id>` focuses that tab. From a terminal outside that session, it also attaches to the
    session. It takes over the terminal, so it is for the human, not for your shell tool.
  - `piloop prompt <agent_session_id> '…'` steers a running agent of any role. The message waits for the end of the
    agent's current turn; an idle agent takes it at once.
- To request one more sweep, run `piloop launch <name> <role> [--harness pi --model …] [--reason '…']`: `watchdog`,
  `research`, or a role that `piloop.toml` defines.
  Or insert a request:
  `INSERT INTO agent_sessions (kind, campaign_id, state, reason) VALUES ('research', <p>, 'requested', '…')`.
  The daemon launches it on the next tick, within the `max_concurrent` of the role.
- When an agent's process exits, or its Herdr tab closes, the agent ends. The daemon records the outcome on its
  `agent_sessions` row.

## Read a candidate or a session

- The commit is in the repo:
  - `SELECT commit_sha, summary FROM candidates WHERE id = <c>`, then `git show <commit_sha>`.
  - `git diff <parent commit_sha> <commit_sha>` compares it with its parent (`candidates.parent_id`).
  - `git cherry-pick <commit_sha>` takes it.
  - The kept lineage in `piloop history` lists the commit and checkpoint of each candidate.
- Why it was made: run `entire checkpoint explain <id>` on the checkpoint of the agent session that made it. It shows
  intent, prompts, files touched and tokens.
  - The checkpoint id is in the kept lineage, or in `agent_sessions.checkpoint_id` of any agent, sweeps included.
  - `git show refs/entire/checkpoints/<id[-2:]>/<id>:0/full.jsonl` is the full transcript. It is long: read it only
    for a specific question.
- Every agent session is attached to Entire when the agent ends, if `entire` is installed
  (`curl -fsSL https://entire.io/install.sh | bash`). A session that a daemon stop cut short is not attached.

## Past research

Every campaign that ever ran is in the database. `campaigns.repo` names the code that each campaign worked on, the same
for every clone. Before you add an idea, and when an agent asks whether something was tried, query it. `piloop
schema` ("Remember past campaigns") has every query: implementations to copy from any campaign, documents, the
session that worked a past idea.

- Prior attempts on this repo, in every campaign, with what judged them. A discard under one evaluator is not dead
  under another: `SELECT p.name, i.id, i.status, i.title FROM ideas_fts JOIN ideas i ON i.id = ideas_fts.rowid JOIN
  campaigns p ON p.id = i.campaign_id WHERE ideas_fts MATCH '<mechanism>' AND p.repo = (SELECT repo FROM campaigns WHERE
  id = <p>) ORDER BY bm25(ideas_fts) LIMIT 20`.
- Lessons: the standing conclusion about each mechanism family, written by the closing watchdog when a campaign stops.
  The newest for each campaign and family is the current one, and `piloop wake` lists them. You may write one too: a
  row of `lessons` with the family, the status (`dead`, `promising` or `untested`), the best effect and its metric
  (both NULL when none was measured), and a body that says why, citing ids.
- Documents: the reports and logs that `[memory] documents` in `piloop.toml` names, copied in on `piloop init` and
  when the campaign stops. Search them with `documents_fts MATCH '<words>'`. Point the globs at reports, never at
  anything derived from validation or test data.
- Briefs of implementers and research sweeps open with the current lessons of the repo, in `[memory] wake_budget` tokens
  (0 turns it off; `[roles.<kind>] wake_budget` changes it for one role).

## Judge candidates

The daemon's verdict is mechanical:

- `keep`: the candidate's primary metric beat the bar with confidence, and the guardrails held.
- `interesting`: it gained, but without that confidence, or a guardrail went beyond its margin.
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

```bash
PILOOP_ACTOR=orchestrator piloop candidate verdict <c> interesting --reason '<what the number does not say, with ids>'
```

- The daemon never overwrites a verdict that it did not set.
- `keep|interesting|discard|crash`, and only over a judgement: a pending, reference, unmeasured or recovered
  candidate is refused. A keep must build on the head (unless its run is a branch run): a keep from an older bar
  would drop what the head kept. To land such a result, add an idea that merges it onto the head.
- You measured it yourself (the daemon's evaluation timed out, say): `piloop measurement add <c> --report <file>`,
  with `{"metrics": {"<name>": <value>}, "seconds": <s>}` in the file. The daemon judges its `crash`, `discard` or
  `interesting` again with every measurement; the output says when. Never `INSERT INTO measurements`: nothing judges
  it.
- A suspicious jump: `piloop candidate remeasure <c>`. The daemon measures it 3 times next to its bar and judges its
  `crash`, `discard` or `interesting` again on the pairs. A time that load inflates needs `repeats` on its metric in
  `piloop.toml`, not a remeasure or a larger margin.
- A `crash` that the host caused (a full disk, a lost GPU, a load that hit `evaluate_timeout`): `piloop candidate
  remeasure <c>`, after you raise the timeout and run `piloop init`. Never queue an idea with a no-op change to get a
  new tree measured.
- After a new evaluator, the daemon measures only the head and the bars that runs need. An older keep keeps its old
  score until you ask for it with `piloop candidate remeasure <c>`.
- Evaluations wait for one of the daemon's slots (`evaluations` of `[plugins.executor.pools.serve]`): `piloop eval queue` lists them. `piloop eval cancel
  <c>` cancels one that is no longer worth its slot; the candidate becomes a `crash` that names you. Never kill an
  evaluator process by hand.

Rules:

- Never change `runs.status` or `events`. Never change `agent_sessions`, except to insert a request.
- If they look wrong, record what you see in a `finding` on the run, and fix it through a piloop command. When no
  command fixes it, escalate to the human.

Find who changed what:

```sql
SELECT at, actor, kind, payload FROM events WHERE target_kind = '<k>' AND target_id = <t> ORDER BY id
```

- Changes to an idea's status, priority or track, a candidate's verdict and a campaign's status or pause carry
  `{old, new}` and the actor (`human`, `orchestrator`, `daemon`, `watchdog#7` …).
- `idea.base_candidate_id` carries `{old, new}` too, and `idea.linked` and `idea.unlinked` the target and the kind.
  The commands put their reason in a note on the idea. `measurement.recorded` has the metrics.
- Deletes of ideas, comments and candidates leave `*.deleted` tombstones.
