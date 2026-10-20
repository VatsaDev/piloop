# Changelog

All notable changes to piloop are in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and piloop follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). Before 1.0, a minor release may contain breaking changes.
A deprecated configuration key shows a warning for at least one minor release before it is removed.

## [Unreleased]

### Added

- `piloop eval queue` lists the evaluations that run or wait for a slot. `piloop eval cancel <candidate>` cancels one:
  the candidate gets a crashed measurement that names who cancelled it, at once, so a daemon restart does not run it.
- Architecture proposal for shared plugin composition, public research operations, campaign types, measurements by
  evaluator, benchmark and artifact, and an incremental migration that keeps today's workflow as the optimization
  campaign type. The proposal does not change runtime behavior.
- Roles are configuration. A `[roles.<name>]` table with a new name defines a role: its prompt, its triggers, its
  runtime, its working copy (`workdir`), its harnesses and limits, and what it may file (`files`). The worker, the
  watchdog, research and the scout are the same tables with defaults, and each built-in role accepts every key.
- Triggers are entries of `[roles.<name>] triggers`: `anomaly`, `cron`, `seed`, `low_water`, `stopped`, and `sql`, a
  query on the piloop database. Any sweeping role may use any trigger.
- A sweeping role's `incident_prompt` and `routine_prompt` add a file after its prompt in an incident sweep (one that
  got anomalies) and in every other sweep. The watchdog's defaults are its incident and progress prompts.
- Packages can add triggers and roles through the `piloop.triggers` and `piloop.roles` entry point groups.
- `piloop launch` takes any sweeping role of the project.
- A role that you define can do runs: `runs = true`. It takes the ideas that its `takes` condition selects, fills its
  own `max_concurrent` slots before the worker, and its commits are scored and judged. `runs.role` records the role of
  each run.
- Runtimes are a registry. Packages can add runtimes through the `piloop.runtimes` entry point group. A runtime class
  states which argument list of a harness it starts (`invocation`: `interactive` or `headless`).
- Research memory: past research can be found by SQL from every agent, across every project on the same code.
  - The `lessons` table: the standing conclusion about one mechanism family, with its status (`dead`, `promising` or
    `untested`), the best effect and its metric. The newest for a project and family is the current one: a lesson is
    never changed, a new conclusion is a new row. The closing watchdog writes one for each family that the campaign tried.
  - The `documents` table holds reports, logs and design notes. `[memory] documents` globs are copied in on `piloop
    init` and when a campaign stops. Agents search them with `documents_fts`, and add their own with `hash =
    blake3(body)`.
  - `projects.repo` names the repository, whatever the clone.
  - The briefs of workers and research sweeps open with the current lessons of the repo, in `[memory] wake_budget`
    tokens (`[roles.<kind>] wake_budget` per role, 0 turns it off). Lessons in the project's own metrics come first,
    and effects are ranked within their metric, never by raw size across units.
  - The procedures, the briefs and the operate and review skills have the queries.
- The orchestrator's journal: `piloop journal "<line>"` appends one line of at most 280 bytes, and `piloop wake`
  prints a bounded overview for a new orchestrator session: the latest journal lines, the open inbox, the active
  projects' heads and the current lessons.
- The artifact store. The daemon archives the shared workspace in the background, one job at a time: the entries of
  a done idea (`workspace/idea<id>…`) once its last run is judged, and what a stopped campaign left. `NOTES.md` and
  `research/` are never archived. Files go into one content-addressed store per `PILOOP_HOME` (`$PILOOP_HOME/store`:
  BLAKE3 hashes of whole files, zstd at rest), and each manifest is recorded in the new `archives` table. The local
  copies stay: the daemon removes them 7 days after their archiving, or sooner, oldest archiving first, while the
  store's disk has less than `[storage] min_free_gb` (20 GB) free. It removes only a file that still holds what was
  stored. `piloop restore <project> [<idea>|<path>]` writes files back. `piloop store status|evict|verify` show the
  store and the backlog, and check it. The new global settings file `$PILOOP_HOME/config.toml` holds `[storage]`:
  `[storage.remote]` picks a backend by `kind` (`s3` is built in: Glacier Instant Retrieval, STANDARD under 128 KB;
  `gcs` is built in with the `piloop[gcs]` extra; packages add backends through the `piloop.stores` entry point group,
  and `piloop.store.contract.check_remote` tests one), with eviction of the store's local copies by
  age and size, or remote only (`evict_after_days = 0`). The worker brief asks for output under `workspace/idea<id>/`,
  no copies of the code tree, and evaluator output left in the project's cache. The research pack lists workspace
  files from a bounded walk instead of the whole workspace. The first daemon start after the upgrade archives every
  done idea and stopped campaign that is already there. When not one more file fits above the reserve, archiving
  leaves the rest, writes an `anomaly.disk` event on the project and waits 10 minutes.
- `[harness.<name>] busy_screens`, `failed_turn_screens` and `queue_key`: what the Herdr runtime reads on the screen
  of an agent of the harness, and how it queues a message for it. They were built into the runtime for Claude Code and
  Codex; the built-in harnesses set them now, and a definition that leaves one unset takes it from the built-in
  harness with the same `executable`, so an override of `[harness.codex]` keeps Codex's queue key.
- A usage glob reads an environment variable with `{env:NAME:default}`. The built-in Claude Code and Codex globs read
  `CLAUDE_CONFIG_DIR` and `CODEX_HOME` this way, with the same defaults as before.
- The `Runtime` protocol has four new methods: `focus` and `attach` show an agent, `view` names where you can see it,
  and `checks` reports to `piloop doctor`. `NotAttachable` says that a runtime cannot show an agent, and where to look
  instead. `RuntimeFailure` is the base of the errors that a runtime reports. **Breaking** for a runtime that a
  package adds: it must implement the four methods.
- The docs have a [Runtimes](https://piloop.autoresearch.inc/concepts/runtimes) page. The tables of the built-in
  runtimes, triggers, harnesses and role defaults, the `Runtime` methods, the `[stop]` keys and the environment
  variables are generated from the code, so they no longer drift. The extension points page gives each extension
  point its contract, a generated table of the built-ins, and how to add your own, or says that you cannot yet.
- `piloop candidate remeasure <id>` asks the daemon to measure a candidate again with the current evaluator, for a
  measurement that a fault of the host spoiled or for one more repeat. A `crash` that the daemon judged is judged
  again with the new measurement. Before, the only way was an idea with a no-op change, to get a new tree.
- `piloop idea develop` takes `--after` and `--supersedes`, like `piloop idea add`. The links are written in the same
  transaction as the idea.
- Under the Herdr runtime, a worker that left its finding and went idle while jobs that it started still run is told
  once which jobs keep its run open.
- `piloop pause <project> --reason "…"` and `piloop resume <project>`: nothing launches, no run is queued and no role
  is requested, while running work finishes and is scored. A pause raises no escalation. The daemon still measures
  what a new evaluator needs, unless `--evals` holds that too. A sweep that runs during a pause files its ideas as
  `proposed`. Paused time does not count toward `[stop] max_hours`. The pause is `projects.paused`, logged as
  `project.paused` events. Before, a campaign was paused with a `[spend]` command that read a file, which escalated
  each time it held and stopped every measurement.
- `piloop candidate verdict <id> keep|interesting|discard|crash --reason "…"` overrides a verdict, and `piloop
  measurement add <id> --report <file>` records a measurement made by hand. The daemon then judges again a `crash`,
  `discard` or `interesting` that it set. Before, both were raw SQL, and nothing judged a measurement inserted by hand.
- `piloop idea status`, `piloop idea link`, `piloop idea unlink`, `piloop idea base` and `piloop idea track` change an
  idea with a reason in a note. A link that closes a cycle (also from `idea add` and `idea develop`), a status other
  than parked or rejected for an idea whose run is open, an `after` link or a track change for such an idea, and a
  track that `piloop.toml` does not have are refused. The change log now records `idea.base_candidate_id`,
  `idea.linked` and `idea.unlinked`, from a command or from raw SQL. Before, these were raw SQL and unlogged.

### Changed

- The daemon runs at most `max_evaluations` evaluate commands at the same time, across all projects (2 by default,
  in `config.toml` or `PILOOP_MAX_EVALUATIONS`). The others wait in the evaluation queue, the `evaluations` table. A
  free slot goes to the project with the fewest running evaluations, and in that project to its head first, then to
  the oldest request, so no project waits behind another. A request for a candidate whose evaluation waits or runs
  waits for that evaluation, so two runs that reach one tree no longer evaluate it twice, and a crash is not evaluated
  again.
- `piloop attach`, `piloop prompt`, `enter` in `piloop watch`, the place of an agent in `piloop status` and `piloop
  watch`, and the runtime checks of `piloop doctor` ask the runtime of the agent. No command checks the name of a
  runtime, so a runtime that a package adds works with all of them. `piloop attach` on an agent that its runtime
  cannot show exits with an error that names its output file and its transcript.
- `workers.kind` holds any role name of the project. A trigger refuses a name that is not a role of the project.
- The scout is a built-in sweep role, `[roles.scout]`, instead of a mode of research. It is off until you set its
  `interval`: its `cron` trigger then requests a sweep every `interval` seconds. `piloop launch <project> scout` asks
  for one; `--scout` is gone. Its agents still sign `scout#<id>`. It has its own `max_concurrent`, so a scout sweep and
  a research sweep can now run at the same time. Migration `0011` makes the earlier scout sweeps `scout` rows.
- The closing review when a campaign stops is the `stopped` trigger, in the watchdog's default triggers.
  `[roles.watchdog] triggers = []` now means no closing review, and a list that you set needs `{ kind = "stopped" }`
  to keep it. Migration `0011` adds it to the stored copy of a list that you set, so a project keeps its closing review
  until its next `piloop init`.
- Any sweeping role can fill the backlog: `buffer` is a parameter of the `low_water` trigger, `max_ideas` is a key of
  every sweeping role, and `low_water` counts the sweeps of its own role. The brief of a sweep that defines ideas
  states its `max_ideas`.
- The analysis files of the research pack are the project's top-level `context`.
- The history shows the findings on the project of every role ("History: findings on the project"), not only those of
  research and the scout. The overview no longer lists the watchdog's findings: its section is the inbox, the
  decisions and the escalations.
- The `input` of `workers.usage` is the uncached input for every harness. For Codex it included `cache_read`.
  Migration `0012` subtracts it in the stored rows of every harness whose tally is `codex` (by the project's stored
  config, or the built-in harness of that name; by the rollout file name when the harness is gone).
- The service unit (`piloop service-unit`) carries the variables that the usage globs of the built-in harnesses and
  of the projects' harnesses name, instead of a fixed `CLAUDE_CONFIG_DIR` and `CODEX_HOME`.
- A commit marked `Piloop-Eval: skip` gets the new verdict `unmeasured` instead of `discard`, so scaffolding no longer
  counts as a dead end in the history, the research pack or agents' queries. Migration `0003` converts the daemon's
  existing skip discards.
- The reason of every verdict names each guardrail past its floor or beyond its margin. A `discard` on the primary
  metric, a gain that is not significant and a floor discard named none of them, so a large regression of another
  metric was hidden.
- A run whose worker stops while its own escalation waits for an answer fails with `blocked on escalation
  comment#<id>`, and its idea is `parked` with a note, instead of `succeeded` and `done`. It is not an anomaly: the
  escalation is in the inbox already.
- The worker brief says to ask for what a worker cannot do itself with an escalation, never a note; to wait for a job
  by its process id, never with `pgrep -f`, which finds the waiter itself; and that only the comments on its run or
  its idea are for it.
- The SQL procedures and the operate skill link an idea when it is written (`--after`, `--supersedes`), not with an
  `INSERT` after a `ready` add.

### Deprecated

- `[roles.research.scout]`: write `[roles.scout]`. The keys of `[roles.research]` that the scout used (`runtime`,
  `harnesses`, `model`, `timeout`, `mcp`, `addendum`, `workdir`) move with it. Removed in 0.3.0.
- `{ kind = "scout" }` in `[roles.research] triggers`: delete it; the scout has its own `cron` trigger. Removed in
  0.3.0.
- `[roles.research] buffer`: write `buffer` on research's `low_water` trigger. Removed in 0.3.0.
- `[roles.research] context`: write the top-level `context`. Removed in 0.3.0.
- `herdr_<key>` in `config.toml`: write `<key>` in its `[runtimes.herdr]` table. The `PILOOP_HERDR_<KEY>` variables
  do not change. Removed in 0.3.0.
- `{claude_config_dir}` and `{codex_home}` in `[harness.<name>.usage] file`: write `{env:CLAUDE_CONFIG_DIR:~/.claude}`
  and `{env:CODEX_HOME:~/.codex}`. Removed in 0.3.0.

### Fixed

- A worker whose worktree could not be made (a full disk, `Could not write new index file`) no longer stays
  `starting` and holds a slot forever. The failure is a failed start, like one that the runtime reports: the row is closed with
  the error, the run is relaunched, and the `anomaly.startup` event counts toward the launch pause. Any other fault
  before an agent starts closes its row too.
- A run whose every agent failed to start no longer counts as an attempt at its idea. Its error is `no agent of the run
  started to work`, and its idea is `parked`, attempts left or not, instead of being retried or set `done`.
- A worker that ends within two minutes without a commit, an uncommitted change or a comment did not start to work,
  even when it spent tokens, and whatever its harness: a harness whose tools fail at launch still reads its brief and
  replies that it is blocked. Before, such a run counted as an attempt (`worker produced no commits`) and could set its
  idea `done`.
- Ending an agent under the subprocess runtime (its timeout, a stop, the end of its run) now also ends what it
  started in sessions of its own, such as Claude Code's Bash calls and the builds they run. Before, only its process
  group was killed, and those processes ran on for hours. Every agent now has `PILOOP_WORKER_TOKEN`, as under Herdr.
  Under both runtimes, the processes are stopped before they are killed, so a build that forks meanwhile leaves no
  children. A shared server that an agent starts (sccache, Gradle, Bazel) ends with it.
- What piloop starts from an agent's shell no longer carries the agent's `PILOOP_WORKER_TOKEN` or `PILOOP_ACTOR`: a
  daemon started with `piloop serve --detach` from an agent signed as that agent and died with it.
- An agent whose start failed after its process was spawned is now stopped and waited for. Before, the process
  ran on with nothing that watched it, and was never reaped.
- A daemon stop no longer sets the idea of a run whose worker had left its finding back to `ready`, which started the
  same work again. The stop closes the run as finished, and the next daemon scores its pinned commits and settles the
  idea.
- A Herdr server that does not answer for a moment (`server_unavailable`, or a `herdr` call that times out) no longer
  ends the agents the daemon watches: reads are retried for `herdr_unavailable_seconds` (300) before they fail.
- A Herdr agent is no longer closed during its first turn. Its brief has a startup grace
  (`PILOOP_HERDR_STARTUP_GRACE_SECONDS`, 60 s) to be picked up instead of 5 s, and a quiet screen within the grace is
  not a finished turn: on a loaded host a Codex that had started its turn was closed as `prompt_lost` or as idle after
  3 s. A session id that Herdr had not reported yet is asked for again before the pane closes.
- The daemon no longer collects zombie processes. An in-place restart (`piloop restart`, `piloop upgrade`) execs the
  daemon while its threads still wait on `herdr` and `git` subprocesses; the new program had no handle on them, so
  each became a zombie when it exited (26 on one daemon after two restarts). The daemon now reaps the children it
  inherits on every tick.
- The subprocess runtime no longer fails the start of an agent that exits at once on macOS: it asked the system for
  the process group of an exited child (ESRCH); the group is the child's own pid.
- What a worker leaves uncommitted is a `recovered` candidate (new verdict, migration `0005`): committed on its own
  with a `Piloop-Recovered: run#<id>` trailer, never amended into the worker's commit, never measured, never kept.
  Before, it was scored and could become the head, though the worker never declared it ready. When the run did not
  succeed, the idea goes back to the queue with the recovered candidate as its base, up to `[roles.worker]
  recovery_requeues` times (2), so a new worker finishes the work, and that run is judged as any other.
- The worker brief no longer says that commits are scored while the worker works. They are scored after the run ends.
- A message to a Codex agent (a pushed comment, `piloop prompt`, the wrap-up) no longer ends its turn. Submitted with
  Enter mid-turn, it made Codex send `response.interrupt`, which its websocket backend refuses with `400 unsupported
  websocket request type`, and the turn was lost. The message is now typed on one line and queued with Codex's queue
  key (Tab): Codex runs it when the turn ends, or at once when idle. The 400 banner also counts as a failed turn, which
  is resumed, instead of the end of the run.
- A message to an agent under the `subprocess` runtime is no longer refused. It waits for the agent's turn to end,
  then piloop continues the agent's session with it (`[harness.<name>] resume`, set for pi, Claude Code and Codex):
  the same queue-for-the-next-turn as under Herdr.
- `[stop] max_runs` and `[stop] plateau` count the same runs, by one rule: a run counts when it reached the
  evaluator, with a candidate that is measured or waits for its measurement. A run that ended at a cheap check with no
  commit or only `Piloop-Eval: skip` commits, or that a harness or host fault ended before it committed, counts toward
  neither: it says nothing about the metric. `[spend]`, `max_tokens` and `max_hours` stay hard limits on what such runs
  cost. `plateau` counted every finished run, so a stream of failed launches could stop a campaign. The status line
  and research's stall mark count the same way.
- `piloop init` in an existing repo commits only its own files: the ones it wrote and its template's files. It
  committed every dirty file (`git add -A`), and in a shared checkout it swept another agent's uncommitted work into
  a "piloop init" commit.
- `piloop init` validates `piloop.toml` before it commits it. It committed an invalid config, then reported it
  invalid; now the commit waits until the config is valid.
- A run of a role that you define with `runs = true` is requeued from its recovered work up to that role's
  `recovery_requeues`. It used the worker's value.
- The inbox line of an anomaly names the agent that it requested by its signature (`<role>#<id>`). It said
  `watchdog worker#<id>` for every role.
- An agent that died at startup is no incident when its role has the `anomaly` trigger, whatever the role's name: the
  agent it would request could die the same way. Only a watchdog was treated so, and its anomaly had a key of its
  own (`dead at startup, a watchdog`).
- `[stop] max_runs` stops the campaign only when no run that it counts runs or is being scored. It stopped the
  campaign as soon as its last run was queued, then ended that run early. A project stopped with `max_runs: 10 runs`
  showed `run 9/10`. A finished run also counts while its commits are scored, so no extra run is queued meanwhile.
  The new `runs.scoring` column (migration `0010`) records that; a run keeps its place even when its idea is parked
  or rejected meanwhile, and a restarted daemon resumes the scoring of every such run.
- A Herdr agent whose first prompt stays typed in its composer, with no turn started (a submit key the TUI lost), is
  sent Enter, at most twice; the prompt is never pasted again. Before, it was closed as `prompt_lost` after the
  60 s startup grace. A `prompt_lost` error now says whether the composer still held the brief.
  `[harness.<name>] composer` names the marker of the agent's input line; the built-in `claude` and `codex` harnesses
  have one.
- After an evaluator change, unpinned runs wait only until the head has a score from the new evaluator, and the
  head is measured first. They waited for every keep, measured one at a time: 4 worker slots sat idle for 70
  minutes. Research does not sweep while the queue holds its runnable ideas for that score.
- After an evaluator change, the daemon measures again only the head, the bar of each open run, the candidates that
  someone asked for (`piloop candidate remeasure`) and the bar of each `ready` pinned idea. An older keep is measured
  again only on request. Before, every keep and reference was measured again, one at a time, and an asked re-measure,
  such as a crash on `evaluate_timeout` after the timeout was raised, waited behind them for hours. A requested crash
  is measured after the bar that it is judged against again.
- A project set `active` again after a stop (`piloop init`, or an UPDATE of `projects.status`) starts a fresh
  `[stop] plateau` window: the count starts at the later of the last keep and the reactivation. A campaign that
  stopped on `plateau` stopped again on the next tick after it was reactivated.
- `Piloop-Eval` and `Piloop-Note` trailers count on any line after the subject, not only in the last paragraph of
  the commit message. A harness puts its own trailers (`Co-Authored-By`) in a paragraph after the worker's, so git
  read none of the worker's: skipped commits were evaluated, and notes were lost.
- A trunk run that merges a branch keep onto the head is judged against the head. The branch keep became the run's
  bar, so a merge that beat the head but not the branch keep was discarded.
- Every commit of a run is a `pending` candidate as soon as the run ends. A commit became a candidate only when the
  commits before it were measured, so on a busy evaluator the last commit of a run had no candidate for 33 minutes
  and looked lost.
- A worker of a stopped campaign that the daemon ends after the grace period, before it left its finding, closes its
  run as `campaign stopped`, with a `worker.stop_ended` event on the worker. A worker in the middle of a turn, such as
  Codex, reads the wrap-up prompt only when the turn ends, so it often never read it. The run closed as `worker
  produced no commits`, or with the exit status of the kill, and counted as an attempt at its idea. Its commits, and
  what it left uncommitted, are still scored.
- An idea that a sweep adds while its role's `max_ideas` is 0 is `proposed`, not `ready`. A sweep that started
  before an operator set `max_ideas = 0` to pause the role kept filing ready ideas.

## [0.1.0] - 2026-10-13

Initial public release.

### Added

- `piloop init`: makes a git repo a project, measures the baseline and registers it. Two bundled examples:
  `enwik8` (compress Wikipedia text) and `titanic-tracks` (accuracy and CPU cost on Kaggle's Titanic data).
- The daemon, `piloop serve`: starts coding agents (Claude Code, Codex or pi) in git worktrees, scores every commit
  with the project's evaluate command, and keeps what beats the head.
- The verdict: one primary metric that must improve with confidence, guardrail metrics with margins and floors, and
  a paired bootstrap over per-unit results.
- Three agent roles: the worker, the watchdog and the research agent, with a scout mode for outside techniques.
- Ideas, seeds, links between ideas, branch runs from pinned starting points, notes on regressed results, and
  imported reference candidates.
- Tracks: several objectives for one project, on one trunk.
- The orchestrator skills (setup, operate, review) for Claude Code, Codex, pi, Gemini CLI and Cursor, and the inbox
  where the orchestrator records its decisions.
- `piloop watch`: the campaign on one screen, with keys to act on it.
- Stop conditions, token accounting for every agent, and a hard spend limit in USD.
- Session capture of every agent with Entire.
- MCP servers for the agents of a role, defined once for every harness, with keys from the daemon's environment or
  `PILOOP_SECRETS_COMMAND`.
- `piloop restart`, `piloop upgrade`, `piloop doctor` and `piloop service-unit` for campaigns that run for days.

[Unreleased]: https://github.com/hiloopai/piloop/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/hiloopai/piloop/releases/tag/v0.1.0
