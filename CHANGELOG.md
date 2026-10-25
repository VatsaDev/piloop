# Changelog

All notable changes to piloop are in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and piloop follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). Before 1.0, a minor release may contain breaking changes.
A deprecated configuration key shows a warning for at least one minor release before it is removed.

## [0.3.1](https://github.com/hiloopai/piloop/compare/v0.3.0...v0.3.1) - 2026-10-25

0.3.0 was tagged but never published: its release checks failed on macOS. 0.3.1 is 0.3.0 plus this fix.

### Fixed

- On macOS, an evaluator that exited before its stale lease killed it no longer fails the job, and is reaped.

## [0.3.0](https://github.com/hiloopai/piloop/compare/v0.2.1...v0.3.0) - 2026-10-25

This release has breaking changes, marked **Breaking** below. Read
[Upgrading](https://piloop.autoresearch.inc/guides/upgrading) before you upgrade: upgrade to 0.2.1 first, and move
every active campaign off the herdr runtime with piloop 0.2, or the upgrade refuses.

The upgrade migrates the database from 0.2.0 or newer only. A 0.1.0 database upgrades through 0.2 first.

piloop 0.3.0 pins `covalence==0.1.0.dev6`, a pre-release of the library that computes the blob addresses. It has no
prebuilt wheel for Intel macOS, where the install builds it from source with a Rust toolchain.

### Added

- Remote workers. `piloop serve --listen` serves leases over HTTPS, with bearer tokens (`piloop token create`), and
  `piloop worker --server` takes them on another machine. `piloop cas serve` shares the home's CAS with it, and
  implementers run there, with the workspace through the CAS. A proxy network trusts what an authenticating proxy
  stamps.
- A pool's provisioner starts its workers on demand; `modal` is the first.
- The executor capability: dispatch is a provider, and `workers` is the built-in one. Pools bound the agents and the
  evaluations of each worker, `piloop worker list` shows why one got no lease, and `piloop limits` changes a live key.
- The CAS capability: `[plugins.cas]` in `config.toml` selects `disk`, `cached`, `s3` or `gcs`.
- The vcs capability: `[plugins.vcs]` selects how a campaign saves versions; `git` is the default and only provider.
- Secrets for agents: `piloop secret`, and secret references in `[harness.<name>] env`.
- The daemon serves an HTTP API on its Unix socket. `piloop status`, `inbox` and `events` are its clients.
- `piloop prompt` leaves a note that the daemon delivers through the executor.
- A remeasure, and a metric with repeats, judge on pairs of the candidate and its bar.
- An `agent_session.identified` event once an agent's harness session is known.
- Each role sets its reasoning effort. Research defaults to Opus 5.5 and GPT-6.1-Sol.
- `piloop.capabilities` is the one module that a plugin imports, and every role is a provider: a plugin role ships its
  prompt.
- `piloop status`, the API's overview and the briefs are made of sections, which `[status] sections` and
  `[roles.<name>] sections` list, and which a plugin can add. The watchdog's brief has the lessons too.

### Changed

- **Breaking:** The herdr runtime is removed. Agents run on `subprocess`, and the upgrade refuses an active campaign
  or a live agent on herdr.
- **Breaking:** `[roles.<name>] workdir` is removed: every agent works in a worktree of its own.
- **Breaking:** `./workspace/` is a read-only copy of the shared workspace as of the agent's start; its writes and
  removals land when it ends, and the evaluate command gets none. Linked inputs are read-only too.
- **Breaking:** Entire is removed: `entire_agent`, `entire_attach_timeout_seconds`, `agent_sessions.checkpoint_id` and
  the kept lineage's `checkpoint`. piloop finds each session by its session id.
- **Breaking:** `runs.end` says how a run ended. Compare it, not `runs.error`.
- **Breaking:** A candidate's verdict is only a judgement. `pending`, `reference`, `unmeasured` and `recovered` are
  `origin` and `cause` now, and the CLI says `skipped` for `unmeasured`.
- **Breaking:** `candidates.commit_sha` is `candidates.revision`, and `documents.commit_sha` is gone.
- **Breaking:** A run's brief is in the CAS: `runs.brief` is `runs.brief_cid`, and the brief is in
  `<runs.log_dir>/brief.md`.
- **Breaking:** Event payloads are typed and hold their `kind`. The upgrade rewrites the old events, and `piloop sql`
  refuses an `INSERT INTO events`. `comment.delivered` means the agent read the prompt; what it meant before is
  `comment.queued`.
- **Breaking:** Blob addresses are CIDs, and archive manifests are DASL values. The upgrade stores each manifest again.
- **Breaking:** `piloop status`, `inbox` and `events` need a running daemon.
- **Breaking:** A `[harness.<built-in>]` table merges over the built-in's keys. A full override takes a name of its own.
- **Breaking:** Every plugin contract changed: a provider is its own parameters, and its class subclasses its
  capability's contract. Runtimes, triggers, stop rules, verdict rules, roles and executors change shape.
- **Breaking:** A watch metric is not part of an evaluator's identity. Adding, removing or changing one measures
  nothing again, and making one a guardrail or a primary, or the reverse, makes a new evaluator. The upgrade gives
  each campaign's current evaluator its new key.
- The daemon wakes on the event feed and on the times its rules name, not on a 2-second tick.
- A database upgrades one minor release at a time.

### Deprecated

These still load, with a warning, until 0.4.0:

- `[stop]`, `[spend] limit` and `confidence`: write them in `[plugins.stop_rule]` (`max_spend`) and
  `[plugins.verdict_rule]`. `PILOOP_CAMPAIGN_SPEND__LIMIT` is not bridged.
- `kind` in a trigger entry: write `provider`.
- `[storage.backend]` in `config.toml`: write `[plugins.cas] provider = "cached"`.
- `max_evaluations` and `PILOOP_MAX_EVALUATIONS`: write `evaluations` in `[plugins.executor.pools.serve]`.
- `tick_seconds` in `config.toml`: delete it.

Until 0.5.0:

- `PILOOP_SECRETS_COMMAND`: keep the secrets with `piloop secret set`.
- `[memory] wake_budget` and `[roles.<name>] wake_budget`: write the `budget` of the `lessons` entry of the role's
  `sections`.

### Removed

- The keys that 0.2.0 deprecated: `[roles.worker]`, `[roles.research.scout]`, the `scout` trigger,
  `[roles.research] buffer` and `context`, `{claude_config_dir}` and `{codex_home}`, `[storage.remote]` and
  `PILOOP_PROJECT_<KEY>`. Each fails with a link to the upgrading guide.
- The compat code for agents that 0.1.0 started.

### Fixed

- Runs end cleanly: a finding ends the run, the brief states the deadline with a wrap-up, Claude reads messages
  mid-turn, and a failed run retries from its last commit. A run's deadline stops at `max_hours`.
- An agent that the daemon ended can no longer leave a finding, and an answer to an escalation is no longer lost.
- The closing review waits for the stopped campaign's last commits.
- Research may add its own idea when the campaign has stalled, even with a full backlog.
- A hand keep off the head is rebased, and a skipped commit is measured on request.
- `piloop doctor` warns, and does not fail, on the `piloop.toml` and the missing secrets of a stopped campaign.
- A home whose socket path is too long for the system, as a deep `PILOOP_HOME` on macOS is, serves its API on a
  short path in `/tmp/piloop-<uid>/`.
- A failed evaluation's measurement keeps the `METRIC` lines that it printed.
- `piloop sql` explains a sqlite3 dot-command, and event times have one format.
- A database that another piloop build migrated is refused, even at a revision id that this build also has.
- `piloop watch <campaign>` refuses a home without that campaign before it opens the database.
- Linked inputs keep their modification times, so a linked venv's bytecode stays current.
- The examples measure time that load does not inflate.
- The database closes every connection that it handed out.

## [0.2.1](https://github.com/hiloopai/piloop/compare/v0.2.0...v0.2.1) - 2026-10-24

Upgrade to 0.2.1 before you upgrade to 0.3.0: `uv tool install 'piloop==0.2.1'`, then `piloop restart`. The running
agents keep going.

### Fixed

- The daemon reads the installed entry points once per process, not on every tick. A 0.2.0 daemon reads them while
  `piloop upgrade` installs 0.3.0, loads the plugins of 0.3.0 into its own code, and stops, with its agents.

## [0.2.0](https://github.com/hiloopai/piloop/compare/v0.1.0...v0.2.0) - 2026-10-21

This release has breaking changes, marked **Breaking** below. Read
[Upgrading](https://piloop.autoresearch.inc/guides/upgrading) before you upgrade.

The upgrade migrates the database. piloop backs up the database before it migrates it, and the upgrading guide says
how to go back to 0.1.0 from that copy.

### Added

- Campaign control from the CLI, each change validated and logged:
  - `piloop campaign stop <campaign> [--kill] [--reason "…"]` stops a campaign by hand, as a `[stop]` condition does,
    also while a pause or the `[spend]` limit holds it. The daemon drops the queued work, tells the running
    implementers to commit and stop, ends them 10 minutes later, scores their commits and runs the closing review.
    `--kill` ends them at once. `[spend] closing_review` (`run`, `skip` or `wait`) decides the closing review of a
    campaign that stops at its spend limit.
  - `piloop pause <campaign> --reason "…"` and `piloop resume <campaign>`. During a pause nothing launches, no run is
    queued and no role is requested, while running work finishes and is scored. A pause raises no escalation, and
    paused time does not count toward `[stop] max_hours`. `--evals` also holds the measurements of a new evaluator.
  - `piloop run cancel <run> [--wrap-up]` and `piloop agent cancel <agent> [--wrap-up]` end one run or one agent of any
    role, also an agent that 0.1.0 started, with the processes of its tool calls. A cancelled run closes as
    `cancelled`, counts toward no run limit, parks its idea, and its commits are scored. `--wrap-up` tells the agent
    to commit and stop first.
  - `piloop candidate remeasure <id>` measures a candidate again with the current evaluator, for a measurement that a
    fault of the host spoiled or for one more repeat. A stopped campaign is measured only on request.
  - `piloop candidate verdict <id> keep|interesting|discard|crash --reason "…"` overrides a verdict, and
    `piloop measurement add <id> --report <file>` records a measurement made by hand. The daemon then judges again a
    verdict that it set.
  - `piloop idea status`, `link`, `unlink`, `base` and `track` change an idea, with the reason in a note. A link that
    closes a cycle is refused, and so is a change that conflicts with an open run. `piloop idea develop` takes
    `--after` and `--supersedes`, like `piloop idea add`.
- Campaign types and a tree of campaigns. Each campaign has a type (`campaigns.type`; `optimization` is the only type
  for now) and an optional parent. `piloop campaign parent` sets the parent, and `piloop campaign tree` prints the
  tree. The database refuses a cycle of parents. Each evaluator records its runner, `evaluators.runner`; the only
  runner is `process`.
- The evaluation queue. The daemon runs at most `max_evaluations` evaluate commands at the same time, across all
  campaigns (2 by default; `config.toml` or `PILOOP_MAX_EVALUATIONS`). A free slot goes to the campaign with the
  fewest running evaluations, and there to its head first, then to the oldest request. Two runs that reach one tree
  share one evaluation. `piloop eval queue` lists the evaluations that run or wait, and `piloop eval cancel
  <candidate>` cancels one.
- An event feed for programs. `piloop status --json` and `piloop inbox --json` print one JSON document with the
  cursor they were read at, and `piloop events --after <cursor> [--campaign] [--kind] [--limit] [--follow] --json`
  prints the events after it, in one versioned envelope. A program reads a snapshot, then follows it, and misses no
  event. See the [event feed](https://piloop.autoresearch.inc/reference/event-feed) page.
- Roles are configuration. A `[roles.<name>]` table with a new name defines a role: its prompt, its triggers, its
  runtime, its working copy (`workdir`), its harnesses and limits, and what it may file (`files`). The built-in roles
  are the same tables with defaults, and each accepts every key. A role that you define can do runs
  (`runs = true`): it takes the ideas that its `takes` query selects, and its commits are scored and judged.
  `runs.role` records the role of each run. `piloop launch <campaign> <role>` takes any sweeping role.
- Triggers start the sweeping roles: `anomaly`, `cron`, `seed`, `low_water`, `stopped` (the closing review, in the
  watchdog's defaults) and `sql`, a query on the piloop database. Any sweeping role can use any trigger, and any
  sweeping role can fill the backlog: `max_ideas` is a key of every sweeping role, and `buffer` a parameter of
  `low_water`. `incident_prompt` and `routine_prompt` add a prompt file to an incident sweep and to every other
  sweep.
- Plugins. One plugin host loads every capability, and `piloop plugins` shows each capability, its providers and the
  package of each. A package adds runtimes, storage backends, triggers, roles, verdict rules and stop rules through
  entry points, and can declare a capability of its own for other packages to extend. `[plugins.verdict_rule]` and
  `[plugins.stop_rule]` select the rule that judges a candidate and the rule that decides whether a campaign goes,
  holds or stops; the built-in rules keep 0.1.0's behavior. With `[spend]` set, a stop rule must read the spend.
  `examples/plugins/` has a runtime, a storage backend, verdict rules and a stop rule. See the
  [extension points](https://piloop.autoresearch.inc/architecture/extension-points) page.
- The runtime contract: `focus`, `attach`, `view` and `checks`. `piloop attach`, `piloop prompt`, `enter` in
  `piloop watch`, the place of an agent in `piloop status` and the runtime checks of `piloop doctor` ask the runtime
  of the agent, so a runtime that a package adds works with all of them.
- Research memory, which every agent can query with SQL, across every campaign on the same repository:
  - The `lessons` table: the standing conclusion about one mechanism family, with its status (`dead`, `promising` or
    `untested`), the best effect and its metric. A new conclusion is a new row. The closing watchdog writes one for
    each family that the campaign tried.
  - The `documents` table holds reports, logs and design notes, searchable with `documents_fts`. `piloop init` and
    the stop of a campaign copy in the files that `[memory] documents` globs match.
  - `campaigns.repo` names the repository, whatever the clone.
  - The briefs of implementers and research sweeps open with the current lessons of the repository, in
    `[memory] wake_budget` tokens (`wake_budget` of a role; 0 turns it off).
- The orchestrator's journal. `piloop journal "<line>"` appends one line, and `piloop wake` prints a bounded overview
  for a new orchestrator session: the latest journal lines, the open inbox, the heads of the active campaigns and the
  current lessons.
- The artifact store. The daemon archives the shared workspace in the background: the entries of a done idea
  (`workspace/idea<id>…`) once its last run is judged, and what a stopped campaign left. Files go into one
  content-addressed store per `PILOOP_HOME` (`$PILOOP_HOME/store`, zstd at rest), and the `archives` table records
  each manifest. The daemon removes the local copies 7 days after their archiving, or sooner while the disk has less
  than `[storage] min_free_gb` (20 GB) free. `piloop restore <campaign> [<idea>|<path>]` writes files back, and
  `piloop store status|evict|verify` show the store and check it. The new settings file `$PILOOP_HOME/config.toml`
  holds `[storage]`: `[storage.backend]` mirrors the store to `s3` or, with the `piloop[gcs]` extra, `gcs`. The first
  daemon start after the upgrade archives every done idea and stopped campaign that is already there.
- piloop backs up the database before it migrates it: `$PILOOP_HOME/piloop.db.bak-<revision>-<UTC time>`, made with
  SQLite's `VACUUM INTO`. `piloop migrate`, `piloop upgrade` and the daemon's log say where the copy is. piloop
  keeps the newest 3; `migration_backups` in `config.toml` sets how many, and 0 turns backups off. When the copy
  cannot be written, nothing is migrated.
- Run `main` next to a release on one machine, each with its own home: see "Run main next to a release" in
  CONTRIBUTING.md. Agents run their daemon's own piloop (`PILOOP_BIN`), and every piloop that piloop starts runs
  with `python -P`, so a file in a worktree cannot shadow piloop's imports. `PILOOP_CLAUDE_SKILLS_DIR` keeps the
  Claude Code skills of a second install apart, and `PILOOP_CLI` names the command that `piloop doctor` prints.
- `piloop doctor` notes a newer piloop at the source that you installed from. The orchestrator then asks you before
  it upgrades, unless `PILOOP_AUTO_UPGRADE=1` is set. `piloop doctor` also checks that git is 2.40 or later.
- `piloop sql -` reads the statement from stdin. The briefs and skills write free text this way, in a quoted
  heredoc, so the agent's shell never expands it.
- Harness keys: `busy_screens`, `failed_turn_screens`, `queue_key` and `composer` (what the Herdr runtime reads on the
  screen of an agent, and how it queues a message), and `resume` (how piloop continues a session). The built-in
  harnesses set them, and a definition that leaves one unset takes it from the built-in harness with the same
  `executable`.
- A usage glob reads an environment variable with `{env:NAME:default}`. The built-in Claude Code and Codex globs read
  `CLAUDE_CONFIG_DIR` and `CODEX_HOME` this way.
- Under the Herdr runtime, an implementer that left its finding and went idle while jobs that it started still run
  is told once which jobs keep its run open.
- The docs have a [Runtimes](https://piloop.autoresearch.inc/concepts/runtimes) page. The tables of the built-in
  runtimes, triggers, harnesses and role defaults, the `[stop]` keys and the environment variables are generated
  from the code.

### Changed

- **Breaking:** A project is a campaign. The `projects` table is `campaigns`, and every `project_id` column is
  `campaign_id`. The target kind `project` is `campaign`, and the `project.*` events are `campaign.*`. The upgrade
  migrates the database and the queries of the stored configs. Change the SQL in your `piloop.toml`, your prompt
  files and your scripts: a query with an old table or column name fails and names the new one. A query on an old
  value, such as `kind = 'project.status'`, finds nothing.
- **Breaking:** The worker role is the implementer, and the `workers` table, which has one row for each agent session
  of any role, is `agent_sessions`. Every `worker_id` column is `agent_session_id`. The role value `worker` is
  `implementer` (in `agent_sessions.kind` and `runs.role`), and the signature `worker#<id>` is `implementer#<id>`.
  The target kind `worker` is `agent_session`, `worker.wrap_up` is `agent_session.wrap_up`, and `worker.started` and
  `worker.finished` are `implementer.started` and `implementer.finished`. Restart the running agents after the
  upgrade, so that they read the new names.
- **Breaking:** The environment variable that tags every process of an agent is `PILOOP_PROCESS_TAG`, not
  `PILOOP_WORKER_TOKEN`. piloop still finds the processes of an agent that 0.1.0 started.
- **Breaking:** Only `piloop serve` (when the daemon starts), `piloop migrate` and `piloop upgrade` migrate the
  database. Every other command, such as `status`, `sql`, `inbox` and `watch`, refuses a database at another schema
  revision, changes nothing, and says what to run: `piloop migrate`, `piloop restart` when the daemon of an older
  piloop runs, or a newer piloop when a newer one migrated it. `piloop init` still creates a missing database. After
  you install a piloop that changes the schema, run `piloop upgrade` or `piloop migrate` before any other command.
- piloop needs git 2.40 or later (0.1.0 needed 2.38). `piloop doctor` checks it.
- While the daemon of an older piloop runs, whoever started it, `piloop migrate` and `piloop serve` do not migrate
  the database: they exit and say what to run. All pending migrations run in one transaction, so an interrupted
  upgrade leaves the database as it was.
- The scout is a built-in role, `[roles.scout]`, instead of a mode of research. It is off until you set its
  `interval`, and it has its own `max_concurrent`, so a scout sweep and a research sweep can run at the same time.
  Its agents still sign `scout#<id>`. The upgrade makes the earlier scout sweeps `scout` rows.
- Events are append-only for every piloop connection, `piloop sql` included: an UPDATE or a DELETE of `events` is
  refused, and so is an insert that sets its own id or a value of the wrong type.
- A commit marked `Piloop-Eval: skip` gets the new verdict `unmeasured` instead of `discard`, so scaffolding no longer
  counts as a dead end. The upgrade converts the existing skip discards.
- The reason of every verdict names each guardrail past its floor or beyond its margin, also on a `discard` for the
  primary metric, so a large regression of another metric is no longer hidden.
- A run whose implementer stops while its own escalation waits for an answer fails with `blocked on escalation
  comment#<id>`, and its idea is `parked` with a note. It is not an anomaly: the escalation is in the inbox already.
- The `input` of `agent_sessions.usage` is the uncached input for every harness. For Codex it included the cache
  reads; the upgrade corrects the stored rows.
- The history shows the findings of every role on the campaign. The overview no longer lists the watchdog's findings:
  its section is the inbox, the decisions and the escalations.
- The implementer's brief names its worktree as the place for every edit, evaluation and commit, and the campaign
  repo as read-only. It asks for output under `workspace/idea<id>/`, for an escalation when the implementer needs
  what it cannot do itself, and for a wait on a job by its process id.
- The service unit (`piloop service-unit`) carries the variables that the usage globs of the harnesses name, instead
  of a fixed `CLAUDE_CONFIG_DIR` and `CODEX_HOME`.
- After an evaluator change, the daemon measures again only the head, the bar of each open run and of each `ready`
  pinned idea, and the candidates that you ask for. Unpinned runs wait only for the head's new score. Before, every
  keep and reference was measured again, one at a time, and runs waited for all of them.

### Deprecated

These keep working, with a `PiloopDeprecationWarning`, and are removed in 0.3.0.

- `[roles.worker]`: write `[roles.implementer]`.
- `PILOOP_PROJECT_<KEY>`: set `PILOOP_CAMPAIGN_<KEY>`. When both are set, `PILOOP_CAMPAIGN_<KEY>` wins.
- `[roles.research.scout]`: write `[roles.scout]`. The keys of `[roles.research]` that the scout used (`runtime`,
  `harnesses`, `model`, `timeout`, `mcp`, `addendum`, `workdir`) move with it.
- `[roles.research] buffer`: write `buffer` on research's `low_water` trigger.
- `[roles.research] context`: write the top-level `context`.
- `{claude_config_dir}` and `{codex_home}` in `[harness.<name>.usage] file`: write `{env:CLAUDE_CONFIG_DIR:~/.claude}`
  and `{env:CODEX_HOME:~/.codex}`.
- An agent that 0.1.0 started and that runs on after the upgrade still signs `worker#<id>`. piloop counts its rows as
  the implementer's.
- If you ran piloop from `main` after 0.1.0: `{ kind = "scout" }` in `[roles.research] triggers` (delete it),
  `herdr_<key>` in `config.toml` (write `<key>` in `[runtimes.herdr]`) and `[storage.remote]` (write
  `[storage.backend]`). The upgrading guide lists the names of `main` that changed with no alias.

### Removed

- **Breaking:** `piloop launch <campaign> research --scout`. Run `piloop launch <campaign> scout`.

### Fixed

- Ending an agent under the subprocess runtime (its timeout, a stop, the end of its run) also ends what it started in
  sessions of its own, such as Claude Code's Bash calls and the builds they run. Before, those processes ran on for
  hours. Under both runtimes, the processes are stopped before they are killed, so a build that forks meanwhile
  leaves no children.
- What piloop starts from an agent's shell no longer carries the agent's process tag or `PILOOP_ACTOR`: a daemon
  started with `piloop serve --detach` from an agent signed as that agent and died with it.
- An agent whose start failed after its process was spawned is stopped and waited for, instead of running on with
  nothing that watched it.
- An agent whose worktree could not be made (a full disk) no longer stays `starting` and holds a slot forever. It is a
  failed start: the row is closed with the error, and the run is launched again. A run whose every agent failed to
  start is not an attempt at its idea: the idea is `parked`.
- An implementer that ends within two minutes without a commit, an uncommitted change or a comment did not start to
  work, whatever its harness. Before, such a run counted as an attempt and could set its idea `done`.
- What an implementer leaves uncommitted is a `recovered` candidate: committed on its own with a
  `Piloop-Recovered: run#<id>` trailer, never measured, never kept. Before, it was scored and could become the head.
  When the run did not succeed, the idea goes back to the queue with that candidate as its base, up to
  `recovery_requeues` times (2), so a new implementer finishes the work.
- A run whose implementer left its finding is finished, however its process then ends: a timeout, an error exit, a
  cancel or a stop no longer sends its idea back to the queue, and a daemon stop no longer starts the same work
  again.
- A message to an agent (a pushed comment, `piloop prompt`, the wrap-up) waits for the agent's turn to end, under both
  runtimes. Under the `subprocess` runtime it was refused. A message to a Codex agent no longer ends its turn with
  `400 unsupported websocket request type`.
- A Herdr server that does not answer for a moment no longer ends the agents that the daemon watches: reads are
  retried for `PILOOP_HERDR_UNAVAILABLE_SECONDS` (300) before they fail.
- A Herdr agent has a startup grace for its first turn (`PILOOP_HERDR_STARTUP_GRACE_SECONDS`, 60 s, instead of 5 s).
  On a loaded host, a Codex that had started its turn was closed as `prompt_lost` or as idle. A first prompt that
  stays typed in the composer is submitted with Enter, at most twice.
- The daemon reaps the child processes that an in-place restart (`piloop restart`, `piloop upgrade`) hands over.
  Before, each became a zombie when it exited.
- The subprocess runtime no longer fails the start of an agent that exits at once on macOS.
- `[stop] max_runs` and `[stop] plateau` count the same runs: a run counts when it reached the evaluator with a
  candidate. A run that ended before it committed, or with only `Piloop-Eval: skip` commits, counts toward neither,
  so a stream of failed launches no longer stops a campaign. `max_runs` stops the campaign only when no counted run
  runs or is being scored, so the last run is no longer cut short. A campaign set `active` again after a stop starts
  a fresh `plateau` window.
- `piloop init` in an existing repo commits only the files that it wrote, never another agent's uncommitted work, and
  it validates `piloop.toml` before it commits it.
- `Piloop-Eval` and `Piloop-Note` trailers count on any line after the subject. A harness's own trailers
  (`Co-Authored-By`) hid them, so skipped commits were evaluated and notes were lost.
- A trunk run that merges a branch keep onto the head is judged against the head, not against the branch keep.
- Every commit of a run is a `pending` candidate as soon as the run ends, instead of after the commits before it were
  measured.
- The daemon sets a verdict only on a pending candidate or on its own earlier non-keep verdict, in the transaction
  that writes it: another track's run no longer overwrites a keep. A run's commits are its first-parent history, so
  the line that a merge brings in is not scored again.
- Two runs that reach the same tree share one candidate, instead of failing. A rebase merges by tree, so only the real
  changes on each side merge. A rejected compose seed stays rejected.
- An evaluation that another process kills names the signal in its error, and the daemon runs it once more in a fresh
  worktree.
- An implementer of a stopped campaign that the daemon ends after the grace period closes its run as `campaign
  stopped`, with an `agent_session.stop_ended` event, instead of counting as an attempt at its idea. Its commits are
  still scored. A stopped campaign no longer files compose seeds when a merge onto the head conflicts.
- An idea that a sweep adds while its role's `max_ideas` is 0 is `proposed`, not `ready`.
- An agent's free text (a comment, a note) that holds backticks, `$(…)` or `$VAR` is no longer expanded by the
  agent's shell.

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

[0.1.0]: https://github.com/hiloopai/piloop/releases/tag/v0.1.0
