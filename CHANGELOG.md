# Changelog

All notable changes to piloop are in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and piloop follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). Before 1.0, a minor release may contain breaking changes.
A deprecated configuration key shows a warning for at least one minor release before it is removed.

## [Unreleased]

### Added

- Roles are configuration. A `[roles.<name>]` table with a new name defines a role: its prompt, its triggers, its
  runtime, its working copy (`workdir`), its harnesses and limits, and what it may file (`files`). The worker, the
  watchdog and research are the same tables with defaults, and each built-in role accepts every key.
- Triggers are entries of `[roles.<name>] triggers`: `anomaly`, `cron`, `seed`, `low_water`, `scout`, and `sql`, a
  query on the piloop database.
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

### Changed

- `workers.kind` holds any role name of the project. A trigger refuses a name that is not a role of the project.
- A commit marked `Piloop-Eval: skip` gets the new verdict `unmeasured` instead of `discard`, so scaffolding no longer
  counts as a dead end in the history, the research pack or agents' queries. Migration `0003` converts the daemon's
  existing skip discards.

### Fixed

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
- `[stop] plateau` counts every finished run without a keep, also one with no commit or only unmeasured commits.
  Ten tools-only runs after the last keep (77M worker tokens) read "1/15 runs without a keep", so only the other
  limits could end the campaign. The status line's count follows it. `max_runs` still skips such runs.
- `piloop init` in an existing repo commits only its own files: the ones it wrote and its template's files. It
  committed every dirty file (`git add -A`), and in a shared checkout it swept another agent's uncommitted work into
  a "piloop init" commit.
- `piloop init` validates `piloop.toml` before it commits it. It committed an invalid config, then reported it
  invalid; now the commit waits until the config is valid.

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
