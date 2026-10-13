# Changelog

All notable changes to piloop are in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and piloop follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). Before 1.0, a minor release may contain breaking changes.
A deprecated configuration key shows a warning for at least one minor release before it is removed.

## [Unreleased]

### Added

- Research memory: past research can be found by SQL from every agent, across every project on the same code.
  - A `lesson` comment is the standing conclusion about one mechanism family. The newest for a project and family is
    the current one. CHECKs keep it on its project, with the first line `family: <family> | status:
    dead|promising|untested | best: <+N metric> (<evaluator/eval>)`. The closing watchdog writes one for each family
    that the campaign tried.
  - The `documents` table holds reports, logs and design notes. `[memory] documents` globs are copied in on `piloop
    init` and when a campaign stops. Agents search them with `documents_fts`, and add their own with `hash =
    blake3(body)`.
  - `projects.repo` names the repository, whatever the clone.
  - The briefs of workers and research sweeps open with the current lessons of the repo, in `[memory] wake_budget`
    tokens (`[roles.<kind>] wake_budget` per role, 0 turns it off).
  - The procedures, the briefs and the operate and review skills have the queries.
- The orchestrator's journal: `piloop journal "<line>"` appends one line of at most 280 bytes, and `piloop wake`
  prints a bounded overview for a new orchestrator session: the latest journal lines, the open inbox, the active
  projects' heads and the current lessons.
- The artifact store. The daemon archives the shared workspace in the background, one job at a time: the entries of
  a done idea (`workspace/idea<id>…`, and `w<n>…`/`worker<n>…` of the workers that ran it) once its last run is judged,
  and what a stopped campaign left. `NOTES.md` and `research/` are never archived. Files go into one
  content-addressed store per `PILOOP_HOME` (`$PILOOP_HOME/store`: BLAKE3 hashes of whole files, zstd at rest). Each
  manifest is recorded in the new `archives` table, and the local copies are removed only once the store holds every
  blob. `piloop restore <project> [<idea>|<path>]` writes files back. `piloop store status|evict|verify` show the
  store and the backlog, and check it. `[storage]` in the new global settings file `$PILOOP_HOME/config.toml`
  mirrors the store to S3 (Glacier Instant Retrieval, STANDARD under 128 KB), with eviction of local copies by age
  and size, or remote only (`evict_after_days = 0`). The worker brief asks for output under `workspace/idea<id>/`, no
  copies of the code tree, and evaluator output left in the project's cache. The research pack lists workspace files
  from a bounded walk instead of the whole workspace. The first daemon start after the upgrade archives every done
  idea and stopped campaign that is already there. Archiving leaves `[storage] min_free_gb` (20 GB) free on the
  store's disk: it stores a workspace in batches that fit above it, and when not one more file fits it leaves the
  rest, writes an `anomaly.disk` event on the project and waits. Each failed or deferred archiving in a row doubles
  the wait, from 10 minutes up to 6 hours.

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
