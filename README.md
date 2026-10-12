# piloop

[![CI](https://github.com/hiloopai/piloop/actions/workflows/ci.yml/badge.svg)](https://github.com/hiloopai/piloop/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/hiloopai/piloop/blob/main/LICENSE)
[![Python 3.14](https://img.shields.io/badge/python-3.14-blue.svg)](https://www.python.org/downloads/)

**Autoresearch for any git repo: coding agents try ideas, piloop scores every commit and keeps what wins.**

Documentation: [piloop.autoresearch.inc](https://piloop.autoresearch.inc)

You give piloop a goal that a program can measure. Coding agents (Claude Code, Codex or pi) then try ideas in
parallel, each one in its own git worktree. After each commit, piloop runs your evaluate command, which prints the
metrics. piloop keeps a change only when it beats the best result so far with statistical confidence, and when your
guardrail metrics stay within their limits. The next ideas start from that result.

A research agent keeps the backlog of ideas full, and a watchdog agent looks for problems. An orchestrator agent, in
the coding agent of your choice, answers what the other agents escalate. It records each decision in your inbox, so
you can review and revert it later. You do not have to wait at the screen.

Everything stays on your machine: one SQLite database, and the git refs in your repo. Every candidate commit stays in
your repo, so `git show` and `git cherry-pick` work on all of them.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/hiloopai/piloop/main/install.sh | sh
```

The installer installs uv, Entire, the `piloop` CLI and the piloop skills for your coding agents. If you already have
uv, you can install the CLI alone:

```bash
uv tool install piloop
piloop install-skills
```

## Quickstart

Compress the first 10 MB of Wikipedia, as in the Hutter Prize. The agents edit `compressor.py`, and the baseline is
zlib:

```bash
piloop init --example enwik8 ~/piloop-enwik8
cd ~/piloop-enwik8
piloop idea add enwik8 "Try lzma at the highest preset"
piloop serve --detach
piloop watch
```

The example uses Claude Code and stops after 20 worker runs. To use Codex or pi, edit `harnesses` in `piloop.toml` and
run `piloop init` again. Without Herdr, also set `runtime = "subprocess"`. Stop early with `piloop stop`. The
[Quickstart](https://piloop.autoresearch.inc/quickstart) explains each step.

## Use it on your own repo

Open Claude Code, Codex or pi in your repo and ask:

```text
set up an autoresearch campaign for this repo
```

The agent checks your machine, asks you about the goal and the limits, writes `piloop.toml`, `brief.md` and the
evaluate command, measures the baseline and starts the campaign. To set it up by hand, see
[Set up your own repo](https://piloop.autoresearch.inc/setup).

## Requirements

- Linux or macOS.
- git, with a commit identity.
- [uv](https://docs.astral.sh/uv/). The installer installs it.
- At least one coding agent CLI, logged in: [Claude Code](https://code.claude.com),
  [Codex](https://github.com/openai/codex) or [pi](https://github.com/badlogic/pi-mono).
- Optional: [Herdr](https://herdr.dev), to see every agent in a terminal pane. `runtime = "herdr"` is the default.
  With `runtime = "subprocess"`, the agents run in the background.
- Optional: [Entire](https://github.com/entireio/cli), to keep the session of every agent in your repo.

## Documentation

- [Introduction](https://piloop.autoresearch.inc): what piloop does, and how a campaign works.
- [Concepts](https://piloop.autoresearch.inc/concepts/projects): projects, ideas, runs, candidates and the verdict.
- [Guides](https://piloop.autoresearch.inc/guides/running-for-days): running for days, secrets, MCP servers, upgrading.
- [Reference](https://piloop.autoresearch.inc/reference/cli): the CLI, the configuration, the database schema.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Agents that operate piloop start at [AGENTS.md](AGENTS.md).

## License

MIT. See [LICENSE](LICENSE).
