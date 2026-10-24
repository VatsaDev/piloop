# piloop

[![CI](https://github.com/hiloopai/piloop/actions/workflows/ci.yml/badge.svg)](https://github.com/hiloopai/piloop/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/hiloopai/piloop/blob/main/LICENSE)
[![Python 3.14](https://img.shields.io/badge/python-3.14-blue.svg)](https://www.python.org/downloads/)

**Autoresearch for any git repo: coding agents try ideas, piloop scores every commit and keeps what wins.**

Documentation: [piloop.autoresearch.inc](https://piloop.autoresearch.inc)

piloop runs coding agents on a goal that a program can measure. Each agent tries one idea in its own git worktree
and commits. piloop scores every commit with your evaluate command, and keeps a change only when it beats the best
result so far.

![piloop watch on the enwik8 example: 10 Claude Code runs took the compressed size from 3,688,770 to 2,361,893 bytes](https://raw.githubusercontent.com/hiloopai/piloop/main/docs/images/watch-campaign.svg)

*The enwik8 example after 10 runs: Claude Code agents took the first 10 MB of Wikipedia from 3,688,770 bytes (zlib) to 2,361,893 bytes (−36%) in 1.6 hours.*

piloop keeps its state on your machine: a SQLite database in `~/.piloop` and git refs in your repo. It has no hosted
service. The agents send your code and prompts to their model providers, as they do when you use them yourself.

## Start

1. Install piloop and its skills for your coding agents:

   ```bash
   curl -fsSL https://raw.githubusercontent.com/hiloopai/piloop/main/install.sh | sh
   ```

2. Open your coding agent in your repo: Claude Code, Codex, pi, Gemini CLI or Cursor.

3. Tell it what to improve. Say "piloop", so that the agent uses the piloop skills:

   ```text
   Use piloop to make our tokenizer faster without changing its output.
   ```

The agent sets up the campaign, runs it, and asks you only what needs you. To try piloop on an example first, ask for
"the enwik8 example campaign from piloop", or [run it by hand](https://piloop.autoresearch.inc/by-hand).

## Requirements

- Linux (x86-64 or ARM, glibc 2.28 or later) or macOS on Apple silicon. On an Intel Mac, installing builds
  [covalence](https://pypi.org/project/covalence/) from source, which needs a recent Rust.
- git 2.40 or later, with a commit identity.
- [uv](https://docs.astral.sh/uv/). The installer installs it.
- At least one of [Claude Code](https://code.claude.com), [Codex](https://github.com/openai/codex) or
  [pi](https://github.com/badlogic/pi-mono), logged in.

## Documentation

- [Quickstart](https://piloop.autoresearch.inc/quickstart): your first campaign, through your agent.
- [Concepts](https://piloop.autoresearch.inc/concepts/campaigns): campaigns, ideas, runs and the verdict.
- [Reference](https://piloop.autoresearch.inc/reference/cli): the CLI, the configuration and the database schema.

## Contributing

See [CONTRIBUTING.md](https://github.com/hiloopai/piloop/blob/main/CONTRIBUTING.md). Agents that operate piloop
start at [AGENTS.md](https://github.com/hiloopai/piloop/blob/main/AGENTS.md).

## License

MIT. See [LICENSE](https://github.com/hiloopai/piloop/blob/main/LICENSE).
