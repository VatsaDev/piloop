# piloop

[![CI](https://github.com/hiloopai/piloop/actions/workflows/ci.yml/badge.svg)](https://github.com/hiloopai/piloop/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/hiloopai/piloop/blob/main/LICENSE)
[![Python 3.14](https://img.shields.io/badge/python-3.14-blue.svg)](https://www.python.org/downloads/)

**Autoresearch for any git repo: coding agents try ideas, piloop scores every commit and keeps what wins.**

Documentation: [piloop.autoresearch.inc](https://piloop.autoresearch.inc)

You talk to one coding agent. It sets up a campaign in your repo and runs it for you. Coding agents try ideas in
parallel, each one in its own git worktree. piloop runs your evaluate command on every commit. It keeps a change only
when the change beats the best result so far. When your evaluate command reports a result for each example, the gain
must also hold with statistical confidence.

![piloop watch on the enwik8 example: 10 Claude Code runs took the compressed size from 3,688,770 to 2,361,893 bytes](https://raw.githubusercontent.com/hiloopai/piloop/main/docs/images/watch-campaign.svg)

*The enwik8 example after 10 worker runs: Claude Code agents took the first 10 MB of Wikipedia from 3,688,770 bytes (zlib) to 2,361,893 bytes (−36%) in 1.6 hours.*

piloop keeps its state on your machine: a SQLite database and the run directories in `PILOOP_HOME` (`~/.piloop` by
default), and every candidate commit as a git ref in your repo. piloop has no hosted service. The coding agents send
your code and their prompts to their model providers, as they do when you use them yourself.

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

   ```text
   Use piloop to improve the holdout accuracy of the model in train.py.
   ```

   ```text
   Use piloop to make the test suite run faster. Do not delete or skip tests.
   ```

## What happens next

1. **Setup.** The agent checks your machine and asks you a few questions: the metric, how to measure it, what must
   not get worse, which files the agents may edit, and the budget. Then it writes `brief.md`, `piloop.toml` and the
   evaluate command, measures the baseline and starts the campaign.
2. **The campaign runs.** Worker agents try ideas. piloop scores each commit and keeps what wins. A research agent
   adds ideas, and a watchdog agent looks for problems.
3. **The agent reports.** Your agent answers what the other agents escalate and records each decision in your inbox.
   It asks you only what needs you. Ask it "how is the campaign going?" at any time.

To look at the campaign yourself, run `piloop watch`.

## Try it on an example

Ask your agent:

```text
Set up the enwik8 example campaign from piloop.
```

The agents compress the first 10 MB of Wikipedia, as in the Hutter Prize. The baseline is zlib.

<details>
<summary>Run the example by hand</summary>

```bash
piloop init --example enwik8 ~/piloop-enwik8
cd ~/piloop-enwik8
piloop idea add enwik8 "Try lzma at the highest preset"
piloop serve --detach
piloop watch
```

The example uses Claude Code and stops after 20 scored worker runs. See
[Run a campaign by hand](https://piloop.autoresearch.inc/by-hand).

</details>

## Requirements

- Linux or macOS.
- git, with a commit identity.
- [uv](https://docs.astral.sh/uv/). The installer installs it.
- At least one worker agent CLI, logged in: [Claude Code](https://code.claude.com),
  [Codex](https://github.com/openai/codex) or [pi](https://github.com/badlogic/pi-mono).
- Optional: [Herdr](https://herdr.dev), to see and steer every agent in a terminal pane. By default, the agents run
  in the background. To use Herdr, set `runtime = "herdr"`.
- Optional: [Entire](https://github.com/entireio/cli), to keep the session of every agent in your repo.

## Documentation

- [Introduction](https://piloop.autoresearch.inc): what piloop does, and how a campaign works.
- [Quickstart](https://piloop.autoresearch.inc/quickstart): your first campaign, through your agent.
- [Concepts](https://piloop.autoresearch.inc/concepts/campaigns): campaigns, ideas, runs, candidates and the verdict.
- [Reference](https://piloop.autoresearch.inc/reference/cli): the CLI, the configuration, the database schema.

## Contributing

See [CONTRIBUTING.md](https://github.com/hiloopai/piloop/blob/main/CONTRIBUTING.md). Agents that operate piloop
start at [AGENTS.md](https://github.com/hiloopai/piloop/blob/main/AGENTS.md).

## License

MIT. See [LICENSE](https://github.com/hiloopai/piloop/blob/main/LICENSE).
