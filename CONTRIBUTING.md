# Contributing to piloop

This file explains how to set up a development checkout, run the checks and send a change.

## Set up

You need git, [uv](https://docs.astral.sh/uv/) and `make`.

```bash
git clone https://github.com/hiloopai/piloop
cd piloop
make install
```

`make install` installs the dependencies and the pre-commit hooks.

To try the installer from your checkout, as CI does:

```bash
PILOOP_SRC=. ./install.sh
```

## Run main next to a release

To try `main`, or your checkout, without touching the campaigns of the piloop release that you use, install it as a
second piloop, `piloop-canary`, with a home of its own. The home must be separate: a newer piloop migrates the
database of its home when it opens it, and the release then refuses to open that database until it knows the schema.

```bash
C=~/.piloop-canary
UV_TOOL_DIR=$C/uv/tools UV_TOOL_BIN_DIR=$C/uv/bin uv tool install 'git+https://github.com/hiloopai/piloop'
cat > ~/.local/bin/piloop-canary <<'EOF'
#!/bin/sh
C="$HOME/.piloop-canary"
export PILOOP_HOME="$C" UV_TOOL_DIR="$C/uv/tools" UV_TOOL_BIN_DIR="$C/uv/bin" PILOOP_CLI=piloop-canary \
    PILOOP_CLAUDE_SKILLS_DIR="$C/skills" PILOOP_AGENT_SKILLS_DIR="$C/skills" PILOOP_HERDR_SESSION=piloop-canary
exec "$C/uv/bin/piloop" "$@"
EOF
chmod +x ~/.local/bin/piloop-canary
piloop-canary install-skills
piloop-canary --version
```

To install your checkout instead, run the `uv tool install` line with `--reinstall .` in place of the URL.
`piloop-canary --version` prints the commit, and `piloop --version` still prints the release. uv warns that
`~/.piloop-canary/uv/bin` is not on your `PATH`: only the wrapper runs the canary.

Each variable keeps one thing apart:

| Variable | What it keeps apart |
|---|---|
| `PILOOP_HOME` | The database, the runs, the artifact store, `config.toml`, and the daemon's `serve.pid` and `serve.log`. One daemon runs for each home. |
| `UV_TOOL_DIR`, `UV_TOOL_BIN_DIR` | The installed package. uv keeps one tool named `piloop` per tool directory, and `piloop upgrade` upgrades the tool of this directory only. Keep the bin directory outside the tool directory. |
| `PILOOP_CLAUDE_SKILLS_DIR`, `PILOOP_AGENT_SKILLS_DIR` | The orchestrator skills, which `install-skills` would otherwise put over the release's in `~/.claude/skills` and `~/.agents/skills`. |
| `PILOOP_HERDR_SESSION` | The Herdr session of the agents, for projects with `runtime = "herdr"`. |
| `PILOOP_CLI` | The command that `piloop-canary doctor` names in what it tells you, or an orchestrator, to run. |

The canary's agents run the canary's piloop: the daemon gives each agent `PILOOP_BIN`, its own `piloop` under its
home, and puts that directory first on the agent's `PATH`.

Use `piloop-canary` wherever you would use `piloop`, on a separate clone of the project repo: two daemons that run the
same project name in one repo would write the same `refs/piloop/` refs. To orchestrate a canary campaign, tell your
agent to read `~/.piloop-canary/skills/piloop-operate/SKILL.md` and to run `piloop-canary` wherever it says `piloop`.

```bash
piloop-canary init <clone>
piloop-canary serve --detach
```

To keep its daemon running, write its service unit under a name of its own. The unit carries the wrapper's variables.
On macOS, the launchd label of a home other than `~/.piloop` ends with the home's name.

```bash
piloop-canary service-unit > ~/.config/systemd/user/piloop-canary.service  # Linux
piloop-canary service-unit > ~/Library/LaunchAgents/dev.piloop.serve.piloop-canary.plist  # macOS
```

`piloop-canary upgrade` installs the latest `main` (or reinstalls the checkout), installs its skills into
`~/.piloop-canary/skills`, and restarts the canary's daemon in place. The release and its daemon do not change.

## Run the checks

```bash
make
```

`make` formats the code, runs the linter (ruff), runs the type checker (ty), runs the tests with coverage, and
regenerates the generated files. Coverage must be 100%. The tests run in parallel, one process for each logical CPU
([pytest-xdist](https://pytest-xdist.readthedocs.io)). While you work, skip coverage with `make test`, or run only the
tests that you need:

```bash
make test
COLUMNS=150 uv run pytest -n logical tests/daemon
COLUMNS=150 uv run pytest tests/scoring
```

On a machine that other jobs share, set `PYTEST_XDIST_AUTO_NUM_WORKERS` (for example to 4) to start fewer processes.

inline-snapshot cannot fix a snapshot in a parallel run: run that test without `-n` and with `--inline-snapshot=fix`.

Keep the suite fast:

- Take a database from the `db` fixture, or copy one with `at_head` from `tests/conftest.py`. Both copy a database
  that is migrated once per session. Migrating from scratch costs about a second.
- Wait for a condition with `wait_until`, never with a fixed sleep or a loop that goes on after its deadline.
- To test a timeout of the code, give it a fake clock that a sleep advances, as `FakeClock` in
  `tests/runtime/test_herdr.py` does, rather than a short real timeout: that one is slow alone and flaky under load.

The tests marked `herdr` need a local [Herdr](https://herdr.dev). They are skipped when `herdr` is not on your
`PATH`.

Other targets: `make format`, `make lint`, `make typecheck`, `make test` (no coverage), `make testcov`, `make docs`
and `make docs-check`. CI runs the pre-commit hooks, `make testcov` on Linux, and `install.sh` with the enwik8
example. In `hiloopai/piloop`, not in a fork, CI also runs `make test` on macOS.

## The smoke campaign

The tests use fake harnesses. The smoke campaign runs a tiny real campaign, with real agents, on the bundled enwik8
example. Run it after you merge a change to the daemon, the runner, the scheduler, a runtime or the schema:

```bash
uv run -m scripts.smoke_campaign
```

It needs `claude` on the `PATH`, logged in, and network access. It installs piloop from your checkout into a new
temporary directory, with its own `PILOOP_HOME` and skills directories, so it never touches `~/.piloop`, a running
daemon or your skills. It pauses the project, adds three ideas, resumes, cancels the first run, lets the second
finish and be judged, then stops the campaign while the third runs. Each check prints one `pass` or `FAIL` line: the
runs, the verdicts, `piloop eval queue`, `piloop pause` and `resume`, `piloop events --after … --json` read page by
page, `piloop status --json`, `piloop run cancel`, `piloop candidate remeasure`, `piloop project stop`,
`piloop candidate verdict`, `piloop measurement add`, no `anomaly.*` event, no process left behind, and
`piloop doctor`. The exit code is 1 when a check fails.

One run takes about 3 minutes on `claude-haiku-4-5-20251001`, and its three agents spend about 0.50 USD at API
prices, about 1.8 million tokens, most of them cache reads. `--model` picks another model, and `--harness` another
harness.

The report is `report.md` in the temporary directory (`--report <path>` writes a copy). It has one row per check,
with its time and the evidence of a failure, then one row per agent, with its run, how it ended, its tokens and its
cost. Next to it are the daemon's log (`home/serve.log`) and each agent's output (`home/runs/<run>`). The directory
stays for these logs: remove it when you are done. Before you report a failure as a bug, check that the agent did what
its idea asked: a failed check can be an agent that did not commit.

To add a check, add a function and its entry to `CHECKS` in `scripts/smoke_campaign.py`.

## Generated files

`make docs` runs `uv run -m scripts.generate_all`, which writes these files from the code. Never edit them by hand:

- `docs/reference/configuration.mdx`, from the models that `piloop.config.ProjectConfig` reaches (in
  `piloop/config.py`, `piloop/harnesses.py`, `piloop/mcp/` and `piloop/usage/`), from `piloop/deprecated.py`, and from
  the header in `scripts/generate/configuration.py`.
- `docs/reference/schema.mdx`, from `piloop/db/models.py`.
- `piloop/db/actor_triggers.sql`, from `piloop/db/triggers/`.
- `docs/snippets/generated/`: the generated parts of other pages. These are the diagram of the tables, the values of
  the state enums and the graph of the imports, and the tables of the built-in runtimes, triggers, harnesses, role
  keys and defaults, `Runtime` methods and `[stop]` keys. They also include the environment variables of the settings
  classes, and how each harness takes a message under Herdr. They come from the docstrings and the built-in
  registries, so write those docstrings for the reader of the page. A missing docstring stops `make docs`.

Each file starts with a comment that names the command and the source to change. A page shows a generated part as a
[Mintlify snippet](https://www.mintlify.com/docs/create/reusable-snippets): it imports the file, then uses it as a component.

```text
import Runtimes from '/snippets/generated/runtimes.mdx';

<Runtimes />
```

One module of `scripts/generate/` makes each file: its `generate()` returns the full content, and `PATH` is the file.
To add a generated file, add a module and list it in `scripts/generate_all.py`.

`make docs-check` (`--mode check`) fails on the first stale file and shows its diff. The pre-commit hook `docs` runs it
on every commit, and CI runs the hooks on every file.
`--mode dry-run` prints every file and writes nothing.

## Documentation

The documentation site is in `docs/`. It is a [Mintlify](https://mintlify.com) site: `docs/docs.json` and one `.mdx`
file for each page. To preview it, with Node.js installed:

```bash
cd docs
npx mint dev
npx mint broken-links --check-anchors
```

Write docs, skills and agent prompts in Simplified Technical English: one idea per sentence, most sentences 20 words or
fewer, active voice, present tense, and the imperative for instructions. Use piloop's own terms with one meaning each.
Write no history: no "used to", no PR numbers, no dates.

The docs describe what piloop does now. A design proposal is not a page: it is discussed in an issue, and the change
that builds it documents it here.

In `.mdx` files, `{`, `}`, `<` and `>` in text are syntax. Put them in code spans, or escape them.

`tests/test_docs.py` checks every code block in `docs/`, `README.md`, `AGENTS.md`, this file and the skills. A TOML
block must be a valid `piloop.toml`, or a part of one. Every `piloop` command in a shell block must parse against the
CLI. Each SQL statement, also in `piloop sql`, must compile on the current schema. Write a placeholder as `<name>` or
`…`: the test replaces it with `1`. Give every code block a language. The test fails on a language that it does not
know.

## Change the database schema

1. Change the models in `piloop/db/models.py`.
2. Create a new Alembic revision:

   ```bash
   uv run alembic revision --autogenerate -m "add the foo column to ideas"
   ```

3. Read the generated revision and fix what autogenerate missed.
4. Run `make docs`.

Never edit an existing revision. Users have databases at every revision, and piloop migrates them when it starts. A
batch migration that rebuilds a table drops its triggers, so call `piloop.db.triggers.recreate` after it.

## Deprecate a configuration key

To rename or remove a key of `piloop.toml`, add a bridge in `piloop/deprecated.py`. The module docstring explains
how. The old key then keeps working, with a warning, for at least one minor release. The release notes list it under
`Deprecated`, so say in the commit subject that the key is deprecated.

## Code style

- Python 3.14, single quotes, lines of at most 120 characters, Google-style docstrings.
- No new `Any` (the existing uses type untyped JSON), no `# type: ignore` or `# ty: ignore`, no new `noqa`, and no
  weakened test or type to make something pass. Fix the cause instead.
- Never swallow an error. Re-raise with `from e`, and say what failed and what to do. A broad `except` needs a comment
  that says why.
- No `logging`. Emit spans and span events with `piloop.otel`.
- Comments explain a reason that is not obvious, a gotcha or a TODO. They do not repeat the code.

## Package layout

The package has one subpackage for each concern. Imports only point down this list:

1. `db/`: storage, the SQL gate for agents, the events feed. `store/`: the artifact store, its blobs, manifests
   and remote backends.
2. `mcp/`, `usage/`, and `config.py` with `harnesses.py`: MCP servers, token usage, the project configuration.
3. `runtime/`: where the agent processes run (a background process, or a Herdr pane).
4. `scoring/`: the evaluate command, the ledger, the verdict.
5. `agents/`: the roles and their prompts, the agent lifecycle, session capture.
6. `campaign/`: projects, ideas, the inbox, stop conditions, the spend limit.
7. `reports/`: status, history, the research pack.
8. `daemon/`: the process, the scheduler, the run executor.
9. `watch/` and `cli/`.

The top-level modules `settings`, `otel`, `git`, `proc`, `text`, `version` and `deprecated` are leaves that every
subpackage may use.

## Send a change

- Open an issue first for a large change, so that we can agree on the approach.
- Use [Conventional Commits](https://www.conventionalcommits.org/): `fix: keep the head when a merge conflicts`,
  `feat(watch): filter the agents panel`.
- Write the subject as one sentence that a user understands: the release notes come from the commit subjects.
- A bug fix comes with a test that fails without the fix.
- Keep the design simple: no second way to do something that exists, no retry loop that hides a design flaw, and no
  flag or knob without a clear user.
- The repository is public. Write no customer or company-internal name, private repository, internal hostname, home
  path, private ticket, key or token, also not in test data.
- Make sure that `make` passes before you open the pull request.

## Release

Maintainers only.

1. Set `version` in `pyproject.toml`, and run `uv lock`.
2. Write the release notes in `CHANGELOG.md`. List the subjects of the commits since the last tag:

   ```bash
   git log --reverse --format='- %s' "$(git describe --tags --abbrev=0)"..main
   ```

   Add a section for the version and the date under `[Unreleased]`. Sort the lines into `Added`, `Changed`,
   `Deprecated`, `Removed` and `Fixed`, remove the type prefixes and the PR numbers, and remove the lines that a user
   cannot see (`test:`, `ci:`, `chore:`). Move the lines under `[Unreleased]`, if any, into the new section, and drop
   the commits that they describe. Update the links at the bottom.
3. Merge that change, then tag the commit on `main`: `git tag v0.2.0 && git push origin v0.2.0`.

The `Release` workflow runs CI, checks that the tag matches the version, creates the GitHub release with the
changelog section as its notes, and publishes the package to PyPI.
