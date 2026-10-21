# Contributing to piloop

This file explains how to set up a development checkout, run the checks and send a change.

## Set up

You need git 2.40 or later, [uv](https://docs.astral.sh/uv/) and `make`.

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
second piloop, `piloop-canary`, with a home of its own. The home must be separate: the canary's daemon and
`piloop-canary migrate` migrate the database of its home, and the release then refuses to open that database until it
knows the schema.

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
| `PILOOP_HERDR_SESSION` | The Herdr session of the agents, for campaigns with `runtime = "herdr"`. |
| `PILOOP_CLI` | The command that `piloop-canary doctor` names in what it tells you, or an orchestrator, to run. |

The canary's agents run the canary's piloop: the daemon gives each agent `PILOOP_BIN`, its own `piloop` under its
home, and puts that directory first on the agent's `PATH`.

Use `piloop-canary` wherever you would use `piloop`, on a separate clone of the campaign repo: two daemons that run the
same campaign name in one repo would write the same `refs/piloop/` refs. To orchestrate a canary campaign, tell your
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

Other targets: `make format`, `make lint`, `make typecheck`, `make test` (no coverage), `make testcov`, `make docs`,
`make docs-check` and `make e2e` (see below). CI runs the pre-commit hooks, `make testcov` on Linux, and `install.sh`
with the enwik8 example. In `hiloopai/piloop`, not in a fork, CI also runs `make test` on macOS.

## The end-to-end tests

The tests use fake harnesses. The tests in `tests/e2e/` run small real campaigns with real agents, on the bundled
examples. Run them after a change to the daemon, the runner, the scheduler, a runtime or the schema:

```bash
make e2e
```

These tests carry the `e2e` marker. The pytest configuration leaves this marker out (`-m "not e2e"`), so `make`,
`make test` and CI never run them. They need `claude` on the `PATH`, logged in, and network access. The lifecycle
scenario (`tests/e2e/test_lifecycle.py`) takes about 3 minutes on `claude-haiku-4-5-20251001`. Its three agents use
about 1 million tokens, most of them cache reads. That is about 0.3 USD at API prices. The upgrade scenario
(`tests/e2e/test_upgrade.py`) installs the last release from PyPI, runs a small campaign on it, upgrades to your
checkout while an agent works, and checks that no row, ref, commit or file was lost. It takes about 13 minutes and
0.5 USD, with Claude only. To pick another harness or model:

```bash
uv run pytest -m e2e tests/e2e -rA --e2e-harness claude --e2e-model <model>
```

The tests install piloop from your checkout once per session, into a new temporary directory. Each campaign there has
its own `PILOOP_HOME` and skills directories, so the tests never touch `~/.piloop`, a running daemon or your skills.
A campaign is configured as a user configures one: its keys are in the campaign's `piloop.toml`, and the daemon's
settings are in the `config.toml` of its home. On every exit, also on Ctrl-C and SIGTERM, the fixture stops each
daemon. Then it kills every process that carries the campaign's home or the token of an agent.

At the end, pytest prints the log of each daemon (`home/serve.log`). It also prints one row for each agent: its run,
how it ended, its tokens and its cost. The output of each agent is in `home/runs/<run>`. The temporary directory stays
for these logs. Remove it when you are done. A failed test can be an agent that did not do what its idea asked. Read
the output of the agent before you report a bug.

### Add an e2e scenario

A scenario is one test module in `tests/e2e/`, with `pytestmark = pytest.mark.e2e`:

1. Write a module-scoped `campaign` fixture that calls `make_campaign`. Give it `config`, the keys of `piloop.toml`
   that the scenario changes, and `home_config`, the keys of `config.toml`. `template` names a bundled example; the
   default is `enwik8`.
2. Write each step as a test, in the order of the campaign. A test reads the state that earlier tests leave on the
   `Campaign` (`tests/e2e/campaign.py`).
3. Mark a test with `@needs(...)` to name the earlier tests that it reads. When one of them did not pass, pytest skips
   the test and gives the reason.

To add a check to a scenario, add a test at the step where its state exists.

A scenario that runs another piloop than the checkout, such as the release that `tests/e2e/test_upgrade.py` upgrades
from, makes its campaign itself, and adds its lines to the summary with the `e2e_summary` fixture. When a release
renames a table, a column or a value, add the rename to `RENAMES` in that module, as the upgrading guide lists it.

## Generated files

`make docs` runs `uv run -m scripts.generate_all`, which writes these files from the code. Never edit them by hand:

- `docs/reference/configuration.mdx`, from the models that `piloop.config.CampaignConfig` reaches (in
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

Never edit an existing revision. Users have databases at every revision, and the daemon migrates them when it starts. A
batch migration that rebuilds a table drops its triggers, so call `piloop.db.triggers.recreate` after it.

## Mark compat code

Code that keeps an old name or an old behavior working gets a marker with the release that deletes it. Put it on a
function or a class, or call it where the code is:

```python
from piloop import deprecated


@deprecated.until('0.3.0', '[roles.research] context is the top-level context: delete the research_context bridge')
def research_context(toml, raw): ...


if not handle.data.get('tag'):
    deprecated.until('0.3.0', 'a handle that 0.1.0 started has no tag: delete this branch')
```

Write both arguments as literal strings, and the version as `X.Y.Z`. Mark the definition of an old name or constant:
once it is deleted, the type checker finds each use. `tests/test_deprecated.py` reads every marker in `piloop/`, also
in a branch that never runs. It fails once the version in `pyproject.toml` reaches a
marker's release, and lists each marker's file, line and reason. So a release cannot ship until that code is deleted.
The test also fails on a malformed marker, and on a comment or docstring that says to delete code at a release
(`TODO(0.3.0)`, `remove in 0.3.0`) or a comment that calls code deprecated: mark that code instead.

## Deprecate a configuration key

To rename or remove a key of `piloop.toml`, add a bridge in `piloop/deprecated.py` and mark it. The module docstring
explains how. The old key then keeps working, with a warning, for at least one minor release. The release notes list it
under `Deprecated` when the pull request title says that the key is deprecated.

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
   and storage backends.
2. `mcp/`, `usage/`, and `config.py` with `harnesses.py`: MCP servers, token usage, the campaign configuration.
3. `runtime/`: where the agent processes run (a background process, or a Herdr pane).
4. `scoring/`: the evaluate command, the ledger, the verdict.
5. `agents/`: the roles and their prompts, the agent lifecycle, session capture.
6. `campaign/`: campaigns, ideas, the inbox, stop conditions, the spend limit.
7. `reports/`: status, history, the research pack.
8. `daemon/`: the process, the scheduler, the run executor.
9. `watch/` and `cli/`.

The top-level modules `settings`, `otel`, `git`, `proc`, `text`, `version` and `deprecated` are leaves that every
subpackage may use.

`plugins/` is everything about plugins. `plugins/host.py` loads, checks and shows them, and imports nothing else of
piloop, so every layer may use it. `plugins/contracts/` has one module for each capability: its contract, its entry
point group and where the configuration selects it. A host contract may import the types that it passes; a pure one
imports only the standard library and the host. `plugins/builtin/` has the providers of piloop that live nowhere else:
they are public, and a plugin may import one to call it. piloop declares its capabilities, and its own providers, with
entry points in `pyproject.toml`, in the same groups that a plugin uses (`piloop.capabilities`, then each provider
group). To add a capability: a contract module, a builtin module, an entry point in `piloop.capabilities`, one in
the provider group, and a line in `piloop/capabilities.py`. That module re-exports every capability, its contract types
and its built-in providers, so that a plugin imports from one place; nothing in `piloop/` imports it, and
`tests/test_capabilities.py` fails until the new line is there. The triggers and the roles declare their capability where they are defined (`triggers.py`,
`config.py`): their contract module would import them, and they import their capability. Run `uv sync` after you
change an entry point: piloop finds its own capabilities and providers in its installed metadata.

## Send a change

- Open an issue first for a large change, so that we can agree on the approach.
- Give the pull request a title that is a [Conventional Commit](https://www.conventionalcommits.org/). See
  [The pull request title](#the-pull-request-title).
- A bug fix comes with a test that fails without the fix.
- Keep the design simple: no second way to do something that exists, no retry loop that hides a design flaw, and no
  flag or knob without a clear user.
- The repository is public. Write no customer or company-internal name, private repository, internal hostname, home
  path, private ticket, key or token, also not in test data.
- Make sure that `make` passes before you open the pull request.

## The pull request title

A pull request is squashed when it merges, and its title becomes the commit subject. The release notes of the next
version come from these subjects, so write the title for a user:

```text
fix: a stopped campaign is measured again on request
feat(watch): filter the agents panel
feat!: remove the herdr_ keys of config.toml
refactor: the [roles.research.scout] table is deprecated, use [roles.scout]
```

The form is `type(scope): description`. The scope is optional. The description is one sentence that a user
understands. The type sets where the line goes in `CHANGELOG.md`:

| Type | Use it for | Release notes |
| --- | --- | --- |
| `feat` | Something new that a user can see or use, or a change to what a user sees | `Added` |
| `fix` | A bug fix that a user can see | `Fixed` |
| `perf` | Faster or cheaper, with the same behavior | `Changed` |
| `revert` | The revert of an earlier change | `Changed` |
| `refactor`, `docs`, `test`, `ci`, `build`, `style`, `chore` | A change that a user does not see | Not listed |

Some titles go to another place:

- A `feat`, `fix`, `perf`, `revert` or `refactor` whose description says `deprecated` goes under `Deprecated`.
- A breaking change whose description starts with `remove` goes under `Removed`.
- A title with the scope `ci` or `test` is not listed, unless it is breaking.

Add `!` after the type or the scope for a breaking change: a change that makes a user change a `piloop.toml`, a
script, a query or a plugin, or that drops something that a user can use. A breaking change is always listed, with
**Breaking** in front of it, and the release section tells the reader to read the upgrade guide. To say what a user
must change, end the squash commit message with a footer when you merge: `BREAKING CHANGE: <what to change>`. The
release notes add it after the line. Before 1.0, a minor release may break, so a breaking change does not need a major
version.

The `PR title` check fails on a title that is not a Conventional Commit, or whose type is not in the table. The
types are also in `cliff.toml`: change both together.

## Release

Maintainers only.

1. Fetch the tags (`git fetch --tags origin`). Set `version` in `pyproject.toml`, and run `uv lock`. The tests now
   fail on each [compat marker](#mark-compat-code) of this version: delete that code first.
   For a minor release `X.Y.0`, add the release before it to `RELEASES` in `piloop/db/session.py`, with the head
   revision that it shipped, the number of the last file that
   `git ls-tree --name-only vX.(Y-1).0 piloop/db/migrations/versions/` lists. That head is the oldest schema that this
   release migrates, so a database upgrades one minor release at a time. The test of `RELEASES` fails until it is there.
2. Write the release notes:

   ```bash
   make changelog
   ```

   [git-cliff](https://git-cliff.org) reads the commits since the last tag. It writes the section of the version, with
   the date and a link to the comparison with the last release, at the top of `CHANGELOG.md`. It sorts the commits
   into `Added`, `Changed`, `Deprecated`, `Removed` and `Fixed`, as `cliff.toml` says, and stops on a commit subject
   that is not a Conventional Commit of an allowed type.

   Then edit the section by hand. Correct the wording, join the lines that describe one change, and move a line to a
   better group. If `CHANGELOG.md` still has an `[Unreleased]` section, move its entries into the new section, remove
   the generated lines that repeat them, and remove the heading and its link at the bottom.

   The section compares the release with the last release, not with each commit. git-cliff marks every `!` commit
   **Breaking**, but a change to something that the last release did not have breaks no user of a release. Describe
   that thing as it ships, without **Breaking**, and list its old names in the upgrading guide, for the users of
   `main`. Each **Breaking** line needs its section in `docs/guides/upgrading.mdx`.
3. Check that the `Release` workflow finds the section:

   ```bash
   awk -v v="$(uv version --short)" '$0 ~ "^## \\[" v "\\]" {on=1; next} on && /^## \[/ {exit} on' CHANGELOG.md
   ```

4. Run `make` and `make e2e`. Set the date in the heading of the section to the day that you tag.
5. Open the pull request of that change from a branch named `release-<version>`, such as `release-0.2.0`, so CI
   also runs the tests on macOS. Merge it. When you merge it in the private
   mirror, wait until the `Sync main` workflow has pushed it to `main` here, and check that both repositories have the
   same commit on `main`. Then tag that commit:

   ```bash
   git fetch origin
   git log -1 origin/main
   git tag "v$(uv version --short)" origin/main
   git push origin "v$(uv version --short)"
   ```

The `Release` workflow runs CI, checks that the tag matches the version, creates the GitHub release with the
changelog section as its notes, and publishes the package to PyPI.
