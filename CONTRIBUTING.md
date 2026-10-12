# Contributing to piloop

Thank you for your interest in piloop. This file explains how to set up a development checkout, run the checks and
send a change.

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

## Run the checks

```bash
make
```

`make` formats the code, runs the linter (ruff), runs the type checker (ty), runs the tests with coverage, and
regenerates the generated files. Coverage must be 100%. The full test suite takes several minutes. While you work,
run only the tests that you need:

```bash
COLUMNS=150 uv run pytest tests/scoring
```

The tests marked `herdr` need a local [Herdr](https://herdr.dev). They are skipped when `herdr` is not on your
`PATH`.

Other targets: `make format`, `make lint`, `make typecheck`, `make test` (no coverage), `make testcov`, `make docs`
and `make docs-check`. CI runs the pre-commit hooks and `make testcov` on Linux and macOS.

## Generated files

`make docs` writes these files from the code. Never edit them by hand:

- `docs/reference/configuration.mdx`, from `piloop/config.py`.
- `docs/reference/schema.mdx`, from `piloop/db/models.py`.
- `piloop/db/actor_triggers.sql` and `piloop/db/persistent_triggers.sql`, from `piloop/db/triggers/`.

`make docs-check` fails when one of them is stale. The pre-commit hook runs it.

## Documentation

The documentation site is in `docs/`. It is a [Mintlify](https://mintlify.com) site: `docs/docs.json` and one `.mdx`
file for each page. To preview it, with Node.js installed:

```bash
cd docs
npx mint dev
npx mint broken-links --check-anchors
```

In `.mdx` files, `{`, `}`, `<` and `>` in text are syntax. Put them in code spans, or escape them.

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
how. The old key then keeps working, with a warning, for at least one minor release. Add a line under `Deprecated` in
the changelog.

## Code style

- Python 3.14, single quotes, lines of at most 120 characters, Google-style docstrings.
- No `Any`, no `# type: ignore`, no new `noqa`. Fix the cause instead.
- No `logging`. Emit spans and span events with `piloop.otel`.
- Comments explain a reason that is not obvious, a gotcha or a TODO. They do not repeat the code.

## Package layout

The package has one subpackage for each concern. Imports only point down this list:

1. `db/`: storage, the SQL gate for agents, the events feed.
2. `mcp/`, `usage/`, and `config.py` with `harnesses.py`: MCP servers, token usage, the project configuration.
3. `runtime/`: where the agent processes run (Herdr or a subprocess).
4. `scoring/`: the evaluate command, the ledger, the verdict.
5. `agents/`: the roles and their prompts, the agent lifecycle, session capture.
6. `campaign/`: projects, ideas, the inbox, stop conditions, the spend limit.
7. `reports/`: status, history, the research pack.
8. `daemon/`: the process, the scheduler, the run executor.
9. `watch/` and `cli/`.

The top-level modules `settings`, `otel`, `git`, `proc`, `text` and `deprecated` are leaves that every subpackage may
use.

## Send a change

- Open an issue first for a large change, so that we can agree on the approach.
- Use [Conventional Commits](https://www.conventionalcommits.org/): `fix: keep the head when a merge conflicts`,
  `feat(watch): filter the agents panel`.
- Add a line under `[Unreleased]` in [CHANGELOG.md](CHANGELOG.md) for every change that a user can see.
- Make sure that `make` passes before you open the pull request.

## Release

Maintainers only.

1. Set `version` in `pyproject.toml`, and run `uv lock`.
2. In CHANGELOG.md, rename `[Unreleased]` to the version and the date, add a new empty `[Unreleased]`, and update the
   links at the bottom.
3. Merge that change, then tag the commit on `main`: `git tag v0.2.0 && git push origin v0.2.0`.

The `Release` workflow runs CI, checks that the tag matches the version, creates the GitHub release with the
changelog section as its notes, and publishes the package to PyPI.
