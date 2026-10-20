.DEFAULT_GOAL := all

.PHONY: install
install:  ## Install deps and pre-commit hooks
	uv sync --frozen
	uv run pre-commit install --install-hooks

.PHONY: format
format:  ## Format the code
	uv run ruff format
	uv run ruff check --fix --fix-only

.PHONY: lint
lint:  ## Lint the code
	uv run ruff format --check
	uv run ruff check

.PHONY: typecheck
typecheck:  ## Static type check
	uv run ty check

.PHONY: test
test:  ## Run tests in parallel (no coverage)
	COLUMNS=150 uv run pytest -n logical --durations=10

.PHONY: testcov
testcov:  ## Run tests in parallel with coverage; fails under 100%
	COLUMNS=150 uv run pytest -n logical --durations=10 --cov --cov-report=
	uv run coverage report

.PHONY: e2e
e2e:  ## Run the end-to-end scenarios with real agents (about 3 minutes and 0.45 USD)
	COLUMNS=150 uv run pytest -m e2e tests/e2e -rA

.PHONY: docs
docs:  ## Regenerate the generated docs and the trigger SQL from code
	uv run -m scripts.generate_all

.PHONY: docs-check
docs-check:  ## Fail if a generated file is stale
	uv run -m scripts.generate_all --mode check

.PHONY: changelog
changelog:  ## Write the section of the version in pyproject.toml into CHANGELOG.md, from the commits since the last tag
	uv run --group release git-cliff --unreleased --tag "v$$(uv version --short)" --prepend CHANGELOG.md

.PHONY: all
all: format lint typecheck testcov docs
