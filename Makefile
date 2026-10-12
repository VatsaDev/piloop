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
test:  ## Run tests (no coverage)
	COLUMNS=150 uv run pytest --durations=10

.PHONY: testcov
testcov:  ## Run tests with coverage; fails under 100%
	COLUMNS=150 uv run coverage run -m pytest --durations=10
	uv run coverage report

.PHONY: docs
docs:  ## Regenerate the configuration and schema reference pages and the trigger SQL from code
	uv run python scripts/gen_docs.py

.PHONY: docs-check
docs-check:  ## Fail if a generated file is stale
	uv run python scripts/gen_docs.py --check

.PHONY: all
all: format lint typecheck testcov docs
