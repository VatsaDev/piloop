#!/bin/sh
# piloop installer: uv, Entire, the piloop CLI and the orchestrator skills. It needs no sudo and is safe to re-run.
# Upgrade later with `piloop upgrade`. Entire has its own installer: run the curl line below again to upgrade it.
#
#   curl -fsSL https://raw.githubusercontent.com/hiloopai/piloop/main/install.sh | sh
#
# PILOOP_SRC=<checkout path or pip-style URL> installs piloop from there instead of from PyPI (local use, CI).
set -eu
# pipefail is not POSIX. Enable it where the shell has it.
(set -o pipefail) 2>/dev/null && set -o pipefail

PACKAGE='piloop'  # from PyPI
UV_INSTALL_URL='https://astral.sh/uv/install.sh'
ENTIRE_INSTALL_URL='https://entire.io/install.sh'

say() { printf '\033[1mpiloop install:\033[0m %s\n' "$*"; }

export PATH="$HOME/.local/bin:$PATH"

if ! command -v uv >/dev/null 2>&1; then
    say "installing uv"
    curl -LsSf "$UV_INSTALL_URL" | sh
fi

if ! command -v entire >/dev/null 2>&1; then
    say "installing Entire (agent session capture)"
    curl -fsSL "$ENTIRE_INSTALL_URL" | bash
fi

if [ -n "${PILOOP_SRC:-}" ] && [ -d "$PILOOP_SRC" ]; then
    PILOOP_SRC=$(cd "$PILOOP_SRC" && pwd)
else
    PILOOP_SRC=${PILOOP_SRC:-$PACKAGE}
fi

say "installing the piloop CLI from $PILOOP_SRC"
uv tool install --reinstall --quiet "$PILOOP_SRC"

say "installing the orchestrator skills for each harness on PATH (Claude Code, Codex, pi, Gemini CLI, Cursor)"
piloop install-skills

say "done. Try the example:  piloop init --example enwik8 ~/piloop-enwik8"
say "or, in your own repo, start claude (or codex, or pi) and ask: set up an autoresearch campaign for this repo"
say "upgrade later with:  piloop upgrade"
