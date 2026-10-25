"""The commands that call the API start fast: ``piloop.cli.main`` loads no database, server, git or TUI."""

import subprocess
import sys

HEAVY = ('sqlalchemy', 'alembic', 'git', 'textual', 'fastapi', 'uvicorn', 'covalence')


def test_the_cli_loads_no_heavy_dependency_until_a_command_that_needs_one_runs() -> None:
    probe = f'import sys, piloop.cli.main; print(*sorted(m for m in {HEAVY!r} if m in sys.modules))'
    loaded = subprocess.run([sys.executable, '-c', probe], capture_output=True, text=True, check=True).stdout
    assert loaded.split() == []
