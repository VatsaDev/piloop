"""The commands that call the API start fast: their modules load no database, server, git or TUI."""

import subprocess
import sys

import pytest

HEAVY = ('sqlalchemy', 'alembic', 'git', 'textual', 'fastapi', 'uvicorn', 'covalence')


@pytest.mark.parametrize(
    'module',
    [
        'main',
        'views',
        'control',
        'inbox',
        'idea',
        'candidate',
        'measurement',
        'evaluation',
        'limits',
        'token',
        'memory',
    ],
)
def test_a_client_command_loads_no_heavy_dependency(module: str) -> None:
    probe = f'import sys, piloop.cli.{module}; print(*sorted(m for m in {HEAVY!r} if m in sys.modules))'
    loaded = subprocess.run([sys.executable, '-c', probe], capture_output=True, text=True, check=True).stdout
    assert loaded.split() == []
