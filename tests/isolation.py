"""The environment of the test session: a temporary HOME and PILOOP_HOME, and none of the caller's piloop or Herdr.

``tests/conftest.py`` imports this first, because piloop reads its home when it is imported (``piloop.settings``).
Every process a test starts inherits it, and ``PILOOP_UNDER_TEST`` makes each piloop among them refuse the home of a
real install (``piloop.settings.UNDER_TEST_ENV``).
"""

import atexit
import os
import re
import shutil
import tempfile
from pathlib import Path

CALLER_ENV = dict(os.environ)
"""The environment the session started with. The e2e campaigns start from it: their agents need the real HOME to log
in."""
DROPPED = re.compile(r'^(PILOOP_|HERDR_|XDG_(CONFIG|DATA|STATE)_HOME$|CODEX_HOME$|UV_TOOL_DIR$)')
"""The caller's settings of a real install: piloop's, the Herdr session it runs in, and the directories under the home
that piloop, Herdr, Codex and uv would otherwise read."""

ROOT = Path(tempfile.mkdtemp(prefix='piloop-tests-'))
atexit.register(shutil.rmtree, ROOT, ignore_errors=True)
(ROOT / 'home').mkdir()
for key in [k for k in os.environ if DROPPED.match(k)]:
    del os.environ[key]
os.environ.update(
    {
        'HOME': str(ROOT / 'home'),
        'CLAUDE_CONFIG_DIR': str(ROOT / 'home' / '.claude'),
        'PILOOP_HOME': str(ROOT / 'piloop'),
        'PILOOP_AGENT_SKILLS_DIR': str(ROOT / 'skills' / 'agents'),
        'PILOOP_UNDER_TEST': '1',
    }
)
# PILOOP_CLAUDE_SKILLS_DIR stays unset: its default, under CLAUDE_CONFIG_DIR, is what the skills tests check.
