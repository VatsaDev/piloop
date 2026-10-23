"""The environment of the test session: a temporary HOME and PILOOP_HOME, and none of the caller's piloop or harness
sessions.

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
"""The environment the session started with. The e2e campaigns start from it: their agents need the real harness
homes to log in."""
DROPPED = re.compile(r'^(PILOOP_|XDG_(CONFIG|DATA|STATE)_HOME$|UV_TOOL_DIR$)')
"""The caller's settings of a real install: piloop's, and the directories under the home that piloop and uv would
otherwise read."""
ROOT = Path(tempfile.mkdtemp(prefix='piloop-tests-'))


def isolate() -> None:
    atexit.register(shutil.rmtree, ROOT, ignore_errors=True)
    (ROOT / 'home').mkdir()
    for key in [k for k in os.environ if DROPPED.match(k)]:
        del os.environ[key]
    os.environ.update(
        {
            'HOME': str(ROOT / 'home'),
            'PILOOP_HOME': str(ROOT / 'piloop'),
            'PILOOP_AGENT_SKILLS_DIR': str(ROOT / 'skills' / 'agents'),
            'PILOOP_UNDER_TEST': '1',
        }
    )
    # only now: importing piloop builds its settings from the environment
    from piloop.harnesses import DEFAULT_HARNESSES, SESSION_ENV
    from piloop.settings import env_names

    homes = {name for harness in DEFAULT_HARNESSES.values() for name in env_names(harness.home or '')}
    for name in (SESSION_ENV | homes) & set(os.environ):  # a home falls back to its default under HOME
        del os.environ[name]


isolate()
