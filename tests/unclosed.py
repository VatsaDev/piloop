"""In CI, where each SQLite connection of the test process was opened: a test that fails on an unclosed one names it.

A connection that Python collects unclosed fails whichever test runs at that moment, with only its address. CI has
seen it now and then, and no local run has. This records the stack of every ``sqlite3.connect``, SQLAlchemy's
included, and appends the opener's stack to the report of a test that failed on one. Remove it, and its import in
``tests/conftest.py``, once that leak is found and fixed.
"""

import os
import re
import sqlite3
import sqlite3.dbapi2
import traceback
from collections.abc import Callable, Generator

import pytest

UNCLOSED = re.compile(r'unclosed database in <sqlite3\.Connection object at (0x[0-9a-f]+)>')
OPENED: dict[int, str] = {}
"""The stack that opened each connection, by its id. An id that Python reuses is recorded again by the next connect."""


def _recording(connect: Callable[..., sqlite3.Connection]) -> Callable[..., sqlite3.Connection]:
    def opened(*args: object, **kwargs: object) -> sqlite3.Connection:
        connection = connect(*args, **kwargs)
        OPENED[id(connection)] = ''.join(traceback.format_stack(limit=40)[:-1])
        return connection

    return opened


def pytest_configure(config: pytest.Config) -> None:
    if os.environ.get('CI'):
        patch, recording = pytest.MonkeyPatch(), _recording(sqlite3.dbapi2.connect)
        patch.setattr(sqlite3, 'connect', recording)
        patch.setattr(sqlite3.dbapi2, 'connect', recording)
        config.add_cleanup(patch.undo)


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item, call: pytest.CallInfo[None]
) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    report = yield
    if report.failed and (found := UNCLOSED.search(str(report.longrepr))):
        opener = OPENED.get(int(found[1], 16), 'not recorded: set CI to record where connections are opened')
        report.sections.append(('where the unclosed SQLite connection was opened', opener))
    return report
