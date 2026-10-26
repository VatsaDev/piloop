"""The daemon's API on the test's home, for the commands that are its clients."""

from collections.abc import Iterator

import pytest

from piloop.cli import common
from piloop.net.server import Server
from tests.conftest import measuring, serving_api


@pytest.fixture
def api(isolated_home: object) -> Iterator[Server]:
    """Serve the API of ``common.settings``' home on its socket, and measure each baseline, as the daemon does."""
    with serving_api(common.settings) as [server], measuring(common.settings):
        yield server
