"""The daemon's API on the test's home, for the commands that are its clients."""

from collections.abc import Iterator

import pytest

from piloop.cli import common
from piloop.net.server import Server
from tests.conftest import serving_api


@pytest.fixture
def api(isolated_home: object) -> Iterator[Server]:
    """Serve the API of ``common.settings``' home on its socket, as the daemon does, until the test ends."""
    with serving_api(common.settings) as [server]:
        yield server
