"""Build what ``piloop --version`` prints: the package version, and the git commit when the installer recorded one."""

import json
from importlib.metadata import distribution

SHORT_SHA = 7


def describe(version: str, direct_url: str | None) -> str:
    """Return ``piloop 0.1.0 (a1b2c3d)`` when ``direct_url`` names a git commit, else ``piloop 0.1.0``.

    ``direct_url`` is the PEP 610 ``direct_url.json``. uv writes the resolved commit for a ``git+…`` install. An
    editable or directory install has no commit.
    """
    commit = json.loads(direct_url).get('vcs_info', {}).get('commit_id') if direct_url else None
    return f'piloop {version} ({commit[:SHORT_SHA]})' if commit else f'piloop {version}'


def installed() -> str:
    """Return ``describe`` for the piloop distribution that this process runs from."""
    dist = distribution('piloop')
    return describe(dist.version, dist.read_text('direct_url.json'))
