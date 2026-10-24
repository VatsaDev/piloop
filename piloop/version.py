"""Build what ``piloop --version`` prints: the package version, and the git commit when the installer recorded one."""

import hashlib
import json
import re
from importlib.metadata import distribution
from pathlib import Path
from typing import NamedTuple

SHORT_SHA = 7
_VERSION = re.compile(r'(\d+)\.(\d+)\.(\d+)(?:(?:a|b|rc)\d+)?')


class Version(NamedTuple):
    """A release, ``X.Y.Z``. Versions compare as tuples, so an older release sorts first."""

    major: int
    minor: int
    patch: int

    @classmethod
    def parse(cls, text: str) -> Version:
        """Parse ``X.Y.Z``. A pre-release, ``X.Y.Zrc1`` (``a``, ``b`` or ``rc``), counts as its release.

        Raises:
            ValueError: ``text`` is not such a version.
        """
        found = _VERSION.fullmatch(text)
        if found is None:
            raise ValueError(f'{text!r} is not a version X.Y.Z')
        major, minor, patch = (int(n) for n in found.groups())
        return cls(major, minor, patch)

    def __str__(self) -> str:
        return f'{self.major}.{self.minor}.{self.patch}'

    @property
    def line(self) -> str:
        """The minor release line, ``X.Y``: what ``uv tool install 'piloop==X.Y.*'`` installs the newest of."""
        return f'{self.major}.{self.minor}'


def describe(version: str, direct_url: str | None) -> str:
    """Return ``piloop 0.1.0 (a1b2c3d)`` when ``direct_url`` names a git commit, else ``piloop 0.1.0``.

    ``direct_url`` is the PEP 610 ``direct_url.json``. uv writes the resolved commit for a ``git+…`` install. An
    editable or directory install has no commit.
    """
    commit = json.loads(direct_url).get('vcs_info', {}).get('commit_id') if direct_url else None
    return f'piloop {version} ({commit[:SHORT_SHA]})' if commit else f'piloop {version}'


def installed() -> str:
    """Return ``describe`` for the piloop distribution that this process runs from."""
    return describe(distribution('piloop').version, direct_url())


def direct_url() -> str | None:
    """Return the PEP 610 ``direct_url.json`` of the piloop that this process runs from. None for an index install."""
    return distribution('piloop').read_text('direct_url.json')


def code_digest() -> str:
    """Return the SHA-256 of the files of the installed piloop package, without the bytecode caches.

    Two digests are equal when the code is the same, whatever the version says: a reinstall of a checkout keeps it.
    """
    root = Path(__file__).parent
    digest = hashlib.sha256()
    for path in sorted(p for p in root.rglob('*') if p.is_file() and '__pycache__' not in p.parts):
        digest.update(f'{path.relative_to(root).as_posix()}\0{path.stat().st_size}\0'.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()
