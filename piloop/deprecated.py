"""Deprecated ``piloop.toml`` keys. A deprecated key still loads, with a warning, until the release that removes it.

To deprecate a key, add a bridge to ``BRIDGES``. A bridge is a function that takes the file path and the parsed TOML,
changes the old key to the new form in place and calls ``warn``. Its docstring is its entry under "Deprecated keys" in
the generated configuration reference (``docs/reference/configuration.mdx``). Add a ``Deprecated`` line to CHANGELOG.md.

To remove a bridge, delete its function, its ``BRIDGES`` entry and its tests, run ``make docs`` and move its
CHANGELOG.md line to ``Removed``. The old key then fails as an unknown key.
"""

import warnings
from collections.abc import Callable
from pathlib import Path

Bridge = Callable[[Path, dict[str, object]], None]

# Where the ``default`` action records the warnings it showed, so a file loaded twice in one process warns once.
_shown: dict[str | tuple[str, type[Warning], int], int] = {}


class PiloopDeprecationWarning(DeprecationWarning):
    """A deprecated piloop key that still works until the release that removes it. The CLI shows each one once."""


BRIDGES: tuple[Bridge, ...] = ()


def show_in_cli() -> None:
    """Show each deprecation once. By default, Python hides a ``DeprecationWarning`` outside ``__main__``."""
    warnings.simplefilter('default', PiloopDeprecationWarning)


def warn(toml: Path, key: str, message: str, *, removed_in: str, nth: int = 0) -> None:
    """Warn at the line of ``toml`` that sets ``key`` for the ``nth`` time, or at line 0 if no line sets it.

    The output looks like a compiler error: ``piloop.toml:12: PiloopDeprecationWarning: …``, then the line.
    """
    lines = [n for n, line in enumerate(toml.read_text().splitlines(), 1) if line.partition('=')[0].strip() == key]
    lineno = lines[nth] if nth < len(lines) else 0
    text = f'{message}; removed in piloop {removed_in}'
    warnings.warn_explicit(text, PiloopDeprecationWarning, str(toml), lineno, registry=_shown)


def bridge(toml: Path, raw: dict[str, object], bridges: tuple[Bridge, ...] = BRIDGES) -> None:
    """Run every bridge on the parsed file. ``config.load_project_config`` calls this before it validates the file."""
    for b in bridges:
        b(toml, raw)
