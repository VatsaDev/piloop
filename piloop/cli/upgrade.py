"""Read how uv installed the CLI. ``piloop upgrade`` needs this before it changes anything."""

import tomllib
from pathlib import Path

RECEIPT = 'uv-receipt.toml'
"""The file in which uv records a tool install, next to the venv of the tool: its requirement and its entry points."""

type Requirement = dict[str, str]
"""One entry of the receipt's ``requirements``: ``name``, plus ``git``, ``directory`` or ``editable``.

An install from a package index has none of the three.
"""


def requirement(tool_dir: Path) -> Requirement:
    """Return the requirement that uv installed piloop from, read from the receipt under ``tool_dir``.

    Raises:
        SystemExit: There is no receipt, so piloop is not a uv tool. An example is a checkout run with ``uv sync``,
            which ``git pull`` upgrades.
    """
    receipt = tool_dir / 'piloop' / RECEIPT
    if not receipt.is_file():
        raise SystemExit(f'piloop is not installed as a uv tool (no {receipt}); upgrade the checkout with git pull')
    return tomllib.loads(receipt.read_text())['tool']['requirements'][0]


def source(req: Requirement) -> str:
    """Return where the CLI came from, as uv recorded it: a git URL, a local path, else a package index."""
    return next((req[key] for key in ('git', 'directory', 'editable') if key in req), 'a package index')
