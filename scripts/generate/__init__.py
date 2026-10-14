"""The parts of ``generate-all`` that every generator shares: the mode, the file header and the check.

Each module of this package generates one file. Its ``generate()`` returns the full content of the file, and ``PATH``
is where the file goes. ``scripts/generate_all.py`` runs every module in one mode.

The structure, the names and the messages follow uv's ``crates/uv-dev/src/generate_all.rs`` and
``crates/uv-dev/src/generate_json_schema.rs`` (https://github.com/astral-sh/uv, commit
fec910700fef257e02a3addc26c221c0a59d65c9, Copyright (c) 2025 Astral Software Inc.) and ruff's
``crates/ruff_dev/src/generate_all.rs`` (https://github.com/astral-sh/ruff, commit
355b42a7719d8a555e3ee50c89848b42d8a11eb5, Copyright (c) 2022 Charles Marsh). Both are licensed under the MIT
license (uv also under Apache-2.0). ``Mode`` and ``generated_file_diff`` are ports of their code.
"""

import difflib
from enum import StrEnum
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
SNIPPETS = ROOT / 'docs' / 'snippets' / 'generated'
"""The generated parts of the pages. A page imports one as a Mintlify snippet."""
REGENERATE_ALL_COMMAND = 'uv run -m scripts.generate_all'
MAX_DIFF_LINES = 100
"""Limit generated-file diffs so stale files do not overwhelm CI logs."""


class Mode(StrEnum):
    """How `generate-all` treats each generated file."""

    WRITE = 'write'
    """Update the generated files."""
    CHECK = 'check'
    """Don't write to the file, check if the file is up-to-date and error if not."""
    DRY_RUN = 'dry-run'
    """Write the generated content to stdout."""


def header(source: str) -> str:
    """Return the first line of a generated MDX file: how to regenerate it, and what to change instead."""
    return (
        f'{{/* WARNING: This file is auto-generated ({REGENERATE_ALL_COMMAND}). Update {source} if you want to change'
        ' anything here. */}\n'
    )


def generated_file_diff(current: str, generated: str) -> str:
    """Return the unified diff from ``current`` to ``generated``, cut after ``MAX_DIFF_LINES`` lines."""
    lines = list(difflib.unified_diff(current.splitlines(), generated.splitlines(), lineterm=''))[2:]
    if len(lines) > MAX_DIFF_LINES:
        lines = [*lines[:MAX_DIFF_LINES], f'… diff truncated after {MAX_DIFF_LINES} lines']
    return ''.join(f'{line}\n' for line in lines)


def emit(path: Path, generated: str, mode: Mode) -> None:
    """Print ``generated``, check that ``path`` holds it, or write it to ``path``, as ``mode`` says."""
    filename = path.relative_to(ROOT)
    if mode is Mode.DRY_RUN:
        print(generated)
        return
    current = path.read_text() if path.is_file() else None
    if current == generated:
        print(f'Up-to-date: {filename}')
    elif mode is Mode.WRITE:
        print(f'Updating: {filename}')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(generated)
    elif current is None:
        raise SystemExit(f'`{filename}` not found, please run `{REGENERATE_ALL_COMMAND}`')
    else:
        diff = generated_file_diff(current, generated)
        raise SystemExit(f'`{filename}` changed, please run `{REGENERATE_ALL_COMMAND}`:\n{diff}')
