"""Short text: one line of free text that fits in a table cell, a tab title or an inbox line, markdown headings, ages."""

import re
from datetime import UTC, datetime

CELL_CHARS = 100
NONE = '—'
HEADING = re.compile(r'(#{1,6})[ \t]+(.+?)(?:[ \t]+#+)?[ \t]*$')
"""A markdown ATX heading line: its ``#`` marks (the level) and its text, without closing ``#`` marks."""


def cell(text: str, chars: int = CELL_CHARS) -> str:
    """The first line of ``text``, at most ``chars`` characters long, safe inside a markdown table row."""
    first = next(iter(text.strip().splitlines()), NONE).replace('|', '/')
    return first if len(first) <= chars else first[: chars - 1] + '…'


def metric(value: float, *, signed: bool = False) -> str:
    """A metric value for people: whole numbers in full (``3688770``), others to 4 significant digits (``0.8312``).

    Never scientific notation for an ordinary value, so a byte count keeps every digit. ``signed`` adds the sign of a
    delta (``+12``, ``-0.0041``).
    """
    sign = ('+' if value >= 0 else '-') if signed else ('-' if value < 0 else '')
    size = abs(value)
    if size.is_integer() or size >= 1e4:
        return f'{sign}{size:.0f}'
    return f'{sign}{size:.4g}'


def age(then: datetime, now: datetime) -> str:
    """Format an age as ``12s``, ``3m``, ``2h`` or ``1d``, in the largest whole unit, rounded down.

    A clock skew gives ``0s``, not a negative age.
    """
    seconds = max(0, int((now - then).total_seconds()))
    for unit, size in (('d', 86400), ('h', 3600), ('m', 60)):
        if seconds >= size:
            return f'{seconds // size}{unit}'
    return f'{seconds}s'


LINKED_CHARS = 800
"""How much of each linked comment ``piloop inbox show`` prints under an item."""


def block(text: str, chars: int = LINKED_CHARS) -> str:
    """Return whole lines of ``text`` up to ``chars`` characters, then ``… (N more lines)`` for the rest.

    If the first line is longer than ``chars``, it is cut short, so that a one-line comment always shows.
    """
    lines = text.strip().splitlines() or [NONE]
    kept: list[str] = []
    used = 0
    for line in lines:
        if used + len(line) > chars:
            break
        kept.append(line)
        used += len(line) + 1
    if not kept:
        kept = [lines[0][: chars - 1] + '…']
    omitted = len(lines) - len(kept)
    return '\n'.join([*kept, f'… ({omitted} more lines)'] if omitted else kept)


def iso(at: datetime) -> str:
    """``at`` in ISO 8601 UTC, as every document says a time."""
    return at.astimezone(UTC).isoformat(timespec='microseconds').replace('+00:00', 'Z')
