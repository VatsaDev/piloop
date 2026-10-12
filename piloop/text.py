"""Short text: one line of free text that fits in a table cell, a tab title or an inbox line."""

CELL_CHARS = 100
NONE = '—'


def cell(text: str, chars: int = CELL_CHARS) -> str:
    """The first line of ``text``, at most ``chars`` characters long, safe inside a markdown table row."""
    first = next(iter(text.strip().splitlines()), NONE).replace('|', '/')
    return first if len(first) <= chars else first[: chars - 1] + '…'
