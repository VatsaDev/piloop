"""Short text: one line of free text that fits in a table cell, a tab title or an inbox line."""

CELL_CHARS = 100
NONE = '—'


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
