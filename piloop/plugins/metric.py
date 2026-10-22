"""The metric types that the verdict and stop contracts share."""

from enum import StrEnum


class Direction(StrEnum):
    """Whether a larger or a smaller value of a metric is better."""

    HIGHER = 'higher'
    LOWER = 'lower'

    def better(self, a: float, b: float) -> bool:
        """Whether ``a`` is better than ``b``."""
        return a > b if self is Direction.HIGHER else a < b

    def gain(self, a: float, b: float) -> float:
        """How much better ``a`` is than ``b``, in the metric's units. Positive when ``a`` is better."""
        return a - b if self is Direction.HIGHER else b - a


def number(value: float, *, signed: bool = False) -> str:
    """Format a value in a reason: whole numbers in full (``3688770``), others to 4 significant digits (``0.8312``).

    The format of ``piloop.text.metric``, which a rule cannot import. A reason is stored, so its format does not
    follow a change of how the renderers show a metric. ``signed`` adds the sign of a delta (``+12``).
    """
    sign = ('+' if value >= 0 else '-') if signed else ('-' if value < 0 else '')
    size = abs(value)
    if size.is_integer() or size >= 1e4:
        return f'{sign}{size:.0f}'
    return f'{sign}{size:.4g}'
