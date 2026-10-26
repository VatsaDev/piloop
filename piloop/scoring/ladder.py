"""The stops of the evaluation ladder: whether a climb stops at a rung, and why."""

from pydantic import BaseModel, ConfigDict

from piloop.config import Rung, Tolerance
from piloop.ids import RungName
from piloop.plugins.metric import Direction, number
from piloop.plugins.verdict_rule.contract import Criterion


class Gate(BaseModel):
    """What the stops of the ladder compare with: the bar's mean metrics at each rung, and the metrics' directions."""

    model_config = ConfigDict(frozen=True)

    bar: dict[RungName, dict[str, float]]
    """The bar's mean of each metric at each rung. A rung that it lacks stops only on a ``floor``."""
    directions: dict[str, Direction]
    """Whether a larger or a smaller value of each metric is better."""

    def stop_reason(self, rung: Rung, metrics: dict[str, float]) -> str | None:
        """Why the climb stops at ``rung`` with these ``metrics`` (``stop_reason``), or None to go on."""
        return stop_reason(rung, metrics, self.bar.get(rung.name), self.directions)


def stop_reason(
    rung: Rung, metrics: dict[str, float], bar: dict[str, float] | None, directions: dict[str, Direction]
) -> str | None:
    """Return why the climb stops at ``rung``, with these ``metrics`` against the ``bar``'s there, or None to go on.

    The stops are tried in their order in ``[[ladder]]``. A ``margin`` or ``fraction`` passes while the bar has no value
    at the rung: a climb never stops on missing evidence. Each metric that ``stop`` names is in ``metrics``.
    """
    for name, test in rung.stop.items():
        theirs = None if bar is None else bar.get(name)
        if (why := _reason(name, test, metrics[name], theirs, directions[name])) is not None:
            return why
    return None


def _reason(name: str, test: Tolerance, mine: float, theirs: float | None, direction: Direction) -> str | None:
    """Why ``test`` stops a climb on the metric ``name``, or None: the verdict's own reading of a tolerance."""
    tolerance = Criterion(name, direction, test.margin, test.floor, test.fraction)
    if test.floor is not None and tolerance.past_floor(mine):
        return f'{name} {number(mine)} past its floor {number(test.floor)}'
    if theirs is None or not test.regression or direction.gain(mine, theirs) >= -tolerance.allowed(theirs):
        return None
    return f'{name} {number(mine)} vs {number(theirs)} ({number(mine - theirs, signed=True)}; {tolerance.limit()})'
