"""Facts: what a campaign knows about where it is, as typed keys that its fact sources supply.

A ``Fact`` is a typed key, as a ``contextvars.ContextVar`` is: its name, and the type of its value in its parameter.
The campaign's fact sources (``[[facts]]``, ``piloop.plugins.fact_source.contract``) supply them. A stop rule or a
section names the facts it reads and gets ``Facts``: those values, and None for one that its source does not know yet.

This module imports only the Python standard library, so a pure provider can import it. It also has the keys of
piloop's own sources, so that a rule names them once: those of ``campaign`` (``CAMPAIGN``) and of ``optimization``
(``OPTIMIZATION``), and ``reading`` and ``since_keep`` for the values that a source such as ``command`` reads.
"""

from dataclasses import dataclass, field
from typing import cast


@dataclass(frozen=True)
class Funnel:
    """ideas → attempts → reached the evaluator → measured → keeps: each stage a part of the one before, but the ideas."""

    ideas: int
    attempts: int
    """The attempts that ended, with their commits judged."""
    reached: int
    """Those that left a candidate that is measured or waits for its verdict."""
    measured: int
    """Those with a candidate that has a measurement without an error."""
    keeps: int
    """Those with a kept candidate."""
    unmeasured: dict[str, int]
    """The attempts that were not measured, by how their run ended (``runs.end``), the most first."""


type FactValue = bool | int | float | str | dict[str, float] | Funnel | None
"""The value of a fact: data that any reader, a pure provider in another process too, can take."""


@dataclass(frozen=True)
class Fact[T: FactValue]:
    """A typed key: ``Fact[float]('spend', '…')``. Two facts are the same when their names are."""

    name: str
    doc: str = field(compare=False)
    """What the value is, for the docs and for the error of a rule that reads a fact that nothing supplies."""


@dataclass(frozen=True)
class Facts:
    """The values of the facts that a reader asked for, by name. A fact that its source does not know is None."""

    values: dict[str, FactValue] = field(default_factory=dict)

    def get[T: FactValue](self, fact: Fact[T]) -> T | None:
        """The value of ``fact``, or None."""
        # a source returns the type of each fact that it supplies (``FactSource.gather``)
        return cast(T | None, self.values.get(fact.name))


HEAD = Fact[float]('head', 'The primary metric of the first track at the head, under the current evaluator.')
RUNS = Fact[int](
    'runs', 'The runs that hold a place under a limit on runs: those that count, and the attempts that may still count.'
)
UNSETTLED = Fact[int]('unsettled', 'The attempts whose outcome can still change: queued, running, or being scored.')
HOURS = Fact[float]('hours', 'The hours since the first run started, without the time that the campaign was paused.')
TOKENS = Fact[int]('tokens', 'The tokens that the agents of the campaign spent, summed over the agents that ended.')
WITHOUT_KEEP = Fact[int](
    'without_keep',
    'The finished runs that count since the latest keep, or since the campaign was last set active again.',
)
KEEPS = Fact[int](
    'keeps', 'The candidates of runs that are kept, on the trunk or on a branch. The baseline is not one.'
)
EVALUATIONS = Fact[int](
    'evaluations',
    'The runs of the evaluate command under every evaluator: each measurement, failed ones and bar samples among them.',
)
METRIC_SUMS = Fact[dict[str, float]](
    'metric_sums', 'Each metric summed over every measurement. A rule that reads a cost metric names it.'
)
ATTEMPTS = Fact[int](
    'attempts', 'The runs that are an attempt at their idea: the open ones, and those whose end is one.'
)
FUNNEL = Fact[Funnel]('funnel', 'How far the attempts that ended and are judged got (``Funnel``).')
CAMPAIGN: tuple[Fact[FactValue], ...] = (HOURS, TOKENS)
"""The facts that the ``campaign`` source supplies: those of any campaign."""
OPTIMIZATION: tuple[Fact[FactValue], ...] = (
    HEAD,
    RUNS,
    UNSETTLED,
    WITHOUT_KEEP,
    KEEPS,
    EVALUATIONS,
    METRIC_SUMS,
    ATTEMPTS,
    FUNNEL,
)
"""The facts that the ``optimization`` source supplies: those of the research of an optimization campaign."""


def reading(name: str) -> Fact[float]:
    """The fact of a value that a source reads under ``name``, such as a ``METRIC spend=412.3`` line of ``command``."""
    return Fact[float](
        name, 'The latest value that the source read. None before the first, and after a failed reading.'
    )


def since_keep(name: str) -> Fact[float]:
    """How much the value of ``reading(name)`` grew since the latest keep: ``<name>_since_keep``."""
    return Fact[float](
        f'{name}_since_keep',
        f'{name} now, less the latest reading by the end of the run of the latest keep (else the first after it).'
        f' All of {name} before the first keep.',
    )


def per_keep(facts: Facts, spend: str) -> float | None:
    """The fact ``spend`` per keep. Reads ``reading(spend)`` and ``KEEPS``."""
    return _ratio(facts.get(reading(spend)), facts.get(KEEPS))


def per_evaluation(facts: Facts, cost: str) -> float | None:
    """The sum of the evaluator's metric ``cost`` per evaluation. Reads ``EVALUATIONS`` and ``METRIC_SUMS``."""
    sums = facts.get(METRIC_SUMS)
    return _ratio(sums.get(cost) if sums is not None else None, facts.get(EVALUATIONS))


def worker_spend(facts: Facts, spend: str, cost: str) -> float | None:
    """The fact ``spend`` less the sum of the evaluator's metric ``cost``. Reads ``reading(spend)`` and ``METRIC_SUMS``."""
    total, sums = facts.get(reading(spend)), facts.get(METRIC_SUMS)
    return None if total is None or sums is None else total - sums.get(cost, 0.0)


def per_run(facts: Facts, spend: str, cost: str) -> float | None:
    """``worker_spend`` per attempt. Reads ``reading(spend)``, ``METRIC_SUMS`` and ``ATTEMPTS``."""
    return _ratio(worker_spend(facts, spend, cost), facts.get(ATTEMPTS))


def _ratio(total: float | None, count: int | None) -> float | None:
    return None if total is None or not count else total / count
