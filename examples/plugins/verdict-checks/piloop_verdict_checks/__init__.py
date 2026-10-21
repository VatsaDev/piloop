"""``checked``: the verdict of ``bootstrap``, but a check can turn a keep into ``interesting``.

piloop calls this rule with a ``Case``. The rule calls the built-in rule ``bootstrap`` with the same ``Case``. If
``bootstrap`` keeps the candidate, the rule calls each verdict check that its parameter ``checks`` names. A check is a
provider of the capability ``verdict check``, which this plugin declares in ``contract.py``.
"""

import json
from dataclasses import dataclass

from piloop.capabilities import BOOTSTRAP, Case, Judgement, Outcome
from piloop_verdict_checks.contract import VERDICT_CHECKS


@dataclass(frozen=True)
class Parameters:
    """``[plugins.verdict_rule]``: ``checks``, the names of the verdict checks to call, in order."""

    checks: list[str]

    def __post_init__(self) -> None:
        for name in self.checks:
            VERDICT_CHECKS.get(name)  # an unknown name fails when piloop.toml loads, with the names that there are


class Checked:
    """What ``bootstrap`` decides, but a keep that a check refuses is ``interesting``, with the check's reason."""

    Parameters = Parameters

    def decide(self, case: Case) -> Judgement:
        judged = BOOTSTRAP.decide(case)
        if judged.verdict is not Outcome.KEEP:
            return judged
        names = Parameters(**json.loads(case.parameters)).checks
        held = [f'{name}: {why}' for name in names if (why := VERDICT_CHECKS.get(name).check(case)) is not None]
        return Judgement(Outcome.INTERESTING, f'{judged.reason}; held by {"; ".join(held)}') if held else judged


CHECKED = Checked()
"""The provider: an instance, as the entry point of a pure provider names."""
