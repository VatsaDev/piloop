"""``keep_cost``: tell a human when a keep costs too much, and stop at a harder line. No state, no I/O.

The rule compares the fact ``spend`` names, since the latest keep, with its value per keep so far. Past ``escalate_at``
times, it asks a human once for each keep; past ``stop_at`` times, it stops the campaign. It is the built-in rule
``limits`` with these two lines, so ``max_hours``, ``hold_at`` and the others still apply. Before the first keep there
is no value per keep, and the rule is ``limits``.
"""

from pydantic import Field

from piloop.capabilities import (
    KEEPS,
    Escalate,
    Fact,
    FactValue,
    Go,
    Hold,
    Limits,
    Stop,
    StopCase,
    per_keep,
    reading,
    since_keep,
)


class KeepCost(Limits):
    """The cost since the latest keep against the cost per keep: escalate at ``escalate_at``×, stop at ``stop_at``×."""

    spend: str = 'spend'
    """The fact that is what the campaign spent so far, which a source of ``[[facts]]`` reads."""
    escalate_at: float = Field(default=2, gt=0)
    """Ask a human once the cost since the latest keep is this many times the cost per keep."""
    stop_at: float = Field(default=4, gt=0)
    """Stop once the cost since the latest keep is this many times the cost per keep."""

    def reads(self) -> list[Fact[FactValue]]:
        return list(dict.fromkeys([*super().reads(), reading(self.spend), KEEPS, since_keep(self.spend)]))

    def check(self, case: StopCase) -> Go | Hold | Stop | Escalate:
        answer = super().check(case)
        each, since = per_keep(case.facts, self.spend), case.facts.get(since_keep(self.spend))
        if not isinstance(answer, Go) or each is None or since is None:
            return answer
        if since >= self.stop_at * each:
            return Stop(
                f'keep_cost: {self.spend} {since:g} since the latest keep, {self.stop_at:g} × the {each:g} per keep'
            )
        if since >= self.escalate_at * each:
            # one escalation for each keep: the reason names the keep, not the amount, which changes on every pass
            return Escalate(
                f'keep_cost: since keep {case.facts.get(KEEPS)}, {self.spend} passed {self.escalate_at:g} × its value'
                f' per keep; it stops at {self.stop_at:g} ×'
            )
        return answer
