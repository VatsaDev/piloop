"""The reasons to sweep that the backlog triggers share: whether a sweep would be futile, and what is news to the role."""

from piloop.db.models import OPEN_RUNS, AgentSessionState, Run, campaign_runs, count
from piloop.plugins.trigger.contract import Backlog, Tick


def futile(tick: Tick, backlog: Backlog) -> bool:
    """Return whether a sweep now is futile: the role adds no idea, no slot is free, or the queue holds its ideas."""
    return backlog.per_sweep == 0 or backlog.sweeps >= tick.max_concurrent or (backlog.on_hold and backlog.runnable > 0)


def news(tick: Tick, lead: str) -> str | None:
    """Return why a sweep reads something new to the role, after ``lead``: no sweep yet, a killed batch, or new results.

    None when the latest sweep read everything there is, as the back-off of sweeps that defined no idea counts it.
    """
    last = tick.last
    if last is None:
        return lead
    if last.state is AgentSessionState.KILLED and tick.killed_streak() <= tick.max_concurrent:
        return f'{lead}; retrying after agent_session#{last.id} was killed: {last.error}'
    mine = campaign_runs(tick.campaign.id)
    finished = count(tick.s, mine.where(Run.finished_at > last.started_at))
    in_flight = count(tick.s, mine.where(Run.status.in_(OPEN_RUNS)))
    barren = len(tick.empty_sweeps((AgentSessionState.EXITED,)))
    if finished >= max(min(2**barren, finished + in_flight), 1):
        backoff = f' (after {barren} sweeps that defined no idea)' if barren else ''
        return f'{lead}; {finished} runs finished since agent_session#{last.id} started{backoff}'
    return None
