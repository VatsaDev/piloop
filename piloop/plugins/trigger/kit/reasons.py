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
    if (retrying := retry(tick)) is not None:
        return f'{lead}; {retrying}'
    mine = campaign_runs(tick.campaign.id)
    finished = count(tick.s, mine.where(Run.finished_at > last.started_at))
    in_flight = count(tick.s, mine.where(Run.status.in_(OPEN_RUNS)))
    barren = len(tick.empty_sweeps((AgentSessionState.EXITED,)))
    if finished >= max(min(2**barren, finished + in_flight), 1):
        backoff = f' (after {barren} sweeps that defined no idea)' if barren else ''
        return f'{lead}; {finished} runs finished since agent_session#{last.id} started{backoff}'
    return None


def retry(tick: Tick) -> str | None:
    """Return why the role's latest row is retried, or None.

    A row that was killed, in the first killed batch in a row, is retried once: a killed batch says nothing about the
    backlog. A row that someone cancelled is not: it was not wanted.
    """
    last = tick.last
    if last is None or last.state is not AgentSessionState.KILLED or tick.cancel(last) is not None:
        return None
    if tick.killed_streak() > tick.max_concurrent:
        return None
    return f'retrying after agent_session#{last.id} was killed: {last.error}'
