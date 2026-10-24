from sqlalchemy import select

from piloop.db.models import Idea, IdeaStatus, count
from piloop.plugins.trigger.contract import Fire, Tick, Trigger
from piloop.plugins.trigger.kit.reasons import retry


class SeedTrigger(Trigger):
    """A seed was filed after the role's latest agent started, and a slot of the role is free.

    It fires however full the backlog is. If the latest row got the seed but was killed, the seed is retried, once per
    batch, as ``low_water`` retries, unless someone cancelled that row. A seed that a sweep read and did not develop is in the next sweep's brief,
    whatever trigger fires it. Gotcha: a sweep's brief is read a moment before its row's ``started_at`` is set, so a
    seed filed in that moment waits for the next sweep.
    """

    def fire(self, tick: Tick) -> Fire | None:
        if tick.live() >= tick.max_concurrent:
            return None
        seeds = select(Idea).where(Idea.campaign_id == tick.campaign.id, Idea.status == IdeaStatus.SEED)
        waiting = count(tick.s, seeds)
        if not waiting:
            return None
        last = tick.last
        fresh = waiting if last is None else count(tick.s, seeds.where(Idea.created_at > last.started_at))
        if fresh:
            return Fire(f'{fresh} new seeds to develop')
        assert last is not None  # With no row yet, every waiting seed is fresh.
        if (retrying := retry(tick)) is not None:
            return Fire(f'{waiting} seeds to develop; {retrying}')
        return None
