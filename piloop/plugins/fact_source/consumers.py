"""The fact source capability's machinery: it reads each source that reads on its own when it is due."""

from datetime import datetime
from pathlib import Path

from pydantic import ValidationError
from sqlalchemy import select

from piloop.campaign.campaigns import CampaignAdded
from piloop.campaign.stop import review_waits
from piloop.daemon.loop import Consumer, Kinds, Phase, Soonest, Turn
from piloop.db.models import NOT_RETIRED, Campaign
from piloop.db.triggers.changelog import CampaignConfigChanged, CampaignStatusChanged
from piloop.ids import CampaignStatus
from piloop.plugins.contribution import Contribution, DaemonHost
from piloop.plugins.fact_source.contract import FactReadError, FactSource

FACTS: Kinds = frozenset({CampaignAdded, CampaignConfigChanged, CampaignStatusChanged})
"""What wakes the facts' reading: a campaign, its ``[[facts]]`` or its status may make a source due."""


def contribution(host: DaemonHost) -> Contribution:
    """The consumer ``facts``, which reads before the campaigns' pass plans: its stop rule reads the new values."""

    def facts(turn: Turn) -> datetime | None:
        return read_due(host, turn.now)

    return Contribution(consumers=(Consumer('facts', FACTS, facts, Phase.OBSERVE),))


def read_due(host: DaemonHost, now: datetime) -> datetime | None:
    """Read each fact source of each campaign that is due (``FactSource.due``), and record what it read.

    That is each active campaign, and each stopped one whose closing review waits for the hold to end
    (``stop.review_waits``). Returns when the next reading is due.

    The source reads outside any transaction, because a command can take a minute. A write lock held for that long
    locks all other clients out. What ``record`` writes wakes the campaigns' pass (``FactSource.CHANGES``).
    """
    with host.db.session() as s:
        todo: list[tuple[int, Path, FactSource]] = []
        later = Soonest()
        for campaign in s.scalars(select(Campaign).where(NOT_RETIRED)):
            try:
                cfg = campaign.config
            except ValidationError:
                continue  # The campaigns' pass records this error for each campaign.
            if not (campaign.status is CampaignStatus.ACTIVE or review_waits(s, campaign, cfg)):
                continue
            for source in cfg.fact_sources():
                if (at := source.due(s, campaign)) is None:
                    continue
                if at > now:
                    later.add(at)
                    continue
                todo.append((campaign.id, Path(campaign.path), source))
    for campaign_id, path, source in todo:
        try:
            reading: dict[str, float] | FactReadError = source.read(path)
        except FactReadError as e:
            reading = e
        with host.db.session() as s:
            campaign = s.get_one(Campaign, campaign_id)
            source.record(s, campaign, reading)
            s.commit()
            later.add(source.due(s, campaign))
        host.tracer.event('fact.read', campaign=campaign_id, reading=str(reading))
    return later.at
