"""The functions that answer the API's endpoints (``piloop.api.contract``), each bound to its endpoint.

A handler reads with the daemon's database and calls the function of the layer that owns the answer; it holds no logic
of its own. ``endpoints`` pairs every endpoint of ``ENDPOINTS`` with one.
"""

import os
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path

from sqlalchemy.orm import Session

from piloop.api.contract import EVENTS, INBOX, OVERVIEW, STATUS, EventsQuery, InboxQuery, Located, Problem, Selection
from piloop.api.views import Batch, CampaignOverview, DaemonState, Inbox, Overview, Status
from piloop.campaign.campaigns import UnknownCampaign, named, selected
from piloop.db import feed as db_feed
from piloop.db.models import Campaign, utcnow
from piloop.db.session import Database
from piloop.ids import CampaignId
from piloop.net import Pending, Refusal, Served
from piloop.net.tokens import Caller
from piloop.plugins.limits import Origin, live_keys
from piloop.reports import feed
from piloop.reports.overview import overview
from piloop.settings import Settings


@dataclass(frozen=True)
class Handlers:
    """The handlers, over the daemon's database and settings."""

    db: Database
    cfg: Settings
    daemon: DaemonState

    def status(self, caller: Caller, params: Selection) -> Status:
        with self.db.session() as s, db_feed.snapshot(s) as cursor:
            states = feed.status_view(s, _selected(s, params))
        return Status(cursor=cursor, daemon=self.daemon, campaigns=states)

    def overview(self, caller: Caller, params: Selection) -> Overview:
        now = utcnow()
        with self.db.session() as s:
            campaigns = [
                CampaignOverview(
                    id=campaign.id,
                    name=campaign.name,
                    path=campaign.path,
                    parent=feed.parent_of(s, campaign),
                    text=overview(s, campaign, campaign.config, now, tools=False),
                )
                for campaign in _selected(s, params)
            ]
            limits = live_keys(s, self.cfg.providers, (Origin.CONFIG, Origin.LIMIT))
        return Overview(daemon=self.daemon, limits=limits, campaigns=campaigns)

    def inbox(self, caller: Caller, params: InboxQuery) -> Inbox:
        with self.db.session() as s, db_feed.snapshot(s) as cursor:
            campaign_id = _campaign(s, params)
            return feed.inbox_view(s, cursor, campaign_id=campaign_id, kinds=params.kind, acked=params.acked)

    def events(self, caller: Caller, params: EventsQuery) -> Batch | Pending[EventsQuery, Batch]:
        with self.db.session() as s:
            try:
                batch = feed.read(
                    s, params.after, campaign_id=_campaign(s, params), kinds=params.kind, limit=params.limit
                )
            except db_feed.CursorAhead as e:
                raise Refusal(HTTPStatus.CONFLICT, Problem.CURSOR_AHEAD, str(e)) from e
        if batch['events'] or batch['more']:
            return batch
        return Pending(batch, params.model_copy(update={'after': batch['next']}))


def endpoints(db: Database, cfg: Settings, started: str, listen: str | None) -> list[Served]:
    """Every endpoint, with its handler. ``started`` and ``listen`` describe the daemon (``DaemonState``)."""
    handlers = Handlers(db, cfg, DaemonState(pid=os.getpid(), started=started, listen=listen))
    return [
        Served(STATUS, handlers.status),
        Served(OVERVIEW, handlers.overview),
        Served(INBOX, handlers.inbox),
        Served(EVENTS, handlers.events, db.changed),
    ]


def _selected(s: Session, params: Selection) -> list[Campaign]:
    try:
        return selected(s, params.campaign, _cwd(params))
    except UnknownCampaign as e:
        raise Refusal(HTTPStatus.NOT_FOUND, Problem.UNKNOWN_CAMPAIGN, str(e)) from e


def _campaign(s: Session, params: Located) -> CampaignId | None:
    if params.campaign is None:
        return None
    try:
        return named(s, params.campaign, _cwd(params)).id
    except UnknownCampaign as e:
        raise Refusal(HTTPStatus.NOT_FOUND, Problem.UNKNOWN_CAMPAIGN, str(e)) from e


def _cwd(params: Located) -> Path | None:
    return None if params.cwd is None else Path(params.cwd)
