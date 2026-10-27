"""``campaign``: the built-in fact source of what any campaign records, whatever its type: its hours and its tokens."""

from datetime import datetime

from sqlalchemy.orm import Session

from piloop.campaign.stop import hours
from piloop.db.models import Campaign
from piloop.plugins.fact_source.contract import FactSource
from piloop.plugins.fact_source.facts import CAMPAIGN, HOURS, TOKENS, Fact, FactValue
from piloop.usage.totals import campaign_tokens


class CampaignFacts(FactSource):
    """``campaign``: the hours that the campaign ran and the tokens that its agents spent.

    It is on by default (``DEFAULT``). A campaign that lists its ``[[facts]]`` lists it too, to keep these facts.
    """

    DEFAULT = True

    def supplies(self) -> tuple[Fact[FactValue], ...]:
        """``CAMPAIGN``."""
        return CAMPAIGN

    def gather(
        self, s: Session, campaign: Campaign, wanted: frozenset[Fact[FactValue]], now: datetime
    ) -> dict[Fact[FactValue], FactValue]:
        """``HOURS`` and ``TOKENS``, those of them in ``wanted``."""
        found: dict[Fact[FactValue], FactValue] = {
            HOURS: hours(s, campaign, now),
            TOKENS: campaign_tokens(s, campaign.id),
        }
        return {fact: value for fact, value in found.items() if fact in wanted}
