"""The campaign type capability: what one kind of campaign stores, how it runs and what it shows.

``campaigns.type`` names the type of a campaign. Every installed type's schema is migrated. ``optimization``, built in,
is the research model: ideas, runs, candidates and their evaluations.
"""

from abc import ABC

from piloop.plugins.host import Capability, Parameters


class CampaignType(Parameters, ABC):
    """A kind of campaign. ``campaigns.type`` names it."""


CAMPAIGN_TYPES = Capability('campaign_type', CampaignType, noun='campaign type')
"""Every campaign type by name. No key selects one: each campaign names its own."""
