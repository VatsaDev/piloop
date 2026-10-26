"""``optimization``: the research model, which improves the metrics of a repo through ideas, runs and candidates."""

from piloop.plugins.campaign_type.contract import CampaignType


class Optimization(CampaignType):
    """``type = "optimization"``: ideas, runs that implement them, candidates that the evaluator measures."""
