"""``optimization``: the research model, which improves the metrics of a repo through ideas, runs and candidates."""

from piloop.plugins.campaign_type.contract import CampaignType, Planner, TypeHost


class Optimization(CampaignType):
    """``type = "optimization"``: ideas, runs that implement them, candidates that the evaluator measures."""

    def planner(self, host: TypeHost) -> Planner:
        """Its run queue, its runs' ends and its measurements."""
        from piloop.plugins.campaign_type.optimization.planner import OptimizationPlanner  # config imports this module

        return OptimizationPlanner(host)
