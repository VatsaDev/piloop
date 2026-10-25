from piloop.api.views import Rendered, Text
from piloop.plugins.section.contract import Rendering, Section, Surface
from piloop.reports.pack import pack


class Pack(Section):
    """The research pack, which ``piloop history`` prints: what a sweep needs to propose work.

    It has the metrics, the analysis files and the workspace, what was tried by family, the seeds, the history, and
    the latest findings and digests.

    Nothing for an agent that got incidents to investigate: the pack is for proposing work.
    """

    SHOWN_IN = frozenset({Surface.SWEEP})
    ORDER = 910

    def render(self, r: Rendering) -> Rendered | None:
        if r.given:
            return None
        text = pack(r.s, r.campaign, r.campaign.config, workspace=r.workspace)
        return Rendered(title='Research pack', blocks=(Text(text=text.strip()),))
