from piloop.api.views import Figure, Figures, Rendered
from piloop.db.models import count
from piloop.ids import CampaignStatus
from piloop.plugins.section.contract import Rendering, Section, Surface
from piloop.reports.overview import head_scores, holds, produced
from piloop.scoring.ledger import keeps, references
from piloop.text import age
from piloop.usage.totals import campaign_tokens


class Headline(Section):
    """The head and its scores, the keeps, the candidates, the tokens, the evaluator, and why the campaign is held.

    A score of the head comes with its delta against the baseline. The section says why the campaign stopped or is
    paused. With several tracks, the head shows the primary of each track. Imported references are not candidates.
    """

    SHOWN_IN = frozenset({Surface.STATUS, Surface.SWEEP})
    ORDER = 100

    def render(self, r: Rendering) -> Rendered:
        s, campaign = r.s, r.campaign
        figures: list[Figure] = []
        if (at := head_scores(s, campaign, campaign.config)) is None:
            figures.append(Figure(label='head', value='no trunk keep'))
        else:
            trunk, values = at
            figures.append(Figure(label='head', value=f'candidate#{trunk.id}'))
            for track, value, delta in values:
                figures.append(Figure(label=track.primary.name, value=value))
                if delta is not None:
                    figures.append(Figure(label=f'{track.primary.name} vs baseline', value=delta))
        figures += [
            Figure(label='kept', value=count(s, keeps(campaign))),
            Figure(label='candidates', value=count(s, produced(campaign))),
        ]
        if imported := len(references(s, campaign)):
            figures.append(Figure(label='starting points', value=imported))
        if spent := campaign_tokens(s, campaign.id):
            figures.append(Figure(label='tokens', value=spent))
        if (evaluator := campaign.evaluator) is not None:
            figures.append(Figure(label='evaluator', value=f'#{evaluator.id}, {age(evaluator.created_at, r.now)} old'))
        if campaign.status is CampaignStatus.STOPPED:
            figures.append(Figure(label='stopped because', value=campaign.stopped_reason))
        if campaign.paused is not None:
            figures += [
                Figure(label='paused', value=holds(campaign)),
                Figure(label='paused because', value=campaign.paused_reason),
            ]
        return Rendered(title='Campaign', blocks=(Figures(figures=tuple(figures)),))
