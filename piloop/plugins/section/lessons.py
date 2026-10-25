from pydantic import Field

from piloop.api.views import Lines, Rendered, Text
from piloop.plugins.section.contract import Rendering, Section, Surface
from piloop.reports.wake import LESSONS_MORE, Relevance, cost, current_lessons, fitted, lesson_line

TITLE = 'What past campaigns on this repo concluded'
INTRO = (
    "The current lesson of each mechanism family, from the newest campaign that tried it; those in this campaign's"
    ' metrics first, then related metrics; within them, dead or promising first, then the largest effects of each'
    ' metric. Before you propose or build a mechanism, read more with the queries under "Remember past campaigns" in'
    ' the procedures.'
)


class Lessons(Section):
    """The current lessons of the campaign's repo, most relevant first, in a reading budget.

    Nothing when there are no lessons, or when not one fits in the budget after the title and the introduction.
    """

    SHOWN_IN = frozenset({Surface.SWEEP, Surface.RUN})
    ORDER = 10

    budget: int = Field(default=400, ge=1)
    """The size of the section in tokens, its title and introduction included, at about 4 characters each. A line
    that does not fit gives way to one that counts the rest and says how to read them."""

    def render(self, r: Rendering) -> Rendered | None:
        campaign = r.campaign
        lessons = current_lessons(r.s, [campaign.repo], Relevance([campaign.config]))
        lines = [lesson_line(lesson, name) for lesson, name in lessons]
        kept = fitted(lines, self.budget - cost([f'## {TITLE}', '', INTRO, '']), LESSONS_MORE)
        return Rendered(title=TITLE, blocks=(Text(text=INTRO), Lines(lines=tuple(kept)))) if kept else None
