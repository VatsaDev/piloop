"""Tests for the sections: the typed blocks that every reader renders, and the sections that depend on their reader."""

from collections.abc import Iterator
from importlib.metadata import EntryPoint
from pathlib import Path

import pytest
from inline_snapshot import snapshot

from piloop.api.views import CampaignOverview, Figure, Figures, Lines, Rendered, Table, Text
from piloop.config import StatusConfig
from piloop.db.models import Campaign, Pause
from piloop.db.session import Database
from piloop.ids import CampaignStatus
from piloop.plugins.role.contract import ImplementerRole
from piloop.plugins.section.contract import SECTIONS, Rendering, Section, Surface, defaults, markdown
from tests.daemon.test_roles import forget_loaded, installed
from tests.reports.test_overview import NOW


def test_a_section_reads_back_from_its_json_and_renders_each_block_kind_its_own_way() -> None:
    section = Rendered(
        title='Kinds',
        blocks=(
            Text(text='A paragraph.'),
            Lines(lines=('- one', '  - under it')),
            Figures(figures=(Figure(label='score', value=0.91234), Figure(label='spend', value=12, unit='USD'))),
            Table(columns=('candidate', 'score', 'summary'), rows=(('#1', None, 'a | b'), ('#2', 3.5, ''))),
        ),
    )
    shown = CampaignOverview(id=1, name='demo', path='/demo', parent=None, sections=[section])
    assert CampaignOverview.model_validate_json(shown.model_dump_json()) == shown, 'the client reads the same model'
    assert section.markdown() == snapshot("""\
## Kinds

A paragraph.

- one
  - under it

score: 0.9123 · spend: 12 USD

| candidate | score | summary |
|---|---|---|
| #1 | — | a / b |
| #2 | 3.5 |  |\
""")


def sections(*names: str) -> list[Section]:
    return [SECTIONS.validated({'provider': name}) for name in names]


def test_the_incidents_and_the_pack_depend_on_what_the_agent_got(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        routine = markdown(sections('incidents', 'pack'), Rendering(s, NOW, p, Path('/no/workspace')))
        incident = markdown(sections('incidents', 'pack'), Rendering(s, NOW, p, Path('/no/workspace'), ('- run#1',)))
    assert routine.startswith('## Research pack\n\n### Research pack: metrics'), 'no incidents: the pack'
    assert incident == '## Incidents to investigate\n\n- run#1', 'an incident sweep reads its incidents alone'


def test_the_headline_says_why_the_campaign_is_held(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        p.status, p.stopped_reason = CampaignStatus.STOPPED, 'plateau'
        p.paused, p.paused_reason = Pause.ALL, 'new data'
        text = markdown(sections('headline'), Rendering(s, NOW, p, Path('/no/workspace')))
    assert text.endswith('stopped because: plateau · paused: launches and evaluations · paused because: new data')


class KeepRate(Section):
    """A plugin's section: it declares where it shows by default, and where."""

    SHOWN_IN = frozenset({Surface.STATUS})
    ORDER = 175

    def render(self, r: Rendering) -> Rendered:
        return Rendered(title='Keep rate', blocks=(Figures(figures=(Figure(label='keeps per hour', value=0.5),)),))


@pytest.fixture
def keep_rate(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    installed(monkeypatch, [EntryPoint('keep_rate', f'{__name__}:KeepRate', SECTIONS.group)])
    yield
    forget_loaded()


def names(sections: list[Section]) -> list[str]:
    return [SECTIONS.name_of(s) for s in sections]


@pytest.mark.usefixtures('keep_rate')
def test_an_installed_section_shows_where_it_declares_in_its_order_unless_a_list_is_set() -> None:
    status = names(StatusConfig().shown())
    assert status[status.index('evaluations') + 1] == 'keep_rate', 'ORDER 175: after the evaluations'
    assert 'keep_rate' not in names(defaults(Surface.SWEEP)), 'only where it declares'
    listed = StatusConfig.model_validate({'sections': [{'provider': 'goal'}]})
    assert names(listed.shown()) == ['goal'], 'a list in the configuration replaces the defaults'


def test_each_surface_shows_its_built_in_sections_by_default() -> None:
    overview = ['headline', 'goal', 'tracks', 'trajectory', 'runs', 'queue', 'agents', 'evaluations', 'candidates']
    overview += ['anomalies', 'inbox', 'events']
    assert names(defaults(Surface.STATUS)) == overview
    assert names(defaults(Surface.SWEEP)) == ['lessons', 'incidents', *overview, 'tools', 'pack']
    assert names(defaults(Surface.RUN)) == ['lessons', 'history']
    assert names(ImplementerRole().shown()) == ['lessons', 'history']
