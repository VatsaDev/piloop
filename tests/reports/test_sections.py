"""Tests for the sections: the typed blocks that every reader renders, and the sections that depend on their reader."""

from collections.abc import Iterator
from importlib.metadata import EntryPoint
from pathlib import Path

import pytest
from inline_snapshot import snapshot

from piloop.api.views import CampaignOverview, Figure, Figures, Lines, Rendered, Table, Text
from piloop.config import StatusConfig
from piloop.db.models import Campaign, Pause, RunEnd, RunStatus
from piloop.db.session import Database
from piloop.ids import CampaignStatus
from piloop.plugins import host
from piloop.plugins.fact_source.kit.readings import Readings
from piloop.plugins.role.contract import ImplementerRole
from piloop.plugins.section.contract import SECTIONS, Rendering, Section, Surface, defaults, markdown
from piloop.plugins.stop_rule.contract import STOP_RULES
from tests.campaign.test_stop import add_run
from tests.conftest import cas_of
from tests.daemon.test_roles import PLUGIN, forget_loaded, installed
from tests.daemon.test_scheduler import set_config
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
        routine = markdown(sections('incidents', 'pack'), Rendering(s, NOW, p, Path('/no/workspace'), cas_of(s)))
        incident = markdown(
            sections('incidents', 'pack'), Rendering(s, NOW, p, Path('/no/workspace'), cas_of(s), ('- run#1',))
        )
    assert routine.startswith('## Research pack\n\n### Research pack: metrics'), 'no incidents: the pack'
    assert incident == '## Incidents to investigate\n\n- run#1', 'an incident sweep reads its incidents alone'


def test_the_headline_says_why_the_campaign_is_held(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        p.status, p.stopped_reason = CampaignStatus.STOPPED, 'plateau'
        p.paused, p.paused_reason = Pause.ALL, 'new data'
        text = markdown(sections('headline'), Rendering(s, NOW, p, Path('/no/workspace'), cas_of(s)))
    assert text.endswith('stopped because: plateau · paused: launches and evaluations · paused because: new data')


class KeepRate(Section):
    """A plugin's section: it declares where it shows by default, and where."""

    SHOWN_IN = frozenset({Surface.STATUS})
    ORDER = 172

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
    assert status[status.index('evaluations') + 1] == 'keep_rate', 'ORDER 172: between the evaluations and the funnel'
    assert 'keep_rate' not in names(defaults(Surface.SWEEP)), 'only where it declares'
    listed = StatusConfig.model_validate({'sections': [{'provider': 'goal'}]})
    assert names(listed.shown()) == ['goal'], 'a list in the configuration replaces the defaults'


def test_each_surface_shows_its_built_in_sections_by_default() -> None:
    overview = ['headline', 'goal', 'tracks', 'trajectory', 'runs', 'queue', 'idle_queue', 'agents', 'evaluations']
    overview += ['funnel', 'candidates', 'same_as_parent', 'reported', 'inbox', 'events']
    assert names(defaults(Surface.STATUS)) == overview
    assert names(defaults(Surface.SWEEP)) == ['lessons', 'incidents', *overview, 'tools', 'pack']
    assert names(defaults(Surface.RUN)) == ['lessons', 'history']
    assert names(ImplementerRole().shown()) == ['lessons', 'history']


def test_the_funnel_shows_how_far_the_attempts_got_and_where_the_others_ended(db: Database, campaign: Campaign) -> None:
    add_run(db, campaign)
    add_run(db, campaign, measured=False)
    add_run(db, campaign, status=RunStatus.FAILED, end=RunEnd.FAILED, measured=False)
    add_run(db, campaign, keep_score=4)
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        text = markdown(sections('funnel'), Rendering(s, NOW, p, Path('/no/workspace'), cas_of(s)))
    assert text == snapshot("""\
## Funnel

ideas: 4 · attempts: 4 · reached the evaluator: 2 · measured: 2 · keeps: 1

| ended | unmeasured attempts |
|---|---|
| failed | 1 |
| finished | 1 |\
""")


def test_the_funnel_shows_nothing_without_the_source_of_its_fact(
    db: Database, campaign: Campaign, monkeypatch: pytest.MonkeyPatch
) -> None:
    installed(monkeypatch, [EntryPoint('goes_on', f'{PLUGIN}:GoesOn', STOP_RULES.group)])
    try:
        set_config(db, campaign, facts=[{'provider': 'campaign'}], plugins={'stop_rule': {'provider': 'goes_on'}})
        with db.session() as s:
            p = s.get_one(Campaign, campaign.id)
            assert markdown(sections('funnel'), Rendering(s, NOW, p, Path('/no/workspace'), cas_of(s))) == ''
    finally:
        host.close()


def test_cost_reads_the_metric_it_names_and_shows_nothing_without_a_spend_or_a_metric(
    db: Database, campaign: Campaign
) -> None:
    """piloop knows no cost: the section names the fact of the spend and the cost metric in its own entry."""
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        assert markdown(sections('cost'), Rendering(s, NOW, p, Path('/no/workspace'), cas_of(s))) == '', (
            'nothing to show'
        )
    source = {'provider': 'command', 'command': ['true'], 'metrics': ['spend']}
    set_config(db, campaign, facts=[{'provider': 'campaign'}, {'provider': 'optimization'}, source])
    add_run(db, campaign, keep_score=4)
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        [_, _, reads] = p.config.fact_sources()
        assert isinstance(reads, Readings)
        reads.record(s, p, {'spend': 412.3})
        s.flush()
        cost = [SECTIONS.validated({'provider': 'cost', 'metric': 'size'})]
        text = markdown(cost, Rendering(s, NOW, p, Path('/no/workspace'), cas_of(s)))
        plain = markdown(sections('cost'), Rendering(s, NOW, p, Path('/no/workspace'), cas_of(s)))
    assert text == snapshot("""\
## Cost

spend: 412.3 · per keep: 412.3 · since the latest keep: 0

evaluations (size): 5 · per evaluation: 2.5 · without the evaluator: 407.3 · per attempt: 407.3\
""")
    assert plain == snapshot("""\
## Cost

spend: 412.3 · per keep: 412.3 · since the latest keep: 0\
""")
