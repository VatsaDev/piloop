"""The fact sources that read: the ``command`` source, its readings as events, its notices, and the holds of ``limits``.

The daemon reads each source of the campaign that reads and posts each notice once. At a limit of ``hold_at``, or
while the value is unknown, the stop rule ``limits`` holds the campaign: it launches nothing and starts no evaluation.
At the limit it tells a human once; a source tells a human once when its readings start to fail. The hold lasts until
a human raises the limit or the source works again.
"""

from datetime import timedelta
from importlib.metadata import EntryPoint
from pathlib import Path

import pytest
from sqlalchemy import select, update

from piloop.campaign.campaigns import sync
from piloop.campaign.stop import check, gather, review_waits
from piloop.config import CampaignConfig, ConfigError, load_campaign_config
from piloop.db.events import has, in_order
from piloop.db.models import AgentSession, Campaign, Comment, Event, Idea, Run, RunStatus, utcnow
from piloop.db.session import Database
from piloop.ids import CampaignStatus, IdeaStatus, TargetKind, TargetRef
from piloop.plugins import host
from piloop.plugins.fact_source import command as command_module
from piloop.plugins.fact_source.command import Command
from piloop.plugins.fact_source.contract import FactReadError
from piloop.plugins.fact_source.facts import KEEPS, Facts, reading, since_keep
from piloop.plugins.fact_source.kit.readings import FactRead, Readings
from piloop.plugins.stop_rule.contract import STOP_RULES, Hold
from piloop.reports.overview import progress
from tests.campaign.test_stop import add_run
from tests.conftest import WATCHDOG, ScriptRuntime, write_toml
from tests.daemon.test_roles import PLUGIN, installed
from tests.daemon.test_scheduler import add_ideas, make_scheduler, request_watchdog, set_config, stop_campaign

OWN = [{'provider': 'campaign'}, {'provider': 'optimization'}]
"""piloop's own sources, which a list of ``[[facts]]`` lists too."""


def spending(**source: object) -> dict[str, object]:
    """``[[facts]]`` with piloop's own sources and a ``command`` source that reads ``spend``, with ``source`` over its keys."""
    return {'facts': [*OWN, {'provider': 'command', 'command': ['true'], 'metrics': ['spend']} | source]}


def limited(limit: float, while_held: str | None = None, **source: object) -> dict[str, object]:
    """``spending``, and the stop rule's hold at ``limit`` of the spend, with its ``while_held``."""
    rule: dict[str, object] = {'hold_at': {'spend': limit}}
    if while_held is not None:
        rule['while_held'] = while_held
    return spending(**source) | {'plugins': {'stop_rule': rule}}


def read(command: list[str], repo: Path, *names: str) -> dict[str, float]:
    return Command(command=command, metrics=list(names or ['spend'])).read(repo)


def test_a_hold_on_a_fact_needs_a_source_of_it_and_a_source_keeps_its_own_facts(campaign: Campaign) -> None:
    refused = "the stop rule 'limits' reads spend, which no fact source of this campaign supplies"
    with pytest.raises(ConfigError, match=refused):
        load_campaign_config(Path(campaign.path), plugins={'stop_rule': {'hold_at': {'spend': 100}}})
    cfg = load_campaign_config(Path(campaign.path), **spending(notices={'spend': [50]}))
    assert [type(source).__name__ for source in cfg.fact_sources()] == ['CampaignFacts', 'OptimizationFacts', 'Command']
    with pytest.raises(ConfigError, match='notices for gpu_hours, which the source does not read'):
        load_campaign_config(Path(campaign.path), **spending(notices={'gpu_hours': [5]}))
    command = {'provider': 'command', 'command': ['true'], 'metrics': ['spend']}
    with pytest.raises(
        ConfigError, match=r"the fact 'spend' comes from two sources of \[\[facts\]\]: command and command"
    ):
        load_campaign_config(Path(campaign.path), facts=[*OWN, command, command])
    with pytest.raises(ConfigError, match='reads runs, hours, which no fact source'):
        load_campaign_config(Path(campaign.path), facts=[command])


@pytest.mark.parametrize(
    ('command', 'error'),
    [
        (['sh', '-c', 'echo billing down >&2; exit 3'], 'exit 3: billing down'),
        (['echo', '$12'], 'printed no METRIC line for: spend'),
        (['echo', 'METRIC spend=nan'], 'printed no METRIC line for: spend'),
        (['/nonexistent/spend'], 'could not start: .*No such file'),
    ],
)
def test_a_command_that_prints_no_value_is_an_error_never_zero(tmp_path: Path, command: list[str], error: str) -> None:
    with pytest.raises(FactReadError, match=error):
        read(command, tmp_path)


def test_a_hung_command_times_out(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(command_module, 'TIMEOUT', 0.1)
    with pytest.raises(FactReadError, match=r'timed out after 0.1s'):
        read(['sh', '-c', 'sleep 5; :'], tmp_path)  # The child process holds the pipe open.


def test_the_command_runs_at_the_repo_root_and_prints_metric_lines_as_the_evaluator_does(tmp_path: Path) -> None:
    (tmp_path / 'bill').write_text('noise\nMETRIC spend=412.30\nMETRIC gpu_hours=3\nMETRIC other=1\n')
    assert read(['cat', 'bill'], tmp_path, 'spend', 'gpu_hours') == {'spend': 412.3, 'gpu_hours': 3.0}, (
        'the names it reads, whatever else it prints'
    )


def test_each_reading_is_a_fact_read_event_and_the_events_are_the_one_record(db: Database, campaign: Campaign) -> None:
    set_config(db, campaign, **spending())
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        [_, _, source] = p.config.fact_sources()
        assert isinstance(source, Readings)
        due = source.due(s, p)
        assert due is not None and due <= utcnow(), 'never read: due now'
        assert source.supplies() == (reading('spend'), since_keep('spend'))
        for values in ({'spend': 10.0}, FactReadError('down'), {'spend': 12.0}):
            source.record(s, p, values)
            s.flush()
        events = in_order(s, FactRead, Event.target == TargetRef(TargetKind.CAMPAIGN, p.id))
        assert [(r.values, r.error) for _, r in events] == [
            ({'spend': 10.0}, None),
            ({'spend': None}, 'down'),
            ({'spend': 12.0}, None),
        ]
        last = events[-1][0].at
        assert source.due(s, p) == last + timedelta(seconds=300), 'interval after the latest reading'


def test_since_keep_is_the_growth_of_a_value_since_the_latest_keep(db: Database, campaign: Campaign) -> None:
    """Before the first keep it is all of the value; after it, the value less its reading by the end of the keep's run."""
    set_config(db, campaign, **spending())
    wanted = [reading('spend'), since_keep('spend'), KEEPS]
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        [_, _, source] = p.config.fact_sources()
        assert isinstance(source, Readings)
        source.record(s, p, {'spend': 40.0})
        s.commit()
        assert gather(s, p, p.config, wanted, utcnow()) == Facts({'spend': 40.0, 'spend_since_keep': 40.0, 'keeps': 0})
    add_run(db, campaign, keep_score=4)
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        [_, _, source] = p.config.fact_sources()
        assert isinstance(source, Readings)
        source.record(s, p, {'spend': 100.0})
        s.commit()
        assert gather(s, p, p.config, wanted, utcnow()) == Facts({'spend': 100.0, 'spend_since_keep': 60.0, 'keeps': 1})


def daemon_comments(db: Database) -> list[tuple[str, int | None, str]]:
    with db.session() as s:
        rows = s.scalars(select(Comment).where(Comment.author == 'daemon').order_by(Comment.id))
        return [(c.kind, c.priority, c.body) for c in rows]


def bill(db: Database, campaign: Campaign, file: Path, spend: str) -> None:
    """Set the spend that the command prints from now on, and make the next reading due."""
    file.write_text(f'METRIC spend={spend}\n')
    with db.session() as s:
        readings = select(Event.id).where(has(FactRead), Event.target == TargetRef(TargetKind.CAMPAIGN, campaign.id))
        s.execute(update(Event).where(Event.id.in_(readings)).values(at=utcnow() - timedelta(hours=1)))
        s.commit()


HELD = 'nothing launches until you raise it and run piloop init'
"""How the escalation of ``limits`` at a limit of ``hold_at`` ends."""


def test_notices_once_then_at_the_limit_nothing_starts_until_the_limit_is_raised(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    file, hold = tmp_path / 'bill', tmp_path / 'hold'
    hold.touch()  # The first agent works until the limit is reached, so its run is in progress then.
    write_toml(
        Path(campaign.path),
        f'#!/bin/sh\nwhile [ -f {hold} ]; do sleep 0.05; done\necho line >> solve.txt\ngit commit -qam "add a line"\n',
    )
    set_config(db, campaign, **limited(300, command=['cat', str(file)], notices={'spend': [100, 200]}))
    first, second = add_ideas(db, campaign, 'first', 'second')
    scheduler = make_scheduler(db, runtime, tmp_path)

    file.write_text('METRIC spend=150\n')
    assert scheduler.run_all() == [1], 'below the limit: launches go on'
    assert daemon_comments(db) == [('note', 1, 'Notice: spend is 150, past the 100 notice.')], (
        'the first reading posts the notices it is already past'
    )
    assert scheduler.run_all() == [] and len(daemon_comments(db)) == 1, 'the interval is not up: no new reading'

    bill(db, campaign, file, '160')
    assert scheduler.run_all() == [], 'below the limit, but the first run still holds the one slot'
    assert len(daemon_comments(db)) == 1, 'each notice once'

    bill(db, campaign, file, '1012')
    request_watchdog(db, campaign)
    assert scheduler.run_all() == [], 'at the limit nothing launches, not even a row inserted by hand'
    assert daemon_comments(db)[1:] == [
        ('note', 1, 'Notice: spend is 1012, past the 200 notice.'),
        ('escalation', 0, f'Stop rule limits: spend reached its hold_at of 300: {HELD}'),
    ]
    hold.unlink()
    scheduler.wait()
    with db.session() as s:
        assert [(r.idea_id, r.status) for r in s.scalars(select(Run))] == [(first, RunStatus.SUCCEEDED)], (
            'what ran finished and was scored; nothing else was queued'
        )
        assert s.get_one(Idea, second).status is IdeaStatus.READY
        p = s.get_one(Campaign, campaign.id)
        assert progress(s, p, CampaignConfig.model_validate(p.config), p.created_at).endswith(
            ' · spend 1012/300 · no limit stops it: open-ended (launches held: spend at its limit)'
        )
    bill(db, campaign, file, '1013')
    assert scheduler.run_all() == [] and len(daemon_comments(db)) == 3, 'one escalation for each limit'

    (Path(campaign.path) / 'piloop.toml').write_text(
        (Path(campaign.path) / 'piloop.toml').read_text()
        + f'\n[[facts]]\nprovider = "campaign"\n\n[[facts]]\nprovider = "optimization"\n\n[[facts]]\nprovider = "command"\ncommand = ["cat", "{file}"]\n'
        'metrics = ["spend"]\n\n[plugins.stop_rule]\nhold_at = { spend = 2000 }\n'
    )
    with db.session() as s:
        sync(s, s.get_one(Campaign, campaign.id))  # This is what `piloop init` does.
        s.commit()
    assert len(scheduler.run_all()) == 2, 'the limit raised: the watchdog and the next run launch'
    scheduler.wait()


def test_a_failing_source_holds_launches_and_escalates_once_for_each_series_of_failures(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    file = tmp_path / 'bill'
    set_config(db, campaign, **limited(300, command=['cat', str(file)]))
    add_ideas(db, campaign, 'idea')
    scheduler = make_scheduler(db, runtime, tmp_path)

    assert scheduler.run_all() == [], 'no bill yet: the spend is unknown, not 0'
    [(kind, priority, body)] = daemon_comments(db)
    assert (kind, priority) == ('escalation', 0)
    assert body.startswith('Fact source command (spend): the reading failed (exit 1: cat: ')
    bill(db, campaign, file, 'oops')
    assert scheduler.run_all() == [] and len(daemon_comments(db)) == 1, 'the same series: no second escalation'
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        assert progress(s, p, CampaignConfig.model_validate(p.config), p.created_at).endswith(
            ' · spend —/300 · no limit stops it: open-ended (launches held: spend not known)'
        )

    bill(db, campaign, file, '12')
    assert scheduler.run_all() == [1], 'a reading again: launches resume'
    scheduler.wait()
    bill(db, campaign, file, 'oops')
    scheduler.run_all()
    assert len(daemon_comments(db)) == 2, 'a new series, a new escalation'


def test_progress_says_when_a_campaign_has_no_reading_yet(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        line = progress(s, p, CampaignConfig.model_validate(p.config), p.created_at)
    assert 'spend' not in line and 'held' not in line, 'no hold on the spend: nothing to show'
    set_config(db, campaign, **limited(10))
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        assert progress(s, p, CampaignConfig.model_validate(p.config), p.created_at).endswith(
            ' · spend —/10 · no limit stops it: open-ended (launches held: spend not known)'
        )


def test_a_stopped_campaign_is_held_only_while_its_closing_review_waits(db: Database, campaign: Campaign) -> None:
    """piloop asks the stop rule about a stopped campaign only while its review waits for the hold (``wait``)."""
    set_config(db, campaign, **limited(10, while_held='wait'))
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        cfg = CampaignConfig.model_validate(p.config)
        assert check(s, p, cfg) == Hold('spend not known')
        p.status = CampaignStatus.STOPPED
        assert progress(s, p, cfg, p.created_at).endswith(' · spend —/10 · no limit stops it: open-ended'), (
            'stopped, no review waits'
        )
        s.add(AgentSession(kind=WATCHDOG, campaign_id=p.id, reason='campaign stopped'))
        s.flush()
        assert progress(s, p, cfg, p.created_at).endswith(' (launches held: spend not known)'), 'its review waits'


def test_a_closing_review_waits_only_on_a_stopped_campaign_with_wait_and_a_requested_agent(
    db: Database, campaign: Campaign
) -> None:
    def waits(while_held: str | None = None) -> bool:
        set_config(db, campaign, **limited(10, while_held))
        with db.session() as s:
            p = s.get_one(Campaign, campaign.id)
            return review_waits(s, p, CampaignConfig.model_validate(p.config))

    assert not waits('wait'), 'active'
    stop_campaign(db, campaign, 'max_runs: 3 runs')
    assert not waits(), 'run by default'
    assert not waits('wait'), 'nothing requested'
    request_watchdog(db, campaign)
    assert not waits('run')
    assert waits('wait')


def test_the_progress_line_is_what_the_stop_rule_describes(
    db: Database, campaign: Campaign, monkeypatch: pytest.MonkeyPatch
) -> None:
    """piloop formats no fact: the rule shows its own, and a rule that describes nothing shows none."""
    installed(
        monkeypatch,
        [
            EntryPoint('watches', f'{PLUGIN}:Watches', STOP_RULES.group),
            EntryPoint('goes_on', f'{PLUGIN}:GoesOn', STOP_RULES.group),
        ],
    )

    def line(provider: str) -> str:
        set_config(db, campaign, **spending(), plugins={'stop_rule': {'provider': provider}})
        with db.session() as s:
            p = s.get_one(Campaign, campaign.id)
            return progress(s, p, CampaignConfig.model_validate(p.config), p.created_at)

    try:
        assert line('watches') == 'evaluations 1/100 · attempts 0/50 · spend_since_keep —/300 · funnel —/1'
        assert line('goes_on') == '—'
    finally:
        host.close()
