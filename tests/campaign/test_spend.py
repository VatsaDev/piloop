"""``[spend]``: the reading, its notices, and the hold of the built-in stop rule.

The daemon reads the campaign's spend command and posts each notice once. At the limit, or after a failed reading, the
stop rule ``limits`` holds the campaign: it launches nothing and starts no evaluation. The hold lasts until a human
raises the limit or the command works again.
"""

from pathlib import Path

import pytest
from sqlalchemy import select

from piloop.campaign import spend
from piloop.campaign.campaigns import sync
from piloop.campaign.spend import SpendError
from piloop.campaign.stop import check
from piloop.config import CampaignConfig, ConfigError, load_campaign_config
from piloop.db.models import AgentSession, Campaign, CampaignStatus, Comment, Idea, IdeaStatus, Run, RunStatus
from piloop.db.session import Database
from piloop.plugins.stop_rule.contract import Hold
from piloop.reports.overview import progress
from tests.conftest import WATCHDOG, ScriptRuntime, write_toml
from tests.daemon.test_scheduler import add_ideas, make_scheduler, request_watchdog, set_config, stop_campaign


def limited(limit: float, **spend: object) -> dict[str, object]:
    """The overrides of a ``[spend]`` table and of the stop rule's limit on the spend."""
    return {'spend': {'command': ['true']} | spend, 'plugins': {'stop_rule': {'max_spend': limit}}}


def test_notices_must_be_below_the_limit_and_a_limit_needs_the_spend(campaign: Campaign) -> None:
    with pytest.raises(ConfigError, match=r'\[spend\] notices \[100.0, 150.0\] are not below the spend limit 100'):
        load_campaign_config(Path(campaign.path), **limited(100, notices=[50, 100, 150]))
    with pytest.raises(ConfigError, match=r"the stop rule 'limits' reads the spend: add \[spend\]"):
        load_campaign_config(Path(campaign.path), plugins={'stop_rule': {'max_spend': 100}})
    notices = load_campaign_config(Path(campaign.path), spend={'command': ['true'], 'notices': [50]}).spend
    assert notices is not None and notices.notices == [50], 'notices alone, without a limit'


@pytest.mark.parametrize(
    ('command', 'error'),
    [
        (['sh', '-c', 'echo billing down >&2; exit 3'], 'exit 3: billing down'),
        (['echo', '$12'], r"printed '\$12', not a number of USD"),
        (['echo', '-1'], 'printed -1.0, not a spend'),
        (['echo', 'nan'], 'printed nan, not a spend'),
        (['/nonexistent/spend'], 'could not start: .*No such file'),
    ],
)
def test_a_command_that_prints_no_spend_is_an_error_never_zero(tmp_path: Path, command: list[str], error: str) -> None:
    with pytest.raises(SpendError, match=error):
        spend.read(command, tmp_path)


def test_a_hung_command_times_out(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(spend, 'TIMEOUT', 0.1)
    with pytest.raises(SpendError, match=r'timed out after 0.1s'):
        spend.read(['sh', '-c', 'sleep 5; :'], tmp_path)  # The child process holds the pipe open.


def test_the_command_runs_at_the_repo_root_and_its_number_is_the_spend(tmp_path: Path) -> None:
    (tmp_path / 'bill').write_text(' 412.30\n')
    assert spend.read(['cat', 'bill'], tmp_path) == 412.3


def daemon_comments(db: Database) -> list[tuple[str, int | None, str]]:
    with db.session() as s:
        rows = s.scalars(select(Comment).where(Comment.author == 'daemon').order_by(Comment.id))
        return [(c.kind, c.priority, c.body) for c in rows]


def bill(db: Database, campaign: Campaign, file: Path, usd: str) -> None:
    """Set the spend that the command prints from now on, and make the next reading due."""
    file.write_text(usd)
    with db.session() as s:
        s.get_one(Campaign, campaign.id).spend_at = None
        s.commit()


def test_notices_once_then_at_the_limit_nothing_starts_until_the_limit_is_raised(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    file, hold = tmp_path / 'bill', tmp_path / 'hold'
    hold.touch()  # The first agent works until the limit is reached, so its run is in progress then.
    write_toml(
        Path(campaign.path),
        f'#!/bin/sh\nwhile [ -f {hold} ]; do sleep 0.05; done\necho line >> solve.txt\ngit commit -qam "add a line"\n',
    )
    set_config(db, campaign, **limited(300, command=['cat', str(file)], notices=[100, 200]))
    first, second = add_ideas(db, campaign, 'first', 'second')
    scheduler = make_scheduler(db, runtime, tmp_path)

    file.write_text('150')
    assert scheduler.run_all() == [1], 'below the limit: launches go on'
    assert daemon_comments(db) == [
        ('note', 1, 'Spend notice: $150.00 spent, past the $100.00 notice. The limit is $300.00.')
    ], 'the first reading posts the notices it is already past'
    assert scheduler.run_all() == [] and len(daemon_comments(db)) == 1, 'the interval is not up: no new reading'

    bill(db, campaign, file, '160')
    assert scheduler.run_all() == [], 'below the limit, but the first run still holds the one slot'
    assert len(daemon_comments(db)) == 1, 'each notice once'

    bill(db, campaign, file, '1012')
    request_watchdog(db, campaign)
    assert scheduler.run_all() == [], 'at the limit nothing launches, not even a row inserted by hand'
    assert daemon_comments(db)[1:] == [
        ('note', 1, 'Spend notice: $1,012.00 spent, past the $200.00 notice. The limit is $300.00.'),
        (
            'escalation',
            0,
            'Spend limit reached: $1,012.00 spent, limit $300.00. Nothing launches and no evaluation starts. Runs in'
            ' progress finish and are scored. To resume, raise [plugins.stop_rule] max_spend in piloop.toml and run'
            ' `piloop init`.',
        ),
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
            ' · $1,012.00/$300.00 spend (launches held: spend limit reached)'
        )
    bill(db, campaign, file, '1013')
    assert scheduler.run_all() == [] and len(daemon_comments(db)) == 3, 'one escalation for the limit'

    (Path(campaign.path) / 'piloop.toml').write_text(
        (Path(campaign.path) / 'piloop.toml').read_text()
        + f'\n[spend]\ncommand = ["cat", "{file}"]\n\n[plugins.stop_rule]\nmax_spend = 2000\n'
    )
    with db.session() as s:
        sync(s, s.get_one(Campaign, campaign.id))  # This is what `piloop init` does.
        s.commit()
    assert len(scheduler.run_all()) == 2, 'the limit raised: the watchdog and the next run launch'
    scheduler.wait()


def test_a_failing_command_holds_launches_with_one_escalation_per_streak(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    file = tmp_path / 'bill'
    set_config(db, campaign, **limited(300, command=['cat', str(file)]))
    add_ideas(db, campaign, 'idea')
    scheduler = make_scheduler(db, runtime, tmp_path)

    assert scheduler.run_all() == [], 'no bill yet: the spend is unknown, not $0'
    [(kind, priority, body)] = daemon_comments(db)
    assert (kind, priority) == ('escalation', 0)
    assert body.startswith('Spend unknown: the [spend] command failed (exit 1: cat: ')
    bill(db, campaign, file, 'oops')
    assert scheduler.run_all() == [] and len(daemon_comments(db)) == 1, 'still failing: no second escalation'
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        assert p.spend_error == "printed 'oops', not a number of USD"
        assert progress(s, p, CampaignConfig.model_validate(p.config), p.created_at).endswith(
            " · $?/$300.00 spend (launches held: spend unknown: printed 'oops', not a number of USD)"
        )

    bill(db, campaign, file, '12')
    assert scheduler.run_all() == [1], 'a reading again: launches resume'
    scheduler.wait()
    bill(db, campaign, file, 'oops')
    scheduler.run_all()
    assert len(daemon_comments(db)) == 2, 'a new streak, a new escalation'


def test_progress_says_when_a_campaign_has_no_reading_yet(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        line = progress(s, p, CampaignConfig.model_validate(p.config), p.created_at)
    assert 'spend' not in line and 'held' not in line, 'no [spend]: nothing to show'
    set_config(db, campaign, **limited(10))
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        assert progress(s, p, CampaignConfig.model_validate(p.config), p.created_at).endswith(
            ' · $?/$10.00 spend (launches held: spend not read yet)'
        )


def test_a_stopped_campaign_is_held_only_while_its_closing_review_waits(db: Database, campaign: Campaign) -> None:
    """piloop asks the stop rule about a stopped campaign only while its review waits for the hold (``wait``)."""
    set_config(db, campaign, **limited(10, closing_review='wait'))
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        cfg = CampaignConfig.model_validate(p.config)
        assert check(s, p, cfg) == Hold('spend not read yet')
        p.status = CampaignStatus.STOPPED
        assert progress(s, p, cfg, p.created_at).endswith(' · $?/$10.00 spend'), 'stopped, no review waits'
        s.add(AgentSession(kind=WATCHDOG, campaign_id=p.id, reason='campaign stopped'))
        s.flush()
        assert progress(s, p, cfg, p.created_at).endswith(' (launches held: spend not read yet)'), 'its review waits'


def test_a_closing_review_waits_only_on_a_stopped_campaign_with_wait_and_a_requested_agent(
    db: Database, campaign: Campaign
) -> None:
    def waits(**spend_table: object) -> bool:
        set_config(db, campaign, **(limited(10, **spend_table) if spend_table else {}))
        with db.session() as s:
            p = s.get_one(Campaign, campaign.id)
            return spend.review_waits(s, p, CampaignConfig.model_validate(p.config))

    assert not waits(closing_review='wait'), 'active'
    stop_campaign(db, campaign, 'max_runs: 3 runs')
    assert not waits(), 'no [spend]'
    assert not waits(closing_review='wait'), 'nothing requested'
    request_watchdog(db, campaign)
    assert not waits(closing_review='run')
    assert waits(closing_review='wait')
