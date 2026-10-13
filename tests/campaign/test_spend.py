"""The ``[spend]`` guard.

The daemon reads the project's spend command and posts each notice once. At the limit, or after a failed reading, it
launches nothing and starts no evaluation. This lasts until a human raises the limit or the command works again.
"""

from pathlib import Path

import pytest
from pydantic import ValidationError
from sqlalchemy import select

from piloop.campaign import spend
from piloop.campaign.project import sync
from piloop.campaign.spend import SpendError
from piloop.config import ProjectConfig, Spend
from piloop.db.models import Comment, Idea, IdeaStatus, Project, Run, RunStatus
from piloop.db.session import Database
from piloop.reports.overview import progress
from tests.conftest import ScriptRuntime
from tests.daemon.test_scheduler import add_ideas, make_scheduler, request_watchdog, set_config


def test_notices_must_be_below_the_limit() -> None:
    with pytest.raises(ValidationError, match=r'notices \[100.0, 150.0\] are not below the limit 100'):
        Spend(command=['true'], limit=100, notices=[50, 100, 150])


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


def bill(db: Database, project: Project, file: Path, usd: str) -> None:
    """Set the spend that the command prints from now on, and make the next reading due."""
    file.write_text(usd)
    with db.session() as s:
        s.get_one(Project, project.id).spend_at = None
        s.commit()


def test_notices_once_then_at_the_limit_nothing_starts_until_the_limit_is_raised(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    file = tmp_path / 'bill'
    budget = {'command': ['cat', str(file)], 'limit': 300, 'notices': [100, 200]}
    set_config(db, project, spend=budget)
    first, second = add_ideas(db, project, 'first', 'second')
    scheduler = make_scheduler(db, runtime, tmp_path)

    file.write_text('150')
    assert scheduler.tick() == [1], 'below the limit: launches go on'
    assert daemon_comments(db) == [
        ('note', 1, 'Spend notice: $150.00 spent, past the $100.00 notice. The limit is $300.00.')
    ], 'the first reading posts the notices it is already past'
    assert scheduler.tick() == [] and len(daemon_comments(db)) == 1, 'the interval is not up: no new reading'

    bill(db, project, file, '160')
    scheduler.tick()
    assert len(daemon_comments(db)) == 1, 'each notice once'

    bill(db, project, file, '1012')
    request_watchdog(db, project)
    assert scheduler.tick() == [], 'at the limit nothing launches, not even a row inserted by hand'
    assert daemon_comments(db)[1:] == [
        ('note', 1, 'Spend notice: $1,012.00 spent, past the $200.00 notice. The limit is $300.00.'),
        (
            'escalation',
            0,
            'Spend limit reached: $1,012.00 spent, limit $300.00. Nothing launches and no evaluation starts. Runs in'
            ' progress finish and are scored. To resume, raise [spend] limit in piloop.toml and run `piloop init`.',
        ),
    ]
    scheduler.wait(30)
    with db.session() as s:
        assert [(r.idea_id, r.status) for r in s.scalars(select(Run))] == [(first, RunStatus.SUCCEEDED)], (
            'what ran finished and was scored; nothing else was queued'
        )
        assert s.get_one(Idea, second).status is IdeaStatus.READY
        p = s.get_one(Project, project.id)
        assert progress(s, p, ProjectConfig.model_validate(p.config), p.created_at).endswith(
            ' · $1,012.00/$300.00 spend (launches held: spend limit reached)'
        )
    bill(db, project, file, '1013')
    assert scheduler.tick() == [] and len(daemon_comments(db)) == 3, 'one escalation for the limit'

    (Path(project.path) / 'piloop.toml').write_text(
        (Path(project.path) / 'piloop.toml').read_text() + f'\n[spend]\ncommand = ["cat", "{file}"]\nlimit = 2000\n'
    )
    with db.session() as s:
        sync(s, s.get_one(Project, project.id))  # This is what `piloop init` does.
        s.commit()
    assert len(scheduler.tick()) == 2, 'the limit raised: the watchdog and the next run launch'
    scheduler.wait(30)


def test_a_failing_command_holds_launches_with_one_escalation_per_streak(
    db: Database, project: Project, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    file = tmp_path / 'bill'
    set_config(db, project, spend={'command': ['cat', str(file)], 'limit': 300})
    add_ideas(db, project, 'idea')
    scheduler = make_scheduler(db, runtime, tmp_path)

    assert scheduler.tick() == [], 'no bill yet: the spend is unknown, not $0'
    [(kind, priority, body)] = daemon_comments(db)
    assert (kind, priority) == ('escalation', 0)
    assert body.startswith('Spend unknown: the [spend] command failed (exit 1: cat: ')
    bill(db, project, file, 'oops')
    assert scheduler.tick() == [] and len(daemon_comments(db)) == 1, 'still failing: no second escalation'
    with db.session() as s:
        p = s.get_one(Project, project.id)
        assert p.spend_error == "printed 'oops', not a number of USD"
        assert spend.progress(p, ProjectConfig.model_validate(p.config)) == (
            "$?/$300.00 spend (launches held: spend unknown: printed 'oops', not a number of USD)"
        )

    bill(db, project, file, '12')
    assert scheduler.tick() == [1], 'a reading again: launches resume'
    scheduler.wait(30)
    bill(db, project, file, 'oops')
    scheduler.tick()
    assert len(daemon_comments(db)) == 2, 'a new streak, a new escalation'


def test_progress_says_when_a_project_has_no_reading_yet(project: Project) -> None:
    cfg = ProjectConfig.model_validate(project.config)
    assert spend.progress(project, cfg) is None, 'no [spend]: nothing to show'
    budget = cfg.model_copy(update={'spend': Spend(command=['true'], limit=10)})
    assert spend.progress(project, budget) == '$?/$10.00 spend (launches held: spend not read yet)'
