import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
from collections.abc import Callable
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path

import cyclopts
import pytest
from alembic.script import ScriptDirectory
from git import Repo
from pytest_mock import MockerFixture
from rich.console import Console
from sqlalchemy import select

from piloop.agents.scout import is_scout
from piloop.campaign import inbox, project as project_module
from piloop.campaign.ideas import defined_by
from piloop.cli import main as cli
from piloop.config import ConfigError
from piloop.daemon.main import Daemon
from piloop.daemon.serve import running_pid
from piloop.db import sql
from piloop.db.models import (
    Candidate,
    Event,
    Idea,
    IdeaStatus,
    Measurement,
    Project,
    ProjectStatus,
    Run,
    RunStatus,
    TargetKind,
    Verdict,
    Worker,
    WorkerKind,
    WorkerState,
    utcnow,
)
from piloop.db.session import Database
from piloop.settings import Settings
from tests.conftest import hold_lock
from tests.daemon.test_serve import alive, wait_until
from tests.db.test_migrations import alembic
from tests.fixtures.fake_herdr import SOCKET

HEAD = ScriptDirectory.from_config(alembic(Path('unused.db'))).get_current_head()
"""The schema revision every command migrates to; the migrations, not this file, say which."""


def run_cli(tokens: list[str]) -> None:
    cli.app(tokens, result_action='return_value', exit_on_error=False)


@pytest.fixture(autouse=True)
def isolated_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Console:
    monkeypatch.setattr(
        cli,
        'settings',
        Settings(home=tmp_path, uv_tool_dir=tmp_path / 'uv-tools'),
    )
    monkeypatch.setattr(project_module, 'settings', cli.settings)
    console = Console(record=True, width=200, force_terminal=False)
    monkeypatch.setattr(cli, 'console', console)
    return console


@pytest.fixture
def out(isolated_home: Console):
    def read() -> str:
        return isolated_home.export_text(clear=True)

    return read


def test_project_and_idea_lifecycle(project_path: Path, out) -> None:
    run_cli(['init', str(project_path)])
    assert out().startswith(
        "wrote .entire/.gitignore\nwrote .entire/settings.json\nproject demo #1 registered; baseline candidate#1 {'score': 1.0"
    ), 'a hand-made project gets the Entire settings, committed before the baseline is measured'

    run_cli(['idea', 'add', 'demo', 'more lines', '--hypothesis', 'lines help', '--priority', '1'])
    run_cli(['idea', 'add', str(project_path), 'later', '--proposed'])
    run_cli(['idea', 'add', 'demo', 'even more lines', '--after', '1', '--after', '2', '--supersedes', '2'])
    assert out().splitlines() == [
        'idea#1 ready p1: more lines',
        'idea#2 proposed p2: later',
        'idea#3 ready p2: even more lines',
    ]

    run_cli(['history', 'demo'])
    text = out()
    assert 'History: ideas (3 of 3)' in text and 'History: kept lineage' in text
    assert 'Research pack: ideas by source' in text and 'Research pack: open escalations' in text, 'the pack'
    assert 'more lines' in text and 'blocked by idea#1 (ready), idea#2 (proposed); supersedes idea#2' in text
    assert 'lines help' in text and 'piloop init: .entire' in text and '## ' not in text, (
        'a terminal table, not the brief markdown'
    )

    run_cli(['status'])
    text = re.sub(r'\b\d+s (old|ago)\b', r'Ns \1', out())  # the second may tick over since init
    assert 'demo · head score 1.0000 (candidate#1) · 1 kept / 1 candidates · evaluator #1, Ns old' in text
    assert '- Ideas: 1 runnable, 1 blocked (idea#3 on idea#1 (ready), idea#2 (proposed));' in text
    assert '  - idea#1 p1 "more lines" by human, Ns ago\n- Agents live: none' in text
    assert '### Tools' not in text, 'the cheat sheet is for agents'

    run_cli(['init', str(project_path)])
    assert out().strip() == 'project demo #1 already registered; config re-read from piloop.toml'


def test_every_role_but_the_worker_files_a_seed_and_only_research_the_orchestrator_and_humans_add_ideas(
    project_path: Path, tmp_path: Path, out, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_cli(['init', str(project_path)])

    def as_(actor: str, *argv: str) -> None:
        monkeypatch.setattr(cli, 'settings', Settings(home=tmp_path, uv_tool_dir=tmp_path / 'uv-tools', actor=actor))
        run_cli(list(argv))

    out()
    as_('orchestrator', 'idea', 'seed', 'demo', 'pad lines', '--from', 'finding comment#9: short lines lose')
    as_('watchdog#5', 'idea', 'seed', 'demo', 'retry crashed evals', '--from', 'run#2 crashed twice', '--base', '1')
    as_('research#7', 'idea', 'add', 'demo', 'longer lines', '--family', 'padding', '--base', '1', '--from', 'l.md')
    as_('orchestrator', 'idea', 'add', 'demo', 'from the digest', '--proposed')
    as_('human', 'idea', 'add', 'demo', 'just try it')
    assert out().splitlines() == [
        'idea#1 seed: pad lines; research develops it next',
        'idea#2 seed: retry crashed evals; research develops it next',
        'idea#3 ready p2: longer lines',
        'idea#4 proposed p2: from the digest',
        'idea#5 ready p2: just try it',
    ], "seeds wait for research; everyone else's ideas run unless asked to wait"
    with pytest.raises(SystemExit, match=r'watchdog#5 files seeds, not ideas: `piloop idea seed demo'):
        as_('watchdog#5', 'idea', 'add', 'demo', 'just do it')
    for verb in (['add', 'demo', 'just do it'], ['seed', 'demo', 'a hunch', '--from', 'run#2']):
        with pytest.raises(SystemExit, match="worker#3 files no ideas or seeds: say the hunch in your run's finding"):
            as_('worker#3', 'idea', *verb)
    with pytest.raises(SystemExit, match='a seed needs pointers'):
        as_('human', 'idea', 'seed', 'demo', 'a whim', '--from', ' ')
    with pytest.raises(SystemExit, match='no candidate#99 in project demo'):
        as_('human', 'idea', 'seed', 'demo', 'from nowhere', '--from', 'x', '--base', '99')

    as_('orchestrator', 'inbox')
    assert out().splitlines()[-3:] == [
        '2 seed(s) waiting for research:',
        'seed idea#1 (demo) by orchestrator: pad lines — finding comment#9: short lines lose',
        'seed idea#2 (demo) by watchdog#5: retry crashed evals — run#2 crashed twice',
    ]

    define = ['--hypothesis', 'pad to 80', '--evidence', 'loss table l.md', '--check', 'one file; stop at no gain']
    define += ['--metric', 'score', '--effect', '+1']
    with pytest.raises(SystemExit, match='watchdog#5 files seeds; research develops them'):
        as_('watchdog#5', 'idea', 'develop', '1', *define)
    with pytest.raises(SystemExit, match='worker#3 files no ideas or seeds'):
        as_('worker#3', 'idea', 'develop', '1', *define)
    with pytest.raises(SystemExit, match='a defined idea needs effect'):
        as_('research#8', 'idea', 'develop', '1', *define[:-2], '--effect', ' ')
    with pytest.raises(SystemExit, match='no idea#99'):
        as_('research#8', 'idea', 'develop', '99', *define)
    with pytest.raises(SystemExit, match='no candidate#99 in project demo'):
        as_('research#8', 'idea', 'develop', '1', *define, '--base', '99')
    as_('research#8', 'idea', 'develop', '1', *define, '--title', 'pad short lines to 80', '--priority', '1')
    as_('research#8', 'idea', 'develop', '2', *define, '--plan', 'then all files', '--proposed', '--family', 'retry')
    assert out().splitlines() == ['idea#1 ready p1: pad short lines to 80', 'idea#2 proposed p2: retry crashed evals']
    with pytest.raises(SystemExit, match='idea#1 is ready, not a seed: someone took it already'):
        as_('research#9', 'idea', 'develop', '1', *define)

    db = Database(cli.settings.database_url, actor='test')
    with db.session() as s:
        rows = [(i.created_by, i.provenance, i.family, i.base_candidate_id) for i in s.scalars(select(Idea))]
        assert rows == [
            ('orchestrator', 'loss table l.md; seed pointers: finding comment#9: short lines lose', None, None),
            ('watchdog#5', 'loss table l.md; seed pointers: run#2 crashed twice', 'retry', 1),
            ('research#7', 'l.md', 'padding', 1),
            ('orchestrator', None, None, None),
            ('human', None, None, None),
        ], "a developed seed keeps its filer: whether that role's hunches pay off stays measurable"
        assert s.get_one(Idea, 2).plan == (
            'First check, stop if it fails: one file; stop at no gain\nSuccess: score; expected effect: +1\n'
            'then all files'
        )
        assert [e.actor for e in s.scalars(select(Event).where(Event.kind == 'idea.created'))] == [
            'orchestrator',
            'watchdog#5',
            'research#7',
            'orchestrator',
            'human',
        ]
        assert defined_by(s, 1) == {'research#7', 'research#8', 'orchestrator', 'human'}
    db.close()
    with pytest.raises(SystemExit, match='no candidate#99 in project demo'):
        as_('human', 'idea', 'add', 'demo', 'from nowhere', '--base', '99')


def test_status_shows_why_a_campaign_stopped_and_init_resumes_it_when_the_stop_table_changes(
    project_path: Path, out
) -> None:
    run_cli(['init', str(project_path)])
    db = Database(cli.settings.database_url, actor=cli.settings.actor)

    def stop(reason: str) -> None:
        with db.session() as s:
            project = s.get_one(Project, 1)
            project.status, project.stopped_reason = ProjectStatus.STOPPED, reason
            s.commit()

    def state() -> tuple[ProjectStatus, str | None]:
        with db.session() as s:
            project = s.get_one(Project, 1)
            return project.status, project.stopped_reason

    stop('target: score 3.0000 ≥ 3')
    out()
    run_cli(['status', 'demo'])
    lines = out().splitlines()
    assert f' demo #1  {project_path} ' in lines[1] and lines[3].endswith(' · stopped: target: score 3.0000 ≥ 3'), (
        'in the headline'
    )
    run_cli(['init', str(project_path)])
    assert out().strip() == 'project demo #1 already registered; config re-read from piloop.toml'
    assert state() == (ProjectStatus.STOPPED, 'target: score 3.0000 ≥ 3'), 'the same conditions: still stopped'

    toml = project_path / 'piloop.toml'
    toml.write_text(toml.read_text() + '\n[stop]\ntarget = 4\n')
    run_cli(['init', str(project_path)])
    assert out().strip() == (
        'project demo #1 already registered; config re-read from piloop.toml; resumed: stop conditions changed'
    )
    assert state() == (ProjectStatus.ACTIVE, None)
    run_cli(['status', 'demo'])
    assert 'stopped' not in out()
    with db.session() as s:
        assert [
            (e.kind, e.actor, e.payload)
            for e in s.scalars(select(Event).where(Event.target_kind == TargetKind.PROJECT))
        ] == [
            ('project.added', 'cli', {'baseline': 1}),
            (
                'project.status',
                'human',
                {'old': 'active', 'new': 'stopped', 'stopped_reason': 'target: score 3.0000 ≥ 3'},
            ),
            ('project.status', 'human', {'old': 'stopped', 'new': 'active', 'stopped_reason': None}),
        ], 'the stop and the resume are the same change-log event, whoever wrote the row'
    db.close()


def test_a_subdirectory_or_worktree_of_a_project_resolves_to_it(
    project_path: Path, tmp_path: Path, out, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_cli(['init', str(project_path)])
    out()
    sub = project_path / 'src' / 'deep'
    sub.mkdir(parents=True)
    monkeypatch.chdir(sub)
    run_cli(['status'])
    assert f' demo #1  {project_path} ' in out(), 'cwd in a subdirectory'
    monkeypatch.chdir(tmp_path)
    run_cli(['status', str(sub)])
    assert f' demo #1  {project_path} ' in out(), 'a path ref inside the repo'
    worktree = tmp_path / 'worktree'
    Repo(project_path).git.worktree('add', '--detach', '--quiet', str(worktree), 'HEAD')
    monkeypatch.chdir(worktree)
    run_cli(['history'])
    assert f' demo #1  {project_path} ' in out(), 'cwd in a linked worktree outside the repo'


def test_outside_every_project_the_only_active_one_is_meant_else_all_of_them(
    project_path: Path, tmp_path: Path, out, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_cli(['init', str(project_path)])
    out()
    monkeypatch.chdir(tmp_path)
    db = Database(cli.settings.database_url, actor=cli.settings.actor)
    with db.session() as s:
        project = s.get_one(Project, 1)
        s.add(Project(name='other', path='/other', config=project.config, status=ProjectStatus.STOPPED))
        s.add(
            Project(
                name='third',
                path='/third',
                config=project.config,
                status=ProjectStatus.STOPPED,
                stopped_reason='max_runs: 2 runs',
            )
        )
        s.commit()
    run_cli(['status'])
    text = out()
    assert f' demo #1  {project_path} ' in text and 'other' not in text, 'one active project: that one'

    with db.session() as s:
        s.get_one(Project, 2).status = ProjectStatus.ACTIVE
        s.commit()
    run_cli(['status'])
    text = out()
    assert ' demo #1 ' in text and ' other #2 ' in text and ' third #3 ' in text, 'several active: every project'

    with db.session() as s:
        for project in s.scalars(select(Project)):
            project.status = ProjectStatus.STOPPED
        s.commit()
    run_cli(['status'])
    text = out()
    assert ' demo #1 ' in text and ' other #2 ' in text and ' third #3 ' in text, 'none active: every project'
    db.close()


def test_init_blank_then_register_then_reread(tmp_path: Path, out) -> None:
    path = tmp_path / 'fresh'
    run_cli(['init', str(path)])
    text = out()
    assert text.startswith(
        'wrote .entire/.gitignore\nwrote .entire/settings.json\nwrote .gitignore\nwrote brief.md\nwrote evaluate.py\n'
        'wrote piloop.toml\n'
    )
    assert 'The evaluate command is the referee' in text and 'still the template stub' in text
    assert Repo(path).head.commit.summary == 'project: baseline'
    (path / 'evaluate.py').write_text('print("METRIC score=2")\n')
    run_cli(['init', str(path)])
    text = out()
    assert "project fresh #1 registered; baseline candidate#1 {'score': 2.0}" in text and 'wrote' not in text
    assert 'next: add ideas: `piloop idea add fresh' in text
    assert Repo(path).head.commit.summary == 'piloop init: evaluate.py' and not Repo(path).is_dirty(), (
        'the edit was committed'
    )
    run_cli(['init', str(path)])
    assert 'project fresh #1 already registered; config re-read' in out()


def test_init_example(tmp_path: Path, out, monkeypatch: pytest.MonkeyPatch) -> None:
    template = tmp_path / 'templates' / 'demo'
    template.mkdir(parents=True)
    (template / 'piloop.toml').write_text(
        'name = "demo"\nevaluate = ["sh", "-c", "echo METRIC score=3"]\nmodel = { pi = "m" }\n'
        '[[metrics]]\nname = "score"\ndirection = "higher"\nrole = "primary"\n'
    )
    monkeypatch.setattr(cli, 'TEMPLATES', tmp_path / 'templates')
    monkeypatch.setattr(cli, 'EXAMPLE_DATA', {'demo': {}})
    with pytest.raises(SystemExit, match='no example'):
        run_cli(['init', '--example', 'missing', str(tmp_path / 'x')])
    toml = template / 'piloop.toml'
    toml.write_text(toml.read_text().replace('"sh"', '"false"'))
    with pytest.raises(SystemExit, match='baseline failed to evaluate'):
        run_cli(['init', '--example', 'demo', str(tmp_path / 'broken')])
    toml.write_text(toml.read_text().replace('"false"', '"sh"'))
    run_cli(['init', '--example', 'demo', str(tmp_path / 'd')])
    assert "project demo #1 registered; baseline candidate#1 {'score': 3.0}" in out()
    with pytest.raises(SystemExit, match="name 'demo' is already used by"):
        run_cli(['init', '--example', 'demo', str(tmp_path / 'e')])


def test_sql_and_status_while_another_client_holds_the_write_lock(
    writer: subprocess.Popen[str], monkeypatch: pytest.MonkeyPatch, out
) -> None:
    """Reads succeed without a wait, and a write fails with the name of the lock holder."""
    monkeypatch.setattr(sql, 'CONNECTION_PRAGMAS', ('busy_timeout=0', *sql.CONNECTION_PRAGMAS[1:]))
    run_cli(['sql', 'SELECT count(*) FROM projects'])
    run_cli(['status'])
    assert out().endswith('│ 0        │\n└──────────┘\ndaemon: not running\n')
    holder = rf' \(pid {writer.pid} holds the write lock: ' if sys.platform == 'linux' else ''  # from /proc/locks
    with pytest.raises(SystemExit, match=rf'^sql error: database is locked{holder}'):
        run_cli(['sql', "UPDATE projects SET name = 'x'"])


def test_sql(project_path: Path, out) -> None:
    run_cli(['init', str(project_path)])
    out()
    run_cli(['sql', 'SELECT id, name FROM projects'])
    text = out()
    assert 'id' in text and 'demo' in text
    many = (
        'WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 501) '
        'INSERT INTO comments (target_kind, target_id, author, kind, body, created_at) '
        "SELECT 'idea', 1, 'a', 'note', i, '2026' FROM n"
    )
    run_cli(['sql', many])
    out()
    run_cli(['sql', 'SELECT id FROM comments'])
    assert 'truncated to 500 rows' in out()
    run_cli(['sql', "UPDATE projects SET name='renamed'"])
    assert out().strip() == '1 row(s) changed, last insert id 0'
    with pytest.raises(SystemExit, match='not authorized'):
        run_cli(['sql', 'DROP TABLE projects'])
    with pytest.raises(SystemExit, match=r'^sql error: CHECK constraint failed: comments\.body must be a text value$'):
        run_cli(['sql', "INSERT INTO comments (target_kind, target_id, author, body) VALUES ('idea', 1, 'a', x'00')"])
    run_cli(['sql', 'SELECT updated_by, updated_at > created_at FROM projects'])
    assert 'human' in out(), 'the CLI without PILOOP_ACTOR is the human'
    run_cli(
        [
            'sql',
            "INSERT INTO ideas (track, project_id, title, hypothesis, created_by) VALUES ('score', 1, '[wip] Soft voting', 'h', 'me')",
        ]
    )
    out()
    run_cli(
        ['sql', "SELECT snippet(ideas_fts, 0, '[', ']', '…', 8) AS hit FROM ideas_fts WHERE ideas_fts MATCH 'vote'"]
    )
    assert '[wip] Soft [voting]' in out(), 'cells are data: brackets are printed, not read as rich markup'


def test_inbox_lists_what_waits_shows_one_with_its_thread_and_acknowledges(
    project_path: Path, out, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_cli(['init', str(project_path)])
    other = shutil.copytree(project_path, project_path.parent / 'other')
    toml = other / 'piloop.toml'
    toml.write_text(re.sub(r'(?m)^name = .*$', 'name = "other"', toml.read_text(), count=1))
    run_cli(['init', str(other)])
    now = datetime(2026, 10, 7, 12, tzinfo=UTC)
    monkeypatch.setattr(cli, 'utcnow', lambda: now)  # what the ages count from
    monkeypatch.setattr(inbox, 'utcnow', lambda: now)  # when an item is acknowledged
    insert = 'INSERT INTO comments (target_kind, target_id, author, kind, body, priority, created_at) VALUES '
    at = f"'{now:%Y-%m-%d %H:%M:%S}'"
    run_cli(
        [
            'sql',
            insert + f"('project', 1, 'pi', 'decision', 'requeued idea#4, re comment#2\nRevert: park it', 3, {at}),"
            f"('project', 1, 'watchdog#2', 'escalation', 'SSO expired; log in?', 1, {at}),"
            f"('project', 2, 'pi', 'decision', '[goalpost, reversible] floor 2713 → 2687', 0, {at}),"
            f"('project', 1, 'research#3', 'finding', 'the forum, not the inbox', NULL, {at}),"
            f"('comment', 2, 'pi', 'note', 'asked the team in chat', NULL, {at})",
        ]
    )
    out()
    run_cli(['inbox'])
    listing = out()
    assert [line.split()[:4] for line in listing.splitlines()[3:6]] == [
        ['│', '3', '│', '0'],
        ['│', '2', '│', '1'],
        ['│', '1', '│', '3'],
    ], 'most urgent first, every project'
    assert 'other' in listing and 'the forum' not in listing
    assert listing.splitlines()[-1] == '3 item(s) unacknowledged; `piloop inbox show <id>`, `piloop inbox ack <id>`'
    run_cli(['inbox', 'demo', '--kind', 'decision'])
    assert 'requeued idea#4' in (text := out()) and 'SSO' not in text and 'floor' not in text
    run_cli(['inbox', 'show', '2'])
    assert out().splitlines() == [
        'comment#2 escalation on project#1 by watchdog#2, 0s ago · p1, unacknowledged',
        'SSO expired; log in?',
        '',
        'linked: the comments it cites, those citing it, its replies',
        '- comment#1 decision by pi, 0s ago: requeued idea#4, re comment#2',
        '  Revert: park it',
        '- comment#5 note by pi, 0s ago: asked the team in chat',
    ]
    run_cli(['inbox', 'ack', '2', '4'])
    assert out().splitlines() == ['comment#2 acknowledged', 'comment#4 is not in the inbox']
    run_cli(['inbox', 'show', '4'])
    assert out().splitlines()[0].endswith('· not in the inbox')
    run_cli(['inbox', 'ack', '--up-to', '3', '--project', 'other'])
    assert out().splitlines() == ['comment#3 acknowledged']
    run_cli(['inbox', 'show', '3'])
    assert out().splitlines()[0].endswith('· p0, acknowledged by human 0s ago')
    run_cli(['inbox', 'ack', '--up-to', '3', '--kind', 'escalation'])
    assert out().strip() == 'nothing to acknowledge'
    run_cli(['inbox', '--all'])
    assert 'human, 0s ago' in (text := out()) and text.splitlines()[-1].startswith('3 item(s);')
    run_cli(['inbox', 'ack', '--up-to', '9'])
    run_cli(['inbox'])
    assert out().splitlines() == [
        'comment#1 acknowledged',
        '0 item(s) unacknowledged; `piloop inbox show <id>`, `piloop inbox ack <id>`',
    ]
    with pytest.raises(SystemExit, match='name the ids to acknowledge, or --up-to <id>'):
        run_cli(['inbox', 'ack'])
    with pytest.raises(SystemExit, match='name the ids to acknowledge, or --up-to <id>'):
        run_cli(['inbox', 'ack', '1', '--up-to', '3'])
    with pytest.raises(SystemExit, match='no comment#99'):
        run_cli(['inbox', 'show', '99'])


def test_inbox_wait_blocks_until_an_item_lands_and_hands_over_a_cursor_that_misses_nothing(
    project_path: Path, out, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_cli(['init', str(project_path)])
    out()
    monkeypatch.setattr(cli, 'WAIT_POLL_SECONDS', 0.01)
    run_cli(['inbox', 'wait', '--timeout', '0'])
    quiet, again = out().splitlines()
    assert quiet == 'nothing new in 0:00:00' and re.fullmatch(r'next: piloop inbox wait --after \d+', again)
    cursor = again.split()[-1]
    escalate = "INSERT INTO comments (target_kind, target_id, author, kind, priority, body) VALUES ('project', 1, 'watchdog#2', 'escalation', 1, 'SSO expired')"
    threading.Timer(0.3, run_cli, [['sql', escalate]]).start()
    run_cli(['inbox', 'wait', 'demo', '--timeout', '30'])
    lines = out().splitlines()
    assert lines[0] == '1 row(s) changed, last insert id 1', 'the writer, from the other thread'
    assert lines[1] == 'comment#1 escalation p1 on project#1 (demo) by watchdog#2: SSO expired'
    assert re.fullmatch(r'next: piloop inbox wait demo --after \d+', lines[-1]) and lines[-1].split()[-1] != cursor
    run_cli(['inbox', 'wait', '--after', cursor, '--timeout', '30'])
    assert out().splitlines()[0].startswith('comment#1 escalation'), 'from an old cursor: what landed in between'


def test_cli_writes_as_the_actor_from_the_environment(
    project_path: Path, tmp_path: Path, out, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_cli(['init', str(project_path)])
    monkeypatch.setenv('PILOOP_ACTOR', 'watchdog#4')
    monkeypatch.setattr(cli, 'settings', Settings(home=tmp_path))
    run_cli(['sql', "UPDATE projects SET name='renamed'"])
    out()
    run_cli(['sql', 'SELECT updated_by FROM projects'])
    assert 'watchdog#4' in out()
    run_cli(['sql', 'UPDATE projects SET updated_by = NULL'])
    run_cli(['init', str(project_path)])
    out()
    run_cli(['sql', 'SELECT updated_by FROM projects'])
    assert 'watchdog#4' in out(), 'ORM writes carry the same actor'


def test_candidate_import_registers_a_reference_shown_apart_and_refuses_a_bad_ref_or_a_known_tree(
    project_path: Path, out
) -> None:
    run_cli(['init', str(project_path)])
    out()
    repo = Repo(project_path)
    repo.git.checkout('-qb', 'elsewhere')
    (project_path / 'solve.txt').write_text('')
    repo.git.commit('-qam', 'an empty solution')
    sha = repo.head.commit.hexsha
    repo.git.checkout('-q', 'main')

    run_cli(['candidate', 'import', 'demo', 'elsewhere', '--summary', 'from another campaign'])
    assert out().strip() == (
        'candidate#2 reference: from another campaign; the daemon measures it; start an idea from it with'
        ' `piloop idea add demo "<title>" --base 2`'
    )
    with closing(Database(cli.settings.database_url, actor='test')) as db, db.session() as s:
        c = s.get_one(Candidate, 2)
        assert (c.verdict, c.verdict_by, c.verdict_reason) == (
            Verdict.REFERENCE,
            'human',
            f'imported from elsewhere ({sha[:7]})',
        )
        assert (c.run_id, c.parent_id, c.commit_sha, c.measurements) == (None, None, sha, [])
        [event] = s.scalars(select(Event).where(Event.kind == 'candidate.imported')).all()
        assert (event.actor, event.target_id, event.payload) == ('human', 2, {'ref': 'elsewhere', 'commit': sha})
    repo.git.branch('-qD', 'elsewhere')
    assert repo.git.rev_parse('refs/piloop/demo/reference/2') == sha, 'pinned: it outlives its branch'

    run_cli(['status'])
    text = out()
    assert 'demo · head score 1.0000 (candidate#1) · 1 kept / 1 candidates · 1 starting point ·' in text
    assert '### Recent candidates (1 of 1; score, vs parent)' in text, (
        'a starting point is no candidate of the campaign'
    )
    run_cli(['history'])
    text = out()
    assert 'History: starting points (pin one with' in text
    assert re.search(rf'#2 +{sha[:7]} +— +from another campaign', text), 'unmeasured yet'

    with pytest.raises(
        SystemExit, match=r"the tree of 'refs/piloop/demo/reference/2' already is candidate#2 \(reference\)"
    ):
        run_cli(['candidate', 'import', 'demo', 'refs/piloop/demo/reference/2'])
    with pytest.raises(SystemExit, match=r"the tree of 'main' already is candidate#1 \(keep\)"):
        run_cli(['candidate', 'import', 'demo', 'main'])
    with pytest.raises(SystemExit, match=r"'nope' names no commit in .*: Ref 'nope' did not resolve"):
        run_cli(['candidate', 'import', 'demo', 'nope'])


def test_candidate_note_keeps_a_regressed_result_in_view_with_follow_ups_pinned_to_it(
    project_path: Path, out, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_cli(['init', str(project_path)])
    run_cli(['idea', 'add', 'demo', 'try again'])
    out()
    with closing(Database(cli.settings.database_url, actor='test')) as db, db.session() as s:
        for tree, verdict in (('worse', Verdict.DISCARD), ('waiting', Verdict.PENDING)):
            c = Candidate(
                project_id=1, tree_sha=tree, commit_sha=tree, summary=tree, verdict=verdict, verdict_by='daemon'
            )
            c.verdict_reason = 'against candidate#1: score 0 vs 1' if verdict is Verdict.DISCARD else None
            s.add(c)
            s.flush()
            s.add(Measurement(candidate_id=c.id, evaluator_id=1, metrics={'score': 0.0, 'size': 0.0}))
        s.commit()

    run_cli(['candidate', 'note', '2', 'halves the eval time', '--follow-up', 'try again', '--follow-up', 'cache it'])
    assert out().splitlines() == [
        'candidate#2 interesting: note: halves the eval time; against candidate#1: score 0 vs 1',
        'idea#2 seed, pinned to candidate#2: try again (from candidate#2)',
        'idea#3 seed, pinned to candidate#2: cache it',
    ]
    with closing(Database(cli.settings.database_url, actor='test')) as db, db.session() as s:
        assert s.get_one(Candidate, 2).verdict_by == 'human'
        idea = s.get_one(Idea, 3)
        assert (idea.status, idea.base_candidate_id, idea.created_by, idea.hypothesis) == (
            IdeaStatus.SEED,
            2,
            'human',
            'Follow-up of candidate#2 (worse): halves the eval time',
        )
    run_cli(['candidate', 'note', '2', 'and more', '--follow-up', 'try again'])
    assert out().splitlines() == [
        'candidate#2 interesting: note: and more; note: halves the eval time; against candidate#1: score 0 vs 1'
    ], 'a follow-up already filed from this candidate is not filed twice'

    with pytest.raises(SystemExit, match=r'candidate#1 is keep \(no reason\): only a measured discard or interesting'):
        run_cli(['candidate', 'note', '1', 'why'])
    with pytest.raises(SystemExit, match=r'candidate#3 is pending'):
        run_cli(['candidate', 'note', '3', 'why'])
    with pytest.raises(SystemExit, match='a note needs a reason'):
        run_cli(['candidate', 'note', '2', ' '])
    with pytest.raises(SystemExit, match='no candidate#99'):
        run_cli(['candidate', 'note', '99', 'why'])
    monkeypatch.setattr(cli, 'settings', cli.settings.model_copy(update={'actor': 'worker#3'}))
    with pytest.raises(SystemExit, match='worker#3 files no ideas or seeds'):
        run_cli(['candidate', 'note', '2', 'worth a look', '--follow-up', 'a hunch'])


def test_unknown_project(tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match='no project'):
        run_cli(['status', 'nope'])


def test_baseline_must_evaluate(project_path: Path) -> None:
    (project_path / 'piloop.toml').write_text(
        (project_path / 'piloop.toml').read_text().replace('"sh", "-c"', '"false", "-c"')
    )
    with pytest.raises(SystemExit, match='baseline failed to evaluate'):
        run_cli(['init', str(project_path)])


def herdr_calls(log: Path) -> list[list[str]]:
    return [json.loads(line) for line in log.read_text().splitlines()]


def test_attach_focuses_a_worker_tab(
    project_path: Path, out, herdr_log: Path, mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_cli(['init', str(project_path)])
    db = Database(cli.settings.database_url, actor=cli.settings.actor)
    with db.session() as s:
        for runtime, handle in (
            ('herdr', {'workspace_id': 'w7', 'tab_id': 'w7:t3', 'pane_id': 'w7:p3', 'agent': 'worker-1'}),
            ('subprocess', {'pid': 1, 'pgid': 1, 'start_time': 0, 'started_at': 0, 'log_dir': '/tmp/x'}),
        ):
            s.add(
                Worker(
                    kind='watchdog',
                    project_id=1,
                    harness='pi',
                    model='m',
                    runtime=runtime,
                    handle=handle,
                    state=WorkerState.RUNNING,
                    log_dir='/tmp/x',
                )
            )
        s.commit()
    db.close()
    out()
    execvp = mocker.patch('piloop.cli.main.os.execvp')
    mocker.patch('piloop.cli.main.sys.stdout').isatty.return_value = True
    run_cli(['attach', '1'])
    assert herdr_calls(herdr_log) == [
        ['--session', 'piloop', 'tab', 'focus', 'w7:t3'],
        ['--session', 'piloop', 'session', 'list', '--json'],
    ]
    assert execvp.call_args.args == ('herdr', ['herdr', '--session', 'piloop'])
    tty = mocker.patch('piloop.cli.main.sys.stdout')
    tty.isatty.return_value = False
    run_cli(['attach', '1'])
    assert out().strip() == 'not a terminal; would attach to Herdr session piloop with tab w7:t3 focused'
    tty.isatty.return_value = True
    monkeypatch.setenv('HERDR_SOCKET_PATH', SOCKET)
    monkeypatch.setattr(cli, 'settings', Settings(home=cli.settings.home))
    run_cli(['attach', '1'])
    assert out().strip() == 'focused tab w7:t3' and execvp.call_count == 1, 'already inside the piloop session'
    run_cli(['attach', '2'])
    assert out().strip() == (
        'worker#2 is headless (subprocess runtime): /tmp/x/worker.stdout while it runs, transcript.txt once it ends'
    )
    with pytest.raises(SystemExit, match='worker#3 is not running'):
        run_cli(['attach', '3'])
    gone = {'error': {'code': 'tab_not_found', 'message': 'tab w7:t3 is gone'}}
    monkeypatch.setenv('FAKE_HERDR_SCRIPT', json.dumps({'tab focus': [gone]}))
    with pytest.raises(SystemExit, match='tab_not_found: tab w7:t3 is gone'):
        run_cli(['attach', '1'])


def test_prompt(project_path: Path, out, herdr_log: Path) -> None:
    run_cli(['init', str(project_path)])
    db = Database(cli.settings.database_url, actor=cli.settings.actor)
    with db.session() as s:
        idea = Idea(track='score', project_id=1, title='x', hypothesis='x', created_by='t', status=IdeaStatus.SCHEDULED)
        s.add(idea)
        s.flush()
        s.add(
            Run(idea_id=idea.id, attempt=1, base_candidate_id=1, harness='script', model='m', status=RunStatus.RUNNING)
        )
        s.add(
            Worker(
                kind='worker',
                project_id=1,
                run_id=1,
                harness='script',
                model='m',
                runtime='herdr',
                handle={'workspace_id': 'w7', 'tab_id': 'w7:t2', 'pane_id': 'w7:p2', 'agent': 'worker-1'},
                state=WorkerState.RUNNING,
                log_dir='x',
            )
        )
        s.commit()
    db.close()
    out()
    run_cli(['prompt', '1', 'try harder'])
    assert out().strip() == 'prompted worker#1'
    assert herdr_calls(herdr_log)[-1] == ['--session', 'piloop', 'agent', 'prompt', 'worker-1', 'try harder']
    with pytest.raises(SystemExit, match='worker#2 is not running'):
        run_cli(['prompt', '2', 'x'])


def test_launch_requests_a_worker(project_path: Path, out) -> None:
    run_cli(['init', str(project_path)])
    out()
    run_cli(['launch', 'demo', 'research'])
    run_cli(['launch', str(project_path), 'watchdog', '--harness', 'pi', '--model', 'opus', '--reason', 'eyeball it'])
    assert out().splitlines() == [
        'worker#1 research requested for demo: requested from the CLI',
        'worker#2 watchdog requested for demo: eyeball it',
    ]
    db = Database(cli.settings.database_url, actor=cli.settings.actor)
    with db.session() as s:
        rows = [(w.kind, w.state, w.harness, w.model, w.reason) for w in s.scalars(select(Worker).order_by(Worker.id))]
    db.close()
    assert rows == [
        (WorkerKind.RESEARCH, WorkerState.REQUESTED, None, None, 'requested from the CLI'),
        (WorkerKind.WATCHDOG, WorkerState.REQUESTED, 'pi', 'opus', 'eyeball it'),
    ]
    run_cli(['status', 'demo'])
    assert '- Agents live: none; requested: research#1, watchdog#2' in out(), (
        'a request is not an agent until the daemon launches it'
    )
    with pytest.raises(cyclopts.exceptions.CoercionError, match='Choose from: "worker", "watchdog", "research"'):
        run_cli(['launch', 'demo', 'janitor'])
    run_cli(['launch', 'demo', 'research', '--scout', '--reason', 'look outside'])
    assert out().strip() == 'worker#3 research (scout) requested for demo: look outside'
    db = Database(cli.settings.database_url, actor=cli.settings.actor)
    with db.session() as s:
        assert [is_scout(s, w) for w in (1, 2, 3)] == [False, False, True]
    db.close()
    with pytest.raises(SystemExit, match='--scout is a mode of the research role'):
        run_cli(['launch', 'demo', 'watchdog', '--scout'])


def test_serve_builds_a_daemon(mocker: MockerFixture) -> None:
    daemon = mocker.patch('piloop.cli.main.Daemon')
    run_cli(['serve'])
    assert daemon.call_args.args == (cli.settings,) and daemon.return_value.serve.called


def test_serve_detach_status_stop(out, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('PILOOP_TICK_SECONDS', '0.05')
    run_cli(['status'])
    assert out().strip() == 'daemon: not running'
    run_cli(['serve', '--detach'])
    text = out().strip()
    match = re.fullmatch(rf'daemon started: pid (\d+), log {re.escape(str(cli.settings.log_file))}', text)
    assert match is not None, text
    pid = int(match.group(1))
    wait_until(cli.settings.pid_file.is_file)
    assert int(cli.settings.pid_file.read_text()) == pid
    with pytest.raises(SystemExit, match=f'already running \\(pid {pid}'):
        run_cli(['serve', '--detach'])
    with pytest.raises(SystemExit, match=f'already running \\(pid {pid}'):
        Daemon(cli.settings).serve()
    run_cli(['status'])
    assert out().startswith(f'daemon: pid {pid}, up 0:00:0')

    reaper = threading.Thread(target=os.waitpid, args=(pid, 0))
    reaper.start()  # this process is the daemon's parent: reap it, or its zombie keeps looking alive to `stop`
    run_cli(['stop'])
    reaper.join()
    assert out().strip() == f'daemon pid {pid} stopped'
    assert not alive(pid) and not cli.settings.pid_file.exists()
    with pytest.raises(SystemExit, match='no daemon is running'):
        run_cli(['stop'])


def test_serve_detach_reports_a_child_that_dies_before_claiming(out) -> None:
    (cli.settings.home / 'piloop.db').mkdir()
    with pytest.raises(SystemExit, match=f'exited with 1 before taking {re.escape(str(cli.settings.pid_file))}'):
        run_cli(['serve', '--detach'])
    assert not cli.settings.pid_file.exists() and 'unable to open database file' in cli.settings.log_file.read_text()


def test_stop_reports_a_daemon_that_will_not_die(out) -> None:
    for stubborn in hold_lock(cli.settings.pid_file, 'stubborn'):
        with pytest.raises(SystemExit, match=f'daemon pid {stubborn.pid} did not exit within 0.3s'):
            run_cli(['stop', '--timeout', '0.3'])


def test_restart_reports_a_daemon_that_does_not_come_back_and_needs_one(out) -> None:
    with pytest.raises(SystemExit, match='no daemon is running'):
        run_cli(['restart'])
    for stubborn in hold_lock(cli.settings.pid_file, 'stubborn'):
        with pytest.raises(SystemExit, match=f'daemon pid {stubborn.pid} did not restart within 0.3s'):
            run_cli(['restart', '--timeout', '0.3'])


def test_stop_and_status_do_not_mistake_a_reused_pid_for_the_daemon(out) -> None:
    sleeper = subprocess.Popen(['sleep', '60'])
    cli.settings.pid_file.parent.mkdir(parents=True, exist_ok=True)
    cli.settings.pid_file.write_text(str(sleeper.pid))
    try:
        run_cli(['status'])
        assert out().startswith('daemon: not running')
        with pytest.raises(SystemExit, match='no daemon is running'):
            run_cli(['stop'])
        assert sleeper.poll() is None, 'the lock is free: nothing is ours to signal'
    finally:
        sleeper.kill()
        sleeper.wait()


@pytest.mark.skipif(sys.platform != 'linux', reason='macOS gets a launchd plist; tests/daemon/test_serve.py checks it')
def test_service_unit_prints_a_systemd_unit_on_linux(out) -> None:
    run_cli(['service-unit'])
    text = out()
    assert text.startswith('[Unit]\n') and text.endswith('WantedBy=default.target\n')
    assert (
        f'ExecStart={sys.executable} -m piloop serve\nRestart=on-failure\nEnvironment="PILOOP_HOME={cli.settings.home}"\n'
        in text
    )


def test_status_without_results_and_prompt_not_steerable(project_path: Path, out, mocker: MockerFixture) -> None:
    run_cli(['init', str(project_path)])
    db = Database(cli.settings.database_url, actor=cli.settings.actor)
    with db.session() as s:
        for c in s.scalars(select(Candidate)):
            c.verdict = Verdict.DISCARD
        idea = Idea(track='score', project_id=1, title='x', hypothesis='x', created_by='t', status=IdeaStatus.SCHEDULED)
        s.add(idea)
        s.flush()
        s.add(
            Run(
                idea_id=idea.id,
                attempt=1,
                base_candidate_id=1,
                harness='script',
                model='m',
                status=RunStatus.RUNNING,
                started_at=utcnow() - timedelta(hours=2),  # quiet long enough that status asks for its jobs
            )
        )
        s.add(
            Worker(
                kind='worker',
                project_id=1,
                run_id=1,
                harness='script',
                model='m',
                runtime='subprocess',
                handle={'pid': 1, 'pgid': 1, 'start_time': 0, 'started_at': 0, 'log_dir': '/tmp/x'},
                state=WorkerState.RUNNING,
                log_dir='x',
            )
        )
        s.add(Worker(kind='worker', project_id=1, run_id=1, harness='script', state=WorkerState.RUNNING))
        s.commit()
    db.close()
    out()
    run_cli(['status', 'demo'])
    text = out()
    assert 'demo · no trunk keep · 0 kept / 1 candidates' in text
    assert '  - worker#1 script/m 0s, on idea#1 x: —' in text, 'a headless agent has no tab'
    assert 'silent' not in text, 'a one-shot process tracks no jobs; a row not launched yet has no runtime to ask'

    with pytest.raises(SystemExit, match='cannot be steered'):
        run_cli(['prompt', '1', 'x'])


FAKE_BIN = Path(__file__).parent.parent / 'fixtures' / 'fake_bin.py'
GIT_RECEIPT = """[tool]
requirements = [{ name = "piloop", git = "https://github.com/hiloopai/piloop" }]
entrypoints = [{ name = "piloop", install-path = "/home/me/.local/bin/piloop", from = "piloop" }]
"""


def write_receipt(text: str) -> None:
    receipt = cli.settings.uv_tool_dir / 'piloop' / 'uv-receipt.toml'
    receipt.parent.mkdir(parents=True)
    receipt.write_text(text)


@pytest.fixture
def fake_bins(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Put fake CLIs first on PATH, only those named, and return a reader of their argv log."""
    log = tmp_path / 'bins.log'
    monkeypatch.setenv('FAKE_BIN_LOG', str(log))

    def install(*names: str) -> Callable[[], list[list[str]]]:
        bin_dir = tmp_path / 'fake-bin'
        bin_dir.mkdir()
        for name in names:
            (bin_dir / name).symlink_to(FAKE_BIN)
            assert os.access(bin_dir / name, os.X_OK), 'a dangling fake would let the real tool run'
        # the fakes, git (GitPython needs it) and the venv: the real uv and pi are unreachable whatever the fakes do
        (bin_dir / 'git').symlink_to(shutil.which('git') or 'git')
        monkeypatch.setenv('PATH', f'{bin_dir}{os.pathsep}{Path(sys.executable).parent}')
        return lambda: [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []

    return install


def add_candidate_with_worker(
    db: Database, checkpoint: str | None, *, harness: str = 'pi', session_id: str | None = 's', ended: bool = True
) -> int:
    """Add a worker's run with one candidate, and return the candidate id. The checkpoint is on the worker."""
    with db.session() as s:
        idea = Idea(
            track='score',
            project_id=1,
            title=f'idea {checkpoint} {session_id} {harness} {ended}',
            hypothesis='x',
            created_by='t',
        )
        s.add(idea)
        s.flush()
        run = Run(idea_id=idea.id, attempt=1, base_candidate_id=1, harness=harness, model='m')
        s.add(run)
        s.flush()
        s.add(
            Worker(
                kind=WorkerKind.WORKER,
                project_id=1,
                run_id=run.id,
                harness=harness,
                session_id=session_id,
                checkpoint_id=checkpoint,
                ended_at=utcnow() if ended else None,
            )
        )
        candidate = Candidate(project_id=1, run_id=run.id, parent_id=1, tree_sha=f't{run.id}', commit_sha='c' * 40)
        s.add(candidate)
        s.commit()
        return candidate.id


def test_migrate_reports_the_schema_change_and_a_newer_database_is_refused(out) -> None:
    run_cli(['migrate'])
    assert out().strip() == f'schema: none → {HEAD}', 'a new database, brought to head'
    run_cli(['migrate'])
    assert out().strip() == f'schema: {HEAD}, current'
    with closing(sqlite3.connect(cli.settings.home / 'piloop.db')) as con:
        con.execute("UPDATE alembic_version SET version_num = '9999_from_the_future'")
        con.commit()
    with pytest.raises(SystemExit, match='revision 9999_from_the_future.*Run `piloop upgrade`'):
        run_cli(['status'])
    with pytest.raises(SystemExit, match='revision 9999_from_the_future.*Run `piloop upgrade`'):
        run_cli(['serve'])
    assert running_pid(cli.settings) is None, 'the daemon refused before taking the pid file'


def test_upgrade_refuses_a_checkout_that_is_not_a_uv_tool(fake_bins) -> None:
    calls = fake_bins('uv', 'pi')
    with pytest.raises(
        SystemExit, match='piloop is not installed as a uv tool \\(no .*uv-receipt.toml\\); upgrade the checkout'
    ):
        run_cli(['upgrade'])
    assert calls() == [] and not cli.settings.home.joinpath('piloop.db').exists(), 'nothing was touched'


def test_upgrade_reinstalls_migrates_and_restarts_the_daemon(
    fake_bins, out, monkeypatch: pytest.MonkeyPatch, capfd
) -> None:
    monkeypatch.setenv('PILOOP_TICK_SECONDS', '0.05')
    write_receipt(GIT_RECEIPT)
    calls = fake_bins('uv', 'pi')
    run_cli(['serve', '--detach'])
    pid = int(cli.settings.pid_file.read_text())
    out()
    version = cli.installed().removeprefix('piloop ')

    run_cli(['upgrade'])

    text = out()
    assert int(cli.settings.pid_file.read_text()) == pid and alive(pid), 'restarted in place'
    assert text.splitlines() == [
        f'CLI: {version}, installed by uv from https://github.com/hiloopai/piloop',
        '$ uv tool upgrade --reinstall-package piloop piloop',
        f'CLI: {version} → {version}',
        f'skills: piloop-operate, piloop-review, piloop-setup → {cli.settings.agent_skills_dir} (pi)',
        f'daemon: restarting pid {pid}; its running agents keep going and the new code re-attaches',
        f'daemon: pid {pid} runs the new code',
        f'schema: {HEAD}, current',
        f'ok    daemon: pid {pid} runs the installed {cli.installed()}',
        f'ok    database: {cli.settings.home}/piloop.db at schema {HEAD}, current',
        f'ok    secrets: from the environment of the daemon (pid {pid}) only (PILOOP_SECRETS_COMMAND unset)',
        f'ok    skills (pi): current in {cli.settings.agent_skills_dir}',
        'ok    harnesses: on PATH: none configured',
    ], 'the new code checks the install last'
    assert calls() == [['uv', 'tool', 'upgrade', '--reinstall-package', 'piloop', 'piloop']]
    terminal = capfd.readouterr().out.splitlines()
    assert 'uv: ok' in terminal, 'the tools speak for themselves, on the terminal'
    reaper = threading.Thread(target=os.waitpid, args=(pid, 0))
    reaper.start()
    run_cli(['stop'])
    reaper.join()


def test_upgrade_of_a_checkout_install_without_a_daemon_creates_the_schema(fake_bins, out, tmp_path: Path) -> None:
    write_receipt(f'[tool]\nrequirements = [{{ name = "piloop", directory = "{tmp_path}/src" }}]\n')
    calls = fake_bins('uv')
    run_cli(['upgrade'])
    lines = out().splitlines()
    assert lines[0].endswith(f', installed by uv from {tmp_path}/src') and lines[3:] == [
        'skills: none of claude, codex, pi, gemini, cursor-agent is on PATH, nothing installed',
        f'schema: none → {HEAD}',
        'daemon: not running, left stopped',
        'ok    daemon: not running',
        f'ok    database: {cli.settings.home}/piloop.db at schema {HEAD}, current',
        'ok    secrets: from the environment of this shell only (PILOOP_SECRETS_COMMAND unset)',
        'ok    skills: none of claude, codex, pi, gemini, cursor-agent is on PATH: nothing to check',
        'ok    harnesses: on PATH: none configured',
    ]
    assert (
        calls() == [['uv', 'tool', 'upgrade', '--reinstall-package', 'piloop', 'piloop']]
        and cli.settings.home.joinpath('piloop.db').is_file()
    )

    with closing(sqlite3.connect(cli.settings.home / 'piloop.db')) as con:
        con.execute("UPDATE alembic_version SET version_num = '9999_from_the_future'")
        con.commit()
    with pytest.raises(
        SystemExit,
        match='piloop migrate on the new code failed with exit 1: .*revision 9999_from_the_future.*Run `piloop upgrade`',
    ):
        run_cli(['upgrade'])


def test_upgrade_fails_on_what_doctor_finds_after_it(fake_bins, out, project_path: Path) -> None:
    run_cli(['init', str(project_path)])
    shutil.rmtree(project_path)
    write_receipt(GIT_RECEIPT)
    fake_bins('uv')
    out()
    with pytest.raises(SystemExit, match='piloop doctor found problems after the upgrade'):
        run_cli(['upgrade'])
    assert f'FAIL  project demo: no piloop.toml in {project_path}' in out()


def test_doctor_exits_non_zero_on_a_failed_check_and_writes_nothing(fake_bins, out) -> None:
    fake_bins()  # no pi on PATH
    with pytest.raises(SystemExit) as exited:
        run_cli(['doctor'])
    assert exited.value.code == 1
    assert out().splitlines()[1] == f'FAIL  database: none at {cli.settings.home}/piloop.db'
    assert not cli.settings.home.joinpath('piloop.db').exists(), 'no database appears'
    run_cli(['migrate'])
    out()
    run_cli(['doctor'])
    assert 'FAIL' not in out()


def test_upgrade_stops_when_uv_fails_or_is_missing_or_the_daemon_does_not_come_back(
    fake_bins, out, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_receipt(GIT_RECEIPT)
    calls = fake_bins('uv', 'pi')
    monkeypatch.setenv('FAKE_BIN_FAIL', 'uv')
    with pytest.raises(SystemExit, match='uv tool upgrade --reinstall-package piloop piloop failed with exit 1'):
        run_cli(['upgrade'])
    assert calls() == [['uv', 'tool', 'upgrade', '--reinstall-package', 'piloop', 'piloop']], (
        'pi is not touched after a failed CLI upgrade'
    )
    monkeypatch.setenv('FAKE_BIN_FAIL', 'none')
    for stubborn in hold_lock(cli.settings.pid_file, 'stubborn'):
        with pytest.raises(SystemExit, match=f'daemon pid {stubborn.pid} did not restart within 0.3s'):
            run_cli(['upgrade', '--timeout', '0.3'])
    assert not cli.settings.home.joinpath('piloop.db').exists(), 'no migration while the daemon is not back'
    monkeypatch.setenv('PATH', str(cli.settings.home))
    with pytest.raises(SystemExit, match='uv is not on PATH'):
        run_cli(['upgrade'])


def test_main(mocker: MockerFixture) -> None:
    app = mocker.patch.object(cli, 'app')
    cli.main()
    assert app.called


def test_main_turns_a_bad_config_and_any_unexpected_error_into_one_message(
    mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = mocker.patch.object(cli, 'app', side_effect=ConfigError('/p/piloop.toml is not a valid piloop config'))
    with pytest.raises(SystemExit, match='^/p/piloop.toml is not a valid piloop config$'):
        cli.main()
    app.side_effect = KeyError('body')
    with pytest.raises(SystemExit) as exited:
        cli.main()
    assert exited.value.code == (
        "piloop: KeyError: 'body'\nrun with PILOOP_DEBUG=1 for the traceback; `piloop doctor` checks the install"
    )
    assert isinstance(exited.value.__cause__, KeyError), 'the traceback stays attached'
    monkeypatch.setattr(cli, 'settings', Settings(debug=True))
    with pytest.raises(KeyError, match='body'):
        cli.main()


def test_watch_needs_a_terminal_and_runs_the_app_as_the_actor(project_path: Path, out, mocker: MockerFixture) -> None:
    run_cli(['init', str(project_path)])
    out()
    with pytest.raises(
        SystemExit, match='piloop watch reads keys from a terminal; `piloop status` prints the same once'
    ):
        run_cli(['watch'])
    mocker.patch('piloop.cli.main.sys.stdin').isatty.return_value = True
    ran: list[tuple[str | None, str, Path]] = []
    mocker.patch.object(cli.Watch, 'run', lambda app: ran.append((app.ref, app.actor, app.db_file)))
    run_cli(['watch', 'demo'])
    assert ran == [('demo', 'human', cli.settings.home / 'piloop.db')]


def test_schema_prints_the_procedures_and_tables_pi_puts_in_its_sql_tool(out) -> None:
    run_cli(['schema'])
    text = out()
    assert text.startswith('# Working with the piloop database') and 'CREATE TABLE comments (' in text


def test_with_several_tracks_every_idea_and_seed_names_its_track(project_path: Path, out) -> None:
    toml = project_path / 'piloop.toml'
    text = toml.read_text().replace('role = "primary"', 'role = "primary"\nmargin = 1')
    text = text.replace(
        'name = "size"\ndirection = "lower"', 'name = "size"\ndirection = "lower"\nrole = "primary"\nmargin = 2'
    )
    toml.write_text(text + '\n[tracks.score]\nshare = 0.5\n\n[tracks.size]\nshare = 0.5\n')
    run_cli(['init', str(project_path)])
    out()
    with pytest.raises(SystemExit, match='demo has tracks score, size: name one with --track'):
        run_cli(['idea', 'add', 'demo', 'more lines'])
    with pytest.raises(SystemExit, match="demo has tracks score, size: there is no track 'speed'"):
        run_cli(['idea', 'seed', 'demo', 'faster', '--from', 'x', '--track', 'speed'])
    run_cli(['idea', 'add', 'demo', 'shorter lines', '--track', 'size'])
    run_cli(['idea', 'seed', 'demo', 'more lines', '--from', 'run#1', '--track', 'score'])
    with closing(Database(cli.settings.database_url, actor='test')) as db, db.session() as s:
        worse = Candidate(project_id=1, tree_sha='w', commit_sha='w', summary='w', verdict=Verdict.DISCARD)
        s.add(worse)
        s.commit()
    run_cli(['candidate', 'note', '2', 'cheaper', '--follow-up', 'trim it', '--track', 'size'])
    run_cli(['candidate', 'note', '2', 'and', '--follow-up', 'pad it'])
    with closing(Database(cli.settings.database_url, actor='test')) as db, db.session() as s:
        rows = [(i.title, i.track) for i in s.scalars(select(Idea).order_by(Idea.id))]
    assert rows == [('shorter lines', 'size'), ('more lines', 'score'), ('trim it', 'size'), ('pad it', 'score')], (
        'a follow-up of a candidate no run made is on the first track unless it says otherwise'
    )
