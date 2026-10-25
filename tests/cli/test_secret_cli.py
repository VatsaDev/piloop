"""``piloop secret``: a value goes in on stdin and never comes out, of the CLI, ``piloop sql`` or the events."""

import io
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from rich.console import Console

from piloop.actor import Agent
from piloop.cli import common
from piloop.db import sql
from piloop.db.models import AgentSession, Campaign
from piloop.db.session import Database
from piloop.settings import Settings
from tests.cli.test_cli import run_cli


@pytest.fixture
def out(db: Database, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(common, 'settings', Settings(home=tmp_path, uv_tool_dir=tmp_path / 'uv-tools'))
    console = Console(record=True, width=200, force_terminal=False)
    monkeypatch.setattr(common, 'console', console)
    return lambda: console.export_text(clear=True)


def piped(monkeypatch: pytest.MonkeyPatch, args: list[str], stdin: str) -> None:
    monkeypatch.setattr('sys.stdin', io.StringIO(stdin))
    run_cli(['secret', *args])


def test_a_secret_is_set_from_stdin_listed_by_name_and_deleted_and_no_output_shows_its_value(
    out, campaign: Campaign, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    piped(monkeypatch, ['set', 'EXA_API_KEY'], 'home-value\n')
    assert out().strip() == 'EXA_API_KEY set for the home'
    piped(monkeypatch, ['set', 'EXA_API_KEY', '--campaign', 'demo'], 'campaign-value')
    assert out().strip() == 'EXA_API_KEY set for campaign demo'
    run_cli(['secret', 'list'])
    rows = [line.split() for line in out().splitlines() if 'EXA_API_KEY' in line]
    assert [(r[0], r[1], r[-1]) for r in rows] == [('EXA_API_KEY', 'home', 'human'), ('EXA_API_KEY', 'demo', 'human')]
    with closing(sql.connect(tmp_path / 'piloop.db', actor=Agent(role='watchdog', session=1))) as con:
        assert sql.run(con, 'SELECT name, value FROM secrets ORDER BY id').rows == [
            ('EXA_API_KEY', None),
            ('EXA_API_KEY', None),
        ], 'piloop sql reads every value as NULL'
        for statement in ("UPDATE secrets SET value = 'x'", "INSERT INTO secrets (name, value) VALUES ('A', 'b')"):
            with pytest.raises(sqlite3.DatabaseError, match='not authorized'):
                sql.run(con, statement)
        events = sql.run(con, "SELECT payload FROM events WHERE kind LIKE 'secret.%'").rows
    assert len(events) == 2 and 'value' not in str(events)
    run_cli(['secret', 'rm', 'EXA_API_KEY'])
    assert out().strip() == 'EXA_API_KEY of the home deleted'
    with pytest.raises(SystemExit, match=r'the home has no secret EXA_API_KEY: see `piloop secret list`'):
        run_cli(['secret', 'rm', 'EXA_API_KEY'])
    with pytest.raises(SystemExit, match="'1X' is not an environment variable name"):
        piped(monkeypatch, ['set', '1X'], 'v')
    with pytest.raises(SystemExit, match='A: the value is empty'):
        piped(monkeypatch, ['set', 'A'], '\n')


def test_an_agent_sets_only_its_own_campaigns_secrets(
    out, db: Database, campaign: Campaign, monkeypatch: pytest.MonkeyPatch
) -> None:
    with db.session() as s:
        s.add(AgentSession(kind='watchdog', campaign_id=campaign.id))
        s.commit()
    monkeypatch.setattr(
        common, 'settings', common.settings.model_copy(update={'actor': Agent(role='watchdog', session=1)})
    )
    with pytest.raises(SystemExit, match='watchdog#1 writes only the secrets of its own campaign'):
        piped(monkeypatch, ['set', 'KEY'], 'v')
    piped(monkeypatch, ['set', 'KEY', '--campaign', 'demo'], 'v')
    assert out().strip() == 'KEY set for campaign demo'
    with pytest.raises(SystemExit, match='watchdog#1 writes only the secrets of its own campaign'):
        run_cli(['secret', 'rm', 'KEY'])
