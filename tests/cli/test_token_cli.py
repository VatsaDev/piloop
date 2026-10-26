from pathlib import Path

import cyclopts
import pytest
from rich.console import Console

from piloop.api.contract import Access
from piloop.cli import common
from piloop.db.session import Database
from piloop.net.tokens import Caller, Tokens
from piloop.settings import Settings
from tests.cli.test_cli import run_cli
from tests.conftest import at_head


@pytest.fixture
def isolated_home(tmp_path: Path, schema: Path, monkeypatch: pytest.MonkeyPatch) -> Console:
    at_head(schema, tmp_path / 'piloop.db')
    monkeypatch.setattr(common, 'settings', Settings(home=tmp_path, uv_tool_dir=tmp_path / 'uv-tools'))
    console = Console(record=True, width=200, force_terminal=False)
    monkeypatch.setattr(common, 'console', console)
    return console


@pytest.fixture
def out(isolated_home: Console, api: object):
    return lambda: isolated_home.export_text(clear=True)


def test_create_prints_the_secret_once_list_never_shows_it_and_revoke_ends_it(out) -> None:
    run_cli(['token', 'create', 'gpu1'])
    secret = out().strip()
    db = Database(common.settings.database_url, actor=common.settings.actor)
    try:
        assert Tokens(db).verify(secret) == Caller('gpu1', Access.WORKER)
        with pytest.raises(SystemExit, match="a token is called 'gpu1' already"):
            run_cli(['token', 'create', 'gpu1'])
        run_cli(['token', 'list'])
        listed = out()
        assert 'gpu1' in listed and 'worker' in listed and secret not in listed
        run_cli(['token', 'revoke', 'gpu1'])
        assert out().strip() == 'token gpu1 revoked'
        assert Tokens(db).verify(secret) is None
        with pytest.raises(SystemExit, match="no token is called 'gpu1'"):
            run_cli(['token', 'revoke', 'gpu1'])
    finally:
        db.close()


def test_create_takes_the_access_of_its_caller(out) -> None:
    run_cli(['token', 'create', 'ana', '--access', 'read'])
    secret = out().strip()
    db = Database(common.settings.database_url, actor=common.settings.actor)
    try:
        assert Tokens(db).verify(secret) == Caller('ana', Access.READ)
    finally:
        db.close()
    with pytest.raises(cyclopts.CoercionError):
        run_cli(['token', 'create', 'bo', '--access', 'owner'])
