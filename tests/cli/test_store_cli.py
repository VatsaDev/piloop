import os
from pathlib import Path

import pytest
from rich.console import Console
from sqlalchemy import select

from piloop.campaign import project as project_module
from piloop.campaign.archive import Job, archive, open_store, workspace_of
from piloop.cli import main as cli
from piloop.db.models import Archive, IdeaStatus
from piloop.db.session import Database
from piloop.settings import Settings, StorageSettings
from piloop.store import key
from tests.campaign.test_archive import add_idea, add_run, files, write
from tests.cli.test_cli import run_cli


@pytest.fixture(autouse=True)
def isolated_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Console:
    monkeypatch.setattr(cli, 'settings', Settings(home=tmp_path, uv_tool_dir=tmp_path / 'uv-tools'))
    monkeypatch.setattr(project_module, 'settings', cli.settings)
    console = Console(record=True, width=200, force_terminal=False)
    monkeypatch.setattr(cli, 'console', console)
    return console


@pytest.fixture
def out(isolated_home: Console):
    def read() -> str:
        return isolated_home.export_text(clear=True)

    return read


@pytest.fixture
def archived(project_path: Path, out) -> tuple[Database, int, dict[str, bytes]]:
    """``demo`` registered, idea 1 done and archived with two files, then the rest at stop; what the workspace held."""
    run_cli(['init', str(project_path)])
    out()
    db = Database(cli.settings.database_url, actor=cli.settings.actor)
    idea = add_idea(db, 1, 'idea')
    add_run(db, idea)
    workspace = workspace_of(cli.settings.home, 1)
    write(workspace / f'idea{idea}' / 'a.npy', os.urandom(3069))
    write(workspace / f'idea{idea}-run.log', 'ok\n')
    write(workspace / 'scratch.md', '# Notes\n')
    before = files(workspace)
    store = open_store(cli.settings)
    archive(db, store, cli.settings.home, Job(1, idea))
    archive(db, store, cli.settings.home, Job(1, None))
    assert not list(workspace.iterdir())
    return db, idea, before


def test_restore_writes_an_idea_a_path_or_everything_back(archived, out) -> None:
    db, idea, before = archived
    workspace = workspace_of(cli.settings.home, 1)
    run_cli(['restore', 'demo', f'idea#{idea}'])
    assert out().strip() == f'restored 2 file(s), 3.0 KB, into {workspace}; 0 already there'
    run_cli(['restore', 'demo', 'scratch.md'])
    assert out().startswith('restored 1 file(s), 8 B,')
    assert files(workspace) == before
    run_cli(['restore', 'demo'])
    assert out().strip().endswith('; 3 already there')
    (workspace / 'scratch.md').write_text('# Rewritten\n')
    run_cli(['restore', 'demo', str(idea)])
    run_cli(['restore', 'demo', 'scratch.md'])
    assert out().strip().endswith('kept 1 existing file(s) whose content differs: scratch.md')
    with pytest.raises(SystemExit, match=r'nothing archived matches idea#99 in 2 archive\(s\) of demo'):
        run_cli(['restore', 'demo', '99'])
    with pytest.raises(SystemExit, match='nothing archived matches nope/'):
        run_cli(['restore', 'demo', 'nope/'])
    db.close()


def test_restore_lists_at_most_ten_kept_files(project_path: Path, out) -> None:
    run_cli(['init', str(project_path)])
    db = Database(cli.settings.database_url, actor=cli.settings.actor)
    workspace = workspace_of(cli.settings.home, 1)
    for n in range(12):
        write(workspace / 'notes' / f'{n:02}.md', 'a')
    archive(db, open_store(cli.settings), cli.settings.home, Job(1, None))
    for n in range(12):
        write(workspace / 'notes' / f'{n:02}.md', 'b')
    out()
    with pytest.raises(SystemExit, match=r'nothing archived matches idea#1 in 1 archive'):
        run_cli(['restore', 'demo', 'idea#1'])  # no idea 1: nothing of it
    run_cli(['restore', 'demo'])
    assert out().strip().endswith('notes/09.md and 2 more')
    db.close()


def test_store_status_evict_and_verify(archived, out, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db, _, _ = archived
    run_cli(['store', 'status'])
    lines = out().splitlines()
    assert lines[0].startswith(f'{tmp_path / "store"}: 5 blob(s), 5 local (3.')
    assert lines[0].endswith(' KB), no remote ([storage] remote)')
    assert lines[4].split() == ['demo', '2', '3', '3.0', 'KB']
    assert lines[-1].startswith('to archive: 0 done idea(s), 0 stopped project(s); latest archiving 20')
    run_cli(['store', 'evict'])
    assert out().strip() == 'uploaded 0 blob(s), evicted 0 (0 B)'
    run_cli(['store', 'verify'])
    assert out().strip() == '2 manifest(s), 5 blob(s): 5 ok, 0 missing, 0 corrupt'
    run_cli(['store', 'verify', 'demo'])
    assert out().strip().startswith('2 manifest(s)')
    with db.session() as s:
        manifest = s.scalars(select(Archive.manifest).where(Archive.idea_id.is_not(None))).one()
    assert manifest is not None
    (tmp_path / 'store' / key(manifest)).write_bytes(b'rot')
    with pytest.raises(SystemExit):
        run_cli(['store', 'verify'])
    assert out().strip() == '2 manifest(s), 3 blob(s): 2 ok, 0 missing, 1 corrupt', 'a rotten manifest names nothing'
    remote = Settings(
        home=tmp_path, uv_tool_dir=tmp_path / 'uv-tools', storage=StorageSettings(remote='s3://bucket/prefix')
    )
    monkeypatch.setattr(cli, 'settings', remote)
    monkeypatch.setattr('piloop.store.s3.boto3.client', lambda _: None)
    run_cli(['store', 'status'])
    assert out().splitlines()[0].endswith(', 0 on s3://bucket/prefix')
    db.close()


def test_status_with_a_ready_idea_shows_nothing_archived(project_path: Path, out) -> None:
    run_cli(['init', str(project_path)])
    db = Database(cli.settings.database_url, actor=cli.settings.actor)
    add_idea(db, 1, 'waiting', IdeaStatus.READY)
    out()
    run_cli(['store', 'status'])
    lines = out().splitlines()
    assert lines[0].endswith(': 0 blob(s), 0 local (0 B), no remote ([storage] remote)')
    assert (
        lines[-1]
        == 'to archive: 0 done idea(s), 0 stopped project(s) (the daemon works through them in the background, one at a time)'
    )
    db.close()
