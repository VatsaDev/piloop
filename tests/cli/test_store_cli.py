import os
import shutil
from pathlib import Path

import pytest
from covalence import Cid
from rich.console import Console
from sqlalchemy import select

from piloop.campaign import campaigns as campaign_module
from piloop.campaign.archive import Job, archive, prune
from piloop.campaign.campaigns import workspace_dir
from piloop.cli import common
from piloop.db.models import Archive, Campaign, IdeaStatus
from piloop.db.session import Database
from piloop.ids import CampaignId
from piloop.settings import BackendTable, Settings, StorageSettings
from piloop.store import key
from piloop.store.config import open_store
from tests.campaign.test_archive import add_idea, add_run, files, write
from tests.cli.test_cli import run_cli
from tests.conftest import at_head
from tests.store.test_0_2_0_store import FIXTURE, MANIFEST


@pytest.fixture(autouse=True)
def isolated_home(tmp_path: Path, schema: Path, monkeypatch: pytest.MonkeyPatch) -> Console:
    at_head(schema, tmp_path / 'piloop.db')
    monkeypatch.setattr(common, 'settings', Settings(home=tmp_path, uv_tool_dir=tmp_path / 'uv-tools'))
    monkeypatch.setattr(campaign_module, 'settings', common.settings)
    console = Console(record=True, width=200, force_terminal=False)
    monkeypatch.setattr(common, 'console', console)
    return console


@pytest.fixture
def out(isolated_home: Console):
    def read() -> str:
        return isolated_home.export_text(clear=True)

    return read


@pytest.fixture
def archived(campaign_path: Path, out) -> tuple[Database, int, dict[str, bytes]]:
    """``demo`` registered, idea 1 done and archived with two files, then the rest at stop; what the workspace held."""
    run_cli(['init', str(campaign_path)])
    out()
    db = Database(common.settings.database_url, actor=common.settings.actor)
    idea = add_idea(db, CampaignId(1), 'idea')
    add_run(db, idea)
    workspace = workspace_dir(common.settings.home, CampaignId(1))
    write(workspace / f'idea{idea}' / 'a.npy', os.urandom(3069))
    write(workspace / f'idea{idea}-run.log', 'ok\n')
    write(workspace / 'scratch.md', '# Notes\n')
    before = files(workspace)
    store, home = open_store(common.settings), common.settings.home
    for job in (Job(CampaignId(1), idea), Job(CampaignId(1), None)):
        for row in archive(db, store, home, job, live=(), reserve=0).rows:
            prune(db, store, home, row.id)
    assert not list(workspace.iterdir())
    return db, idea, before


def test_restore_writes_an_idea_a_path_or_everything_back(archived, out) -> None:
    db, idea, before = archived
    workspace = workspace_dir(common.settings.home, CampaignId(1))
    run_cli(['restore', 'demo', f'idea#{idea}'])
    assert out().strip() == f'restored 2 file(s), 3.1 kB, into {workspace}; 0 already there'
    run_cli(['restore', 'demo', 'scratch.md'])
    assert out().startswith('restored 1 file(s), 8 bytes,')
    assert files(workspace) == before
    run_cli(['restore', 'demo'])
    assert out().strip().endswith('; 3 already there')
    (workspace / 'scratch.md').write_text('# Rewritten\n')
    run_cli(['restore', 'demo', str(idea)])
    run_cli(['restore', 'demo', 'scratch.md'])
    assert out().strip().endswith('kept 1 path(s) that hold other content or sit below a symlink: scratch.md')
    with pytest.raises(SystemExit, match=r'nothing archived matches idea#99 in 2 archive\(s\) of demo'):
        run_cli(['restore', 'demo', '99'])
    with pytest.raises(SystemExit, match='nothing archived matches nope/'):
        run_cli(['restore', 'demo', 'nope/'])
    db.close()


def test_restore_lists_at_most_ten_kept_files(campaign_path: Path, out) -> None:
    run_cli(['init', str(campaign_path)])
    db = Database(common.settings.database_url, actor=common.settings.actor)
    workspace = workspace_dir(common.settings.home, CampaignId(1))
    for n in range(12):
        write(workspace / 'notes' / f'{n:02}.md', 'a')
    archive(db, open_store(common.settings), common.settings.home, Job(CampaignId(1), None), live=(), reserve=0)
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
    assert lines[0].endswith(' kB), no storage backend ([storage.backend])'), 'decimal units, as rich.filesize.decimal'
    assert lines[4].split() == ['demo', '2', '3', '3.1', 'kB']
    assert lines[-1].startswith('to archive: 0 done idea(s), 0 stopped campaign(s); latest archiving 20')
    run_cli(['store', 'evict'])
    assert out().strip() == 'uploaded 0 blob(s), evicted 0 (0 bytes)'
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
    storage = StorageSettings(backend=BackendTable(kind='s3', url='s3://bucket/prefix'))
    monkeypatch.setattr(common, 'settings', Settings(home=tmp_path, uv_tool_dir=tmp_path / 'uv-tools', storage=storage))
    monkeypatch.setattr('boto3.client', lambda _: None)
    run_cli(['store', 'status'])
    assert out().splitlines()[0].endswith(', 0 on s3://bucket/prefix')
    db.close()


def test_status_with_a_ready_idea_shows_nothing_archived(campaign_path: Path, out) -> None:
    run_cli(['init', str(campaign_path)])
    db = Database(common.settings.database_url, actor=common.settings.actor)
    add_idea(db, CampaignId(1), 'waiting', IdeaStatus.READY)
    out()
    run_cli(['store', 'status'])
    lines = out().splitlines()
    assert lines[0].endswith(': 0 blob(s), 0 local (0 bytes), no storage backend ([storage.backend])')
    assert (
        lines[-1]
        == 'to archive: 0 done idea(s), 0 stopped campaign(s) (the daemon works through them in the background, one at a time)'
    )
    db.close()


def test_migrate_stores_each_manifest_of_0_2_again_and_names_one_that_it_cannot_read(out) -> None:
    shutil.copytree(FIXTURE / 'local', common.settings.store_dir)
    db = Database(common.settings.database_url, actor=common.settings.actor)
    with db.session() as s:
        s.add(Campaign(name='c', path='/c', config={}))
        s.flush()
        s.add(Archive(campaign_id=1, manifest=MANIFEST))
        s.commit()
    run_cli(['migrate'])
    assert out().splitlines()[-1] == 'manifests: 1 stored again in the new format'
    run_cli(['migrate'])
    assert 'manifests' not in out(), 'nothing left to do'
    with db.session() as s:
        s.add(Archive(campaign_id=1, manifest=Cid.of(b'gone')))
        s.commit()
    db.close()
    with pytest.raises(SystemExit, match=f'archive 2: its manifest {Cid.of(b"gone")} could not be read'):
        run_cli(['migrate'])
