"""A store that piloop 0.2.0 wrote is read in place: the same BLAKE3 digests, the same ``bytes.zst`` layout and markers.

``tests/fixtures/store_0_2_0`` was written by piloop 0.2.0 from PyPI: ``local`` holds ``tree`` (``put_tree``) and its
JSON manifest (``put_manifest``); ``staging`` staged one blob to the directory remote ``remote`` and kept its marker.
``written.json`` has the addresses that 0.2.0 returned.
"""

import json
import os
import shutil
from datetime import timedelta
from pathlib import Path

import pytest

from piloop.campaign.archive import ManifestNotMigrated, migrate_manifests, restore
from piloop.campaign.campaigns import workspace_dir
from piloop.db.models import Archive, Campaign
from piloop.db.session import Database
from piloop.plugins.cas.cached import REMOTE, CachedCas
from piloop.plugins.cas.contract import Integrity, integrity
from piloop.plugins.cas.disk import BOUND, BoundElsewhere, Disk, DiskCas
from piloop.plugins.cas.kit.objects import ObjectsCas, key, named
from piloop.store.tree import Manifest, Missing, cid_of, materialise

FIXTURE = Path(__file__).parents[1] / 'fixtures' / 'store_0_2_0'
WRITTEN = json.loads((FIXTURE / 'written.json').read_text())
MANIFEST = named(WRITTEN['manifest'])
STAGED = named(WRITTEN['staged'])
URL = 'file:///remote'
"""The remote URL that 0.2.0 bound ``staging`` to."""


def tree(root: Path) -> dict[str, bytes | str]:
    """Each file's bytes and each symlink's target, by relative path."""
    found: dict[str, bytes | str] = {}
    for path in sorted(root.rglob('*')):
        rel = str(path.relative_to(root))
        if path.is_symlink():
            found[rel] = os.readlink(path)
        elif path.is_file():
            found[rel] = path.read_bytes()
    return found


def copy(name: str, to: Path) -> Path:
    shutil.copytree(FIXTURE / name, to / name, symlinks=True)
    return to / name


def staging(root: Path, remote: Path, url: str = URL) -> CachedCas:
    """The cache of a 0.2.0 staging home over the directory remote that it uploaded to: the objects as 0.2.0 put them."""
    return CachedCas(DiskCas(root), root, DiskCas(remote), url, timedelta(0))


def test_a_local_store_of_0_2_0_is_read_in_place_and_its_json_manifest_restores_the_tree(tmp_path: Path) -> None:
    root = copy('local', tmp_path)
    cas = Disk(path=root).open(tmp_path)
    before = {p: p.stat().st_mtime_ns for p in root.rglob('*')}
    manifest = Manifest.get(cas, MANIFEST)
    assert all(integrity(cas, cid) is Integrity.OK for cid in manifest.blobs | {MANIFEST})
    assert len(list((root / 'blake3').iterdir())) == 5
    done = materialise(cas, manifest, tmp_path / 'out')
    assert tree(tmp_path / 'out') == tree(FIXTURE / 'tree')
    assert (done.written, done.kept) == (6, [])
    modes = {e.path: e.mode for e in manifest.entries if e.link is None}
    assert modes['notes.txt'] == 0o600 and modes['idea1/run.sh'] == 0o755
    assert all((tmp_path / 'out' / path).stat().st_mode & 0o7777 == mode for path, mode in modes.items())
    for path in ('notes.txt', 'idea1/sub/data.bin'):
        cid = cas.put_file(FIXTURE / 'tree' / path)
        assert cid in manifest.blobs and cid_of(FIXTURE / 'tree' / path) == cid, 'the same content, the same address'
    assert {p: p.stat().st_mtime_ns for p in root.rglob('*') if p.name != 'tmp'} == before, 'nothing rewritten'


def test_a_staging_store_of_0_2_0_reads_through_its_remote_and_stays_bound_to_it(tmp_path: Path) -> None:
    root, remote = copy('staging', tmp_path), copy('remote', tmp_path)
    cas = staging(root, remote)
    assert cas.has(STAGED) and not (root / key(STAGED)).exists() and (root / 'blake3' / WRITTEN['staged'] / REMOTE)
    assert cas.get(STAGED) == b'staged on the remote and evicted here\n'
    assert cas.usage().remote == 1
    with pytest.raises(BoundElsewhere, match=f'mirrors to {URL}'):
        staging(root, remote, 'file:///elsewhere')
    with pytest.raises(BoundElsewhere, match=f'is a cache over {URL}'):
        Disk(path=root).open(tmp_path)


@pytest.fixture
def archived(db: Database, campaign: Campaign) -> int:
    """An archive row as revision 0027 leaves one of 0.2.0: the raw CID of its JSON manifest."""
    with db.session() as s:
        row = Archive(campaign_id=campaign.id, manifest=MANIFEST, files=6, bytes=1)
        s.add(row)
        s.commit()
        return row.id


@pytest.mark.parametrize('staged', [False, True], ids=['local', 'staging'])
def test_migrating_a_0_2_0_archive_stores_a_dasl_manifest_that_restores_every_byte(
    db: Database, campaign: Campaign, archived: int, tmp_path: Path, staged: bool
) -> None:
    """A staging home holds no manifest locally: the migration reads it through the remote, as a restore does."""
    cas: ObjectsCas | CachedCas
    if staged:
        root, objects = tmp_path / 'store', copy('local', tmp_path)
        for d in (objects / 'blake3').iterdir():
            (root / 'blake3' / d.name).mkdir(parents=True)
            (root / 'blake3' / d.name / REMOTE).touch()
        (root / BOUND).write_text(URL)
        cas = staging(root, objects)
    else:
        cas = DiskCas(copy('local', tmp_path))
    assert migrate_manifests(db, cas) == 1
    with db.session() as s:
        dasl = s.get_one(Archive, archived).manifest
        assert dasl is not None and dasl.codec.name == 'dag-cbor'
        assert Manifest.get(cas, dasl) == Manifest.get(cas, MANIFEST), 'the same tree'
        assert cas.has(MANIFEST), 'the JSON manifest stays for a restored database'
        assert migrate_manifests(db, cas) == 0, 'done once'
        restored = restore(s, cas, tmp_path / 'home', s.get_one(Campaign, campaign.id), None)
    assert restored.archives == 1
    assert tree(workspace_dir(tmp_path / 'home', campaign.id)) == tree(FIXTURE / 'tree')


def test_a_manifest_that_the_store_cannot_give_stops_the_migration_and_keeps_its_row(
    db: Database, archived: int, tmp_path: Path
) -> None:
    with pytest.raises(
        ManifestNotMigrated, match=f'archive {archived}: its manifest {MANIFEST} could not be read'
    ) as e:
        migrate_manifests(db, Disk(path=tmp_path / 'empty').open(tmp_path))
    assert isinstance(e.value.__cause__, Missing)
    with db.session() as s:
        assert s.get_one(Archive, archived).manifest == MANIFEST
