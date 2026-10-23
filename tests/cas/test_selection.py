"""``[plugins.cas]`` selects a registered provider, which checks its own keys; ``disk`` without one."""

from collections.abc import Iterator, Mapping
from importlib.metadata import EntryPoint
from pathlib import Path

import pytest
from piloop_sqlite_cas import Sqlite
from pydantic import ValidationError

from piloop.plugins import host as plugins
from piloop.plugins.cas.cached import Cached, CachedCas
from piloop.plugins.cas.contract import CAS, CasProvider
from piloop.plugins.cas.disk import BOUND, Disk
from piloop.plugins.cas.gcs import Gcs
from piloop.plugins.cas.kit.objects import Dir, ObjectsCas, key
from piloop.plugins.cas.s3 import S3
from piloop.plugins.host import PluginError
from piloop.settings import HOME_PLUGINS
from tests.conftest import piloop_entry_points


@pytest.fixture
def registered(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[EntryPoint]]:
    """The entry points of the CAS providers' group, as a test sets them, with the registry unloaded around it."""
    found: list[EntryPoint] = []
    monkeypatch.setattr(
        plugins, 'entry_points', lambda group: [*piloop_entry_points(group), *(ep for ep in found if ep.group == group)]
    )
    plugins.close()
    yield found
    plugins.close()


def provider(table: Mapping[str, object] | None) -> CasProvider:
    """The provider that a ``[plugins.cas]`` table of ``config.toml`` chooses, validated as the daemon does."""
    found = CAS.selected(HOME_PLUGINS.validate_python({'cas': table} if table is not None else {}))
    assert isinstance(found, CasProvider)
    return found


def table(**keys: object) -> dict[str, object]:
    return keys


def test_disk_is_the_default_and_cached_nests_its_remote(tmp_path: Path) -> None:
    default = provider(None)
    assert default == Disk() and default.where(tmp_path) == f'file://{tmp_path / "store"}'
    disk = default.open(tmp_path)
    assert isinstance(disk, ObjectsCas) and disk.objects == Dir(tmp_path / 'store')
    cached = provider(table(provider='cached', evict_after_days=0, remote={'provider': 'disk', 'path': '/r'}))
    assert cached == Cached(remote=Disk(path=Path('/r')), local=Disk(), evict_after_days=0)
    opened = cached.open(tmp_path)
    assert isinstance(opened, CachedCas) and opened.staging and opened.url == 'file:///r'
    assert cached.where(tmp_path) == 'file:///r', 'a cache stands for its remote'


def test_a_provider_and_a_nested_one_check_their_own_keys() -> None:
    for keys, error in (
        ({'provider': 's3'}, 'url\n  Field required'),
        ({'provider': 's3', 'url': 'gs://b'}, 'is no s3://'),
        ({'provider': 'disk', 'bucket': 'b'}, 'Extra inputs are not permitted'),
        ({'provider': 'cached', 'remote': {'provider': 'gcs'}}, 'bucket'),
        ({'provider': 'cached', 'remote': 3}, 'not a table'),
        ({'provider': 'cached', 'remote': {'provider': 'azure'}}, "unknown CAS provider 'azure'"),
    ):
        with pytest.raises(ValidationError, match=error):
            provider(table(**keys))
    with pytest.raises(ValidationError, match="unknown CAS provider 'azure'; registered CAS providers: cached, disk"):
        provider(table(provider='azure'))


def test_a_package_adds_a_provider(tmp_path: Path, registered: list[EntryPoint]) -> None:
    registered.append(EntryPoint('sqlite', 'piloop_sqlite_cas:Sqlite', CAS.group))
    assert {name: p.model for name, p in CAS.providers().items()} == CAS.builtins() | {'sqlite': Sqlite}
    chosen = provider(table(provider='sqlite', path=str(tmp_path / 'b.sqlite')))
    assert chosen == Sqlite(path=tmp_path / 'b.sqlite')
    cid = chosen.open(tmp_path).put(b'x')
    assert chosen.open(tmp_path).get(cid) == b'x', 'in the file, from one open to the next'


def test_an_entry_point_must_name_a_new_provider_class(registered: list[EntryPoint]) -> None:
    registered.append(EntryPoint('s3', 'piloop_sqlite_cas:Sqlite', CAS.group))
    with pytest.raises(PluginError, match="'s3' is a CAS provider already, from piloop "):
        CAS.providers()
    registered[:] = [EntryPoint('odd', 'piloop.plugins.cas.kit.objects:Dir', CAS.group)]
    with pytest.raises(
        PluginError, match='odd = .* in piloop.cas is not a subclass of piloop.plugins.cas.contract.CasProvider'
    ):
        CAS.providers()


def test_the_built_in_providers() -> None:
    assert CAS.builtins() == {'cached': Cached, 'disk': Disk, 'gcs': Gcs, 's3': S3}


def test_cached_nests_a_local_tier_that_can_be_one(tmp_path: Path, registered: list[EntryPoint]) -> None:
    """``local`` takes a ``LocalProvider``; ``remote`` any provider, a package's covalence store too."""
    registered.append(EntryPoint('sqlite', 'piloop_sqlite_cas:Sqlite', CAS.group))
    local = {'provider': 'disk', 'path': str(tmp_path / 'scratch-disk')}
    chosen = provider(table(provider='cached', local=local, remote={'provider': 'sqlite', 'path': str(tmp_path / 'b')}))
    assert chosen == Cached(local=Disk(path=tmp_path / 'scratch-disk'), remote=Sqlite(path=tmp_path / 'b'))
    cas = chosen.open(tmp_path)
    cid = cas.put(b'x')
    assert (tmp_path / 'scratch-disk' / key(cid)).is_file() and (tmp_path / 'scratch-disk' / BOUND).is_file()
    assert not (tmp_path / 'store').exists(), 'the local tier is where local says'
    for other in ({'provider': 's3', 'url': 's3://b'}, {'provider': 'sqlite', 'path': '/b'}):
        with pytest.raises(
            ValidationError, match=f'{other["provider"]} cannot be here: this key takes a LocalProvider'
        ):
            provider(table(provider='cached', local=other, remote={'provider': 'disk'}))
