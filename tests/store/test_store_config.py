"""``[storage]`` opens the store, and ``[storage.backend]`` picks a registered backend that checks its own keys."""

from collections.abc import Iterator
from importlib.metadata import EntryPoint
from pathlib import Path

import pytest
from piloop_dir_store import Dir, DirConfig
from pydantic import ValidationError

from piloop.plugins import host as plugins
from piloop.plugins.contracts.store import STORAGE_BACKENDS
from piloop.plugins.host import PluginError
from piloop.settings import BackendTable, Settings, StorageSettings
from piloop.store.config import backend_config, open_store
from piloop.store.contract import check_storage_backend
from piloop.store.gcs import GcsConfig
from piloop.store.s3 import S3, S3Config
from tests.conftest import piloop_entry_points


@pytest.fixture
def registered(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[EntryPoint]]:
    """The entry points of the storage backends' group, as a test sets them, with the registry unloaded around it."""
    found: list[EntryPoint] = []
    monkeypatch.setattr(plugins, 'entry_points', lambda group: [*piloop_entry_points(group), *found])
    plugins.close()
    yield found
    plugins.close()


def test_the_store_follows_the_storage_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, registered: list[EntryPoint]
) -> None:
    local = open_store(Settings(home=tmp_path))
    assert (local.root, local.remote, local.evict_after) == (tmp_path / 'store', None, None)
    monkeypatch.setattr('boto3.client', lambda _: None)
    remote = BackendTable(kind='s3', url='s3://b/p', standard_below=10)
    staging = open_store(
        Settings(home=tmp_path, storage=StorageSettings(backend=remote, evict_after_days=0, local_max_gb=2))
    )
    assert isinstance(staging.remote, S3) and staging.remote.standard_below == 10
    assert staging.staging and staging.max_local_bytes == 2 * 1024**3


def test_a_package_adds_a_backend_and_the_backend_checks_its_own_keys(
    tmp_path: Path, registered: list[EntryPoint]
) -> None:
    registered.append(EntryPoint('dir', 'piloop_dir_store:DirConfig', STORAGE_BACKENDS.group))
    assert {name: p.value for name, p in STORAGE_BACKENDS.providers().items()} == STORAGE_BACKENDS.builtins() | {
        'dir': DirConfig
    }
    config = backend_config(BackendTable(kind='dir', root=str(tmp_path)))
    assert config == DirConfig(kind='dir', root=tmp_path)
    assert isinstance(config.open(), Dir)
    with pytest.raises(ValidationError, match='Extra inputs are not permitted'):
        backend_config(BackendTable(kind='dir', root='/r', url='s3://b'))
    with pytest.raises(ValidationError, match='url'):
        backend_config(BackendTable(kind='s3'))
    with pytest.raises(
        ValueError, match="kind = 'azure': unknown storage backend 'azure'; registered storage backends: gcs, s3, dir"
    ):
        backend_config(BackendTable(kind='azure'))


def test_an_entry_point_must_name_a_new_backend_settings_class(registered: list[EntryPoint]) -> None:
    registered.append(EntryPoint('s3', 'piloop_dir_store:DirConfig', STORAGE_BACKENDS.group))
    with pytest.raises(PluginError, match="'s3' is a storage backend already, from piloop "):
        STORAGE_BACKENDS.providers()
    registered[:] = [EntryPoint('odd', 'piloop.store:Store', STORAGE_BACKENDS.group)]
    with pytest.raises(
        PluginError, match='odd = piloop.store:Store in piloop.stores is not a piloop.store.StorageBackendConfig'
    ):
        STORAGE_BACKENDS.providers()


def test_s3_and_gcs_are_built_in() -> None:
    assert {'gcs': GcsConfig, 's3': S3Config} == STORAGE_BACKENDS.builtins()


def test_the_example_directory_backend_keeps_the_contract(tmp_path: Path) -> None:
    check_storage_backend(DirConfig(root=tmp_path / 'share').open(), tmp_path / 'scratch')
