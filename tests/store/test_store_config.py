"""``[storage]`` opens the store, and ``[storage.remote]`` picks a registered backend that checks its own keys."""

from collections.abc import Iterator
from importlib.metadata import EntryPoint
from pathlib import Path

import pytest
from pydantic import ValidationError

from piloop.settings import RemoteTable, Settings, StorageSettings
from piloop.store import Remote, RemoteConfig, config as config_module
from piloop.store.config import BUILTIN_REMOTES, STORE_GROUP, open_store, remote_config, remotes
from piloop.store.s3 import S3, S3Config
from tests.store.test_store import DirRemote


class DirConfig(RemoteConfig):
    """A backend that a package adds: a directory."""

    root: Path

    def open(self) -> Remote:
        return DirRemote(self.root)


@pytest.fixture
def registered(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[EntryPoint]]:
    """The entry points of ``STORE_GROUP``, as a test sets them, with the registry cache cleared around it."""
    found: list[EntryPoint] = []
    monkeypatch.setattr(config_module, 'entry_points', lambda group: found if group == STORE_GROUP else [])
    remotes.cache_clear()
    yield found
    remotes.cache_clear()


def test_the_store_follows_the_storage_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, registered: list[EntryPoint]
) -> None:
    local = open_store(Settings(home=tmp_path))
    assert (local.root, local.remote, local.evict_after) == (tmp_path / 'store', None, None)
    monkeypatch.setattr('boto3.client', lambda _: None)
    remote = RemoteTable(kind='s3', url='s3://b/p', standard_below=10)
    staging = open_store(
        Settings(home=tmp_path, storage=StorageSettings(remote=remote, evict_after_days=0, local_max_gb=2))
    )
    assert isinstance(staging.remote, S3) and staging.remote.standard_below == 10
    assert staging.staging and staging.max_local_bytes == 2 * 1024**3


def test_a_package_adds_a_backend_and_the_backend_checks_its_own_keys(
    tmp_path: Path, registered: list[EntryPoint]
) -> None:
    registered.append(EntryPoint('dir', f'{__name__}:DirConfig', STORE_GROUP))
    assert remotes() == BUILTIN_REMOTES | {'dir': DirConfig}
    config = remote_config(RemoteTable(kind='dir', root=str(tmp_path)))
    assert config == DirConfig(kind='dir', root=tmp_path)
    assert isinstance(config.open(), DirRemote)
    with pytest.raises(ValidationError, match='Extra inputs are not permitted'):
        remote_config(RemoteTable(kind='dir', root='/r', url='s3://b'))
    with pytest.raises(ValidationError, match='url'):
        remote_config(RemoteTable(kind='s3'))
    with pytest.raises(ValueError, match="kind = 'gcs': no such backend; there is s3, dir"):
        remote_config(RemoteTable(kind='gcs'))


def test_an_entry_point_must_name_a_new_backend_settings_class(registered: list[EntryPoint]) -> None:
    registered.append(EntryPoint('s3', f'{__name__}:DirConfig', STORE_GROUP))
    with pytest.raises(ValueError, match="'s3' is a backend already"):
        remotes()
    remotes.cache_clear()
    registered[:] = [EntryPoint('odd', 'piloop.store:Store', STORE_GROUP)]
    with pytest.raises(TypeError, match='is not a piloop.store.RemoteConfig'):
        remotes()


def test_s3_is_built_in() -> None:
    assert {'s3': S3Config} == BUILTIN_REMOTES
