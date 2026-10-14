"""The GCS remote, against a stand-in for ``google.cloud.storage``'s client: no network, no credentials."""

import io
import sys
from pathlib import Path

import google.cloud
import pytest
from google.api_core.exceptions import NotFound
from google.cloud import storage
from pydantic import ValidationError

from piloop.settings import RemoteTable
from piloop.store import Store, key
from piloop.store.config import remote_config
from piloop.store.gcs import Gcs, GcsConfig


class FakeBlob:
    def __init__(self, bucket: FakeBucket, name: str) -> None:
        self.bucket, self.name, self.storage_class = bucket, name, None

    def upload_from_filename(self, filename: str) -> None:
        self.bucket.objects[self.name] = (Path(filename).read_bytes(), self.storage_class)

    def download_to_filename(self, filename: str) -> None:
        if self.name not in self.bucket.objects:
            raise NotFound(self.name)
        Path(filename).write_bytes(self.bucket.objects[self.name][0])

    def exists(self) -> bool:
        return self.name in self.bucket.objects


class FakeBucket:
    def __init__(self, name: str) -> None:
        self.name = name
        self.objects: dict[str, tuple[bytes, str | None]] = {}

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(self, name)


class FakeClient:
    buckets: dict[str, FakeBucket] = {}
    made: list[tuple[str, str, str | None]] = []
    """How each client was made: from Application Default Credentials, or from a key file."""

    def __init__(self, project: str | None = None) -> None:
        self.project = project

    @classmethod
    def from_service_account_json(cls, json_credentials_path: str, project: str | None = None) -> FakeClient:
        cls.made.append(('key file', json_credentials_path, project))
        return cls(project)

    def bucket(self, name: str) -> FakeBucket:
        return self.buckets.setdefault(name, FakeBucket(name))


@pytest.fixture
def buckets(monkeypatch: pytest.MonkeyPatch) -> dict[str, FakeBucket]:
    monkeypatch.setattr(FakeClient, 'buckets', {})
    monkeypatch.setattr(FakeClient, 'made', [])
    monkeypatch.setattr(storage, 'Client', FakeClient)
    return FakeClient.buckets


def test_blobs_go_under_the_prefix_in_the_configured_class_and_come_back(
    buckets: dict[str, FakeBucket], tmp_path: Path
) -> None:
    remote = GcsConfig(bucket='piloop-test', prefix='/piloop/store/', storage_class='COLDLINE').open()
    assert (remote.url, remote.prefix) == ('gs://piloop-test/piloop/store', 'piloop/store/')
    blob = Store(tmp_path / 'store', remote).put(io.BytesIO(b'{"a": 1}'))
    assert buckets['piloop-test'].objects[f'piloop/store/{key(blob.hash)}'][1] == 'COLDLINE'
    assert GcsConfig(bucket='b').open().url == 'gs://b', 'a bucket root has no prefix'


def test_a_key_file_replaces_application_default_credentials(buckets: dict[str, FakeBucket], tmp_path: Path) -> None:
    key_file = tmp_path / 'sa.json'
    remote = GcsConfig(bucket='b', project='p', credentials_file=key_file).open()
    assert FakeClient.made == [('key file', str(key_file), 'p')] and remote.url == 'gs://b'


def test_the_gcs_table_has_its_own_keys() -> None:
    """The same ``[storage.remote]`` table, with GCS's keys instead of S3's."""
    config = remote_config(RemoteTable(kind='gcs', bucket='b', prefix='p', storage_class='ARCHIVE', project='x'))
    assert config == GcsConfig(bucket='b', prefix='p', storage_class='ARCHIVE', project='x')
    for table, error in (
        (RemoteTable(kind='gcs', bucket='gs://b/p'), 'is no bucket name'),
        (RemoteTable(kind='gcs', bucket='b', storage_class='GLACIER_IR'), "'STANDARD', 'NEARLINE', 'COLDLINE'"),
        (RemoteTable(kind='gcs', url='s3://b'), 'bucket'),
    ):
        with pytest.raises(ValidationError, match=error):
            remote_config(table)


def test_without_the_gcs_extra_the_store_says_how_to_install_it(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, 'google.cloud.storage', None)
    monkeypatch.delattr(google.cloud, 'storage')
    with pytest.raises(ImportError, match=r"needs the gcs extra: uv tool install --reinstall 'piloop\[gcs\]'"):
        Gcs(GcsConfig(bucket='b'))
