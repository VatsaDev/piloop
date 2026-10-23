"""The GCS store, against a stand-in for ``google.cloud.storage``'s client: no network, no credentials."""

import sys
from pathlib import Path

import google.cloud
import pytest
from covalence import Cid
from google.api_core.exceptions import NotFound
from google.cloud import storage
from pydantic import ValidationError

from piloop.plugins.cas.gcs import Gcs
from piloop.plugins.cas.kit.objects import key
from tests.cas.test_selection import provider


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
    config = Gcs(bucket='piloop-test', prefix='/piloop/store/', storage_class='COLDLINE')
    assert config.where(tmp_path) == 'gs://piloop-test/piloop/store'
    cas = config.open(tmp_path)
    cid = cas.put(b'{"a": 1}')
    assert buckets['piloop-test'].objects[f'piloop/store/{key(cid)}'][1] == 'COLDLINE'
    assert cas.get(cid) == b'{"a": 1}' and cas.get(Cid.of(b'none')) is None
    assert Gcs(bucket='b').where(tmp_path) == 'gs://b', 'a bucket root has no prefix'


def test_a_key_file_replaces_application_default_credentials(buckets: dict[str, FakeBucket], tmp_path: Path) -> None:
    key_file = tmp_path / 'sa.json'
    Gcs(bucket='b', project='p', credentials_file=key_file).open(tmp_path)
    assert FakeClient.made == [('key file', str(key_file), 'p')]


def test_the_gcs_table_has_its_own_keys() -> None:
    """The same ``[plugins.cas]`` table, with GCS's keys instead of S3's."""
    keys = {'provider': 'gcs', 'bucket': 'b', 'prefix': 'p', 'storage_class': 'ARCHIVE', 'project': 'x'}
    expected = Gcs(bucket='b', prefix='p', storage_class='ARCHIVE', project='x')
    assert provider(keys) == expected
    for given, error in (
        ({'bucket': 'gs://b/p'}, 'is no bucket name'),
        ({'bucket': 'b', 'storage_class': 'GLACIER_IR'}, "'STANDARD', 'NEARLINE', 'COLDLINE'"),
        ({'url': 's3://b'}, 'bucket'),
    ):
        with pytest.raises(ValidationError, match=error):
            provider({'provider': 'gcs', **given})


def test_without_the_gcs_extra_the_store_says_how_to_install_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setitem(sys.modules, 'google.cloud.storage', None)
    monkeypatch.delattr(google.cloud, 'storage')
    with pytest.raises(ImportError, match=r"needs the gcs extra: uv tool install --reinstall 'piloop\[gcs\]'"):
        Gcs(bucket='b').open(tmp_path)
