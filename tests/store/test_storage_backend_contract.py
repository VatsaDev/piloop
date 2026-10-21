"""Every built-in storage backend passes the one contract (``piloop.store.contract``), and the check catches one that does
not."""

from collections.abc import Iterator
from pathlib import Path

import boto3
import pytest
from google.cloud import storage
from moto import mock_aws

from piloop.store import StorageBackend
from piloop.store.contract import ContractBroken, check_storage_backend
from piloop.store.gcs import GcsConfig
from piloop.store.s3 import S3, S3Config
from tests.store.test_gcs import FakeClient
from tests.store.test_store import DirRemote

BUCKET = 'piloop-contract'


@pytest.fixture(params=['s3', 'gcs', 'dir'])
def backend(
    request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[StorageBackend]:
    """Each backend over a stand-in that behaves as the service does: moto for S3, a fake client for GCS."""
    if request.param == 's3':
        for name in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY'):
            monkeypatch.setenv(name, 'testing')
        monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
        with mock_aws():
            client = boto3.client('s3')
            client.create_bucket(Bucket=BUCKET)
            yield S3(S3Config(url=f's3://{BUCKET}/piloop', standard_below=0), client)
        return
    if request.param == 'gcs':
        monkeypatch.setattr(FakeClient, 'buckets', {})
        monkeypatch.setattr(storage, 'Client', FakeClient)
        yield GcsConfig(bucket=BUCKET, prefix='piloop').open()
        return
    yield DirRemote(tmp_path / 'remote')


def test_every_built_in_storage_backend_keeps_the_contract(backend: StorageBackend, tmp_path: Path) -> None:
    check_storage_backend(backend, tmp_path / 'scratch')


class Forgetful(DirRemote):
    """A storage backend that says it holds what it never stored."""

    def has(self, key: str) -> bool:
        return True


def test_the_check_names_the_rule_a_backend_breaks(tmp_path: Path) -> None:
    with pytest.raises(ContractBroken, match=r'has\(\) is False for a key that was never uploaded'):
        check_storage_backend(Forgetful(tmp_path / 'remote'), tmp_path / 'scratch')
