"""Every built-in CAS provider, and covalence's stores, pass the one contract (``piloop.plugins.cas.kit.conformance``);
the check catches a store that does not."""

from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import BinaryIO

import boto3
import pytest
from covalence import Cas as Covalence, Cid
from google.cloud import storage
from moto import mock_aws

from piloop.plugins.cas.cached import CachedCas
from piloop.plugins.cas.contract import UNBOUNDED, Cas
from piloop.plugins.cas.disk import Disk, DiskCas
from piloop.plugins.cas.gcs import Gcs
from piloop.plugins.cas.kit.conformance import ContractBroken, check_cas
from piloop.plugins.cas.kit.objects import ObjectsCas
from piloop.plugins.cas.s3 import S3, S3Objects
from tests.cas.test_gcs import FakeClient

BUCKET = 'piloop-contract'


@pytest.fixture(params=['disk', 'cached', 'staging', 's3', 'gcs', 'covalence-memory', 'covalence-sqlite'])
def cas(request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Cas]:
    """Each store over a stand-in that behaves as the service does: moto for S3, a fake client for GCS."""
    disk = Disk(path=tmp_path / 'store')
    match request.param:
        case 'disk':
            yield disk.open(tmp_path)
        case 'cached' | 'staging':
            evict = timedelta(0) if request.param == 'staging' else None
            remote = Disk(path=tmp_path / 'remote').open(tmp_path)
            yield CachedCas(disk.tier(tmp_path), tmp_path / 'store', remote, 'file://remote', evict)
        case 's3':
            for name in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY'):
                monkeypatch.setenv(name, 'testing')
            monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
            with mock_aws():
                client = boto3.client('s3')
                client.create_bucket(Bucket=BUCKET)
                config = S3(url=f's3://{BUCKET}/piloop', standard_below=0)
                yield ObjectsCas(S3Objects(config, tmp_path / 'tmp', client), tmp_path / 'tmp')
        case 'gcs':
            monkeypatch.setattr(FakeClient, 'buckets', {})
            monkeypatch.setattr(storage, 'Client', FakeClient)
            yield Gcs(bucket=BUCKET, prefix='piloop').open(tmp_path)
        case 'covalence-memory':
            yield Covalence.memory()
        case _:
            yield Covalence.sqlite(tmp_path / 'blobs.sqlite')


def test_every_built_in_store_and_covalences_keep_the_contract(cas: Cas, tmp_path: Path) -> None:
    check_cas(cas, tmp_path / 'scratch')


class Forgetful(ObjectsCas):
    """A store that says it holds what it never stored."""

    def has(self, cid: Cid) -> bool:
        return True


def test_the_check_names_the_rule_a_store_breaks(tmp_path: Path) -> None:
    disk = DiskCas(tmp_path / 'store')
    with pytest.raises(ContractBroken, match=r'has\(\) is False for a blob never stored'):
        check_cas(Forgetful(disk.objects, disk.scratch), tmp_path / 'scratch')


class Lenient(ObjectsCas):
    """A store that takes a source of any length."""

    def put_fileobj(self, file: BinaryIO, *, max_bytes: int, expected_length: int | None = None) -> Cid:
        return super().put_fileobj(file, max_bytes=UNBOUNDED)


def test_the_check_catches_a_store_that_ignores_the_limits(tmp_path: Path) -> None:
    disk = DiskCas(tmp_path / 'store')
    with pytest.raises(ContractBroken, match=r'put_fileobj\(\) refuses a source of another length'):
        check_cas(Lenient(disk.objects, disk.scratch), tmp_path / 'scratch')
