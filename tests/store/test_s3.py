import io
import os
from collections.abc import Iterator
from pathlib import Path

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws
from mypy_boto3_s3 import S3Client
from pydantic import ValidationError

from piloop.store import Store, key
from piloop.store.s3 import S3, S3Config

BUCKET = 'piloop-test'


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> Iterator[S3Client]:
    for name in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY'):
        monkeypatch.setenv(name, 'testing')
    monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
    with mock_aws():
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket=BUCKET)
        yield s3


def test_blobs_go_to_glacier_ir_unless_small_enough_that_standard_is_cheaper(client: S3Client, tmp_path: Path) -> None:
    remote = S3(S3Config(url=f's3://{BUCKET}/piloop/store/', standard_below=1024), client)
    assert (remote.bucket, remote.prefix, remote.url) == (BUCKET, 'piloop/store/', f's3://{BUCKET}/piloop/store')
    store = Store(tmp_path / 'store', remote)
    small = store.put(io.BytesIO(b'{"a": 1}'))
    big = store.put(io.BytesIO(os.urandom(4096)))  # incompressible: above 1 KiB compressed
    for blob, expected in ((small, 'STANDARD'), (big, 'GLACIER_IR')):
        head = client.head_object(Bucket=BUCKET, Key=f'piloop/store/{key(blob.hash)}')
        assert head.get('StorageClass', 'STANDARD') == expected


def test_a_bucket_root_has_no_prefix_and_other_errors_are_raised(client: S3Client, tmp_path: Path) -> None:
    remote = S3(S3Config(url=f's3://{BUCKET}', standard_below=0), client)
    assert remote.prefix == ''
    path = tmp_path / 'x'
    path.write_bytes(b'x')
    remote.upload('blake3/x/bytes.zst', path)
    assert client.list_objects_v2(Bucket=BUCKET)['Contents'][0]['Key'] == 'blake3/x/bytes.zst'
    elsewhere = S3(S3Config(url='s3://no-such-bucket/p'), client)
    with pytest.raises(ClientError):
        elsewhere.has('k')
    with pytest.raises(ClientError):
        elsewhere.download('k', tmp_path / 'y')


def test_only_s3_urls() -> None:
    for url in ('gs://bucket/x', 's3:///x'):
        with pytest.raises(ValidationError, match='is no s3://'):
            S3Config(url=url)
    with pytest.raises(ValidationError, match='Extra inputs are not permitted'):
        S3Config.model_validate({'kind': 's3', 'url': 's3://b', 'region': 'eu'})


def test_the_client_defaults_to_boto3s_own_chain(client: S3Client) -> None:
    remote = S3Config(url=f's3://{BUCKET}', storage_class='STANDARD').open()
    assert remote.client.meta.region_name == 'us-east-1' and remote.storage_class_of(10**9) == 'STANDARD'
