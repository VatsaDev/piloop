import os
from collections.abc import Iterator
from pathlib import Path

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws
from mypy_boto3_s3 import S3Client
from pydantic import ValidationError

from piloop.plugins.cas.kit.objects import ObjectsCas, key
from piloop.plugins.cas.s3 import S3, S3Objects

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
    config = S3(url=f's3://{BUCKET}/piloop/store/', standard_below=1024)
    objects = S3Objects(config, tmp_path / 'tmp', client)
    assert (objects.bucket, objects.prefix) == (BUCKET, 'piloop/store/')
    assert config.where(tmp_path) == f's3://{BUCKET}/piloop/store'
    cas = ObjectsCas(objects, tmp_path / 'tmp')
    small = cas.put(b'{"a": 1}')
    big = cas.put(os.urandom(4096))  # incompressible: above 1 KiB compressed
    for cid, expected in ((small, 'STANDARD'), (big, 'GLACIER_IR')):
        head = client.head_object(Bucket=BUCKET, Key=f'piloop/store/{key(cid)}')
        assert head.get('StorageClass', 'STANDARD') == expected


def test_a_bucket_root_has_no_prefix_and_other_errors_are_raised(client: S3Client, tmp_path: Path) -> None:
    objects = S3Objects(S3(url=f's3://{BUCKET}', standard_below=0), tmp_path / 'tmp', client)
    assert objects.prefix == '' and S3(url=f's3://{BUCKET}/').where(tmp_path) == f's3://{BUCKET}'
    path = tmp_path / 'x'
    path.write_bytes(b'x')
    objects.put('blake3/x/bytes.zst', path)
    assert client.list_objects_v2(Bucket=BUCKET)['Contents'][0]['Key'] == 'blake3/x/bytes.zst'
    with objects.open('blake3/y/bytes.zst') as missing:
        assert missing is None
    elsewhere = S3Objects(S3(url='s3://no-such-bucket/p'), tmp_path / 'tmp', client)
    with pytest.raises(ClientError):
        elsewhere.has('k')
    with pytest.raises(ClientError), elsewhere.open('k'):
        pass
    assert not list((tmp_path / 'tmp').iterdir()), 'no download left behind'


def test_only_s3_urls() -> None:
    for url in ('gs://bucket/x', 's3:///x'):
        with pytest.raises(ValidationError, match='is no s3://'):
            S3(url=url)
    with pytest.raises(ValidationError, match='Extra inputs are not permitted'):
        S3.model_validate({'url': 's3://b', 'region': 'eu'})


def test_the_client_defaults_to_boto3s_own_chain(client: S3Client, tmp_path: Path) -> None:
    cas = S3(url=f's3://{BUCKET}', storage_class='STANDARD').open(tmp_path)
    assert isinstance(cas, ObjectsCas) and isinstance(objects := cas.objects, S3Objects)
    assert objects.client.meta.region_name == 'us-east-1' and objects.storage_class_of(10**9) == 'STANDARD'
