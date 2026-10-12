"""The S3 remote of the artifact store: one object per blob under ``s3://<bucket>/<prefix>/blake3/<hash>/bytes.zst``.

An object's storage class is chosen by its compressed size: ``storage_class`` (Glacier Instant Retrieval by default:
millisecond reads at a fraction of STANDARD's price), except below ``standard_below`` bytes, where Glacier IR would
bill a 128 KB minimum for each and STANDARD is cheaper. Uploads and downloads stream through boto3's transfer manager
(multipart past its threshold). Credentials are boto3's own chain: the environment, a profile, an instance role.
GCS fits as the same class pointed at its S3-compatible endpoint.

Imported only when a remote is configured: boto3 takes ~0.5 s to import. Retired with ``piloop.store`` (its module docstring has the condition).
"""

from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

STANDARD = 'STANDARD'
NOT_FOUND = ('404', 'NoSuchKey', 'NotFound')


def _not_found(e: ClientError) -> bool:
    return e.response.get('Error', {}).get('Code') in NOT_FOUND


class S3:
    """Whole objects by key under one bucket and prefix."""

    def __init__(self, url: str, *, storage_class: str, standard_below: int, client: S3Client | None = None) -> None:
        parsed = urlparse(url)
        if parsed.scheme != 's3' or not parsed.netloc:
            raise ValueError(f'{url!r} is no s3://<bucket>[/<prefix>] URL')
        self.url = url
        self.bucket = parsed.netloc
        prefix = parsed.path.strip('/')
        self.prefix = f'{prefix}/' if prefix else ''
        self.storage_class = storage_class
        self.standard_below = standard_below
        self.client = client or boto3.client('s3')

    def storage_class_of(self, size: int) -> str:
        """The class an object of ``size`` compressed bytes is stored in."""
        return STANDARD if size < self.standard_below else self.storage_class

    def upload(self, key: str, path: Path) -> None:
        self.client.upload_file(
            str(path),
            self.bucket,
            self.prefix + key,
            ExtraArgs={'StorageClass': self.storage_class_of(path.stat().st_size)},
        )

    def download(self, key: str, path: Path) -> bool:
        try:
            self.client.download_file(self.bucket, self.prefix + key, str(path))
        except ClientError as e:
            if _not_found(e):
                return False
            raise
        return True

    def has(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=self.prefix + key)
        except ClientError as e:
            if _not_found(e):
                return False
            raise
        return True
