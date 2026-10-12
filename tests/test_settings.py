from pathlib import Path

import pytest
from pydantic import ValidationError

from piloop.settings import Settings, StorageSettings


def test_storage_is_read_from_config_toml_under_the_home_and_the_environment_wins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / 'config.toml').write_text('[storage]\nremote = "s3://bucket/piloop"\nevict_after_days = 30\n')
    given = Settings(home=tmp_path)
    assert given.storage == StorageSettings(remote='s3://bucket/piloop', evict_after_days=30)
    assert given.store_dir == tmp_path / 'store'
    monkeypatch.setenv('PILOOP_HOME', str(tmp_path))
    monkeypatch.setenv('PILOOP_STORAGE__STORAGE_CLASS', 'STANDARD_IA')
    from_env = Settings()
    assert from_env.storage.remote == 's3://bucket/piloop' and from_env.storage.storage_class == 'STANDARD_IA'
    assert Settings(home=tmp_path / 'elsewhere').storage.remote is None, 'no config.toml there'


def test_eviction_needs_a_remote_and_the_remote_must_be_s3() -> None:
    with pytest.raises(ValidationError, match='need a remote'):
        StorageSettings(local_max_gb=10)
    with pytest.raises(ValidationError, match='only s3:// is supported'):
        StorageSettings(remote='gs://bucket')
    assert StorageSettings().storage_class == 'GLACIER_IR' and StorageSettings().standard_below == 128 * 1024
