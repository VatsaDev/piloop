from pathlib import Path

import pytest
from pydantic import ValidationError

from piloop.settings import RemoteTable, Settings, StorageSettings


def test_storage_is_read_from_config_toml_under_the_home_and_the_environment_wins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / 'config.toml').write_text(
        '[storage]\nevict_after_days = 30\n\n[storage.remote]\nkind = "s3"\nurl = "s3://bucket/piloop"\n'
    )
    given = Settings(home=tmp_path)
    assert given.storage == StorageSettings(
        remote=RemoteTable(kind='s3', url='s3://bucket/piloop'), evict_after_days=30
    ), "the backend's own keys ride along, unread here"
    assert given.store_dir == tmp_path / 'store'
    monkeypatch.setenv('PILOOP_HOME', str(tmp_path))
    monkeypatch.setenv('PILOOP_STORAGE__MIN_FREE_GB', '5')
    from_env = Settings()
    assert from_env.storage.min_free_gb == 5 and from_env.storage.remote is not None
    assert from_env.storage.remote.model_dump() == {'kind': 's3', 'url': 's3://bucket/piloop'}
    assert Settings(home=tmp_path / 'elsewhere').storage.remote is None, 'no config.toml there'


def test_eviction_needs_a_remote_and_a_remote_needs_a_kind() -> None:
    with pytest.raises(ValidationError, match='need a remote'):
        StorageSettings(local_max_gb=10)
    with pytest.raises(ValidationError, match='kind'):
        StorageSettings.model_validate({'remote': {'url': 's3://bucket'}})
    with pytest.raises(ValidationError, match='Extra inputs are not permitted'):
        StorageSettings.model_validate({'storage_class': 'STANDARD'}), 'a backend key belongs in [storage.remote]'
