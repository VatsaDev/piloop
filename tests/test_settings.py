import os
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from piloop import settings as settings_module
from piloop.actor import Agent, System
from piloop.deprecated import PiloopDeprecationWarning
from piloop.harnesses import DEFAULT_HARNESSES, SESSION_ENV
from piloop.plugins.runtime.herdr import HerdrSettings
from piloop.settings import UNDER_TEST_ENV, BackendTable, Settings, StorageSettings, env_names, expand_env


def test_the_test_session_runs_in_a_temporary_home() -> None:
    """tests/conftest.py moves HOME and PILOOP_HOME before piloop is imported, so even the import-time ``settings``
    names a temporary home."""
    assert os.environ[UNDER_TEST_ENV] == '1'
    assert Path.home() != settings_module.REAL_HOME
    assert settings_module.settings.home == Path(os.environ['PILOOP_HOME'])
    assert settings_module.settings.home.parent != settings_module.REAL_HOME


def test_each_test_has_a_home_of_its_own_for_every_built_in_harness(tmp_path: Path) -> None:
    for name, harness in DEFAULT_HARNESSES.items():
        assert harness.home is not None and Path(expand_env(harness.home)) == tmp_path / 'harnesses' / name
    assert SESSION_ENV and not SESSION_ENV & set(os.environ), 'no harness session that runs the tests reaches them'


def test_e2e_campaigns_get_the_real_harness_homes_without_the_harness_session_of_the_caller(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.e2e import conftest as e2e

    real = {'PATH': '/bin', 'HOME': '/home/me', 'CLAUDE_CONFIG_DIR': '/home/me/claude', 'CODEX_HOME': '/home/me/codex'}
    caller = real | dict.fromkeys(SESSION_ENV, '1') | {'PILOOP_HOME': '/home/me/.piloop', 'VIRTUAL_ENV': '/venv'}
    monkeypatch.setattr(e2e, 'CALLER_ENV', caller)
    assert e2e.caller_env(Path('/tmp/home')) == real | {UNDER_TEST_ENV: '1', 'PILOOP_HOME': '/tmp/home'}


def test_the_e2e_install_check_runs_in_a_temporary_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``make e2e`` runs with the user's own ``HOME``, whose ``.piloop`` the guard refuses: the check names another."""
    from tests.e2e import conftest as e2e

    monkeypatch.setitem(e2e.CALLER_ENV, 'HOME', str(settings_module.REAL_HOME))
    (tmp_path / 'bin').mkdir()
    (tmp_path / 'bin' / 'piloop').symlink_to(Path(sys.executable).parent / 'piloop')
    assert e2e.Install(tmp_path, tmp_path / 'bin').version().startswith('piloop ')


def test_under_test_the_home_of_a_real_install_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``~/.piloop`` and its siblings in the user's passwd home directory, which a moved ``$HOME`` does not hide."""
    me = tmp_path / 'me'
    monkeypatch.setattr(settings_module, 'REAL_HOME', me)
    for home in (me / '.piloop', me / '.piloop-new'):
        with pytest.raises(ValidationError, match=f'{home} is the home of a real install'):
            Settings(home=home)
        monkeypatch.setenv('PILOOP_HOME', str(home))
        with pytest.raises(ValidationError, match=f'{home} is the home of a real install'):
            Settings()
    assert Settings(home=me / 'scratch').home == me / 'scratch'
    assert Settings(home=tmp_path / '.piloop').home == tmp_path / '.piloop', 'not in the real home directory'
    monkeypatch.delenv(UNDER_TEST_ENV)
    assert Settings().home == me / '.piloop-new', 'outside a test run it is just the home'


def test_storage_is_read_from_config_toml_under_the_home_and_the_environment_wins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / 'config.toml').write_text(
        '[storage]\nevict_after_days = 30\n\n[storage.backend]\nkind = "s3"\nurl = "s3://bucket/piloop"\n'
    )
    given = Settings(home=tmp_path)
    assert given.storage == StorageSettings(
        backend=BackendTable(kind='s3', url='s3://bucket/piloop'), evict_after_days=30
    ), "the backend's own keys ride along, unread here"
    assert given.store_dir == tmp_path / 'store'
    monkeypatch.setenv('PILOOP_HOME', str(tmp_path))
    monkeypatch.setenv('PILOOP_STORAGE__MIN_FREE_GB', '5')
    from_env = Settings()
    assert from_env.storage.min_free_gb == 5 and from_env.storage.backend is not None
    assert from_env.storage.backend.model_dump() == {'kind': 's3', 'url': 's3://bucket/piloop'}
    assert Settings(home=tmp_path / 'elsewhere').storage.backend is None, 'no config.toml there'


def test_eviction_needs_a_storage_backend_and_a_backend_needs_a_kind() -> None:
    with pytest.raises(ValidationError, match='need a storage backend'):
        StorageSettings(local_max_gb=10)
    with pytest.raises(ValidationError, match='kind'):
        StorageSettings.model_validate({'backend': {'url': 's3://bucket'}})
    with pytest.raises(ValidationError, match='Extra inputs are not permitted'):
        StorageSettings.model_validate({'storage_class': 'STANDARD'}), 'a backend key belongs in [storage.backend]'


def test_an_env_placeholder_is_the_variable_or_its_default_in_the_home(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('A_DIR', '/a')
    monkeypatch.setenv('EMPTY', '')
    monkeypatch.delenv('UNSET', raising=False)
    glob = '{env:A_DIR:~/.a}/x {env:UNSET:~/.b}/y {env:EMPTY:/c} {env:UNSET}z {cwd}'
    assert expand_env(glob) == f'/a/x {Path.home()}/.b/y /c z {{cwd}}', 'an empty variable is unset; other braces stay'
    assert env_names(glob + ' {env:A_DIR:other}') == ['A_DIR', 'UNSET', 'EMPTY']


def test_herdr_keys_of_config_toml_still_load_and_are_the_herdr_runtime_s(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``config.toml`` held the Herdr settings as top-level keys while they were core settings."""
    (tmp_path / 'config.toml').write_text('tick_seconds = 1\nherdr_session = "mine"\nherdr_cli_timeout_seconds = 5\n')
    monkeypatch.setenv('PILOOP_HOME', str(tmp_path))
    with pytest.warns(PiloopDeprecationWarning, match=r'herdr_\w+ in config.toml is deprecated'):
        core, herdr = Settings(), HerdrSettings()
    assert core.tick_seconds == 1
    assert (herdr.session, herdr.cli_timeout_seconds) == ('mine', 5)
    (tmp_path / 'config.toml').write_text('[runtimes.herdr]\nsession = "theirs"\n')
    monkeypatch.setenv('PILOOP_HERDR_CLI_TIMEOUT_SECONDS', '7')
    assert (HerdrSettings().session, HerdrSettings().cli_timeout_seconds) == ('theirs', 7), 'the environment wins'


def test_the_storage_remote_table_of_config_toml_is_the_storage_backend(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Builds of main after 0.1.0 wrote the storage backend as ``[storage.remote]``."""
    (tmp_path / 'config.toml').write_text(
        '[storage]\nevict_after_days = 3\n\n[storage.remote]\nkind = "s3"\nurl = "s3://b/p"\n'
    )
    monkeypatch.setenv('PILOOP_HOME', str(tmp_path))
    with pytest.warns(
        PiloopDeprecationWarning, match=r'\[storage.remote\] is deprecated; write \[storage.backend\]'
    ) as w:
        storage = Settings().storage
    assert storage == StorageSettings(backend=BackendTable(kind='s3', url='s3://b/p'), evict_after_days=3)
    assert w[0].lineno == 4, 'at the table header'


def test_a_runtimes_value_that_is_no_table_of_tables_is_refused_and_gives_the_runtime_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv('PILOOP_HOME', str(tmp_path))
    for text in ('runtimes = 3\nherdr_session = "mine"\n', '[runtimes]\nherdr = 3\n'):
        (tmp_path / 'config.toml').write_text(text)
        with pytest.raises(ValidationError, match='runtimes'):
            Settings()
        assert HerdrSettings().session == 'piloop', text


def test_the_signature_of_an_implementer_from_before_0_2_0_is_its_new_signature(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An agent that an older daemon started outlives an upgrade, signed ``worker#<id>``."""
    monkeypatch.setenv('PILOOP_ACTOR', 'worker#12')
    with pytest.warns(PiloopDeprecationWarning, match=r'PILOOP_ACTOR=worker#12 is deprecated; .* implementer#12'):
        assert Settings(home=tmp_path).actor == Agent(role='implementer', session=12)
    monkeypatch.setenv('PILOOP_ACTOR', 'scout#3')
    assert Settings(home=tmp_path).actor == Agent(role='scout', session=3)
    monkeypatch.setenv('PILOOP_ACTOR', 'scout#')
    with pytest.raises(ValidationError, match=r'actor\.Agent\.session'):
        Settings(home=tmp_path)
    monkeypatch.delenv('PILOOP_ACTOR')
    assert Settings(home=tmp_path).actor == System.HUMAN


def test_max_evaluations_of_config_toml_or_the_environment_is_the_evaluations_of_the_executor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """0.2.0 had ``max_evaluations`` (``PILOOP_MAX_EVALUATIONS``), the slots that ``workers`` now takes."""
    monkeypatch.delenv('PILOOP_MAX_EVALUATIONS', raising=False)
    (tmp_path / 'config.toml').write_text('max_evaluations = 3\n')
    with pytest.warns(PiloopDeprecationWarning, match=r'max_evaluations is deprecated; write evaluations in'):
        settings = Settings(home=tmp_path)
    assert (settings.plugins['executor'].provider, settings.plugins['executor'].keys) == ('workers', {'evaluations': 3})
    (tmp_path / 'config.toml').write_text('max_evaluations = 3\n\n[plugins.executor]\nevaluations = 4\n')
    with pytest.warns(PiloopDeprecationWarning):
        settings = Settings(home=tmp_path)
    assert settings.plugins['executor'].keys == {'evaluations': 4}
    (tmp_path / 'config.toml').unlink()
    monkeypatch.setenv('PILOOP_MAX_EVALUATIONS', '5')
    with pytest.warns(PiloopDeprecationWarning, match='PILOOP_MAX_EVALUATIONS is deprecated'):
        settings = Settings(home=tmp_path)
    assert settings.plugins['executor'].keys == {'evaluations': '5'}


def test_max_evaluations_changes_nothing_for_an_executor_without_slots(tmp_path: Path) -> None:
    (tmp_path / 'config.toml').write_text('max_evaluations = 3\n\n[plugins.executor]\nprovider = "inline"\n')
    with pytest.warns(PiloopDeprecationWarning, match='max_evaluations is deprecated'):
        settings = Settings(home=tmp_path)
    assert (settings.plugins['executor'].provider, settings.plugins['executor'].keys) == ('inline', {})
    (tmp_path / 'config.toml').write_text('max_evaluations = 3\nplugins = 1\n')
    with pytest.raises(ValueError), pytest.warns(PiloopDeprecationWarning):
        Settings(home=tmp_path)  # not a table: the validation refuses it
