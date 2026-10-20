"""The deprecation framework, tested with a bridge defined here, and each bridge of ``BRIDGES``."""

import warnings
from pathlib import Path

import pytest

from piloop import deprecated
from piloop.config import ConfigError, load_campaign_config
from piloop.deprecated import PiloopDeprecationWarning
from tests.conftest import HARNESS_APPEND, write_toml


def rename_size(toml: Path, raw: dict[str, object]) -> None:
    """``size = …`` is ``width = …``."""
    if 'size' in raw:
        raw['width'] = raw.pop('size')
        deprecated.warn(toml, 'size', 'size is deprecated; write width instead', removed_in='9.0.0')


def test_a_bridge_rewrites_the_key_and_warns_at_the_line_that_set_it(tmp_path: Path) -> None:
    toml = tmp_path / 'piloop.toml'
    toml.write_text('name = "p"\nsize = 3\n')
    raw: dict[str, object] = {'name': 'p', 'size': 3}
    with pytest.warns(PiloopDeprecationWarning) as record:
        deprecated.bridge(toml, raw, (rename_size,))
    assert raw == {'name': 'p', 'width': 3}
    assert [(w.filename, w.lineno, str(w.message)) for w in record] == [
        (str(toml), 2, 'size is deprecated; write width instead; removed in piloop 9.0.0')
    ]


def test_a_key_no_line_sets_warns_at_line_0(tmp_path: Path) -> None:
    toml = tmp_path / 'piloop.toml'
    toml.write_text('box = { size = 3 }\n')
    with pytest.warns(PiloopDeprecationWarning) as record:
        deprecated.warn(toml, 'size', 'size is deprecated', removed_in='9.0.0')
    assert [w.lineno for w in record] == [0]


def test_the_cli_shows_each_deprecation_once(tmp_path: Path) -> None:
    toml = tmp_path / 'piloop.toml'
    toml.write_text('size = 3\n')
    with warnings.catch_warnings(record=True) as record:
        warnings.resetwarnings()
        deprecated.show_in_cli()
        for _ in range(2):
            rename_size(toml, {'size': 3})
    assert [(w.filename, w.lineno) for w in record] == [(str(toml), 1)], 'a file loaded twice warns once'
    assert warnings.formatwarning(record[0].message, PiloopDeprecationWarning, str(toml), 1).startswith(
        f'{toml}:1: PiloopDeprecationWarning: size is deprecated'
    )


def test_a_dotted_key_warns_at_its_table_header_or_at_the_line_that_sets_its_last_part(tmp_path: Path) -> None:
    toml = tmp_path / 'piloop.toml'
    toml.write_text('[roles.research]\nbuffer = 3\n\n[roles.research.scout]\ninterval = 60\n')
    with pytest.warns(PiloopDeprecationWarning) as record:
        deprecated.warn(toml, 'roles.research.scout', 'old', removed_in='9.0.0')
        deprecated.warn(toml, 'roles.research.buffer', 'old', removed_in='9.0.0')
    assert [w.lineno for w in record] == [4, 2]


def deprecated_config(campaign_path: Path, tables: str) -> Path:
    """Write the test campaign's piloop.toml with ``tables`` in place of its research table."""
    write_toml(campaign_path, HARNESS_APPEND)
    toml = campaign_path / 'piloop.toml'
    text = toml.read_text().replace('[roles.research]\ntimeout = 20\nmax_ideas = 0\nmax_concurrent = 1\n', '')
    toml.write_text(text + tables)
    return toml


def test_research_scout_is_the_scout_role_with_the_research_keys_it_used(campaign_path: Path) -> None:
    toml = deprecated_config(
        campaign_path,
        '[roles.research]\nmcp = []\ntimeout = 1200\nmax_concurrent = 2\n\n'
        '[roles.research.scout]\ninterval = 86400\nseeds = 4\n',
    )
    with pytest.warns(PiloopDeprecationWarning, match=r'\[roles.research.scout\] is deprecated') as record:
        cfg = load_campaign_config(campaign_path)
    assert [(w.filename, w.lineno) for w in record] == [
        (str(toml), toml.read_text().splitlines().index('[roles.research.scout]') + 1)
    ]
    scout = cfg.roles.scout
    assert (scout.interval, scout.seeds, scout.timeout, scout.max_concurrent) == (86400, 4, 1200, 1), (
        "the shared timeout moves with it; max_concurrent is the role's own"
    )
    assert cfg.roles.research.max_concurrent == 2


def test_a_scout_table_wins_over_what_the_bridge_copies_from_research(campaign_path: Path) -> None:
    deprecated_config(
        campaign_path,
        '[roles.research]\ntimeout = 1200\n\n[roles.research.scout]\ninterval = 60\n\n[roles.scout]\ntimeout = 300\n',
    )
    with pytest.warns(PiloopDeprecationWarning):
        scout = load_campaign_config(campaign_path).roles.scout
    assert (scout.interval, scout.timeout) == (60, 300)


def test_the_scout_trigger_of_research_goes(campaign_path: Path) -> None:
    toml = deprecated_config(
        campaign_path, '[roles.research]\ntriggers = [{ kind = "low_water" }, { kind = "seed" }, { kind = "scout" }]\n'
    )
    toml.write_text(
        toml.read_text().replace('[roles.watchdog]\n', '[roles.watchdog]\ntriggers = [{ kind = "cron" }]\n')
    )
    with pytest.warns(PiloopDeprecationWarning, match='the scout trigger is deprecated') as record:
        cfg = load_campaign_config(campaign_path)
    assert [t.kind for t in cfg.roles.research.triggers] == ['low_water', 'seed']
    assert record[0].lineno == toml.read_text().splitlines().index('[roles.research]') + 2, 'research, not the watchdog'


def test_a_buffer_beside_triggers_that_are_no_list_is_left_for_the_validation_to_refuse(campaign_path: Path) -> None:
    deprecated_config(campaign_path, '[roles.research]\nbuffer = 5\ntriggers = "low_water"\n')
    with pytest.warns(PiloopDeprecationWarning), pytest.raises(ConfigError, match=r'roles\.research\.triggers'):
        load_campaign_config(campaign_path)


def test_research_buffer_is_the_buffer_of_its_low_water_triggers(campaign_path: Path) -> None:
    deprecated_config(campaign_path, '[roles.research]\nbuffer = 5\n')
    with pytest.warns(PiloopDeprecationWarning, match=r'\[roles.research\] buffer is deprecated'):
        cfg = load_campaign_config(campaign_path)
    assert [t.model_dump() for t in cfg.roles.research.triggers] == [
        {'kind': 'low_water', 'buffer': 5},
        {'kind': 'seed'},
    ], 'the default triggers, with the buffer'
    deprecated_config(
        campaign_path, '[roles.research]\nbuffer = 0\ntriggers = [{ kind = "low_water" }, { kind = "cron" }]\n'
    )
    with pytest.warns(PiloopDeprecationWarning):
        cfg = load_campaign_config(campaign_path)
    assert [t.model_dump() for t in cfg.roles.research.triggers] == [
        {'kind': 'low_water', 'buffer': 0},
        {'kind': 'cron'},
    ]


def test_research_context_is_the_top_level_context(campaign_path: Path) -> None:
    toml = deprecated_config(campaign_path, '[roles.research]\ncontext = ["docs/b.md"]\n')
    toml.write_text('context = ["docs/a.md"]\n' + toml.read_text())
    with pytest.warns(PiloopDeprecationWarning, match=r'\[roles.research\] context is deprecated'):
        assert load_campaign_config(campaign_path).context == ['docs/a.md', 'docs/b.md']


def test_the_claude_and_codex_placeholders_of_a_usage_glob_are_env_placeholders(campaign_path: Path) -> None:
    toml = campaign_path / 'piloop.toml'
    toml.write_text(
        toml.read_text().replace(
            '[harness.script]',
            '[harness.script.usage]\nfile = "{codex_home}/{session_id}.jsonl"\ntally = "codex"\n\n[harness.script]',
        )
    )
    toml.write_text(toml.read_text().replace('headless = ["{prompt}"]', 'headless = ["{prompt}", "{session_id}"]'))
    with pytest.warns(PiloopDeprecationWarning, match='{claude_config_dir} and {codex_home} are deprecated'):
        usage = load_campaign_config(campaign_path).harness['script'].usage
    assert usage is not None and usage.file == '{env:CODEX_HOME:~/.codex}/{session_id}.jsonl'


TEMPLATES = Path(__file__).parent / 'fixtures' / 'templates'
"""The campaign template that ``piloop init`` wrote in 0.1.0, and in this release before the scout was a role."""


@pytest.mark.parametrize(
    ('template', 'scout_table'),
    [('piloop-0.1.0.toml', False), ('piloop-0.2.0.dev.toml', False), ('piloop-0.2.0.dev.toml', True)],
    ids=['0.1.0', 'dev', 'dev-with-its-scout-table'],
)
def test_a_piloop_toml_that_an_earlier_template_wrote_still_loads(
    template: str, scout_table: bool, tmp_path: Path
) -> None:
    """Every form of the deprecated keys that the earlier templates wrote, the scout table uncommented included."""
    text = (TEMPLATES / template).read_text()
    if scout_table:
        text = text.replace('# [roles.research.scout]', '[roles.research.scout]')
        text = text.replace('# interval = 86400', 'interval = 86400').replace('# seeds = 3 ', 'seeds = 3 ')
    (tmp_path / 'piloop.toml').write_text(text)
    with pytest.warns(PiloopDeprecationWarning):
        cfg = load_campaign_config(tmp_path)
    research = cfg.roles.research
    assert [t.model_dump() for t in research.triggers] == [{'kind': 'low_water', 'buffer': 2}, {'kind': 'seed'}]
    assert (cfg.context, research.max_ideas) == ([], 1)
    assert (cfg.roles.scout.interval, cfg.roles.scout.seeds) == ((86400, 3) if scout_table else (0, 3))


def test_a_piloop_project_variable_still_overrides_after_its_campaign_one(
    campaign_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_toml(campaign_path, HARNESS_APPEND)
    monkeypatch.setenv('PILOOP_PROJECT_RUNTIME', 'herdr')
    monkeypatch.setenv('PILOOP_PROJECT_EVALUATE_TIMEOUT', '7')
    monkeypatch.setenv('PILOOP_CAMPAIGN_EVALUATE_TIMEOUT', '9')
    with pytest.warns(PiloopDeprecationWarning) as record:
        cfg = load_campaign_config(campaign_path)
    assert (cfg.runtime, cfg.evaluate_timeout) == ('herdr', 9), 'the new name wins'
    assert [str(w.message).split(';')[0] for w in record] == [
        'PILOOP_PROJECT_EVALUATE_TIMEOUT is deprecated',
        'PILOOP_PROJECT_RUNTIME is deprecated',
    ]
    assert 'set PILOOP_CAMPAIGN_RUNTIME' in str(record[1].message)
