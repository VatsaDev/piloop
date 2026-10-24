"""The deprecation framework: a bridge defined here, each bridge of ``BRIDGES``, and the ``deprecated.until`` markers.

The marker scan reads the source, so a marker in a branch that never runs counts too. Once ``pyproject.toml`` is at or
past a marker's version, ``test_no_marker_reached_its_release`` fails with the place and the reason of each one, so the
release cannot ship until the code is deleted.
"""

import ast
import io
import re
import tokenize
import tomllib
import warnings
from dataclasses import dataclass
from pathlib import Path

import pytest

from piloop import deprecated
from piloop.config import ConfigError, load_campaign_config
from piloop.deprecated import PiloopDeprecationWarning
from piloop.plugins.stop_rule.contract import STOP_RULES
from piloop.plugins.stop_rule.limits import Limits
from piloop.plugins.trigger.contract import TRIGGERS
from piloop.plugins.trigger.cron import CronTrigger
from piloop.plugins.trigger.low_water import LowWaterTrigger
from piloop.plugins.trigger.seed import SeedTrigger
from piloop.plugins.trigger.stalled import StalledTrigger
from piloop.plugins.verdict_rule.bootstrap import Bootstrap
from piloop.plugins.verdict_rule.contract import VERDICT_RULES
from piloop.version import Version
from tests.conftest import HARNESS_APPEND, write_toml

TEST_BRIDGES: list[deprecated.Bridge] = []


@deprecated.bridge('9.0.0', 'size is width: delete rename_size', TEST_BRIDGES)
def rename_size(toml: Path, raw: dict[str, object], removed_in: Version) -> None:
    """``size = …`` is ``width = …``."""
    if 'size' in raw:
        raw['width'] = raw.pop('size')
        deprecated.warn(toml, 'size', 'size is deprecated; write width instead', removed_in=removed_in)


def test_a_bridge_rewrites_the_key_and_warns_at_the_line_that_set_it(tmp_path: Path) -> None:
    toml = tmp_path / 'piloop.toml'
    toml.write_text('name = "p"\nsize = 3\n')
    raw: dict[str, object] = {'name': 'p', 'size': 3}
    assert [rename_size] == TEST_BRIDGES, 'the declaration registers it'
    with pytest.warns(PiloopDeprecationWarning) as record:
        deprecated.apply(toml, raw, TEST_BRIDGES)
    assert raw == {'name': 'p', 'width': 3}
    assert [(w.filename, w.lineno, str(w.message)) for w in record] == [
        (str(toml), 2, 'size is deprecated; write width instead; removed in piloop 9.0.0')
    ]


def test_a_key_no_line_sets_warns_at_line_0(tmp_path: Path) -> None:
    toml = tmp_path / 'piloop.toml'
    toml.write_text('box = { size = 3 }\n')
    with pytest.warns(PiloopDeprecationWarning) as record:
        deprecated.warn(toml, 'size', 'size is deprecated', removed_in=Version(9, 0, 0))
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
        deprecated.warn(toml, 'roles.research.scout', 'old', removed_in=Version(9, 0, 0))
        deprecated.warn(toml, 'roles.research.buffer', 'old', removed_in=Version(9, 0, 0))
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
        campaign_path,
        '[roles.research]\ntriggers = [{ provider = "low_water" }, { provider = "seed" }, { provider = "scout" }]\n',
    )
    toml.write_text(
        toml.read_text().replace('[roles.watchdog]\n', '[roles.watchdog]\ntriggers = [{ provider = "cron" }]\n')
    )
    with pytest.warns(PiloopDeprecationWarning, match='the scout trigger is deprecated') as record:
        cfg = load_campaign_config(campaign_path)
    assert [TRIGGERS.name_of(t) for t in cfg.roles.research.triggers] == ['low_water', 'seed']
    assert record[0].lineno == toml.read_text().splitlines().index('[roles.research]') + 2, 'research, not the watchdog'


def test_a_buffer_beside_triggers_that_are_no_list_is_left_for_the_validation_to_refuse(campaign_path: Path) -> None:
    deprecated_config(campaign_path, '[roles.research]\nbuffer = 5\ntriggers = "low_water"\n')
    with pytest.warns(PiloopDeprecationWarning), pytest.raises(ConfigError, match=r'roles\.research\.triggers'):
        load_campaign_config(campaign_path)


def test_research_buffer_is_the_buffer_of_its_low_water_triggers(campaign_path: Path) -> None:
    deprecated_config(campaign_path, '[roles.research]\nbuffer = 5\n')
    with pytest.warns(PiloopDeprecationWarning, match=r'\[roles.research\] buffer is deprecated'):
        cfg = load_campaign_config(campaign_path)
    assert cfg.roles.research.triggers == [LowWaterTrigger(buffer=5), SeedTrigger(), StalledTrigger()], (
        'the default triggers, with the buffer'
    )
    deprecated_config(
        campaign_path, '[roles.research]\nbuffer = 0\ntriggers = [{ kind = "low_water" }, { kind = "cron" }]\n'
    )
    with pytest.warns(PiloopDeprecationWarning):
        cfg = load_campaign_config(campaign_path)
    assert cfg.roles.research.triggers == [LowWaterTrigger(buffer=0), CronTrigger()]


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


def test_the_worker_role_is_the_implementer(campaign_path: Path) -> None:
    write_toml(campaign_path, HARNESS_APPEND, implementer_timeout=7)
    toml = campaign_path / 'piloop.toml'
    toml.write_text(toml.read_text().replace('[roles.implementer]', '[roles.worker]'))
    with pytest.warns(
        PiloopDeprecationWarning, match=r'\[roles.worker\] is deprecated; write \[roles.implementer\]'
    ) as w:
        cfg = load_campaign_config(campaign_path)
    assert cfg.roles.implementer.timeout == 7
    assert 'worker' not in cfg.roles.all(), 'no role of its own'
    assert w[0].lineno == toml.read_text().splitlines().index('[roles.worker]') + 1


TEMPLATES = Path(__file__).parent / 'fixtures' / 'templates'
"""The campaign template that ``piloop init`` wrote in 0.1.0, and in this release before the scout was a role."""


@pytest.mark.parametrize(
    ('template', 'scout_table', 'triggers'),
    [
        ('piloop-0.1.0.toml', False, ['low_water', 'seed', 'stalled']),
        ('piloop-0.2.0.dev.toml', False, ['low_water', 'seed']),
        ('piloop-0.2.0.dev.toml', True, ['low_water', 'seed']),
    ],
    ids=['0.1.0', 'dev', 'dev-with-its-scout-table'],
)
def test_a_piloop_toml_that_an_earlier_template_wrote_still_loads(
    template: str, scout_table: bool, triggers: list[str], tmp_path: Path
) -> None:
    """Every form of the deprecated keys that the earlier templates wrote, the scout table uncommented included.

    0.1.0 wrote no research triggers, so research gets the default ones. The dev template listed them.
    """
    text = (TEMPLATES / template).read_text()
    if scout_table:
        text = text.replace('# [roles.research.scout]', '[roles.research.scout]')
        text = text.replace('# interval = 86400', 'interval = 86400').replace('# seeds = 3 ', 'seeds = 3 ')
    (tmp_path / 'piloop.toml').write_text(text)
    with pytest.warns(PiloopDeprecationWarning):
        cfg = load_campaign_config(tmp_path)
    research = cfg.roles.research
    assert [TRIGGERS.name_of(t) for t in research.triggers] == triggers
    assert research.triggers[0] == LowWaterTrigger(buffer=2)
    assert (cfg.context, research.max_ideas) == ([], 1)
    assert (cfg.roles.scout.interval, cfg.roles.scout.seeds) == ((86400, 3) if scout_table else (0, 3))


def test_a_piloop_project_variable_still_overrides_after_its_campaign_one(
    campaign_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_toml(campaign_path, HARNESS_APPEND)
    monkeypatch.setenv('PILOOP_PROJECT_NAME', 'old')
    monkeypatch.setenv('PILOOP_PROJECT_EVALUATE_TIMEOUT', '7')
    monkeypatch.setenv('PILOOP_CAMPAIGN_EVALUATE_TIMEOUT', '9')
    with pytest.warns(PiloopDeprecationWarning) as record:
        cfg = load_campaign_config(campaign_path)
    assert (cfg.name, cfg.evaluate_timeout) == ('old', 9), 'the new name wins'
    assert [str(w.message).split(';')[0] for w in record] == [
        'PILOOP_PROJECT_EVALUATE_TIMEOUT is deprecated',
        'PILOOP_PROJECT_NAME is deprecated',
    ]
    assert 'set PILOOP_CAMPAIGN_NAME' in str(record[1].message)


ROOT = Path(__file__).parents[1]
NOTE = re.compile(
    r'\b(?:TODO|FIXME|XXX)\b.*\d+\.\d+'  # a TODO tied to a version
    r'|\b(?:remov|delet|drop)\w*\b.*\b(?:release|\d+\.\d+)',  # "delete it in the release after 0.2.0"
    re.IGNORECASE,
)
"""A comment or docstring line that says to delete code at a release. That code takes a ``deprecated.until`` marker."""
DEPRECATED = re.compile(r'(?<![\w.])deprecated(?![\w.])', re.IGNORECASE)
"""A comment that calls code deprecated (but not one that names ``piloop.deprecated``). A docstring may: it describes a
deprecated key to the reader."""


@dataclass(frozen=True)
class Marker:
    where: str
    version: Version
    reason: str


def _is_marker(node: ast.Call, module: str) -> bool:
    """Whether ``node`` calls ``until``: ``x.until(…)`` under any name ``x``, or ``until(…)`` or ``bridge(…)`` in its own
    module."""
    func = node.func
    if isinstance(func, ast.Attribute):
        return func.attr == 'until'
    return module == 'piloop/deprecated.py' and isinstance(func, ast.Name) and func.id in ('until', 'bridge')


def _hides_until(node: ast.ImportFrom, alias: ast.alias) -> bool:
    """Whether the import gives ``until`` another name than ``deprecated.until``."""
    if node.module == 'piloop.deprecated':
        return alias.name == 'until'
    return node.module == 'piloop' and alias.name == 'deprecated' and alias.asname not in (None, 'deprecated')


def _marker(node: ast.Call, where: str) -> Marker | str:
    """Return the marker that ``node`` calls, or why it is malformed."""
    if isinstance(node.func, ast.Attribute) and not (
        isinstance(node.func.value, ast.Name) and node.func.value.id == 'deprecated'
    ):
        return f'{where}: write deprecated.until(...), so that the scan finds it'
    match node.args, node.keywords:
        case [ast.Constant(value=str(version)), ast.Constant(value=str(reason)), *_], [] if reason.strip():
            try:
                return Marker(where, Version.parse(version), reason)
            except ValueError as e:
                return f'{where}: {e}'
    return f'{where}: write deprecated.until(<"X.Y.Z">, <"reason">), with two literal strings'


def _notes(source: str, tree: ast.Module) -> list[tuple[int, str]]:
    """Return ``(line number, text)`` of each comment and docstring line of ``source`` that reads as a compat note."""
    comments = [
        (tok.start[0], tok.string.lstrip('#').strip())
        for tok in tokenize.generate_tokens(io.StringIO(source).readline)
        if tok.type == tokenize.COMMENT
    ]
    docstrings = [
        (node.lineno + n, line.strip())
        for node in ast.walk(tree)
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)
        for n, line in enumerate(node.value.value.splitlines())
    ]
    return [(n, t) for n, t in comments if NOTE.search(t) or DEPRECATED.search(t)] + [
        (n, t) for n, t in docstrings if NOTE.search(t)
    ]


def scan(root: Path) -> tuple[list[Marker], list[str]]:
    """Return the markers under ``root / 'piloop'``, and the problems: a malformed marker, a free-text compat note."""
    markers: list[Marker] = []
    problems: list[str] = []
    for path in sorted((root / 'piloop').rglob('*.py')):
        module = path.relative_to(root).as_posix()
        source = path.read_text()
        tree = ast.parse(source, module)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and _is_marker(node, module):
                found = _marker(node, f'{module}:{node.lineno}')
                if isinstance(found, Marker):
                    markers.append(found)
                else:
                    problems.append(found)
            elif isinstance(node, ast.ImportFrom) and any(_hides_until(node, alias) for alias in node.names):
                problems.append(
                    f'{module}:{node.lineno}: write `from piloop import deprecated`, then deprecated.until(...)'
                )
        problems += [
            f'{module}:{n}: mark this with deprecated.until(<"X.Y.Z">, <"reason">): {text}'
            for n, text in _notes(source, tree)
        ]
    return markers, problems


def expired(markers: list[Marker], version: Version) -> list[Marker]:
    """Return the markers whose release ``version`` has reached."""
    return [m for m in markers if m.version <= version]


def piloop_version(root: Path = ROOT) -> Version:
    """The version in ``pyproject.toml``. A pre-release of a version counts as that version (``Version.parse``)."""
    return Version.parse(tomllib.loads((root / 'pyproject.toml').read_text())['project']['version'])


def test_no_marker_reached_its_release() -> None:
    markers, problems = scan(ROOT)
    assert not problems, '\n'.join(problems)
    version = piloop_version()
    due = '\n'.join(f'{m.where}: {m.reason}' for m in expired(markers, version))
    assert not due, f'piloop {version} removes this compat code; delete it and its tests:\n{due}'


def write(root: Path, name: str, source: str) -> None:
    path = root / 'piloop' / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source)


def test_the_scan_finds_a_marker_in_a_branch_and_on_a_function(tmp_path: Path) -> None:
    write(
        tmp_path,
        'a.py',
        'from piloop import deprecated\n'
        '\n'
        "@deprecated.until('0.3.0', 'the old key')\n"
        'def f(x):\n'
        '    if x:\n'
        "        deprecated.until('0.4.0', 'the old env' ' variable')\n",
    )
    assert scan(tmp_path) == (
        [
            Marker('piloop/a.py:3', Version(0, 3, 0), 'the old key'),
            Marker('piloop/a.py:6', Version(0, 4, 0), 'the old env variable'),
        ],
        [],
    )


@pytest.mark.parametrize(
    'call',
    [
        "deprecated.until('0.3', 'r')",
        "deprecated.until('v0.3.0', 'r')",
        "deprecated.until(V, 'r')",
        "deprecated.until('0.3.0', '')",
        "deprecated.until('0.3.0')",
        "deprecated.until('0.3.0', reason='r')",
    ],
)
def test_a_malformed_marker_is_a_problem(tmp_path: Path, call: str) -> None:
    write(tmp_path, 'a.py', f'from piloop import deprecated\n{call}\n')
    markers, problems = scan(tmp_path)
    assert markers == []
    assert len(problems) == 1 and problems[0].startswith('piloop/a.py:2: '), problems


@pytest.mark.parametrize(
    'source',
    [
        'from piloop.deprecated import until\n',
        'from piloop import deprecated as d\n',
        "import piloop.deprecated\npiloop.deprecated.until('0.3.0', 'r')\n",
    ],
)
def test_until_under_another_name_is_a_problem(tmp_path: Path, source: str) -> None:
    write(tmp_path, 'a.py', source)
    markers, problems = scan(tmp_path)
    assert markers == []
    assert [p.partition(': ')[0] for p in problems] == [f'piloop/a.py:{len(source.splitlines())}'], problems


def test_deprecated_py_calls_until_by_its_name(tmp_path: Path) -> None:
    write(tmp_path, 'deprecated.py', "until('0.3.0', 'r')\n")
    assert scan(tmp_path) == ([Marker('piloop/deprecated.py:1', Version(0, 3, 0), 'r')], [])


@pytest.mark.parametrize(
    'text',
    [
        '# TODO(0.3.0): delete it',
        '# TODO: delete this fallback in the release after 0.2.0',
        '# removed in piloop 0.3.0',
        '# the old key is deprecated',
        '"""Return x.\n\nTODO: delete it with the hint one minor release after 0.2.0."""',
    ],
)
def test_a_free_text_compat_note_is_a_problem(tmp_path: Path, text: str) -> None:
    write(tmp_path, 'a.py', f'x = 1\n{text}\n')
    assert [p.partition(': ')[0] for p in scan(tmp_path)[1]] == [f'piloop/a.py:{1 + len(text.splitlines())}']


@pytest.mark.parametrize(
    'text',
    [
        '# TODO: remove it once the harness reports that state',
        '# the directories keep their 0.1 name',
        '# see piloop.deprecated.old_env',
        "warn('the key is deprecated; removed in piloop 0.3.0')",
    ],
)
def test_other_comments_and_strings_are_not_problems(tmp_path: Path, text: str) -> None:
    write(tmp_path, 'a.py', f'{text}\n')
    assert scan(tmp_path) == ([], [])


def test_a_marker_expires_at_its_release_and_its_pre_releases() -> None:
    marker = Marker('piloop/a.py:1', Version(0, 3, 0), 'r')
    assert expired([marker], Version.parse('0.2.9')) == []
    assert expired([marker], Version.parse('0.3.0')) == [marker]
    assert expired([marker], Version.parse('0.3.0rc1')) == [marker]
    assert expired([marker], Version.parse('1.0.0')) == [marker]


@pytest.mark.parametrize('text', ['0.3', 'v0.3.0', '0.3.0.1', '0.3.0-dev', ''])
def test_a_version_that_is_not_x_y_z_fails_to_parse_and_until_refuses_it(text: str) -> None:
    with pytest.raises(ValueError, match=rf'^{re.escape(repr(text))} is not a version X\.Y\.Z$'):
        Version.parse(text)
    with pytest.raises(ValueError, match='is not a version X.Y.Z'):
        deprecated.until(text, 'r')


def test_until_leaves_what_it_marks_as_it_is() -> None:
    def f() -> int:
        return 1

    assert deprecated.until('0.3.0', 'r')(f) is f


def test_stop_the_spend_limit_and_confidence_are_keys_of_the_rules(campaign_path: Path) -> None:
    toml = deprecated_config(
        campaign_path,
        '[stop]\nmax_runs = 7\nplateau = 4\n\n[spend]\ncommand = ["true"]\nlimit = 300\n\n[plugins.stop_rule]\nplateau = 9\n',
    )
    toml.write_text('confidence = 0.9\n' + toml.read_text())
    with pytest.warns(PiloopDeprecationWarning) as record:
        cfg = load_campaign_config(campaign_path)
    assert STOP_RULES.selected(cfg.plugins) == Limits(max_runs=7, plateau=9, max_spend=300), 'the table keeps its own'
    assert VERDICT_RULES.selected(cfg.plugins) == Bootstrap(confidence=0.9)
    lines = toml.read_text().splitlines()
    assert sorted((w.lineno, str(w.message).partition(';')[0]) for w in record) == [
        (1, 'confidence is deprecated'),
        (lines.index('[stop]') + 1, '[stop] is deprecated'),
        (lines.index('limit = 300') + 1, '[spend] limit is deprecated'),
    ]
    toml = deprecated_config(campaign_path, '[stop]\nmax_runs = 7\n')
    toml.write_text('plugins = 3\n' + toml.read_text())
    with pytest.warns(PiloopDeprecationWarning), pytest.raises(ConfigError, match='plugins'):
        load_campaign_config(campaign_path)


def test_the_variables_of_stop_and_confidence_set_the_rules_and_win_over_the_file(
    campaign_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    deprecated_config(campaign_path, '[plugins.stop_rule]\nmax_runs = 7\n')
    monkeypatch.setenv('PILOOP_CAMPAIGN_STOP__MAX_RUNS', '3')
    monkeypatch.setenv('PILOOP_CAMPAIGN_CONFIDENCE', '0.8')
    with pytest.warns(PiloopDeprecationWarning) as record:
        cfg = load_campaign_config(campaign_path)
    assert STOP_RULES.selected(cfg.plugins) == Limits(max_runs=3)
    assert VERDICT_RULES.selected(cfg.plugins) == Bootstrap(confidence=0.8)
    assert sorted(str(w.message) for w in record) == [
        'PILOOP_CAMPAIGN_CONFIDENCE is deprecated; set PILOOP_CAMPAIGN_PLUGINS__VERDICT_RULE__CONFIDENCE; removed in'
        ' piloop 0.4.0',
        'PILOOP_CAMPAIGN_STOP__MAX_RUNS is deprecated; set PILOOP_CAMPAIGN_PLUGINS__STOP_RULE__MAX_RUNS; removed in'
        ' piloop 0.4.0',
    ]
    toml = campaign_path / 'piloop.toml'
    toml.write_text('plugins = 3\n' + toml.read_text().replace('[plugins.stop_rule]\nmax_runs = 7\n', ''))
    with pytest.warns(PiloopDeprecationWarning), pytest.raises(ConfigError, match='plugins'):
        load_campaign_config(campaign_path)
