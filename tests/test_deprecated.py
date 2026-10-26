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
from piloop.plugins.fact_source.campaign import CampaignFacts
from piloop.plugins.fact_source.command import Command
from piloop.plugins.fact_source.optimization import OptimizationFacts
from piloop.plugins.role.research import ResearchRole
from piloop.plugins.role.watchdog import WatchdogRole
from piloop.plugins.section.lessons import Lessons
from piloop.plugins.stop_rule.contract import STOP_RULES, WhileHeld
from piloop.plugins.stop_rule.limits import Limits
from piloop.plugins.trigger.contract import TRIGGERS
from piloop.plugins.verdict_rule.bootstrap import Bootstrap
from piloop.plugins.verdict_rule.contract import VERDICT_RULES
from piloop.version import Version
from tests.conftest import AGENT_TIMEOUT, HARNESS_APPEND, role_of, write_toml

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
    text = toml.read_text().replace(
        f'[roles.research]\ntimeout = {AGENT_TIMEOUT}\nmax_ideas = 0\nmax_concurrent = 1\n', ''
    )
    toml.write_text(text + tables)
    return toml


TEMPLATES = Path(__file__).parent / 'fixtures' / 'templates'
"""The campaign template that ``piloop init`` wrote in 0.1.0, and in this release before the scout was a role."""


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


def test_kind_in_a_trigger_entry_is_provider_and_warns_at_the_line_of_its_role(campaign_path: Path) -> None:
    write_toml(campaign_path, HARNESS_APPEND)
    toml = campaign_path / 'piloop.toml'
    text = toml.read_text().replace(
        '[roles.research]\n', '[roles.research]\ntriggers = [{ kind = "low_water", buffer = 1 }, { kind = "seed" }]\n'
    )
    toml.write_text(text.replace('[roles.watchdog]\n', '[roles.watchdog]\ntriggers = [{ provider = "cron" }]\n'))
    with pytest.warns(PiloopDeprecationWarning, match='kind in a trigger is deprecated') as record:
        cfg = load_campaign_config(campaign_path)
    assert [TRIGGERS.name_of(t) for t in role_of(cfg, ResearchRole).triggers] == ['low_water', 'seed']
    lines = toml.read_text().splitlines()
    assert [w.lineno for w in record] == [lines.index('[roles.research]') + 2], 'research, not the watchdog'


def test_wake_budget_is_the_budget_of_the_lessons_section_of_each_role(campaign_path: Path) -> None:
    write_toml(campaign_path, HARNESS_APPEND)
    toml = campaign_path / 'piloop.toml'
    text = toml.read_text().replace('[roles.research]\n', '[roles.research]\nwake_budget = 0\n')
    toml.write_text(text + '\n[memory]\nwake_budget = 200\n')
    with pytest.warns(PiloopDeprecationWarning, match='wake_budget is deprecated') as record:
        cfg = load_campaign_config(campaign_path)
    assert len(record) == 2
    lessons = {
        name: next((s.budget for s in role.shown() if isinstance(s, Lessons)), None)
        for name, role in cfg.roles.all().items()
    }
    assert lessons == {'implementer': 200, 'watchdog': 400, 'research': None, 'scout': 200}, (
        "[memory]'s budget for every role but the watchdog, whose old default was its own; 0 leaves them out"
    )


def test_the_anomaly_trigger_is_the_two_incident_triggers_in_its_place(campaign_path: Path) -> None:
    write_toml(campaign_path, HARNESS_APPEND)
    toml = campaign_path / 'piloop.toml'
    old = 'triggers = [{ provider = "cron" }, { provider = "anomaly" }]\n'
    toml.write_text(toml.read_text().replace('[roles.watchdog]\n', f'[roles.watchdog]\n{old}'))
    with pytest.warns(PiloopDeprecationWarning, match='the anomaly trigger is deprecated'):
        cfg = load_campaign_config(campaign_path)
    triggers = [TRIGGERS.name_of(t) for t in role_of(cfg, WatchdogRole).triggers]
    assert triggers == ['cron', 'run_failed', 'dead_at_startup']


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
    assert STOP_RULES.selected(cfg.plugins) == Limits(max_runs=7, plateau=9, hold_at={'spend': 300}), (
        'the table keeps its own'
    )
    assert VERDICT_RULES.selected(cfg.plugins) == Bootstrap(confidence=0.9)
    lines = toml.read_text().splitlines()
    assert sorted((w.lineno, str(w.message).partition(';')[0]) for w in record) == [
        (1, 'confidence is deprecated'),
        (lines.index('[stop]') + 1, '[stop] is deprecated'),
        (lines.index('[spend]') + 1, '[spend] is deprecated'),
        (lines.index('limit = 300') + 1, '[spend] limit is deprecated'),
    ]
    toml = deprecated_config(campaign_path, '[stop]\nmax_runs = 7\n')
    toml.write_text('plugins = 3\n' + toml.read_text())
    with pytest.warns(PiloopDeprecationWarning), pytest.raises(ConfigError, match='plugins'):
        load_campaign_config(campaign_path)


def test_spend_is_a_command_source_that_reads_spend_and_closing_review_is_while_held_of_the_stop_rule(
    campaign_path: Path, tmp_path: Path
) -> None:
    (tmp_path / 'bill').write_text('12.5\n')
    toml = deprecated_config(
        campaign_path,
        f'[spend]\ncommand = ["cat", "{tmp_path / "bill"}"]\nnotices = [50]\ninterval = 60\nclosing_review = "wait"\n',
    )
    with pytest.warns(PiloopDeprecationWarning, match=r'\[spend\] is deprecated; write a \[\[facts\]\] source'):
        cfg = load_campaign_config(campaign_path)
    [campaign, optimization, spend] = cfg.fact_sources()
    command = [*deprecated.SPEND_COMMAND, 'cat', str(tmp_path / 'bill')]
    assert (campaign, optimization, spend) == (
        CampaignFacts(),
        OptimizationFacts(),
        Command(command=command, metrics=['spend'], notices={'spend': [50]}, interval=60),
    ), "after piloop's own sources"
    assert isinstance(spend, Command) and spend.read(tmp_path) == {'spend': 12.5}, (
        'the old command, which prints one number, still reads'
    )
    assert STOP_RULES.selected(cfg.plugins).while_held is WhileHeld.WAIT
    assert 'closing_review = "wait"' in toml.read_text(), 'the file is not rewritten'


def test_max_spend_is_hold_at_of_the_spend(campaign_path: Path) -> None:
    deprecated_config(
        campaign_path,
        '[[facts]]\nprovider = "campaign"\n\n[[facts]]\nprovider = "optimization"\n\n[[facts]]\nprovider = "command"\ncommand = ["true"]\nmetrics = ["spend"]\n\n'
        '[plugins.stop_rule]\nmax_spend = 300\n',
    )
    with pytest.warns(PiloopDeprecationWarning, match=r'max_spend is deprecated; write hold_at = \{ spend = \.\.\. \}'):
        cfg = load_campaign_config(campaign_path)
    assert STOP_RULES.selected(cfg.plugins) == Limits(hold_at={'spend': 300})


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
