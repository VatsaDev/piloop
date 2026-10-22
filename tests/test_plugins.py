"""The plugin host: an installed package mounts providers through it, and a composition that fails registers nothing."""

import re
from collections.abc import Callable, Iterator
from contextlib import ExitStack
from dataclasses import dataclass, field
from importlib.metadata import EntryPoint
from pathlib import Path
from typing import IO, Any, Protocol

import piloop_min_gain
import pytest
from piloop_nice import NiceRuntime
from pydantic import BaseModel, ConfigDict
from sqlalchemy.orm import Session

from piloop.cli import common, doctor
from piloop.cli.main import app
from piloop.cli.plugins import selections
from piloop.config import ConfigError, load_campaign_config
from piloop.daemon.main import Daemon
from piloop.db.models import Campaign
from piloop.db.session import Database
from piloop.plugins import host as plugins
from piloop.plugins.host import (
    MissingProvider,
    PluginError,
    PureCapability,
    PythonSource,
    Selection,
    Source,
    Tier,
    compose,
    data_errors,
)
from piloop.plugins.metric import Direction
from piloop.plugins.role.contract import ROLES
from piloop.plugins.runtime.contract import RUNTIMES
from piloop.plugins.storage_backend.contract import STORAGE_BACKENDS
from piloop.plugins.trigger.contract import TRIGGERS
from piloop.plugins.verdict_rule.contract import VERDICT_RULES, Case, Criterion, Judgement, Outcome, Side, VerdictRule
from piloop.runtime.factory import make_runtime
from piloop.settings import Settings
from tests.conftest import example, package
from tests.daemon.test_roles import PLUGIN, installed
from tests.fixtures.plugin import Typed


@pytest.fixture(autouse=True)
def unloaded() -> Iterator[None]:
    """Start and end each test with nothing loaded, so that the entry points of the test are the ones loaded."""
    plugins.close()
    yield
    plugins.close()


def test_an_installed_package_mounts_its_runtime_and_shows_where_it_comes_from(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    example(tmp_path, 'nice-runtime')
    monkeypatch.syspath_prepend(tmp_path)
    assert RUNTIMES.get('nice') is NiceRuntime
    assert isinstance(make_runtime('nice', campaign='p', campaign_path=tmp_path), NiceRuntime)
    assert RUNTIMES.sources()['nice'] == PythonSource('piloop-nice', '0.1.0', 'nice = piloop_nice:NiceRuntime')
    built_in: Source = RUNTIMES.sources()['subprocess']
    assert (built_in.release.split()[0], built_in.built_in) == ('piloop', True)
    assert next(c for c in doctor.checks(Settings(home=tmp_path)) if c.name == 'plugins').found.endswith(
        '; from piloop-nice 0.1.0'
    )
    monkeypatch.setattr(common, 'settings', Settings(home=tmp_path))
    printed: list[str] = []
    monkeypatch.setattr(common.console, 'print', lambda text, **_: printed.append(text))
    app(['plugins'], result_action='return_value', exit_on_error=False)
    lines = printed[0].splitlines()
    runtimes = lines.index('runtimes (host tier, piloop.runtimes):')
    assert lines[runtimes : runtimes + 4] == [
        'runtimes (host tier, piloop.runtimes):',
        f'  herdr       {built_in}',
        f'  subprocess  {built_in}',
        '  nice        piloop-nice 0.1.0, entry point nice = piloop_nice:NiceRuntime',
    ]
    assert lines[-1] == 'selected:', 'no campaign and no remote: nothing is selected'


def test_two_packages_with_the_same_name_fail_before_the_daemon_starts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package(tmp_path / 'a', 'piloop-a', '1.0', f'[piloop.runtimes]\nshown = {PLUGIN}:Shown\n')
    package(tmp_path / 'b', 'piloop-b', '2.0', f'[piloop.runtimes]\nshown = {PLUGIN}:Shown\n')
    monkeypatch.syspath_prepend(tmp_path / 'b')
    monkeypatch.syspath_prepend(tmp_path / 'a')
    with pytest.raises(
        SystemExit,
        match=f"^the daemon does not start: entry point shown = {PLUGIN}:Shown in piloop.runtimes: 'shown'"
        f' is a runtime already, from piloop-a 1.0, entry point shown = {PLUGIN}:Shown$',
    ):
        Daemon(Settings(home=tmp_path / 'home'))
    assert not (tmp_path / 'home').exists(), 'it failed before it opened anything'
    assert not any(c.active for c in plugins.declared())
    found = doctor.checks(Settings(home=tmp_path / 'home'))
    names = [c.name.split(' ')[0] for c in found]
    assert names[:5] == ['cli', 'daemon', 'database', 'plugins', 'secrets'] and set(names[5:]) <= {'skills', 'git'}, (
        'the checks of campaigns, harnesses and runtimes, which use the plugins, do not run'
    )
    found = [c for c in found if c.name == 'plugins']
    assert found[-1].fix is not None and "'shown' is a runtime already" in found[-1].found
    monkeypatch.setattr(common, 'settings', Settings(home=tmp_path / 'home'))
    with pytest.raises(SystemExit, match="'shown' is a runtime already"):
        app(['plugins'], result_action='return_value', exit_on_error=False)


@pytest.mark.parametrize(
    ('config', 'error'),
    [
        (
            '[runtimes.tmux]\nx = 1\n',
            r'config.toml: \[runtimes.tmux\]: no runtime has that name; registered runtimes:',
        ),
        (
            '[storage.backend]\nkind = "azure"\n',
            r"config.toml: \[storage.backend\] kind = 'azure': unknown storage backend 'azure'; registered storage backends",
        ),
        ('[storage.backend]\nkind = "s3"\n', r'config.toml: 1 validation error for S3Config\nurl\n  Field required'),
    ],
)
def test_config_toml_that_binds_a_provider_that_there_is_not_fails_before_the_daemon_starts(
    tmp_path: Path, config: str, error: str
) -> None:
    (tmp_path / 'config.toml').write_text(config)
    with pytest.raises(SystemExit, match=f'^the daemon does not start: .*{error}'):
        Daemon(Settings(home=tmp_path))
    assert not (tmp_path / 'piloop.db').exists()
    assert next(c for c in doctor.checks(Settings(home=tmp_path)) if c.name == 'plugins').fix is not None


def test_an_entry_point_that_does_not_load_fails_with_its_name_and_the_cause(monkeypatch: pytest.MonkeyPatch) -> None:
    installed(monkeypatch, [EntryPoint('gone', 'tests.fixtures.no_such_module:Gone', RUNTIMES.group)])
    with pytest.raises(
        PluginError,
        match=r'^entry point gone = tests.fixtures.no_such_module:Gone in piloop.runtimes does not load:'
        r" ModuleNotFoundError: No module named 'tests.fixtures.no_such_module'$",
    ) as failed:
        RUNTIMES.providers()
    assert isinstance(failed.value.__cause__, ModuleNotFoundError)


def test_a_failed_composition_unregisters_what_it_activated_and_keeps_what_was_active(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A broken role unwinds the runtimes and the triggers of the same call, and leaves the remotes, active before."""
    typed = EntryPoint('typed', f'{PLUGIN}:Typed', RUNTIMES.group)
    installed(monkeypatch, [typed, EntryPoint('auditor', f'{PLUGIN}:NOT_A_ROLE', ROLES.group)])
    STORAGE_BACKENDS.providers()
    with pytest.raises(PluginError, match='is not a piloop SweepRole or ImplementerRole'):
        compose(RUNTIMES, TRIGGERS, ROLES)
    assert [c.active for c in (RUNTIMES, TRIGGERS, ROLES, STORAGE_BACKENDS)] == [False, False, False, True]
    installed(monkeypatch, [typed])
    compose(RUNTIMES, TRIGGERS, ROLES)
    assert [c.active for c in (RUNTIMES, TRIGGERS, ROLES)] == [True, True, True]
    assert RUNTIMES.get('typed') is Typed
    plugins.close()
    plugins.close()
    assert not any(c.active for c in plugins.declared()), 'closing twice is harmless'
    assert 'typed' in RUNTIMES.providers(), 'the next use activates again'


def test_a_broken_package_of_a_capability_that_nothing_binds_is_a_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With no ``[storage.backend]``, the daemon starts without the storage backends: one that does not load warns."""
    installed(monkeypatch, [EntryPoint('gone', 'tests.fixtures.no_such_module:Gone', STORAGE_BACKENDS.group)])
    home = Settings(home=tmp_path)
    Daemon(home).shutdown(timeout=5)
    found = next(c for c in doctor.checks(home) if c.name == 'plugins')
    assert found.fix is None
    assert found.found == (
        '1 executor, 4 roles, 2 runtimes, 1 stop rule, 7 triggers, 1 verdict rule; all built in; warning: nothing selects a storage backend, and they do not load:'
        ' entry point gone = tests.fixtures.no_such_module:Gone in piloop.stores does not load: ModuleNotFoundError:'
        " No module named 'tests.fixtures.no_such_module'"
    )
    monkeypatch.setattr(common, 'settings', home)
    printed: list[str] = []
    monkeypatch.setattr(common.console, 'print', lambda text, **_: printed.append(text))
    app(['plugins'], result_action='return_value', exit_on_error=False)
    lines = printed[0].splitlines()
    shown = lines.index('storage backends (host tier, piloop.stores):') + 1
    assert lines[shown].startswith('  not loaded, and nothing selects a storage backend: entry point gone = ')
    (tmp_path / 'config.toml').write_text('[storage.backend]\nkind = "s3"\nurl = "s3://b/p"\n')
    with pytest.raises(SystemExit, match='the daemon does not start: entry point gone = '):
        Daemon(Settings(home=tmp_path))


def test_a_name_that_no_provider_has_lists_the_ones_there_are() -> None:
    with pytest.raises(MissingProvider, match="^unknown runtime 'tmux'; registered runtimes: herdr, subprocess$"):
        RUNTIMES.get('tmux')


class Reentrant(EntryPoint):
    """An entry point whose import needs the triggers that it is one of."""

    def load(self) -> object:
        return TRIGGERS.providers()


def test_an_entry_point_that_needs_its_own_capability_while_it_loads_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    installed(monkeypatch, [Reentrant('loop', f'{PLUGIN}:EveryTick', TRIGGERS.group)])
    with pytest.raises(
        PluginError,
        match=r'^the triggers are needed while they load: an entry point of piloop.triggers uses its own capability,'
        r' or one that uses it, when it is imported$',
    ):
        TRIGGERS.providers()
    assert not any(c.active for c in plugins.declared())


@dataclass
class Fake:
    """A capability that records when it activates and when its registration is undone."""

    noun: str
    log: list[str]
    tier: Tier = Tier.HOST
    fails: bool = False
    group: str = 'piloop.fakes'
    selection: Selection | None = None
    active: bool = False
    activating: bool = False
    seen: dict[str, Source] = field(default_factory=dict)

    def activate(self, stack: ExitStack) -> None:
        if self.fails:
            raise PluginError(f'{self.noun} fails')
        self.active = True
        self.log.append(f'+{self.noun}')
        stack.callback(self._undo)

    def _undo(self) -> None:
        self.active = False
        self.log.append(f'-{self.noun}')

    def sources(self) -> dict[str, Source]:
        return self.seen

    def get(self, name: str) -> object:
        return self.seen[name]


def test_activation_goes_in_order_teardown_newest_first_and_a_failure_undoes_only_its_own_call() -> None:
    log: list[str] = []
    store, runtime, planner = Fake('store', log), Fake('runtime', log), Fake('planner', log)
    compose(store, runtime, planner)
    plugins.close()
    assert log == ['+store', '+runtime', '+planner', '-planner', '-runtime', '-store']
    log.clear()
    compose(store)
    with pytest.raises(PluginError, match='broken fails'):
        compose(runtime, Fake('broken', log, fails=True))
    assert log == ['+store', '+runtime', '-runtime'], 'only what the failed call activated is undone'
    plugins.close()
    assert log[-1] == '-store'


CASE = Case(
    Criterion('score', Direction.HIGHER),
    (),
    0.95,
    Side('candidate#2', {'score': 13.0}, {}),
    Side('candidate#1', {'score': 10.0}, {}),
    '{"min_gain": 2}',
)
"""A case of the verdict rules, the pure capability that these tests install a provider of."""


def test_a_pure_plugin_is_installed_selected_by_config_and_answers_with_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, db: Database, campaign: Campaign
) -> None:
    """The example ``examples/plugins/min-gain`` provides a verdict rule, and ``[plugins.verdict_rule]`` selects it."""
    campaign_path = Path(campaign.path)
    example(tmp_path / 'site', 'min-gain')
    monkeypatch.syspath_prepend(tmp_path / 'site')
    assert VERDICT_RULES.tier is Tier.PURE and data_errors(VerdictRule) == []
    cfg = load_campaign_config(campaign_path, plugins={'verdict_rule': {'provider': 'min_gain', 'min_gain': 2}})
    table = cfg.plugins['verdict_rule']
    rule = VERDICT_RULES.get(table.provider)
    assert rule is piloop_min_gain.MIN_GAIN
    assert table.parameters == '{"min_gain": 2}'
    assert rule.decide(CASE) == Judgement(
        Outcome.KEEP, 'against candidate#1: score 13 vs 10 (+3; aggregate: no per-unit results)'
    )
    assert VERDICT_RULES.sources()['min_gain'].release == 'piloop-min-gain 0.1.0'
    home = Settings(home=tmp_path)
    assert not any(line.startswith('demo: verdict rule') for line in selections(home)), 'demo selects none yet'
    with db.session() as s:
        s.get_one(Campaign, campaign.id).config = cfg.model_dump(mode='json')
        s.commit()
    assert (
        'demo: verdict rule min_gain, from piloop-min-gain 0.1.0 ([plugins.verdict_rule] provider, in the'
        f' config of demo that the last `piloop init` stored from {campaign_path}/piloop.toml)'
    ) in selections(home)
    plain = load_campaign_config(campaign_path, plugins={'verdict_rule': {'provider': 'bootstrap'}})
    assert plain.plugins['verdict_rule'].parameters == '{}', 'a rule without Parameters, given none'
    for tables, error in [
        (
            {'verdict_rule': {'provider': 'nope'}},
            "[plugins.verdict_rule] provider = 'nope': unknown verdict rule 'nope'",
        ),
        ({'verdict_rule': {'provider': 'min_gain', 'min_gain': 'x'}}, "min_gain = 'x': Input should be a valid number"),
        ({'verdict_rule': {'provider': 'min_gain'}}, 'min_gain: Field required'),
        (
            {'planner': {'provider': 'min_gain'}},
            '[plugins.planner]: no capability has that name; there is stop_rule, verdict_rule',
        ),
        ({'verdict_rule': {'provider': 'bootstrap', 'min_gain': 2}}, "verdict rule 'bootstrap' takes no parameters"),
    ]:
        with pytest.raises(ConfigError, match=re.escape(error)):
            load_campaign_config(campaign_path, plugins=tables)


@pytest.mark.parametrize(
    ('value', 'error'),
    [
        ('piloop_min_gain:MinGain', 'names the class MinGain: the entry point of a pure provider names an'),
        (f'{PLUGIN}:NOT_A_ROLE', 'does not implement VerdictRule: it needs decide$'),
        (
            f'{__name__}:MISNAMED',
            r'does not implement VerdictRule: decide does not take the arguments of VerdictRule.decide \(case\):'
            r" missing a required argument: 'judged'$",
        ),
    ],
)
def test_a_pure_provider_is_an_instance_with_the_methods_and_arguments_of_its_contract(
    monkeypatch: pytest.MonkeyPatch, value: str, error: str
) -> None:
    installed(monkeypatch, [EntryPoint('odd', value, VERDICT_RULES.group)])
    with pytest.raises(PluginError, match=f'^entry point odd = {value} in piloop.verdict_rules {error}'):
        VERDICT_RULES.providers()


class Misnamed:
    """Has the method of the contract, with an argument of another name: the host calls it with the contract's."""

    def decide(self, judged: Case) -> Judgement:
        return Judgement(Outcome.CRASH, 'never called')


MISNAMED = Misnamed()


@dataclass(frozen=True)
class Loop:
    next: Loop | None


class Open(BaseModel):
    """A record that takes any extra field, of any type."""

    model_config = ConfigDict(extra='allow')
    x: int


class Plain:
    """A class that is not a record."""


class Leaky(Protocol):
    """A contract that passes live objects, and so cannot be served out of process."""

    db: Session

    @property
    def session(self) -> Session: ...

    @staticmethod
    def helper(s: Session) -> None: ...

    @classmethod
    def build(cls, s: Session) -> None: ...

    def run(
        self, thing: object, anything: list[Any], then: Callable[[int], int], log: IO[str], plain: Plain
    ) -> None: ...

    def nested(self, open: Open) -> Loop: ...

    def loose(self, x, *args: int) -> int: ...

    def untyped(self, x: int): ...


def test_a_pure_contract_takes_and_returns_data_only() -> None:
    anything = 'has a part of any type (Any, object, or a model that allows extra fields)'
    no_schema = 'is not data: Unable to generate pydantic-core schema for'
    assert data_errors(Leaky) == [
        'db is not a method: a pure contract has methods only, as a WIT interface',
        'session is not a method: a pure contract has methods only, as a WIT interface',
        f"build(s): <class 'sqlalchemy.orm.session.Session'> {no_schema} <class 'sqlalchemy.orm.session.Session'>."
        ' Set `arbitrary_types_allowed=True` in the model_config to ignore this error or implement'
        ' `__get_pydantic_core_schema__` on your type to fully support it.',
        f"helper(s): <class 'sqlalchemy.orm.session.Session'> {no_schema} <class 'sqlalchemy.orm.session.Session'>."
        ' Set `arbitrary_types_allowed=True` in the model_config to ignore this error or implement'
        ' `__get_pydantic_core_schema__` on your type to fully support it.',
        'loose(x) has no type',
        'loose(args) takes any number of arguments',
        f'nested(open): {Open!r} {anything}',
        f'nested returns {Loop!r} contains itself (Loop → Loop), and WIT types are not recursive',
        f"run(thing): <class 'object'> {anything}",
        f'run(anything): list[typing.Any] {anything}',
        'run(then): collections.abc.Callable[[int], int] is not data: Cannot generate a JsonSchema for'
        ' core_schema.CallableSchema',
        f'run(log): typing.IO[str] {no_schema} typing.IO[str]. Set `arbitrary_types_allowed=True` in the model_config'
        ' to ignore this error or implement `__get_pydantic_core_schema__` on your type to fully support it.',
        f'run(plain): {Plain!r} {no_schema} {Plain!r}. Set'
        ' `arbitrary_types_allowed=True` in the model_config to ignore this error or implement'
        ' `__get_pydantic_core_schema__` on your type to fully support it.',
        'untyped has no return type',
    ]
    with pytest.raises(TypeError, match=r'^Leaky is not a pure contract: db is not a method'):
        PureCapability('leak', group='piloop.example.leaks', contract=Leaky)


def test_a_plugin_declares_a_capability_calls_its_providers_and_another_package_provides_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, campaign: Campaign
) -> None:
    """``examples/plugins/verdict-checks`` declares ``verdict check`` and calls its providers from its verdict rule;
    ``examples/plugins/no-unit-regression`` provides one. piloop lists, loads and checks it as one of its own."""
    campaign_path = Path(campaign.path)
    example(tmp_path / 'site', 'verdict-checks')
    example(tmp_path / 'site', 'no-unit-regression')
    monkeypatch.syspath_prepend(tmp_path / 'site')
    plugins.close()  # the campaign's config read the entry points before the packages were installed
    checks = next(c for c in plugins.declared() if c.noun == 'verdict check')
    assert (checks.tier, checks.group, checks.selection) == (Tier.PURE, 'piloop_verdict_checks.checks', None)
    assert checks.sources()['no_unit_regression'].release == 'piloop-no-unit-regression 0.1.0'
    with pytest.raises(
        ConfigError, match=re.escape("unknown verdict check 'none'; registered verdict checks: no_unit")
    ):
        load_campaign_config(campaign_path, plugins={'verdict_rule': {'provider': 'checked', 'checks': ['none']}})
    table = {'provider': 'checked', 'checks': ['no_unit_regression']}
    cfg = load_campaign_config(campaign_path, plugins={'verdict_rule': table})
    rule = VERDICT_RULES.get(cfg.plugins['verdict_rule'].provider)
    units = {f'u{i}': 1.0 for i in range(30)}
    better = {**units, **{f'u{i}': 2.0 for i in range(1, 30)}}

    def case(mine: dict[str, float]) -> Case:
        sides = Side('candidate#2', {'score': sum(mine.values())}, {'score': mine})
        bar = Side('candidate#1', {'score': 30.0}, {'score': units})
        return Case(Criterion('score', Direction.HIGHER), (), 0.95, sides, bar, cfg.plugins['verdict_rule'].parameters)

    assert rule.decide(case(better)).verdict is Outcome.KEEP, 'no unit got worse'
    worse = rule.decide(case({**better, 'u0': 0.5}))
    assert worse.verdict is Outcome.INTERESTING
    assert worse.reason.endswith('; held by no_unit_regression: 1 units of score got worse: u0')
    monkeypatch.setattr(common, 'settings', Settings(home=tmp_path))
    printed: list[str] = []
    monkeypatch.setattr(common.console, 'print', lambda text, **_: printed.append(text))
    app(['plugins'], result_action='return_value', exit_on_error=False)
    lines = printed[0].splitlines()
    listed = lines.index('verdict checks (pure tier, piloop_verdict_checks.checks):')
    assert lines[listed + 1].split() == [
        'no_unit_regression',
        'piloop-no-unit-regression',
        '0.1.0,',
        'entry',
        'point',
        'no_unit_regression',
        '=',
        'piloop_no_unit_regression:NO_UNIT_REGRESSION',
    ]


STORES_AGAIN = PureCapability('copy', group='piloop.stores', contract=VerdictRule)
"""A capability whose provider group the storage backends have already."""


@pytest.mark.parametrize(
    ('value', 'error'),
    [
        ('tests.fixtures.no_such_module:X', 'does not load: ModuleNotFoundError'),
        (f'{PLUGIN}:NOT_A_ROLE', 'is not a piloop.plugins.host.Capability$'),
        (f'{__name__}:STORES_AGAIN', 'the storage backends have the provider group piloop.stores already'),
    ],
)
def test_a_declared_capability_loads_is_a_capability_and_has_a_group_of_its_own(
    monkeypatch: pytest.MonkeyPatch, value: str, error: str
) -> None:
    """Each entry point of ``piloop.capabilities`` names one capability; the next one with its group is refused."""
    installed(monkeypatch, [EntryPoint('odd', value, plugins.CAPABILITIES)])
    with pytest.raises(PluginError, match=f'^entry point odd = {re.escape(value)} in piloop.capabilities.*{error}'):
        plugins.declared()
