"""The plugin host: an installed package mounts providers through it, and a composition that fails registers nothing."""

import re
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from contextlib import ExitStack
from dataclasses import dataclass, replace
from importlib.metadata import EntryPoint
from pathlib import Path
from typing import IO, Any

import piloop_min_gain
import pytest
from piloop_nice import Nice, NiceRuntime
from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy.orm import Session

from piloop.cli import common, doctor
from piloop.cli.main import app
from piloop.cli.plugins import selections
from piloop.config import ConfigError, load_campaign_config
from piloop.daemon.main import Daemon
from piloop.db.models import Campaign
from piloop.db.session import Database
from piloop.plugins import host as plugins
from piloop.plugins.cas.contract import CAS
from piloop.plugins.executor.workers import Workers
from piloop.plugins.host import (
    Capability,
    Choice,
    MissingProvider,
    Parameters,
    PluginError,
    PythonSource,
    Scope,
    Tier,
    compose,
    data_errors,
)
from piloop.plugins.metric import Direction
from piloop.plugins.role.contract import ROLES, SweepRole
from piloop.plugins.runtime.contract import RUNTIMES
from piloop.plugins.runtime.subprocess import Subprocess
from piloop.plugins.trigger.contract import TRIGGERS
from piloop.plugins.verdict_rule.bootstrap import Bootstrap
from piloop.plugins.verdict_rule.contract import VERDICT_RULES, Case, Criterion, Judgement, Outcome, Side, VerdictRule
from piloop.runtime.factory import RUNTIME_TABLES, make_runtime
from piloop.settings import HOME_PLUGINS, Settings
from tests.conftest import example, package
from tests.daemon.test_roles import PLUGIN, installed
from tests.fixtures.plugin import Checked, EveryPass, NoUnitRegression, Typed


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
    assert RUNTIMES.get('nice').model is Nice
    assert isinstance(make_runtime('nice'), NiceRuntime)
    assert RUNTIMES.sources()['nice'] == PythonSource('piloop-nice', '0.1.0', 'nice = piloop_nice:Nice')
    built_in = RUNTIMES.sources()['subprocess']
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
    assert lines[runtimes : runtimes + 3] == [
        'runtimes (host tier, piloop.runtimes):',
        f'  subprocess  {built_in}',
        '  nice        piloop-nice 0.1.0, entry point nice = piloop_nice:Nice',
    ]
    assert lines[-1] == 'selected:', 'no campaign and no remote: nothing is selected'


def test_the_runtimes_of_config_toml_hold_every_runtime_with_its_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    example(tmp_path, 'nice-runtime')
    monkeypatch.syspath_prepend(tmp_path)
    tables = RUNTIME_TABLES.validate_python({'nice': {'niceness': 5}})
    assert tables == {'subprocess': Subprocess(), 'nice': Nice(niceness=5)}, 'every runtime, over its defaults'
    assert RUNTIME_TABLES.validate_python(tables) == tables
    assert RUNTIME_TABLES.dump_python(tables) == {'subprocess': {}, 'nice': {'niceness': 5}}
    with pytest.raises(ValidationError, match=r'\nnice\.niceness\n  Input should be a valid integer'):
        RUNTIME_TABLES.validate_python({'nice': {'niceness': 'low'}})
    with pytest.raises(ValidationError, match='a table of tables'):
        RUNTIME_TABLES.validate_python(3)


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
            r'config.toml: 1 validation error for \[runtimes\]\ntmux\n  Value error, no runtime has that name; registered'
            r' runtimes: subprocess',
        ),
        (
            '[plugins.cas]\nprovider = "azure"\n',
            r"config.toml: 1 validation error for \[plugins\]\ncas\n  Value error, provider = 'azure': unknown CAS"
            r" provider 'azure'; registered CAS providers",
        ),
        (
            '[plugins.cas]\nprovider = "s3"\n',
            r'config.toml: 1 validation error for \[plugins\]\ncas.url\n  Field required',
        ),
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
    CAS.providers()
    with pytest.raises(PluginError, match='is not a subclass of piloop.plugins.role.contract.RoleConfig'):
        compose(*declared('runtime', 'trigger', 'role'))
    assert [c.active for c in (RUNTIMES, TRIGGERS, ROLES, CAS)] == [False, False, False, True]
    installed(monkeypatch, [typed])
    compose(*declared('runtime', 'trigger', 'role'))
    assert [c.active for c in (RUNTIMES, TRIGGERS, ROLES)] == [True, True, True]
    assert RUNTIMES.get('typed').model is Typed
    plugins.close()
    plugins.close()
    assert not any(c.active for c in plugins.declared()), 'closing twice is harmless'
    assert 'typed' in RUNTIMES.providers(), 'the next use activates again'


def declared(*names: str) -> list[Capability[Parameters]]:
    """The capabilities called ``names``, in that order."""
    by_name = {c.name: c for c in plugins.declared()}
    return [by_name[name] for name in names]


def test_a_broken_package_of_a_capability_that_nothing_binds_is_a_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With no ``[plugins.extra]``, the daemon starts without the extras that a plugin declares: one that does not load
    warns."""
    installed(
        monkeypatch,
        [
            EntryPoint('extra', f'{__name__}:EXTRAS', plugins.CAPABILITIES),
            EntryPoint('gone', 'tests.fixtures.no_such_module:Gone', EXTRAS.group),
        ],
    )
    home = Settings(home=tmp_path)
    Daemon(home).shutdown(timeout=5)
    found = next(c for c in doctor.checks(home) if c.name == 'plugins')
    assert found.fix is None
    assert found.found == (
        '5 CAS providers, 1 executor, 2 networks, 1 provisioner, 4 roles, 1 runtime, 1 secrets provider, 17 sections, 1 stop rule, 7 triggers, 1 vcs provider, 1 verdict rule; all built in; warning: nothing selects a extra, and they do not load:'
        ' entry point gone = tests.fixtures.no_such_module:Gone in piloop.example.extras does not load: ModuleNotFoundError:'
        " No module named 'tests.fixtures.no_such_module'"
    )
    monkeypatch.setattr(common, 'settings', home)
    printed: list[str] = []
    monkeypatch.setattr(common.console, 'print', lambda text, **_: printed.append(text))
    app(['plugins'], result_action='return_value', exit_on_error=False)
    lines = printed[0].splitlines()
    shown = lines.index('extras (host tier, piloop.example.extras):') + 1
    assert lines[shown].startswith('  not loaded, and nothing selects a extra: entry point gone = ')
    (tmp_path / 'config.toml').write_text('[plugins.extra]\nprovider = "gone"\n')
    with pytest.raises(SystemExit, match='the daemon does not start: entry point gone = '):
        Daemon(Settings(home=tmp_path))


def test_a_name_that_no_provider_has_lists_the_ones_there_are() -> None:
    with pytest.raises(MissingProvider, match="^unknown runtime 'tmux'; registered runtimes: subprocess$"):
        RUNTIMES.get('tmux')


class Reentrant(EntryPoint):
    """An entry point whose import needs the triggers that it is one of."""

    def load(self) -> object:
        return TRIGGERS.providers()


def test_an_entry_point_that_needs_its_own_capability_while_it_loads_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    installed(monkeypatch, [Reentrant('loop', f'{PLUGIN}:EveryPass', TRIGGERS.group)])
    with pytest.raises(
        PluginError,
        match=r'^the triggers are needed while they load: an entry point of piloop.triggers uses its own capability,'
        r' or one that uses it, when it is imported$',
    ):
        TRIGGERS.providers()
    assert not any(c.active for c in plugins.declared())


class Fake(Capability[Parameters]):
    """A capability that records when it activates and when its registration is undone."""

    def __init__(self, name: str, log: list[str], *, fails: bool = False) -> None:
        super().__init__(name, Parameters)
        self.log = log
        self.fails = fails
        self.on = False

    @property
    def active(self) -> bool:
        return self.on

    def activate(self, stack: ExitStack) -> None:
        if self.fails:
            raise PluginError(f'{self.name} fails')
        self.on = True
        self.log.append(f'+{self.name}')
        stack.callback(self._undo)

    def _undo(self) -> None:
        self.on = False
        self.log.append(f'-{self.name}')


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
    Side('candidate#2', {'score': 13.0}, {}),
    Side('candidate#1', {'score': 10.0}, {}),
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
    rule = VERDICT_RULES.selected(cfg.plugins)
    assert rule == piloop_min_gain.MinGain(min_gain=2)
    assert rule.decide(CASE) == Judgement(
        Outcome.KEEP, 'against candidate#1: score 13 vs 10 (+3; aggregate: no per-unit results)'
    )
    assert VERDICT_RULES.sources()['min_gain'].release == 'piloop-min-gain 0.1.0'
    home = Settings(home=tmp_path)
    assert not any(line.startswith('demo: verdict rule') for line in selections(home)), 'demo selects none yet'
    with db.session() as s:
        s.get_one(Campaign, campaign.id).config = cfg
        s.commit()
    assert (
        'demo: verdict rule min_gain, from piloop-min-gain 0.1.0 ([plugins.verdict_rule] provider, in the'
        f' config of demo that the last `piloop init` stored from {campaign_path}/piloop.toml)'
    ) in selections(home)
    plain = load_campaign_config(campaign_path, plugins={'verdict_rule': {'provider': 'bootstrap'}})
    assert plain.plugins['verdict_rule'] == Bootstrap(), 'a rule without fields, given none'
    assert VERDICT_RULES.selected({}) == Bootstrap(), 'the default'
    for tables, error in [
        (
            {'verdict_rule': {'provider': 'nope'}},
            "plugins.verdict_rule: provider = 'nope': unknown verdict rule 'nope'",
        ),
        ({'verdict_rule': {'provider': 'min_gain', 'min_gain': 'x'}}, "min_gain = 'x': Input should be a valid number"),
        ({'verdict_rule': {'provider': 'min_gain'}}, 'min_gain: Field required'),
        (
            {'planner': {'provider': 'min_gain'}},
            'plugins.planner: no capability has that name; there is stop_rule, vcs, verdict_rule',
        ),
        (
            {'verdict_rule': {'provider': 'bootstrap', 'min_gain': 2}},
            'plugins.verdict_rule.min_gain = 2: Extra inputs are not permitted',
        ),
        ({'verdict_rule': 1}, 'plugins.verdict_rule = 1: not a table'),
    ]:
        with pytest.raises(ConfigError, match=re.escape(error)):
            load_campaign_config(campaign_path, plugins=tables)


@pytest.mark.parametrize(
    ('value', 'error'),
    [
        (f'{PLUGIN}:NOT_A_ROLE', 'is not a subclass of piloop.plugins.verdict_rule.contract.VerdictRule$'),
        (
            f'{PLUGIN}:Undecided',
            'does not implement piloop.plugins.verdict_rule.contract.VerdictRule: it has no decide$',
        ),
        ('piloop.plugins.stop_rule.limits:Limits', 'is not a subclass of piloop.plugins.verdict_rule.contract'),
    ],
)
def test_a_provider_is_a_class_that_implements_its_contract(
    monkeypatch: pytest.MonkeyPatch, value: str, error: str
) -> None:
    installed(monkeypatch, [EntryPoint('odd', value, VERDICT_RULES.group)])
    with pytest.raises(PluginError, match=f'^entry point odd = {value} in piloop.verdict_rules {error}'):
        VERDICT_RULES.providers()


@dataclass(frozen=True)
class Loop:
    next: Loop | None


class Open(BaseModel):
    """A record that takes any extra field, of any type."""

    model_config = ConfigDict(extra='allow')
    x: int


class Plain:
    """A class that is not a record."""


class Leaky(Parameters, ABC):
    """A contract that passes live objects, and so cannot be served out of process."""

    @property
    @abstractmethod
    def session(self) -> Session: ...

    @staticmethod
    @abstractmethod
    def helper(s: Session) -> None: ...

    @classmethod
    @abstractmethod
    def build(cls, s: Session) -> None: ...

    @abstractmethod
    def run(
        self, thing: object, anything: list[Any], then: Callable[[int], int], log: IO[str], plain: Plain
    ) -> None: ...

    @abstractmethod
    def nested(self, open: Open) -> Loop: ...

    @abstractmethod
    def loose(self, x, *args: int) -> int: ...

    @abstractmethod
    def untyped(self, x: int): ...


def test_a_pure_contract_takes_and_returns_data_only() -> None:
    anything = 'has a part of any type (Any, object, or a model that allows extra fields)'
    no_schema = 'is not data: Unable to generate pydantic-core schema for'
    assert data_errors(Leaky) == [
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
    with pytest.raises(TypeError, match=r'^Leaky is not a pure contract: session is not a method'):
        Capability('leak', Leaky, tier=Tier.PURE)


def test_a_plugin_declares_a_capability_calls_its_providers_and_another_package_provides_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, campaign: Campaign
) -> None:
    """A package declares ``verdict_check`` and calls its providers from its verdict rule; another provides one.
    piloop lists, loads and checks it as one of its own."""
    campaign_path = Path(campaign.path)
    installed(
        monkeypatch,
        [
            EntryPoint('verdict_check', f'{PLUGIN}:VERDICT_CHECKS', plugins.CAPABILITIES),
            EntryPoint('no_unit_regression', f'{PLUGIN}:NoUnitRegression', 'piloop.verdict_checks'),
            EntryPoint('checked', f'{PLUGIN}:Checked', VERDICT_RULES.group),
        ],
    )
    checks = next(c for c in plugins.declared() if c.name == 'verdict_check')
    assert (checks.tier, checks.group, checks.choice) == (Tier.PURE, 'piloop.verdict_checks', None)
    with pytest.raises(
        ConfigError, match=re.escape("unknown verdict check 'none'; registered verdict checks: no_unit")
    ):
        none = {'provider': 'checked', 'checks': [{'provider': 'none'}]}
        load_campaign_config(campaign_path, plugins={'verdict_rule': none})
    table = {'provider': 'checked', 'checks': [{'provider': 'no_unit_regression'}]}
    rule = VERDICT_RULES.selected(load_campaign_config(campaign_path, plugins={'verdict_rule': table}).plugins)
    assert rule == Checked(checks=[NoUnitRegression()])
    units = {f'u{i}': 1.0 for i in range(30)}
    better = {**units, **{f'u{i}': 2.0 for i in range(1, 30)}}

    def case(mine: dict[str, float]) -> Case:
        sides = Side('candidate#2', {'score': sum(mine.values())}, {'score': mine})
        return Case(Criterion('score', Direction.HIGHER), (), sides, Side('candidate#1', {'score': 30.0}, {}))

    assert rule.decide(case(better)).verdict is Outcome.KEEP, 'no unit got worse'
    worse = rule.decide(
        replace(case({**better, 'u0': 0.5}), bar=Side('candidate#1', {'score': 30.0}, {'score': units}))
    )
    assert worse.verdict is Outcome.INTERESTING
    assert worse.reason.endswith('; held by no_unit_regression: 1 units of score got worse: u0')
    monkeypatch.setattr(common, 'settings', Settings(home=tmp_path))
    printed: list[str] = []
    monkeypatch.setattr(common.console, 'print', lambda text, **_: printed.append(text))
    app(['plugins'], result_action='return_value', exit_on_error=False)
    lines = printed[0].splitlines()
    listed = lines.index('verdict checks (pure tier, piloop.verdict_checks):')
    assert lines[listed + 1].split()[:3] == ['no_unit_regression', 'an', 'unknown']


STORES_AGAIN = Capability('odd', VerdictRule, group='piloop.cas')
"""A capability whose provider group the CAS providers have already."""


class Extra(Parameters):
    """The contract of a capability of the home that a plugin declares."""


EXTRAS = Capability('extra', Extra, choice=Choice(Scope.HOME), group='piloop.example.extras')
"""A capability of the home that a plugin declares, which nothing selects by default."""


@pytest.mark.parametrize(
    ('value', 'error'),
    [
        ('tests.fixtures.no_such_module:X', 'does not load: ModuleNotFoundError'),
        (f'{PLUGIN}:NOT_A_ROLE', 'is not a piloop.plugins.host.Capability$'),
        (f'{__name__}:STORES_AGAIN', 'the CAS providers have the provider group piloop.cas already'),
        (f'{PLUGIN}:VERDICT_CHECKS', "names the capability 'verdict_check': the names must be the same$"),
    ],
)
def test_a_declared_capability_loads_is_a_capability_and_has_a_group_of_its_own(
    monkeypatch: pytest.MonkeyPatch, value: str, error: str
) -> None:
    """Each entry point of ``piloop.capabilities`` names one capability; the next one with its group is refused."""
    installed(monkeypatch, [EntryPoint('odd', value, plugins.CAPABILITIES)])
    with pytest.raises(PluginError, match=f'^entry point odd = {re.escape(value)} in piloop.capabilities.*{error}'):
        plugins.declared()


class Unregistered(Bootstrap):
    """A verdict rule that no entry point names."""


def test_a_capability_selects_the_table_of_its_file_else_its_default_else_nothing() -> None:
    workers = Workers(renew_seconds=5, grace_seconds=40)
    assert HOME_PLUGINS.validate_python({'executor': workers}) == {'executor': workers}
    with pytest.raises(ValidationError, match=r'\[plugins\] is a table of tables'):
        HOME_PLUGINS.validate_python(1)
    with pytest.raises(MissingProvider, match=r'^Unregistered is no verdict rule of an entry point$'):
        VERDICT_RULES.name_of(Unregistered())
    chosen = Capability('odd', Parameters, choice=Choice(Scope.CAMPAIGN))
    with pytest.raises(MissingProvider, match=r'^no \[plugins.odd\] chooses a odd, and there is no default$'):
        chosen.selected({})
    with pytest.raises(ValueError, match='^provider: missing; registered odds: $'):
        chosen.validated({})


def test_a_role_takes_trigger_objects_as_its_defaults_do() -> None:
    assert SweepRole(prompt='p.md', triggers=[EveryPass()]).triggers == [EveryPass()]
