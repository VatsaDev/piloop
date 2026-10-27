"""The plugin host: the one way that piloop finds, checks, activates, shows and tears down what plugins provide.

Terms:

- A **capability** is a public interface with a registry of named providers: the executors, the verdict rules, the
  runtimes. It declares its contract, a class, and where the configuration has its instances: one at
  ``[plugins.<name>]`` (``One``), several at ``[plugins.<name>.<instance>]`` (``Many``), or a table that holds one
  (``Entry``). A field that names an instance is a reference (``Ref``).
- An **instance** is a provider with its parameters: the table ``{ provider = "<name>", <its parameters> }``, wherever
  it is.
- A **provider** is a class that subclasses the contract, named by an entry point whose name is the provider's name.
  It is a ``Parameters`` model: its fields are its parameters, and its methods are the contract. The host validates
  its table into it once, when the file loads.
- A **plugin** is an installed Python package whose entry points name providers. piloop's own providers are entry
  points of the ``piloop`` package, in the same groups. A provider's ``PythonSource`` says which one, and its version.
- A **tier** is what the contract of a capability lets cross to a provider (``Tier``): live objects of the host, or
  data only. Everything else is the same for both tiers.

Lifecycle:

- A capability activates on first use (``Capability.providers``), or with others in ``compose``. It loads every
  provider, checks each one, and registers all of them or none: no consumer sees part of a registry.
- A provider that looks another capability up while it loads activates that one first.
- When an activation fails, ``compose`` unregisters what it activated, newest first, and raises a ``PluginError`` that
  names the entry point. ``close`` unregisters everything, newest first; the next use activates again.
- The registrations are owned effects on a ``contextlib.ExitStack``. A provider that holds a resource puts its cleanup
  on the same stack when a capability first needs one.

Inspiration:

- pluggy: providers found through entry points, each with the distribution that declares it.
- Cordis (DeepSeek Harness): registrations as owned, reversible effects; a failed activation unwinds what it did.
- The OpenTelemetry Collector: a component's configuration, validated with its defaults, makes the live component,
  and ``<type>/<name>`` names one of several instances of a type, which a pipeline lists by name.
- The WebAssembly component model: pure contracts of data only, and errors as values.
"""

import graphlib
import inspect
import json
import re
from collections.abc import Callable, Mapping
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum, StrEnum
from functools import cache
from importlib.metadata import EntryPoint, entry_points
from pathlib import Path
from threading import RLock
from types import UnionType
from typing import Annotated, Literal, Union, get_args, get_origin, get_type_hints

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    GetCoreSchemaHandler,
    GetJsonSchemaHandler,
    PydanticSchemaGenerationError,
    PydanticUserError,
    TypeAdapter,
    ValidationError,
)
from pydantic.errors import PydanticInvalidForJsonSchema
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema, ErrorDetails, InitErrorDetails, PydanticCustomError, core_schema
from pydantic_core.core_schema import ErrorType

BUILT_IN = 'piloop'
"""The package of the built-in providers: they are entry points of piloop's own ``pyproject.toml``."""


class Tier(StrEnum):
    """What crosses the boundary between piloop and the providers of a capability. Nothing else differs."""

    HOST = 'host'
    """The contract may pass live objects of the host (processes, files, sockets, a session): a runtime, an executor."""
    PURE = 'pure'
    """The contract is data in, data out (``data_errors``): a verdict rule, a stop rule."""


class Scope(StrEnum):
    """The file whose ``[plugins.<name>]`` table chooses a capability's provider."""

    CAMPAIGN = 'campaign'
    """``piloop.toml``."""
    HOME = 'home'
    """``$PILOOP_HOME/config.toml``."""


class PluginError(Exception):
    """The installed providers do not compose.

    An entry point does not load, names an object of the wrong type, reuses a name, or needs its own capability while
    it loads. The message names the entry point.
    """


class ContractBroken(AssertionError):
    """A provider does not do what piloop relies on: a capability's conformance check says which rule it breaks."""


class MissingProvider(ValueError):
    """A capability has no provider of that name. A ``ValueError``, so that a config validator reports it on its key."""


class Parameters(BaseModel):
    """The base of every contract that a ``[plugins.<name>]`` table chooses, so of each of its providers.

    A provider's fields are its parameters: the keys of the table besides ``provider``. A provider without fields takes
    none.
    """

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)


@dataclass(frozen=True)
class PythonSource:
    """A provider from a Python package: the distribution, its version, and the entry point."""

    package: str | None
    """The distribution that declares the entry point, or None when the entry point has no distribution."""
    version: str | None
    entry_point: str
    """``<name> = <module>:<attribute>``."""

    @property
    def built_in(self) -> bool:
        return self.package == BUILT_IN

    @property
    def release(self) -> str:
        return f'{self.package} {self.version}' if self.package is not None else 'an unknown package'

    def __str__(self) -> str:
        return self.release if self.built_in else f'{self.release}, entry point {self.entry_point}'


class Severity(StrEnum):
    """What a check that did not pass means: a failure of ``piloop doctor``, or a warning that changes nothing now."""

    FAIL = 'FAIL'
    WARN = 'WARN'


@dataclass(frozen=True)
class Check:
    """One line of the report of ``piloop doctor``: what was checked, what was found and, when it did not pass, the fix.

    A provider that checks the machine it works on returns these (``Vcs.checks``).
    """

    name: str
    found: str
    fix: str | None = None
    """None when the check passed."""
    severity: Severity = Severity.FAIL
    """What a check with a fix means."""

    @property
    def fails(self) -> bool:
        return self.fix is not None and self.severity is Severity.FAIL


class Live:
    """Marks a parameter of a provider that ``piloop limits set`` may change while the daemon runs.

    ``agents: Annotated[int | None, Live] = None``. The daemon hands the provider its parameters with the limits
    applied at once (``piloop.plugins.limits``).
    """


@dataclass(frozen=True)
class One:
    """``[plugins.<name>]`` of the file of ``scope`` is the capability's one instance: its provider and parameters."""

    scope: Scope
    default: str | None = None
    """The provider when the file has no table, or a table without ``provider``. None: then no provider is chosen.

    A declared default, not a flag of a provider: installing a package never changes the one instance of a file."""


class Pick(StrEnum):
    """How many instances ``[plugins.<name>] use`` names."""

    ONE = 'one'
    """One name: the instance in use."""
    ORDERED = 'ordered'
    """A list of names: the instances in use, in order, such as a rotation."""


@dataclass(frozen=True)
class Use:
    """``[plugins.<name>] use`` in the file of ``scope``: the instances that the campaign uses.

    Unset, it is the providers that are ``DEFAULT`` (``is_default``): all of them for ``ORDERED``, and the one for
    ``ONE``. The instances may be in another file: the runtimes are in ``config.toml``, and ``piloop.toml`` uses one.
    """

    scope: Scope
    pick: Pick
    refuses: Callable[[str], str | None] | None = None
    """Why a name is refused before it is looked up, such as a provider that a release removed. None: none is."""


@dataclass(frozen=True)
class Many[P: Parameters]:
    """``[plugins.<name>.<instance>]`` of the file of ``scope``: the capability's instances, by name.

    A table's ``provider`` is the provider of its own name when it has none. Each provider that needs no keys is also an
    instance of its own name, with its defaults, unless a table of that name changes them. ``[plugins.<name>]`` itself
    holds the capability's own settings: ``use``, when the capability declares it.
    """

    scope: Scope
    new: Callable[[str, Mapping[str, object]], type[P]] | None = None
    """The class of a table with a name that no provider has and without ``provider``, from its name and its keys.
    None: such a table is refused."""
    use: Use | None = None
    """Whether ``[plugins.<name>] use`` picks the instances that the campaign uses, and where."""


USE = 'use'
"""The key of ``[plugins.<name>]`` that names the instances in use: no instance has this name."""


@dataclass(frozen=True)
class Instances[P: Parameters]:
    """``[plugins.<name>]`` of a capability of several instances, validated: what it uses, and its instances by name."""

    capability: Capability[P]
    use: tuple[str, ...]
    """The instances in use, in order. Empty when the file of this table does not hold ``use``."""
    instances: dict[str, P]
    """The instances by name. Empty when the file of this table does not hold them."""


@dataclass(frozen=True)
class Provider[P: Parameters]:
    """One entry of a capability's registry."""

    name: str
    """The name of its entry point: the one place where the name is written."""
    model: type[P]
    source: PythonSource


def _source(ep: EntryPoint) -> PythonSource:
    spec = f'{ep.name} = {ep.value}'
    return PythonSource(None, None, spec) if ep.dist is None else PythonSource(ep.dist.name, ep.dist.version, spec)


def _where(ep: EntryPoint) -> str:
    """The declaration of an entry point, for errors."""
    return f'entry point {ep.name} = {ep.value} in {ep.group}'


_lock = RLock()
"""Held while capabilities activate or close: the daemon's threads look providers up."""
_process = ExitStack()
"""The registrations of every capability of the process, in the order that they activated."""
CAPABILITIES = 'piloop.capabilities'
"""The entry point group that declares the capabilities: piloop's own, and a plugin's. Each names a ``Capability``."""


class Capability[P: Parameters]:
    """A public interface, and the registry of its providers by name: the classes that subclass its contract."""

    def __init__(
        self,
        name: str,
        contract: type[P],
        *,
        tier: Tier = Tier.HOST,
        table: One | Many[P] | None = None,
        group: str | None = None,
        noun: str | None = None,
    ) -> None:
        """Declare a capability.

        Args:
            name: Its entry point in ``piloop.capabilities``, and the key of its ``[plugins.<name>]`` table.
            contract: The class that a provider subclasses: its fields are the provider's parameters. A pure contract
                takes and returns data only (``data_errors``).
            tier: What the contract lets cross to a provider.
            table: Where the file of its scope has its instances: one (``One``) or several (``Many``). Without it, an
                instance is a table that a parameter or a key holds (``entry``).
            group: The entry point group of its providers, piloop's own included. ``piloop.<name>s`` by default.
            noun: What one provider is, for messages, when ``name`` does not say it: ``CAS provider``.

        Raises:
            TypeError: A pure contract is not data in, data out.
        """
        if tier is Tier.PURE and (errors := data_errors(contract)):
            raise TypeError(f'{contract.__qualname__} is not a pure contract: {"; ".join(errors)}')
        self.name = name
        self.noun = noun or name.replace('_', ' ')
        """What one provider is, for messages: ``verdict rule``."""
        self.contract = contract
        self.tier = tier
        self.table = table
        self.group = group or f'piloop.{name}s'
        self.entry = Entry(self)
        """``Annotated[P, CAP.entry]``: a table ``{ provider = "<name>", <its parameters> }``."""
        self.ref = Ref(self)
        """``Annotated[str, CAP.ref]``: the name of an instance, which ``refs`` finds."""
        self._providers: dict[str, Provider[P]] | None = None
        self.activating = False
        """Whether its providers are being loaded now."""

    @property
    def active(self) -> bool:
        """Whether its providers are registered."""
        return self._providers is not None

    def providers(self) -> dict[str, Provider[P]]:
        """Every provider by name: piloop's own, then the others. Activate the capability first if needed.

        Raises:
            PluginError: The capability, or one that a provider looks up while it loads, does not activate.
        """
        if (found := self._providers) is None:
            compose(self)
            return self.providers()
        return found

    def get(self, name: str) -> Provider[P]:
        """Return the provider called ``name``.

        Raises:
            MissingProvider: No provider has that name. The message lists the ones that there are.
        """
        found = self.providers().get(name)
        if found is None:
            raise MissingProvider(
                f'unknown {self.noun} {name!r}; registered {self.noun}s: {", ".join(self.providers())}'
            )
        return found

    def sources(self) -> dict[str, PythonSource]:
        """Where each provider comes from, by name."""
        return {name: provider.source for name, provider in self.providers().items()}

    def builtins(self) -> dict[str, type[P]]:
        """The providers of piloop itself, by name: what the generated docs describe."""
        return {name: p.model for name, p in self.providers().items() if p.source.built_in}

    def name_of(self, provider: P) -> str:
        """The name of the provider that ``provider`` is an instance of.

        Raises:
            MissingProvider: No provider has its class.
        """
        found = next((n for n, p in self.providers().items() if p.model is type(provider)), None)
        if found is None:
            raise MissingProvider(f'{type(provider).__qualname__} is no {self.noun} of an entry point')
        return found

    def selected(self, plugins: Mapping[str, object]) -> P:
        """The one instance that ``[plugins.<name>]`` gives, else the default provider with its defaults.

        ``plugins`` is a file's ``[plugins]``, validated (``PluginTables``).

        Raises:
            MissingProvider: The file chooses no provider, and the capability has no default.
        """
        if isinstance(found := plugins.get(self.name), self.contract):
            return found
        if (default := self._default()) is None:
            raise MissingProvider(f'no [plugins.{self.name}] chooses a {self.noun}, and there is no default')
        return self.get(default).model()

    def instances(self, plugins: Mapping[str, object]) -> dict[str, P]:
        """The instances that ``[plugins.<name>.<instance>]`` gives, by name.

        They are every provider that needs no keys or has a table, in registry order, then the tables with other names,
        in the order of the file.

        ``plugins`` is a file's ``[plugins]``, validated (``PluginTables``), which has every capability of ``Many``.

        Raises:
            ContractBroken: ``plugins`` was not validated.
        """
        return dict(self._table(plugins, self._many().scope).instances)

    def used(self, plugins: Mapping[str, object]) -> tuple[str, ...]:
        """The names of the instances that ``[plugins.<name>] use`` picks, in order, else the default ones.

        Raises:
            ContractBroken: ``plugins`` was not validated, or the capability declares no ``use``.
        """
        if (use := self._many().use) is None:
            raise ContractBroken(f'the {self.noun}s have no [plugins.{self.name}] {USE}')
        return self._table(plugins, use.scope).use

    def _table(self, plugins: Mapping[str, object], scope: Scope) -> Instances[P]:
        """``[plugins.<name>]`` of ``plugins``, else the table of a file of ``scope`` that leaves it out."""
        found = plugins.get(self.name)
        if found is None:
            return self.validated_table({}, scope)
        if not isinstance(found, Instances):
            raise ContractBroken(f'[plugins.{self.name}] is not a table of {self.noun}s')
        return found

    def _many(self) -> Many[P]:
        match self.table:
            case Many() as many:
                return many
            case _:
                raise ContractBroken(f'the {self.noun}s are not of several instances')

    def _default(self) -> str | None:
        """The provider of a table without ``provider``: the declared default of a ``One``."""
        match self.table:
            case One(default=default):
                return default
            case _:
                return None

    def validated(self, table: object) -> P:
        """Validate a table as the provider that its ``provider`` key names, else the default. A provider is itself.

        Raises:
            ValidationError: The provider refuses its parameters.
            ValueError: The table is not a table, or names no provider that there is.
        """
        if isinstance(table, self.contract):
            return table
        if not isinstance(table, Mapping):
            raise ValueError('not a table')
        keys = {str(k): v for k, v in table.items() if k != 'provider'}
        name = table.get('provider', self._default())
        if name is None:
            raise ValueError(f'provider: missing; registered {self.noun}s: {", ".join(self.providers())}')
        return self.model_of(str(name), f'provider = {name!r}: ').model_validate(keys)

    def named_tables(self, table: object) -> list[str]:
        """The keys of a table of one instance that its provider does not take and that hold a table.

        ``[plugins.<name>.<other>]`` writes such a key: an instance by name, which a capability of one instance has not.
        """
        if not isinstance(table, Mapping) or (name := table.get('provider', self._default())) not in self.providers():
            return []
        fields = self.model_of(str(name)).model_fields
        return [str(k) for k, v in table.items() if k != 'provider' and k not in fields and isinstance(v, Mapping)]

    def dumped(self, provider: P, mode: str) -> dict[str, object]:
        """``provider`` as a table that ``validated`` reads back: its name, then the parameters that were set."""
        return {'provider': self.name_of(provider), **provider.model_dump(mode=mode, exclude_unset=True)}

    def model_of(self, name: str, where: str = '') -> type[P]:
        """The class of the provider called ``name``. Raises ``ValueError``, after ``where``, when there is none."""
        try:
            return self.get(name).model
        except MissingProvider as e:
            raise ValueError(f'{where}{e}') from e

    def validated_table(self, value: object, scope: Scope) -> Instances[P]:
        """Validate ``[plugins.<name>]`` of the file of ``scope``: ``use`` when the file holds it, and the instances.

        Raises:
            ValidationError: ``use`` or an instance is refused.
            ValueError: ``value`` is not a table, or sets what the file of ``scope`` does not hold.
        """
        if isinstance(value, Instances):
            return value
        if not isinstance(value, Mapping):
            raise ValueError('a table of tables, one for each instance by its name')
        many, tables = self._many(), {str(k): v for k, v in value.items() if k != USE}
        if many.scope is not scope and tables:
            raise ValueError(f'the instances are in the other file; this one may set only {USE}: {", ".join(tables)}')
        here = many.use is not None and many.use.scope is scope
        if USE in value and not here:
            raise ValueError(f'{USE} of the {self.noun}s is not set in this file')
        try:
            use = self._used(value.get(USE)) if here else ()
        except ValidationError as e:
            errors = [_at((USE, *err['loc']), err) for err in e.errors()]
            raise ValidationError.from_exception_data(self.name, errors) from e
        except ValueError as e:
            error = InitErrorDetails(type='value_error', loc=(USE,), input=value.get(USE), ctx={'error': e})
            raise ValidationError.from_exception_data(self.name, [error]) from e
        return Instances(self, use, self.validated_instances(tables) if many.scope is scope else {})

    def _used(self, value: object) -> tuple[str, ...]:
        """``use`` as the names it picks, else the providers that are ``DEFAULT``.

        Raises:
            ValidationError: ``use`` is not one name, or not a list of at least one (a JSON list, from a variable).
            ContractBroken: Unset, with ``ONE``, but not one installed provider is ``DEFAULT``.
        """
        many = self._many()
        assert many.use is not None  # validated_table asks only a capability that declares it
        defaults = [name for name, p in self.providers().items() if is_default(p.model)]
        match many.use.pick:
            case Pick.ONE if value is None:
                if len(defaults) != 1:
                    raise ContractBroken(
                        f'one installed {self.noun} is DEFAULT, not {len(defaults)}: {", ".join(defaults) or "none"}'
                    )
                return tuple(defaults)
            case Pick.ONE:
                name = _ONE.validate_python(value)
                if many.use.refuses is not None and (why := many.use.refuses(name)) is not None:
                    raise ValueError(why)
                return (name,)
            case Pick.ORDERED if value is None:
                return tuple(defaults)
            case _:  # ORDERED, set
                return tuple(
                    _ORDERED.validate_json(value) if isinstance(value, str) else _ORDERED.validate_python(value)
                )

    def validated_instances(self, value: object) -> dict[str, P]:
        """Validate the tables of ``[plugins.<name>.<instance>]``. A refused one is an error at its key.

        Each provider that needs no keys is an instance of its own name too (``instances``).

        Raises:
            ValidationError: A table is refused: the error is at ``<instance>.<field>``.
            ValueError: ``value`` is not a table of tables.
        """
        if not isinstance(value, Mapping):
            raise ValueError('a table of tables, one for each instance by its name')
        providers = self.providers()
        implied = [name for name, p in providers.items() if name not in value and not _needs_keys(p.model)]
        found: dict[str, P] = {}
        errors: list[InitErrorDetails] = []
        named = [n for n in providers if n in value or n in implied]
        for name in [*named, *(str(n) for n in value if n not in providers)]:
            table = value.get(name, {})
            try:
                found[name] = self._instance(name, table)
            except ValidationError as e:
                errors += [_at((name, *err['loc']), err) for err in e.errors()]
            except ValueError as e:
                errors.append(InitErrorDetails(type='value_error', loc=(name,), input=table, ctx={'error': e}))
        if errors:
            raise ValidationError.from_exception_data(self.name, errors)
        return found

    def _instance(self, name: str, table: object) -> P:
        """The table ``name`` as the provider that its ``provider`` names, else the provider ``name``, else ``new``'s.

        Raises:
            ValidationError: The provider refuses its parameters.
            ValueError: The table is not a table, or no provider is named and ``new`` gives no class.
        """
        if isinstance(table, self.contract):
            return table
        if not isinstance(table, Mapping):
            raise ValueError(f'a {self.noun} is a table')
        if 'provider' in table:
            return self.validated(table)
        if name in self.providers():
            return self.model_of(name).model_validate(table)
        match self.table:
            case Many(new=new) if new is not None:
                return new(name, table).model_validate(table)
            case _:
                raise ValueError(
                    f'no {self.noun} has that name: add provider = "<{self.noun}>"; registered {self.noun}s:'
                    f' {", ".join(self.providers())}'
                )

    def dumped_instance(self, name: str, instance: P, mode: str) -> dict[str, object]:
        """The instance ``name`` as ``validated_instances`` reads it back.

        It has ``provider`` only when that is not ``name``, and never for a class of ``new``.
        """
        own = instance.model_dump(mode=mode, exclude_unset=True)
        provider = next((n for n, p in self.providers().items() if p.model is type(instance)), None)
        return own if provider in (None, name) else {'provider': provider, **own}

    def activate(self, stack: ExitStack) -> None:
        """Load and check every provider, then register them all, with their unregistration on ``stack``.

        Raises:
            PluginError: An entry point does not load, is not a provider, or reuses a name. Nothing is registered.
        """
        self.activating = True
        try:
            found = self._load()
        finally:
            self.activating = False
        self._providers = found
        stack.callback(self._deactivate)

    def _load(self) -> dict[str, Provider[P]]:
        found = _entries(self.group, self.noun)
        return {name: Provider(name, self._provider(ep, _loaded(ep)), _source(ep)) for name, ep in found.items()}

    def _provider(self, ep: EntryPoint, loaded: object) -> type[P]:
        """The provider that ``ep`` names: a concrete subclass of the contract."""
        contract = f'{self.contract.__module__}.{self.contract.__qualname__}'
        if not isinstance(loaded, type) or not issubclass(loaded, self.contract):
            raise PluginError(f'{_where(ep)} is not a subclass of {contract}')
        if inspect.isabstract(loaded):
            missing = ', '.join(sorted(getattr(loaded, '__abstractmethods__', ())))
            raise PluginError(f'{_where(ep)} does not implement {contract}: it has no {missing}')
        return loaded

    def _deactivate(self) -> None:
        self._providers = None


def _entries(group: str, noun: str) -> dict[str, EntryPoint]:
    """The entry points of ``group`` by name: piloop's own first, then the others in their order.

    Each distribution's name is read once: it parses its metadata, and the CLI reads the commands on every start.

    Raises:
        PluginError: Two entry points have one name.
    """
    found: dict[str, EntryPoint] = {}
    for ep in entry_points(group=group):
        if (other := found.get(ep.name)) is not None:
            raise PluginError(f'{_where(ep)}: {ep.name!r} is a {noun} already, from {_source(other)}')
        found[ep.name] = ep
    dists = {id(ep.dist): ep.dist for ep in found.values() if ep.dist is not None}
    ours = {key for key, dist in dists.items() if dist.name == BUILT_IN}
    return dict(sorted(found.items(), key=lambda item: id(item[1].dist) not in ours))


class Slot[T]:
    """An entry point group that a package declares into, which piloop reads before it opens any provider.

    piloop's own entries are in it too, as its providers are in their groups. An entry's name is the name of what it
    declares. The CLI's commands are a slot: their paths are read without importing anything.
    """

    def __init__(
        self, name: str, kind: TypeAdapter[T], noun: str, refuses: Callable[[str, T], str | None] | None = None
    ) -> None:
        """Declare a slot: its entry point group is ``piloop.<name>``, and each entry names a ``kind``.

        ``refuses`` says why an entry, by its name and what it names, cannot be, or None when it can.
        """
        self.group = f'piloop.{name}'
        self.kind = kind
        self.noun = noun
        self.refuses = refuses

    def paths(self) -> dict[str, str]:
        """Each entry's ``<module>:<attribute>`` by name, in order, with nothing imported.

        Raises:
            PluginError: Two entry points have one name.
        """
        return {name: ep.value for name, ep in _entries(self.group, self.noun).items()}

    def sources(self) -> dict[str, PythonSource]:
        """The package of each entry by name, in order, with nothing imported.

        Raises:
            PluginError: Two entry points have one name.
        """
        return {name: _source(ep) for name, ep in _entries(self.group, self.noun).items()}

    def loaded(self) -> dict[str, T]:
        """What each entry names by name, in order, imported and checked.

        Raises:
            PluginError: Two entry points have one name, or one does not load, names no ``kind``, or is refused.
        """
        found: dict[str, T] = {}
        for name, ep in _entries(self.group, self.noun).items():
            try:
                found[name] = self.kind.validate_python(_loaded(ep), strict=True)
            except ValidationError as e:
                raise PluginError(f'{_where(ep)} is not a {self.noun}: {e.errors()[0]["msg"]}') from e
            if self.refuses is not None and (why := self.refuses(name, found[name])) is not None:
                raise PluginError(f'{_where(ep)}: {why}')
        return found


type Plugins = dict[str, Parameters | Instances[Parameters]]
"""A file's ``[plugins]``, validated, by capability: the instance of a ``One``, or the ``Instances`` of a ``Many``.
``Capability.selected``, ``Capability.instances`` and ``Capability.used`` read it."""

_ONE: TypeAdapter[str] = TypeAdapter(Annotated[str, Field(min_length=1)])
_ORDERED: TypeAdapter[list[str]] = TypeAdapter(Annotated[list[str], Field(min_length=1)])


@dataclass(frozen=True)
class PluginTables:
    """``Annotated[Plugins, PluginTables(scope)]``: ``[plugins]`` of the file of ``scope``.

    Each ``[plugins.<name>]`` is validated by the capability of ``scope`` that its key names: as its one instance, the
    provider that its ``provider`` key names (``One``), or as its instances by name (``Many``). Each is dumped back as it
    reads. A ``One`` without a table has its default (``Capability.selected``), and a ``Many`` without one has the
    providers that need no keys. The capabilities are looked up when a file validates, after every plugin can load.
    """

    scope: Scope

    def __get_pydantic_core_schema__(self, source: object, handler: GetCoreSchemaHandler) -> CoreSchema:
        return core_schema.no_info_plain_validator_function(
            self.validate,
            serialization=core_schema.plain_serializer_function_ser_schema(self.dump, info_arg=True),
        )

    def __get_pydantic_json_schema__(self, schema: CoreSchema, handler: GetJsonSchemaHandler) -> JsonSchemaValue:
        return {'type': 'object', 'additionalProperties': {'type': 'object'}}

    def _capabilities(self) -> dict[str, Capability[Parameters]]:
        """The capabilities that have their instances in the file, or their ``use``, by name."""
        return {c.name: c for c in declared() if self._here(c)}

    def _here(self, capability: Capability[Parameters]) -> bool:
        match capability.table:
            case Many(scope=scope, use=Use(scope=used)):
                return self.scope in (scope, used)
            case One(scope=scope) | Many(scope=scope):
                return scope is self.scope
            case _:  # None: no table holds it
                return False

    def validate(self, value: object) -> Plugins:
        """Validate each table. A refused table is an error at its key: ``plugins.<name>.<field>``."""
        if not isinstance(value, Mapping):
            raise ValueError('[plugins] is a table of tables')
        known = self._capabilities()
        found: Plugins = {}
        errors: list[InitErrorDetails] = []
        many = [c.name for c in known.values() if isinstance(c.table, Many) and c.name not in value]
        for name, table in [*value.items(), *((name, {}) for name in many)]:
            try:
                found[str(name)] = self._table(known, str(name), table)
            except ValidationError as e:
                errors += [_at((name, *err['loc']), err) for err in e.errors()]
            except ValueError as e:
                errors.append(InitErrorDetails(type='value_error', loc=(name,), input=table, ctx={'error': e}))
        if errors:
            raise ValidationError.from_exception_data('plugins', errors)
        return found

    def _table(
        self, known: Mapping[str, Capability[Parameters]], name: str, table: object
    ) -> Parameters | Instances[Parameters]:
        match capability := known.get(name):
            case None:
                raise ValueError(f'no capability has that name; there is {", ".join(sorted(known)) or "none"}')
            case Capability(table=Many()):
                return capability.validated_table(table, self.scope)
            case _:
                if named := capability.named_tables(table):
                    raise ValueError(
                        f'a {capability.noun} has one instance, not one for each name ({", ".join(named)}): write its'
                        f' keys in [plugins.{name}], with provider = "<{capability.noun}>" to choose one'
                    )
                return capability.validated(table)

    def dump(self, plugins: Plugins, info: core_schema.SerializationInfo) -> dict[str, dict[str, object]]:
        known = self._capabilities()
        return {name: self._dumped(known[name], found, info.mode) for name, found in plugins.items()}

    @staticmethod
    def _dumped(
        capability: Capability[Parameters], found: Parameters | Instances[Parameters], mode: str
    ) -> dict[str, object]:
        match found:
            case Instances(use=use, instances=instances):
                picked: dict[str, object] = {}
                match capability.table:
                    case Many(use=Use(pick=Pick.ONE)) if use:
                        picked[USE] = use[0]
                    case Many(use=Use(pick=Pick.ORDERED)) if use:
                        picked[USE] = list(use)
                    case _:
                        pass
                return picked | {name: capability.dumped_instance(name, p, mode) for name, p in instances.items()}
            case _:
                return capability.dumped(found, mode)


@dataclass(frozen=True)
class Entry[P: Parameters]:
    """``Annotated[P, CAP.entry]``: a table ``{ provider = "<name>", <its parameters> }``, as the provider it names.

    The table is validated as a ``[plugins.<name>]`` table is (``Capability.validated``: without ``provider``, the
    default), and dumped back with its ``provider``. ``P`` may be narrower than the contract: a provider that is not a
    ``P`` is refused. A trigger of a role, the remote of the ``cached`` CAS and the vcs provider of a job are entries.
    """

    capability: Capability[P]

    def __get_pydantic_core_schema__(self, source: type[object], handler: GetCoreSchemaHandler) -> CoreSchema:
        def validate(value: object) -> object:
            found = self.capability.validated(value)
            if not isinstance(found, source):
                raise ValueError(
                    f'{self.capability.name_of(found)} cannot be here: this key takes a {source.__qualname__}'
                )
            return found

        return core_schema.no_info_plain_validator_function(
            validate, serialization=core_schema.plain_serializer_function_ser_schema(self.dump, info_arg=True)
        )

    def __get_pydantic_json_schema__(self, schema: CoreSchema, handler: GetJsonSchemaHandler) -> JsonSchemaValue:
        return {'type': 'object'}

    def dump(self, provider: P, info: core_schema.SerializationInfo) -> dict[str, object]:
        return self.capability.dumped(provider, info.mode)


@dataclass(frozen=True)
class Ref[P: Parameters]:
    """``Annotated[str, CAP.ref]``: the name of an instance of ``capability``, in the file of its scope.

    It is a marker: ``refs`` finds every reference of a validated model, and the owner of the file checks them against
    the instances (``Capability.instances``), once every table of the file is validated.
    """

    capability: Capability[P]


@dataclass(frozen=True)
class Reference:
    """A name of an instance (``Ref``) in a validated model, and where it is."""

    capability: Capability[Parameters]
    name: str
    at: tuple[str | int, ...]
    """The keys and list indexes from the model to the name, as in a validation error."""


def refs(value: object, annotation: object = None, at: tuple[str | int, ...] = ()) -> list[Reference]:
    """Every name of an instance (``Ref``) in a validated model, in the order of its fields.

    It walks the fields, the items of lists, the keys and values of dicts, and the instances of ``[plugins]``.
    """
    if annotation is None and isinstance(value, BaseModel):
        fields = _referring(type(value))
        return [r for name, field in fields for r in refs(getattr(value, name), field, (*at, name))]
    origin, args = get_origin(annotation), get_args(annotation)
    if origin is Annotated:
        marks = [Reference(m.capability, value, at) for m in args[1:] if isinstance(m, Ref) and isinstance(value, str)]
        return [*marks, *refs(value, args[0], at)]
    if origin in (Union, UnionType):  # the value fits one arm: the first that finds references is it
        return next((found for arg in args if (found := refs(value, arg, at))), [])
    match value:
        case Instances(capability=capability, use=use, instances=instances):
            used = [Reference(capability, name, (*at, USE, n)) for n, name in enumerate(use)]
            return [*used, *(r for name, p in instances.items() for r in refs(p, None, (*at, name)))]
        case BaseModel():
            return refs(value, None, at)
        case list() | tuple():
            item = args[0] if args else None
            return [r for i, v in enumerate(value) for r in refs(v, item, (*at, i))]
        case dict():
            keys, values = (args[0], args[1]) if len(args) == 2 else (None, None)
            return [r for k, v in value.items() for r in (*refs(k, keys, at), *refs(v, values, (*at, str(k))))]
        case _:
            return []


def is_default(model: type) -> bool:
    """Whether the provider class ``model`` itself sets ``DEFAULT = True``: on when nothing is configured.

    A subclass of a default provider, such as a runtime that runs agents as ``subprocess`` does, inherits its code and
    not its place among the defaults.
    """
    return vars(model).get('DEFAULT') is True


INERT = (str, bytes, int, float, bool, type(None), Enum, Path, datetime, timedelta)
"""The types of a value that holds no reference: ``refs`` does not walk a field of only these."""


@cache
def _referring(model: type[BaseModel]) -> tuple[tuple[str, object], ...]:
    """The fields of ``model`` that can hold a reference, with their annotations: those that ``refs`` walks.

    A validation of a campaign's config walks them every time, so the inert fields are left out once per class.
    """
    fields = ((name, info.rebuild_annotation()) for name, info in model.model_fields.items())
    return tuple((name, annotation) for name, annotation in fields if _can_refer(annotation))


def _can_refer(annotation: object) -> bool:
    """Whether a value of ``annotation`` can hold a reference: a ``Ref``, or a type that is not ``INERT``."""
    origin, args = get_origin(annotation), get_args(annotation)
    if origin is Annotated:
        return any(isinstance(m, Ref) for m in args[1:]) or _can_refer(args[0])
    if origin is Literal:
        return False
    if args:
        return any(_can_refer(arg) for arg in args if arg is not Ellipsis)
    return not (isinstance(annotation, type) and issubclass(annotation, INERT))


def _needs_keys(model: type[BaseModel]) -> bool:
    """Whether a provider has a parameter without a default: then only a table makes it an instance."""
    return any(info.is_required() for info in model.model_fields.values())


CORE_ERRORS = frozenset(get_args(ErrorType))
"""The error types that pydantic-core can rebuild from a name."""


def _at(loc: tuple[str | int, ...], error: ErrorDetails) -> InitErrorDetails:
    """``error`` at ``loc``: a provider's error, under the key of its table.

    ``from_exception_data`` rebuilds only pydantic-core's own error types. One of pydantic's, such as
    ``path_not_file``, keeps its message as an ``invalid_value``.
    """
    if error['type'] not in CORE_ERRORS:
        invalid = PydanticCustomError('invalid_value', '{message}', {'message': error['msg']})
        return InitErrorDetails(type=invalid, loc=loc, input=error['input'])
    found = InitErrorDetails(type=error['type'], loc=loc, input=error['input'])
    if 'ctx' in error:
        found['ctx'] = error['ctx']
    return found


type _Method = tuple[Callable[..., object], list[inspect.Parameter]]
"""A method of a contract, and the arguments that a caller passes: after ``self`` or ``cls``, all of a static one."""


def _members(contract: type) -> dict[str, object]:
    """What ``contract`` declares besides its fields, by name: its methods, and anything else that a caller reads."""
    return {n: m for n, m in sorted(vars(contract).items()) if not n.startswith('_') and n != 'model_config'}


def _methods(contract: type) -> dict[str, _Method]:
    """The methods that ``contract`` declares, static and class methods included, by name."""
    found: dict[str, _Method] = {}
    for name, member in _members(contract).items():
        match member:
            case staticmethod(__func__=function):
                found[name] = (function, list(inspect.signature(function).parameters.values()))
            case classmethod(__func__=function):
                found[name] = (function, list(inspect.signature(function).parameters.values())[1:])
            case _ if inspect.isfunction(member):
                found[name] = (member, list(inspect.signature(member).parameters.values())[1:])
    return found


def data_errors(contract: type) -> list[str]:
    """Return why ``contract`` is not data in, data out, or nothing when it is.

    A pure contract declares methods only, as a WIT interface has functions: its fields are its providers' parameters.
    Each argument and return type is data: a type that pydantic can validate and describe as JSON Schema without
    ``arbitrary_types_allowed``. That refuses a session, a file, a callable, a protocol and any class that is not a
    dataclass, a pydantic model or a type that pydantic knows. On top of it, two rules of WIT:

    - no part of any type: ``Any``, ``object``, a model that allows extra fields;
    - no type that contains itself.

    A method returns its error as a value, a case of a union of records, not as an exception.
    """
    methods = _methods(contract)
    others = sorted(set(_members(contract)) - set(methods))
    errors = [f'{n} is not a method: a pure contract has methods only, as a WIT interface' for n in others]
    for name, (function, parameters) in methods.items():
        hints = get_type_hints(function)
        typed: list[tuple[str, str]] = []
        for p in parameters:
            if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
                errors.append(f'{name}({p.name}) takes any number of arguments')
            elif p.name not in hints:
                errors.append(f'{name}({p.name}) has no type')
            else:
                typed.append((f'{name}({p.name}):', p.name))
        if 'return' not in hints:
            errors.append(f'{name} has no return type')
        else:
            typed.append((f'{name} returns', 'return'))
        for label, key in typed:
            try:
                schema = TypeAdapter(hints[key]).json_schema()
            except (PydanticSchemaGenerationError, PydanticInvalidForJsonSchema, PydanticUserError) as e:
                errors.append(f'{label} {hints[key]!r} is not data: {str(e).splitlines()[0]}')
                continue
            if why := _not_wit(schema):
                errors.append(f'{label} {hints[key]!r} {why}')
    return errors


def _not_wit(schema: dict[str, object]) -> str | None:
    """Return why the JSON Schema of a type of data has no WIT type, or None."""
    if _takes_anything(schema):
        return 'has a part of any type (Any, object, or a model that allows extra fields)'
    defs = schema.get('$defs', {})
    bodies = defs.items() if isinstance(defs, dict) else []
    refs = {name: set(re.findall(r'"#/\$defs/([^"]+)"', json.dumps(body))) for name, body in bodies}
    try:
        graphlib.TopologicalSorter(refs).prepare()
    except graphlib.CycleError as e:
        return f'contains itself ({" → ".join(e.args[1])}), and WIT types are not recursive'
    return None


def _takes_anything(schema: object) -> bool:
    """Whether a JSON Schema, or one of the schemas in it, takes any value."""
    if not isinstance(schema, dict):
        return False
    if not schema or schema.get('additionalProperties') is True:
        return True
    nested = [schema.get(k) for k in ('items', 'additionalProperties', 'not')]
    nested += [s for k in ('anyOf', 'oneOf', 'allOf', 'prefixItems') for s in schema.get(k, [])]
    nested += [s for k in ('properties', '$defs') for s in schema.get(k, {}).values()]
    return any(_takes_anything(s) for s in nested)


def _loaded(ep: EntryPoint) -> object:
    """Load what ``ep`` names. Raises ``PluginError``, with the entry point, when it does not load."""
    try:
        return ep.load()
    except PluginError:
        raise
    except Exception as e:
        raise PluginError(f'{_where(ep)} does not load: {type(e).__name__}: {e}') from e


@cache
def declared() -> tuple[Capability[Parameters], ...]:
    """Every capability that an installed package declares in ``CAPABILITIES``, piloop included, by provider group.

    A plugin declares a capability for a seam of its own the same way that piloop declares its own: an entry point
    that names the ``Capability``, whose name is the capability's. Other packages provide it, and piloop lists, checks
    and selects it as any other.

    The installed packages do not change while a process runs, so the entry points are read once, until ``close``.
    Every validation of a ``piloop.toml`` asks for these, and the daemon validates each campaign's on every pass.

    Raises:
        PluginError: An entry point does not load, does not name a capability, names one of another name, or names a
            capability whose provider group another one has.
    """
    found: dict[str, Capability[Parameters]] = {}
    for ep in entry_points(group=CAPABILITIES):
        loaded = _loaded(ep)
        if not isinstance(loaded, Capability):
            raise PluginError(f'{_where(ep)} is not a piloop.plugins.host.Capability')
        if loaded.name != ep.name:
            raise PluginError(f'{_where(ep)} names the capability {loaded.name!r}: the names must be the same')
        if (other := found.get(loaded.group)) is not None and other is not loaded:
            raise PluginError(f'{_where(ep)}: the {other.noun}s have the provider group {loaded.group} already')
        found[loaded.group] = loaded
    return tuple(sorted(found.values(), key=lambda c: c.group))


def compose[P: Parameters](*capabilities: Capability[P]) -> None:
    """Activate the capabilities that are not active yet, in order.

    Raises:
        PluginError: One does not activate, or one is needed while it activates: the capabilities that this call
            activated are unregistered again, newest first.
    """
    with _lock, ExitStack() as stack:
        for capability in capabilities:
            _activate(capability, stack)
        _process.enter_context(stack.pop_all())


def _activate[P: Parameters](capability: Capability[P], stack: ExitStack) -> None:
    if capability.active:
        return
    if capability.activating:
        raise PluginError(
            f'the {capability.noun}s are needed while they load: an entry point of {capability.group} uses its own'
            ' capability, or one that uses it, when it is imported'
        )
    capability.activate(stack)


def close() -> None:
    """Unregister every capability of the process, newest first, and forget ``declared``. Closing twice is harmless."""
    with _lock:
        _process.close()
        declared.cache_clear()
