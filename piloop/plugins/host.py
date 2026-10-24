"""The plugin host: the one way that piloop finds, checks, activates, shows and tears down what plugins provide.

Terms:

- A **capability** is a public interface with a registry of named providers: the executors, the verdict rules, the
  runtimes. It declares its contract, a class, and where the configuration chooses a provider: a ``[plugins.<name>]``
  table (``Choice``), or a field that names one (``Entry``, ``Named``, ``Tables``).
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
- The OpenTelemetry Collector: a component's configuration, validated with its defaults, makes the live component.
- The WebAssembly component model: pure contracts of data only, and errors as values.
"""

import graphlib
import inspect
import json
import re
from collections.abc import Callable, Mapping
from contextlib import ExitStack
from dataclasses import dataclass
from enum import StrEnum
from functools import cache
from importlib.metadata import EntryPoint, entry_points
from threading import RLock
from typing import get_args, get_type_hints

from pydantic import (
    BaseModel,
    ConfigDict,
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


@dataclass(frozen=True)
class Check:
    """One line of the report of ``piloop doctor``: what was checked, what was found and, for a failure, the fix.

    A provider that checks the machine it works on returns these (``Vcs.checks``).
    """

    name: str
    found: str
    fix: str | None = None
    """None when the check passed."""


class Live:
    """Marks a parameter of a provider that ``piloop limits set`` may change while the daemon runs.

    ``agents: Annotated[int | None, Live] = None``. The daemon hands the provider its parameters with the limits
    applied on its next tick (``piloop.plugins.limits``).
    """


@dataclass(frozen=True)
class Choice:
    """``[plugins.<name>]`` of the file of ``scope`` chooses the capability's provider, and gives its parameters."""

    scope: Scope
    default: str | None = None
    """The provider when the file has no table. None: then no provider is chosen."""


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
        choice: Choice | None = None,
        group: str | None = None,
        noun: str | None = None,
    ) -> None:
        """Declare a capability.

        Args:
            name: Its entry point in ``piloop.capabilities``, and the key of its ``[plugins.<name>]`` table.
            contract: The class that a provider subclasses: its fields are the provider's parameters. A pure contract
                takes and returns data only (``data_errors``).
            tier: What the contract lets cross to a provider.
            choice: The ``[plugins.<name>]`` table that chooses the provider. Without it, the fields of the configuration
                that name a provider say where it is used (``entry``, ``named``, ``tables``).
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
        self.choice = choice
        self.group = group or f'piloop.{name}s'
        self.entry = Entry(self)
        """``Annotated[P, CAP.entry]``: a table ``{ provider = "<name>", <its parameters> }``."""
        self.named = Named(self)
        """``Annotated[str, CAP.named]``: the name of a provider."""
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

    def selected(self, plugins: Mapping[str, Parameters]) -> P:
        """The provider that ``[plugins.<name>]`` chooses, with its parameters, else the default with its defaults.

        ``plugins`` is a file's ``[plugins]``, validated (``PluginTables``).

        Raises:
            MissingProvider: The file chooses no provider, and the capability has no default.
        """
        if isinstance(found := plugins.get(self.name), self.contract):
            return found
        if self.choice is None or self.choice.default is None:
            raise MissingProvider(f'no [plugins.{self.name}] chooses a {self.noun}, and there is no default')
        return self.get(self.choice.default).model()

    def validated(self, table: object) -> P:
        """Validate a table as the provider that its ``provider`` key names, else the default.

        Raises:
            ValidationError: The provider refuses its parameters.
            ValueError: The table is not a table, or names no provider that there is.
        """
        if not isinstance(table, Mapping):
            raise ValueError('not a table')
        keys = {str(k): v for k, v in table.items() if k != 'provider'}
        name = table.get('provider', self.choice.default if self.choice is not None else None)
        if name is None:
            raise ValueError(f'provider: missing; registered {self.noun}s: {", ".join(self.providers())}')
        return self.model_of(str(name), f'provider = {name!r}: ').model_validate(keys)

    def dumped(self, provider: P, mode: str) -> dict[str, object]:
        """``provider`` as a table that ``validated`` reads back: its name, then the parameters that were set."""
        return {'provider': self.name_of(provider), **provider.model_dump(mode=mode, exclude_unset=True)}

    def model_of(self, name: str, where: str = '') -> type[P]:
        """The class of the provider called ``name``. Raises ``ValueError``, after ``where``, when there is none."""
        try:
            return self.get(name).model
        except MissingProvider as e:
            raise ValueError(f'{where}{e}') from e

    def tables(self) -> Tables[P]:
        """``Annotated[dict[str, P], CAP.tables()]``: one table for each provider, by its name, over its defaults.

        Every provider is in the result, in registry order. A table with a name that no provider has is refused.
        """
        return Tables(self)

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
        found: dict[str, Provider[P]] = {}
        for ep in entry_points(group=self.group):
            if (other := found.get(ep.name)) is not None:
                raise PluginError(f'{_where(ep)}: {ep.name!r} is a {self.noun} already, from {other.source}')
            found[ep.name] = Provider(ep.name, self._provider(ep, _loaded(ep)), _source(ep))
        return dict(sorted(found.items(), key=lambda item: not item[1].source.built_in))

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


type Plugins = dict[str, Parameters]
"""A file's ``[plugins]``, validated: the provider that each table chooses, with its parameters, by capability."""


@dataclass(frozen=True)
class Chosen[P: Parameters]:
    """``Annotated[P, Chosen(capability)]``: a provider of ``capability`` with its parameters, as its table.

    The table is a ``[plugins.<name>]`` table: its ``provider`` key and its parameters. Data that a file's choice reaches
    carries it so, as a job carries the campaign's vcs provider to its worker (``Source.vcs``).
    """

    capability: Capability[P]

    def __get_pydantic_core_schema__(self, source: object, handler: GetCoreSchemaHandler) -> CoreSchema:
        return core_schema.no_info_plain_validator_function(
            self.validate,
            serialization=core_schema.plain_serializer_function_ser_schema(self.dump, info_arg=True),
        )

    def validate(self, value: object) -> P:
        """The provider that the table's ``provider`` key names, with its parameters (``Capability.validated``)."""
        if isinstance(value, self.capability.contract):
            return value
        found = self.capability.validated(value)
        assert isinstance(found, self.capability.contract)  # it validates as the provider that the table names
        return found

    def dump(self, provider: P, info: core_schema.SerializationInfo) -> dict[str, object]:
        assert isinstance(provider, Parameters)  # a chosen contract subclasses it (Capability.__init__)
        return {
            'provider': self.capability.name_of(provider),
            **provider.model_dump(mode=info.mode, exclude_unset=True),
        }


@dataclass(frozen=True)
class PluginTables:
    """``Annotated[Plugins, PluginTables(scope)]``: ``[plugins]`` of the file of ``scope``.

    Each ``[plugins.<name>]`` is validated by the capability of ``scope`` that its key names, into the provider that its
    ``provider`` key names, and dumped back with that key. A capability without a table has its default
    (``Capability.selected``). The capabilities are looked up when a file validates, after every plugin can load.
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
        """The capabilities that a table of the file chooses, by name."""
        return {c.name: c for c in declared() if c.choice is not None and c.choice.scope is self.scope}

    def validate(self, value: object) -> Plugins:
        """Validate each table. A refused table is an error at its key: ``plugins.<name>.<field>``."""
        if not isinstance(value, Mapping):
            raise ValueError('[plugins] is a table of tables')
        known = self._capabilities()
        found: Plugins = {}
        errors: list[InitErrorDetails] = []
        for name, table in value.items():
            try:
                found[str(name)] = self._table(known, str(name), table)
            except ValidationError as e:
                errors += [_at((name, *err['loc']), err) for err in e.errors()]
            except ValueError as e:
                errors.append(InitErrorDetails(type='value_error', loc=(name,), input=table, ctx={'error': e}))
        if errors:
            raise ValidationError.from_exception_data('plugins', errors)
        return found

    @staticmethod
    def _table(known: Mapping[str, Capability[Parameters]], name: str, table: object) -> Parameters:
        if (capability := known.get(name)) is None:
            raise ValueError(f'no capability has that name; there is {", ".join(sorted(known)) or "none"}')
        found = Chosen(capability).validate(table)
        assert isinstance(found, Parameters)  # a chosen contract subclasses it (Capability.__init__)
        return found

    def dump(self, plugins: Plugins, info: core_schema.SerializationInfo) -> dict[str, dict[str, object]]:
        known = self._capabilities()
        return {name: Chosen(known[name]).dump(provider, info) for name, provider in plugins.items()}


@dataclass(frozen=True)
class Entry[P: Parameters]:
    """``Annotated[P, CAP.entry]``: a table ``{ provider = "<name>", <its parameters> }``, as the provider it names.

    The table is validated as a ``[plugins.<name>]`` table is (``Capability.validated``: without ``provider``, the
    default), and dumped back with its ``provider``. ``P`` may be narrower than the contract: a provider that is not a
    ``P`` is refused. A trigger of a role, and the remote of the ``cached`` CAS, are entries.
    """

    capability: Capability[P]

    def __get_pydantic_core_schema__(self, source: type[object], handler: GetCoreSchemaHandler) -> CoreSchema:
        def validate(value: object) -> object:
            found = value if isinstance(value, self.capability.contract) else self.capability.validated(value)
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
class Named[P: Parameters]:
    """``Annotated[str, CAP.named]``: the name of a provider of ``capability``."""

    capability: Capability[P]

    def __get_pydantic_core_schema__(self, source: type[object], handler: GetCoreSchemaHandler) -> CoreSchema:
        return core_schema.no_info_after_validator_function(self.validate, handler(source))

    def validate(self, name: str) -> str:
        """Raises ``MissingProvider``, a ``ValueError``, when no provider has that name."""
        self.capability.get(name)
        return name


@dataclass(frozen=True)
class Tables[P: Parameters]:
    """``Annotated[dict[str, P], CAP.tables()]``: a table for each provider, by its name, over the provider's defaults.

    Every provider is in the result, in registry order. A name that no provider has is refused at its key. The
    runtimes of ``config.toml`` are tables (``[runtimes.<name>]``).
    """

    capability: Capability[P]

    def __get_pydantic_core_schema__(self, source: object, handler: GetCoreSchemaHandler) -> CoreSchema:
        return core_schema.no_info_plain_validator_function(
            self.validate,
            serialization=core_schema.plain_serializer_function_ser_schema(self.dump, info_arg=True),
        )

    def validate(self, value: object) -> dict[str, P]:
        """Validate each table. A refused one is an error at its key: ``<name>.<field>``."""
        if not isinstance(value, Mapping):
            raise ValueError('a table of tables')
        providers, noun = self.capability.providers(), self.capability.noun
        unknown = ValueError(f'no {noun} has that name; registered {noun}s: {", ".join(providers)}')
        errors = [
            InitErrorDetails(type='value_error', loc=(str(name),), input=table, ctx={'error': unknown})
            for name, table in value.items()
            if name not in providers
        ]
        found: dict[str, P] = {}
        for name in providers:
            model, table = self.capability.model_of(name), value.get(name, {})
            try:
                found[name] = table if isinstance(table, model) else model.model_validate(table)
            except ValidationError as e:
                errors += [_at((name, *err['loc']), err) for err in e.errors()]
        if errors:
            raise ValidationError.from_exception_data(self.capability.name, errors)
        return found

    def dump(self, tables: Mapping[str, P], info: core_schema.SerializationInfo) -> dict[str, object]:
        return {name: p.model_dump(mode=info.mode, exclude_unset=True) for name, p in tables.items()}


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
    Every validation of a ``piloop.toml`` asks for these, and the daemon validates each campaign's on every tick.

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
