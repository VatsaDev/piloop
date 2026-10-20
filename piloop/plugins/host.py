"""The plugin host: the one way that piloop finds, checks, activates, shows and tears down what plugins provide.

Terms:

- A **capability** is a public interface with a registry of named providers: the runtimes, the remote backends of the
  store, the triggers, the roles. It names its entry point group, the check of what an entry point names, and where
  the configuration selects a provider (``Selection``).
- A **provider** is one entry of a registry. Configuration picks a provider by its name.
- A **plugin** is where providers come from: today, an installed Python package and its entry points. piloop's own
  providers are entry points of the ``piloop`` package, in the same groups. A provider's ``Source`` says which one,
  and its version.
- A **tier** is what the contract of a capability lets cross to a provider (``Tier``): live objects of the host, or
  data only (``PureCapability``). Everything else is the same for both tiers.

Lifecycle:

- A capability activates on first use (``Capability.providers``), or with others in ``compose``. It loads every
  provider, checks each one, and registers all of them or none: no consumer sees part of a registry.
- A provider that looks another capability up while it loads activates that one first.
- When an activation fails, ``compose`` unregisters what it activated, newest first, and raises a ``PluginError`` that
  names the entry point. ``close`` unregisters everything, newest first; the next use activates again.
- The registrations are owned effects on a ``contextlib.ExitStack``. A provider that holds a resource puts its cleanup
  on the same stack when a capability first needs one.

Loaders: the only one is ``_python_entry_points``, the entry points of the installed packages, in this process. A
loader yields ``Offer``s and knows nothing of the contracts, so a later loader (a Wasm component named by its hash, a
program in another process) serves the same capabilities. A pure contract describes its types as JSON Schema
(``data_errors``), which a host in another language can read too.

Inspiration:

- pluggy: providers found through entry points, each with the distribution that declares it; an implementation checked
  against its specification, by the names of its arguments, when it is registered.
- Cordis (DeepSeek Harness): registrations as owned, reversible effects; a failed activation unwinds what it did.
- The WebAssembly component model and Extism: pure contracts of data only, and errors as values.
"""

import graphlib
import inspect
import json
import re
import typing
from collections.abc import Callable, Iterator
from contextlib import ExitStack
from dataclasses import dataclass
from enum import StrEnum
from importlib.metadata import EntryPoint, entry_points
from threading import RLock
from typing import Literal, Protocol, TypeIs, get_type_hints

from pydantic import BaseModel, PydanticSchemaGenerationError, PydanticUserError, TypeAdapter
from pydantic.errors import PydanticInvalidForJsonSchema

BUILT_IN = 'piloop'
"""The package of the built-in providers: they are entry points of piloop's own ``pyproject.toml``."""


class Tier(StrEnum):
    """What crosses the boundary between piloop and the providers of a capability. Nothing else differs."""

    HOST = 'host'
    """The contract may pass live objects of the host (processes, files, sockets, a session): a runtime, a storage
    backend, a transport."""
    PURE = 'pure'
    """The contract is data in, data out (``data_errors``): a planner, a policy, an evaluator kind. Its providers can
    move out of process, or to a Wasm component, behind the same contract."""


class PluginError(Exception):
    """The installed providers do not compose.

    An entry point does not load, names an object of the wrong type, reuses a name, or needs its own capability while
    it loads. The message names the entry point.
    """


class MissingProvider(ValueError):
    """A capability has no provider of that name. A ``ValueError``, so that a config validator reports it on its key."""


class Source(Protocol):
    """Where a provider comes from.

    A Python package is one kind (``PythonSource``). A Wasm component named by its content hash, or a program in
    another process, would be others.
    """

    @property
    def built_in(self) -> bool:
        """Whether piloop itself provides it."""
        ...

    @property
    def release(self) -> str:
        """What it is and which version, for a person: ``piloop-nice 0.1.0``."""
        ...


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
class Selection:
    """Where the configuration names the provider of a capability.

    A capability without one is a registry that many entries use by name, as the triggers of the roles do. A new
    capability uses ``[plugins.<name>]`` in ``piloop.toml``: ``Selection('project', 'plugins.<name>.provider')``.
    """

    scope: Literal['project', 'home']
    """``project``: the key is in ``piloop.toml``. ``home``: in ``$PILOOP_HOME/config.toml``."""
    key: str
    """The dotted key whose value is the provider's name: ``runtime``, ``storage.remote.kind``."""
    per_role: bool = False
    """A role's own table can set the key too, for its agents: ``[roles.<role>] runtime``."""
    settings: str | None = None
    """The table of ``config.toml`` with a table of settings for each provider, by its name: ``runtimes``."""

    @property
    def table(self) -> str | None:
        """The table that holds the key, whose other keys are the parameters of the provider: ``storage.remote``."""
        return self.key.rpartition('.')[0] or None


@dataclass(frozen=True)
class Provider[P]:
    """One entry of a capability's registry."""

    name: str
    value: P
    source: Source


class Activatable(Protocol):
    """What the host needs of a capability, whatever the type of its providers."""

    noun: str
    """What one provider is, for messages."""
    tier: Tier
    group: str
    """The entry point group of its providers."""
    selection: Selection | None
    activating: bool
    """Whether its providers are being loaded now."""

    @property
    def active(self) -> bool:
        """Whether its providers are registered."""
        ...

    def activate(self, stack: ExitStack) -> None:
        """Load, check and register every provider, with their unregistration on ``stack``."""
        ...

    def sources(self) -> dict[str, Source]:
        """Where each provider comes from, by name."""
        ...

    def get(self, name: str) -> object:
        """The provider called ``name``."""
        ...


@dataclass(frozen=True)
class Offer:
    """A provider that a loader found, before its capability checks it."""

    name: str
    where: str
    """The provider's declaration, for errors: ``entry point <name> = <value> in <group>``."""
    source: Source
    load: Callable[[], object]
    """Return the object that implements the contract. A loader for another process returns a proxy, which only a
    pure contract allows."""


def _python_entry_points(group: str) -> Iterator[Offer]:
    """The loader of Python entry points: each provider is the object that its entry point names, in this process."""
    for ep in entry_points(group=group):
        yield Offer(ep.name, f'entry point {ep.name} = {ep.value} in {group}', _source(ep), ep.load)


def _source(ep: EntryPoint) -> PythonSource:
    spec = f'{ep.name} = {ep.value}'
    return PythonSource(None, None, spec) if ep.dist is None else PythonSource(ep.dist.name, ep.dist.version, spec)


_lock = RLock()
"""Held while capabilities activate or close: the daemon's threads look providers up."""
_process = ExitStack()
"""The registrations of every capability of the process, in the order that they activated."""
CAPABILITIES = 'piloop.capabilities'
"""The entry point group that declares the capabilities: piloop's own, and a plugin's. Each names a ``Capability``."""


class Capability[P]:
    """A public interface, and the registry of its providers by name. Its contract is of the host tier."""

    tier: Tier = Tier.HOST

    def __init__(
        self, noun: str, *, group: str, check: Callable[[str, object], P], selection: Selection | None = None
    ) -> None:
        """Declare a capability.

        Args:
            noun: What one provider is, for messages: ``runtime``.
            group: The entry point group of its providers, piloop's own included. Its contract is one version of the
                interface: an incompatible interface takes a new group.
            check: Return the provider that an entry point called ``name`` loaded, or raise ``TypeError`` with what
                it is not.
            selection: Where the configuration names the provider.
        """
        self.noun = noun
        self.group = group
        self.selection = selection
        self._check = check
        self._providers: dict[str, Provider[P]] | None = None
        self.activating = False

    @property
    def active(self) -> bool:
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

    def get(self, name: str) -> P:
        """Return the provider called ``name``.

        Raises:
            MissingProvider: No provider has that name. The message lists the ones that there are.
        """
        found = self.providers().get(name)
        if found is None:
            raise MissingProvider(
                f'unknown {self.noun} {name!r}; registered {self.noun}s: {", ".join(self.providers())}'
            )
        return found.value

    def sources(self) -> dict[str, Source]:
        """Where each provider comes from, by name."""
        return {name: provider.source for name, provider in self.providers().items()}

    def builtins(self) -> dict[str, P]:
        """The providers of piloop itself, by name: what the generated docs describe."""
        return {name: p.value for name, p in self.providers().items() if p.source.built_in}

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
        for offer in _python_entry_points(self.group):
            if (other := found.get(offer.name)) is not None:
                raise PluginError(f'{offer.where}: {offer.name!r} is a {self.noun} already, from {other.source}')
            loaded = _loaded(offer)
            try:
                value = self._check(offer.name, loaded)
            except TypeError as e:
                raise PluginError(f'{offer.where} {e}') from e
            found[offer.name] = Provider(offer.name, value, offer.source)
        return dict(sorted(found.items(), key=lambda item: not item[1].source.built_in))

    def _deactivate(self) -> None:
        self._providers = None


class PureCapability[P](Capability[P]):
    """A capability of the pure tier: each provider is an object with the methods of ``contract``, data in, data out."""

    tier: Tier = Tier.PURE

    def __init__(self, noun: str, *, group: str, contract: type[P], selection: Selection | None = None) -> None:
        """Declare a pure capability.

        Args:
            noun: What one provider is, for messages.
            group: The entry point group of the providers that packages add.
            contract: The protocol that a provider implements (``data_errors`` says what it may use). A provider is
                checked by its methods and the names of their arguments, not by its class, so a proxy can implement
                it.
            selection: Where the configuration names the provider: for a new capability, ``plugins.<name>.provider``.

        Raises:
            TypeError: The contract is not data in, data out.
        """
        if errors := data_errors(contract):
            raise TypeError(f'{contract.__qualname__} is not a pure contract: {"; ".join(errors)}')
        methods = {name: [p.name for p in parameters] for name, (_, parameters) in _methods(contract).items()}

        def check(name: str, loaded: object) -> P:
            if isinstance(loaded, type):
                raise TypeError(
                    f'names the class {loaded.__qualname__}: the entry point of a pure provider names an instance'
                )
            if not _implements(loaded, contract, list(methods)):
                raise TypeError(f'does not implement {contract.__qualname__}: it needs {", ".join(methods)}')
            for method, arguments in methods.items():
                try:
                    inspect.signature(getattr(loaded, method)).bind(**dict.fromkeys(arguments))
                except TypeError as e:
                    raise TypeError(
                        f'does not implement {contract.__qualname__}: {method} does not take the arguments of'
                        f' {contract.__qualname__}.{method} ({", ".join(arguments)}): {e}'
                    ) from e
            return loaded

        super().__init__(noun, group=group, check=check, selection=selection)
        self.contract = contract


def _implements[P](loaded: object, contract: type[P], methods: list[str]) -> TypeIs[P]:
    """Whether ``loaded`` has every method of the contract. Structural, so an object of any class, or a proxy, can."""
    return all(callable(getattr(loaded, m, None)) for m in methods)


type _Method = tuple[Callable[..., object], list[inspect.Parameter]]
"""A method of a contract, and the arguments that a caller passes: after ``self`` or ``cls``, all of a static one."""


def _methods(contract: type) -> dict[str, _Method]:
    """The methods of a protocol, static and class methods included, by name. Other members are not here."""
    found: dict[str, _Method] = {}
    for name in sorted(typing.get_protocol_members(contract)):
        member = inspect.getattr_static(contract, name, None)
        if isinstance(member, staticmethod):
            found[name] = (member.__func__, list(inspect.signature(member.__func__).parameters.values()))
        elif isinstance(member, classmethod) or inspect.isfunction(member):
            function = member.__func__ if isinstance(member, classmethod) else member
            found[name] = (function, list(inspect.signature(function).parameters.values())[1:])
    return found


def data_errors(contract: type) -> list[str]:
    """Return why ``contract`` is not data in, data out, or nothing when it is.

    A pure contract is a protocol of methods only, as a WIT interface has functions: no property or attribute. Each
    argument and return type is data: a type that pydantic can validate and describe as JSON Schema without
    ``arbitrary_types_allowed``. That refuses a session, a file, a callable, a protocol and any class that is not a
    dataclass, a pydantic model or a type that pydantic knows. On top of it, two rules of WIT:

    - no part of any type: ``Any``, ``object``, a model that allows extra fields;
    - no type that contains itself.

    A method returns its error as a value, a case of a union of records, not as an exception, which does not cross a
    Wasm or Extism boundary with its type.
    """
    methods = _methods(contract)
    others = sorted(set(typing.get_protocol_members(contract)) - set(methods))
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


def _loaded(offer: Offer) -> object:
    """Load what ``offer`` names. Raises ``PluginError``, with the offer's declaration, when it does not load."""
    try:
        return offer.load()
    except PluginError:
        raise
    except Exception as e:
        raise PluginError(f'{offer.where} does not load: {type(e).__name__}: {e}') from e


def declared() -> list[Activatable]:
    """Every capability that an installed package declares in ``CAPABILITIES``, piloop included, by provider group.

    A plugin declares a capability for a seam of its own the same way that piloop declares its own: an entry point
    that names the ``Capability``. Other packages provide it, and piloop lists, checks and selects it as any other.

    Raises:
        PluginError: An entry point does not load, does not name a capability, or names a capability whose provider
            group another one has.
    """
    found: dict[str, Activatable] = {}
    for offer in _python_entry_points(CAPABILITIES):
        loaded = _loaded(offer)
        if not isinstance(loaded, Capability):
            raise PluginError(f'{offer.where} is not a piloop.plugins.host.Capability')
        if (other := found.get(loaded.group)) is not None and other is not loaded:
            raise PluginError(f'{offer.where}: the {other.noun}s have the provider group {loaded.group} already')
        found[loaded.group] = loaded
    return sorted(found.values(), key=lambda c: c.group)


def lookup(data: object, key: str) -> object | None:
    """The value at the dotted ``key`` of a parsed configuration, or None."""
    for part in key.split('.'):
        if not isinstance(data, dict):
            return None
        data = data.get(part)
    return data


def check_selection(capability: Activatable, data: dict[str, object], scope: Literal['project', 'home']) -> None:
    """Check what a parsed file of ``scope`` selects for ``capability``: the provider, then its parameters.

    The parameters are the other keys of the table of the selecting key. A provider that is a pydantic model class
    (a remote backend) checks them itself; any other declares ``Parameters``, a type that pydantic validates, or takes
    none. In ``config.toml``, a ``settings`` table may name only providers that there are.

    Raises:
        ValueError: The configuration names a provider that there is not, or one that refuses its parameters. The
            message names the key.
    """
    selection = capability.selection
    if selection is None:
        return
    if selection.scope == scope and (name := lookup(data, selection.key)) is not None:
        where = f'[{selection.table}] ' if selection.table else ''
        leaf = selection.key.rpartition('.')[2]
        try:
            provider = capability.get(str(name))
        except MissingProvider as e:
            raise ValueError(f'{where}{leaf} = {name!r}: {e}') from e
        table = lookup(data, selection.table) if selection.table else {}
        given = {k: v for k, v in table.items() if k != leaf} if isinstance(table, dict) else {}
        schema = provider if isinstance(provider, type) and issubclass(provider, BaseModel) else None
        schema = schema or getattr(provider, 'Parameters', None)
        if schema is None and given:
            raise ValueError(f'{where}{capability.noun} {name!r} takes no parameters: {", ".join(sorted(given))}')
        if schema is not None:
            TypeAdapter(schema).validate_python(given)
    tables = lookup(data, selection.settings) if selection.settings and scope == 'home' else None
    if isinstance(tables, dict) and (unknown := sorted(set(tables) - set(capability.sources()))):
        raise ValueError(
            f'[{selection.settings}.{unknown[0]}]: no {capability.noun} has that name; registered'
            f' {capability.noun}s: {", ".join(capability.sources())}'
        )


def compose(*capabilities: Activatable) -> None:
    """Activate the capabilities that are not active yet, in order.

    Raises:
        PluginError: One does not activate, or one is needed while it activates: the capabilities that this call
            activated are unregistered again, newest first.
    """
    with _lock, ExitStack() as stack:
        for capability in capabilities:
            _activate(capability, stack)
        _process.enter_context(stack.pop_all())


def _activate(capability: Activatable, stack: ExitStack) -> None:
    if capability.active:
        return
    if capability.activating:
        raise PluginError(
            f'the {capability.noun}s are needed while they load: an entry point of {capability.group} uses its own'
            ' capability, or one that uses it, when it is imported'
        )
    capability.activate(stack)


def close() -> None:
    """Unregister every capability of the process, newest first. Closing twice is harmless."""
    with _lock:
        _process.close()
