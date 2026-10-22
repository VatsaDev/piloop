"""Live limits: values for the ``Live`` keys of a provider's parameters, changed while the daemon runs.

A **limit** is a row of ``limits``: a dotted key, the capability then the path in its parameters
(``executor.pools.serve.evaluations``), a value, a reason and who set it. It wins over ``config.toml``. The daemon
hands the provider its parameters with the limits applied (``applied``, then ``Executor.configure``) on its next tick,
so a busy GPU takes fewer evaluations without a restart.

Only a key that the provider marks ``Live`` takes a limit, and only a value that its parameters accept.
"""

import copy
from collections.abc import Iterator, Mapping, Sequence
from enum import StrEnum

from pydantic import BaseModel, JsonValue, ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.db.models import Limit, utcnow
from piloop.plugins.host import Activatable, Live, PluginTable, declared


class Refused(ValueError):
    """A limit that is not set: its key is not ``Live``, or its parameters refuse the value. The message says why."""


class Origin(StrEnum):
    """Where the value of a ``Live`` key comes from."""

    DEFAULT = 'default'
    CONFIG = 'config.toml'
    LIMIT = 'limit'


def name(capability: Activatable) -> str:
    """The name of a home capability's table, ``[plugins.<name>]``, which is the first segment of its limits' keys."""
    assert capability.selection is not None  # a capability with Live keys is selected by a table
    return capability.selection.key.split('.')[1]


def selected(plugins: Mapping[str, PluginTable]) -> dict[str, type[BaseModel]]:
    """The ``Parameters`` of the provider of each capability of the home that ``[plugins.<name>]`` selects, by name.

    A capability without a table has its default provider. A provider without ``Parameters`` takes no limit.
    """
    out: dict[str, type[BaseModel]] = {}
    for capability in declared():
        selection = capability.selection
        if selection is None or selection.scope != 'home' or not selection.key.startswith('plugins.'):
            continue
        table = plugins.get(key := name(capability))
        provider = table.provider if table is not None else selection.default
        model = getattr(capability.get(provider), 'Parameters', None) if provider is not None else None
        if isinstance(model, type) and issubclass(model, BaseModel):
            out[key] = model
    return out


def applied[P: BaseModel](table: PluginTable | None, limits: Sequence[Limit], model: type[P], capability: str) -> P:
    """The parameters of ``table`` with the limits of ``capability`` over them.

    Raises:
        ValidationError: The parameters refuse a value.
    """
    raw: dict[str, object] = copy.deepcopy(table.keys) if table is not None else {}
    for limit in limits:
        name, _, path = limit.key.partition('.')
        if name == capability:
            _put(raw, path.split('.'), limit.value)
    return model.model_validate(raw)


def _put(raw: dict[str, object], path: list[str], value: JsonValue) -> None:
    head, *rest = path
    if not rest:
        raw[head] = value
        return
    inner = raw.get(head)
    if not isinstance(inner, dict):
        inner = raw[head] = {}
    _put(inner, rest, value)


def live(model: BaseModel, prefix: str) -> Iterator[tuple[str, object]]:
    """Each ``Live`` key of ``model`` and of the models in it, under ``prefix``, with its value.

    A dict's keys are path segments: ``pools.serve.evaluations``.
    """
    for name, field in type(model).model_fields.items():
        value, key = getattr(model, name), f'{prefix}.{name}'
        match value:
            case _ if Live in field.metadata:
                yield key, value
            case BaseModel():
                yield from live(value, key)
            case dict():
                for item, inner in value.items():
                    if isinstance(inner, BaseModel):
                        yield from live(inner, f'{key}.{item}')


def set_limit(
    s: Session, plugins: Mapping[str, PluginTable], key: str, value: JsonValue, *, reason: str, actor: str
) -> Limit:
    """Set the limit of ``key``, for the daemon's next tick. The caller commits.

    Raises:
        Refused: ``key`` is not a ``Live`` key of the selected provider's parameters, or they refuse ``value``.
    """
    capability = key.partition('.')[0]
    if (model := selected(plugins).get(capability)) is None:
        raise Refused(f'{key}: no provider of the home that has parameters is [plugins.{capability}]')
    table, others = plugins.get(capability), list(s.scalars(select(Limit).where(Limit.key != key)))
    keys = [k for k, _ in live(applied(table, others, model, capability), capability)]
    if key not in keys:  # a limit changes a key that there is: it never adds a pool
        raise Refused(f'{key} is not a live key; the live keys are {", ".join(keys) or "none"}')
    try:
        applied(table, [*others, Limit(key=key, value=value)], model, capability)
    except ValidationError as e:
        raise Refused(f'{key} = {value!r}: {e.errors()[0]["msg"]}') from e
    limit = s.get(Limit, key) or Limit(key=key)
    limit.value, limit.reason, limit.actor, limit.at = value, reason, actor, utcnow()
    s.add(limit)
    return limit


def unset_limit(s: Session, key: str) -> bool:
    """Remove the limit of ``key``, and return whether there was one. ``config.toml`` holds again. The caller commits."""
    limit = s.get(Limit, key)
    if limit is not None:
        s.delete(limit)
    return limit is not None


def sources(s: Session, plugins: Mapping[str, PluginTable]) -> list[tuple[str, object, Origin, Limit | None]]:
    """Each ``Live`` key of the home's providers: its value, where it comes from, and its limit if one sets it."""
    limits = {limit.key: limit for limit in s.scalars(select(Limit))}
    out: list[tuple[str, object, Origin, Limit | None]] = []
    for capability, model in selected(plugins).items():
        table = plugins.get(capability)
        raw = table.keys if table is not None else {}
        for key, value in live(applied(table, list(limits.values()), model, capability), capability):
            match limits.get(key):
                case Limit() as limit:
                    out.append((key, value, Origin.LIMIT, limit))
                case None if _has(raw, key.split('.')[1:]):
                    out.append((key, value, Origin.CONFIG, None))
                case _:
                    out.append((key, value, Origin.DEFAULT, None))
    return out


def _has(raw: object, path: list[str]) -> bool:
    """Whether the table ``raw`` sets the key at ``path``."""
    head, *rest = path
    return isinstance(raw, dict) and head in raw and (not rest or _has(raw[head], rest))
