"""Live limits: values for the ``Live`` keys of a provider's parameters, changed while the daemon runs.

A **limit** is a row of ``limits``: a dotted key, the capability then the path in its provider's fields
(``executor.pools.serve.evaluations``), a value, a reason and who set it. It wins over ``config.toml``. The daemon
hands the executor its provider with the limits applied (``applied``, then ``Executor.configure``) at once,
so a busy GPU takes fewer evaluations without a restart.

Only a key that the provider marks ``Live`` takes a limit, and only a value that its parameters accept.
"""

from collections.abc import Collection, Iterator, Mapping, Sequence
from enum import StrEnum

from pydantic import BaseModel, JsonValue, ValidationError
from pydantic_core import to_jsonable_python
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.api.views import LiveKey
from piloop.db.models import Limit, utcnow
from piloop.plugins.host import Choice, Live, Parameters, Scope, declared


class Refused(ValueError):
    """A limit that is not set: its key is not ``Live``, or its parameters refuse the value. The message says why."""


class Origin(StrEnum):
    """Where the value of a ``Live`` key comes from."""

    DEFAULT = 'default'
    CONFIG = 'config.toml'
    LIMIT = 'limit'


def selected(plugins: Mapping[str, Parameters]) -> dict[str, Parameters]:
    """The provider of each capability of the home that ``[plugins.<name>]`` chooses, or its default, by name."""
    found: dict[str, Parameters] = {}
    for c in declared():
        match c.choice:
            case Choice(scope=Scope.HOME, default=default) if c.name in plugins or default is not None:
                provider = c.selected(plugins)
                assert isinstance(provider, Parameters)  # Capability refuses a Choice of a contract that is not one
                found[c.name] = provider
    return found


def applied[P: Parameters](provider: P, limits: Sequence[Limit], capability: str) -> P:
    """``provider`` with the limits of ``capability`` over its parameters.

    Raises:
        ValidationError: The parameters refuse a value.
    """
    raw = provider.model_dump(exclude_unset=True)
    for limit in limits:
        name, _, path = limit.key.partition('.')
        if name == capability:
            _put(raw, path.split('.'), limit.value)
    return type(provider).model_validate(raw)


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
    s: Session, plugins: Mapping[str, Parameters], key: str, value: JsonValue, *, reason: str, actor: str
) -> Limit:
    """Set the limit of ``key``, for the daemon's next pass. The caller commits.

    Raises:
        Refused: ``key`` is not a ``Live`` key of the selected provider's parameters, or they refuse ``value``.
    """
    capability = key.partition('.')[0]
    if (provider := selected(plugins).get(capability)) is None:
        raise Refused(f'{key}: no provider of the home is [plugins.{capability}]')
    others = list(s.scalars(select(Limit).where(Limit.key != key)))
    keys = [k for k, _ in live(applied(provider, others, capability), capability)]
    if key not in keys:  # a limit changes a key that there is: it never adds a pool
        raise Refused(f'{key} is not a live key; the live keys are {", ".join(keys) or "none"}')
    try:
        applied(provider, [*others, Limit(key=key, value=value)], capability)
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


def sources(s: Session, plugins: Mapping[str, Parameters]) -> list[tuple[str, object, Origin, Limit | None]]:
    """Each ``Live`` key of the home's providers: its value, where it comes from, and its limit if one sets it."""
    limits = {limit.key: limit for limit in s.scalars(select(Limit))}
    out: list[tuple[str, object, Origin, Limit | None]] = []
    for capability, provider in selected(plugins).items():
        raw = provider.model_dump(exclude_unset=True)
        for key, value in live(applied(provider, list(limits.values()), capability), capability):
            match limits.get(key):
                case Limit() as limit:
                    out.append((key, value, Origin.LIMIT, limit))
                case None if _has(raw, key.split('.')[1:]):
                    out.append((key, value, Origin.CONFIG, None))
                case _:
                    out.append((key, value, Origin.DEFAULT, None))
    return out


def live_keys(s: Session, plugins: Mapping[str, Parameters], origins: Collection[Origin]) -> list[LiveKey]:
    """``sources`` whose value comes from one of ``origins``, as the API answers them."""
    return [
        LiveKey(
            key=key,
            value=to_jsonable_python(value),
            origin=str(origin),
            actor=None if limit is None else limit.actor,
            reason=None if limit is None else limit.reason,
        )
        for key, value, origin, limit in sources(s, plugins)
        if origin in origins
    ]


def _has(raw: object, path: list[str]) -> bool:
    """Whether the table ``raw`` sets the key at ``path``."""
    head, *rest = path
    return isinstance(raw, dict) and head in raw and (not rest or _has(raw[head], rest))
