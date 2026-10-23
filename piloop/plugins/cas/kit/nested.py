"""A provider's parameter that is itself a CAS: a nested table ``{ provider = "<name>", <its parameters> }``.

A nested table is validated as ``[plugins.cas]`` is (``Capability.validated``: without ``provider``, ``disk``), when its
parent's parameters are, and checked against the kind of provider that its key takes: an unknown provider, one of
another kind, or a refused key fails on the parent's key.
"""

from collections.abc import Callable
from typing import Annotated

from pydantic import PlainValidator, WithJsonSchema

from piloop.plugins.cas.contract import CAS, CasProvider, LocalProvider


def child[P: CasProvider](kind: type[P]) -> Callable[[object], P]:
    """Validate a nested table through ``CAS``, refusing a provider that is not a ``kind``."""

    def validate(value: object) -> P:
        found = value if isinstance(value, CasProvider) else CAS.validated(value)
        assert isinstance(found, CasProvider)  # the providers of CAS are its contract's subclasses
        if not isinstance(found, kind):
            raise ValueError(f'{CAS.name_of(found)} cannot be here: this key takes a {kind.__name__}')
        return found

    return validate


type Nested = Annotated[CasProvider, PlainValidator(child(CasProvider)), WithJsonSchema({'type': 'object'})]
"""Any CAS provider, given as a nested table."""
type NestedLocal = Annotated[LocalProvider, PlainValidator(child(LocalProvider)), WithJsonSchema({'type': 'object'})]
"""A provider whose store can be a local tier (``LocalProvider``), given as a nested table."""
