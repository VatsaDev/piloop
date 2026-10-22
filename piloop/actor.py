"""Who writes: an agent that the daemon launched, or one of piloop's own actors.

``PILOOP_ACTOR``, ``updated_by``, ``events.actor`` and the ``*_by`` and ``author`` columns hold the actor's signature:
``<role>#<agent_session_id>`` for an agent, such as ``implementer#12``, else ``human``, ``orchestrator`` or ``daemon``.
"""

from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, PlainSerializer, PositiveInt

ROLE_NAME = r'^[a-z][a-z0-9_-]{0,19}$'
"""A role's name, which is also its config table, CLI name, signature and pane prefix."""


class Agent(BaseModel):
    """An agent that the daemon launched for an agent session of a role."""

    model_config = ConfigDict(frozen=True)

    role: Annotated[str, Field(pattern=ROLE_NAME)]
    session: PositiveInt

    @property
    def signature(self) -> str:
        return f'{self.role}#{self.session}'

    def __str__(self) -> str:
        return self.signature


class System(StrEnum):
    """An actor that the daemon did not launch: the person at the terminal, the orchestrator, or the daemon itself."""

    HUMAN = 'human'
    ORCHESTRATOR = 'orchestrator'
    DAEMON = 'daemon'

    @property
    def signature(self) -> str:
        return self.value


def _fields(value: object) -> object:
    """Split an agent's signature, ``<role>#<id>``, into its fields. Anything else passes through."""
    if isinstance(value, str) and '#' in value:
        role, _, session = value.partition('#')
        return {'role': role, 'session': session}
    return value


def _signed(actor: Actor) -> str:
    return actor.signature


type Actor = Annotated[Agent | System, BeforeValidator(_fields), PlainSerializer(_signed, return_type=str)]
