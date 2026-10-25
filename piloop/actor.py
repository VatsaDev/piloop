"""Who writes: an agent that the daemon launched, a person whom the network named, or one of piloop's own actors.

``PILOOP_ACTOR``, ``updated_by``, ``events.actor`` and the ``*_by`` and ``author`` columns hold the actor's signature:
``<role>#<agent_session_id>`` for an agent, such as ``implementer#12``, ``human:<name>`` for a person, else ``human``,
``orchestrator`` or ``daemon``.
"""

from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, PlainSerializer

from piloop.ids import ROLE_NAME, AgentSessionId, RoleName

PERSON = 'human:'
"""What a person's signature starts with."""
PERSON_NAME = r'^[^\s#]{1,128}$'
"""A person's name, as the network names the caller: the proxy's ``name_claim``, or a token's name."""


class Agent(BaseModel):
    """An agent that the daemon launched for an agent session of a role."""

    model_config = ConfigDict(frozen=True)

    role: Annotated[RoleName, Field(pattern=ROLE_NAME)]
    session: Annotated[AgentSessionId, Field(gt=0)]

    @property
    def signature(self) -> str:
        return f'{self.role}#{self.session}'

    @property
    def launched(self) -> Agent:
        """The agent that the daemon launched, when the actor is one: what a check of a role or a session reads.

        None for a person and for piloop's own actors, which a role's limits do not bind.
        """
        return self

    def __str__(self) -> str:
        return self.signature


class Person(BaseModel):
    """A person whom the network named, who calls the API from elsewhere, such as a web console's user."""

    model_config = ConfigDict(frozen=True)

    name: Annotated[str, Field(pattern=PERSON_NAME)]

    @property
    def signature(self) -> str:
        return f'{PERSON}{self.name}'

    @property
    def launched(self) -> None:
        return None

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

    @property
    def launched(self) -> None:
        return None


def _fields(value: object) -> object:
    """Split a signature into its fields: ``<role>#<id>`` for an agent, ``human:<name>`` for a person."""
    match value:
        case str() if '#' in value:
            role, _, session = value.partition('#')
            return {'role': role, 'session': session}
        case str() if value.startswith(PERSON):
            return {'name': value.removeprefix(PERSON)}
        case _:
            return value


def _signed(actor: Actor) -> str:
    return actor.signature


type Actor = Annotated[Agent | Person | System, BeforeValidator(_fields), PlainSerializer(_signed, return_type=str)]
type SignedAgent = Annotated[Agent, BeforeValidator(_fields), PlainSerializer(_signed, return_type=str)]
"""An ``Agent`` that is read from and written as its signature, as an ``Actor`` is."""
