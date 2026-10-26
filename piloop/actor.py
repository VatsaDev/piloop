"""Who writes: an agent that the daemon launched, a person or a service whom the network named, or piloop's own actors.

``PILOOP_ACTOR``, ``updated_by``, ``events.actor`` and the ``*_by`` and ``author`` columns hold the actor's signature:
``<role>#<agent_session_id>`` for an agent, such as ``implementer#12``, ``human:<name>`` for a person,
``service:<name>`` for a service, ``orchestrator via <its signature>`` for either acting as the orchestrator, else
``human``, ``orchestrator`` or ``daemon``.
"""

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, PlainSerializer, TypeAdapter, ValidationError

from piloop.ids import ROLE_NAME, AgentSessionId, RoleName

PERSON = 'human:'
"""What a person's signature starts with."""
SERVICE = 'service:'
"""What a service's signature starts with."""
CALLER_NAME = r'^[^\s#]{1,128}$'
"""A caller's name, as the network names it: the proxy's ``name_claims``, or a token's name."""
ORCHESTRATING = 'orchestrator via '
"""What the signature of a person or a service acting as the orchestrator starts with, before its own."""


def _fields(value: object) -> object:
    """Split a signature into its fields: ``<role>#<id>``, ``orchestrator via <its>``, ``human:<…>``, ``service:<…>``."""
    match value:
        case str() if '#' in value:
            role, _, session = value.partition('#')
            return {'role': role, 'session': session}
        case str() if value.startswith(ORCHESTRATING):
            return {'by': value.removeprefix(ORCHESTRATING)}
        case str() if value.startswith(PERSON):
            return {'kind': CallerKind.PERSON, 'name': value.removeprefix(PERSON)}
        case str() if value.startswith(SERVICE):
            return {'kind': CallerKind.SERVICE, 'name': value.removeprefix(SERVICE)}
        case _:
            return value


class Agent(BaseModel):
    """An agent that the daemon launched for an agent session of a role."""

    model_config = ConfigDict(frozen=True)

    role: Annotated[RoleName, Field(pattern=ROLE_NAME)]
    session: Annotated[AgentSessionId, Field(gt=0)]

    @property
    def signature(self) -> str:
        return f'{self.role}#{self.session}'

    @property
    def party(self) -> str:
        """Whom it writes for: its role. Every agent of a role is one party, so one never waits for another's writes."""
        return self.role

    @property
    def launched(self) -> Agent:
        """The agent that the daemon launched, when the actor is one: what a check of a role or a session reads.

        None for a person and for piloop's own actors, which a role's limits do not bind.
        """
        return self

    def __str__(self) -> str:
        return self.signature


class CallerKind(StrEnum):
    """What a caller whom the network named is, which sets how it signs: a ``Person`` or a ``Service``."""

    PERSON = 'person'
    SERVICE = 'service'


class Person(BaseModel):
    """A person whom the network named, who calls the API from elsewhere, such as a web console's user."""

    model_config = ConfigDict(frozen=True)

    kind: Literal[CallerKind.PERSON] = CallerKind.PERSON
    name: Annotated[str, Field(pattern=CALLER_NAME)]

    @property
    def signature(self) -> str:
        return f'{PERSON}{self.name}'

    @property
    def party(self) -> str:
        return self.signature

    @property
    def launched(self) -> None:
        return None

    def __str__(self) -> str:
        return self.signature


class Service(BaseModel):
    """A machine whom the network named, such as a service account's key: it signs as itself, never as a person."""

    model_config = ConfigDict(frozen=True)

    kind: Literal[CallerKind.SERVICE] = CallerKind.SERVICE
    name: Annotated[str, Field(pattern=CALLER_NAME)]

    @property
    def signature(self) -> str:
        return f'{SERVICE}{self.name}'

    @property
    def party(self) -> str:
        return self.signature

    @property
    def launched(self) -> None:
        return None

    def __str__(self) -> str:
        return self.signature


type Named = Annotated[Person | Service, Field(discriminator='kind')]
"""A caller whom the network named: a person or a service, told apart by its ``kind``."""


class Orchestrating(BaseModel):
    """A person or a service whom the network named, acting as the orchestrator: the write is the orchestrator's.

    Its signature says whose.
    """

    model_config = ConfigDict(frozen=True)

    by: Annotated[Named, BeforeValidator(_fields)]

    @property
    def signature(self) -> str:
        return f'{ORCHESTRATING}{self.by.signature}'

    @property
    def party(self) -> str:
        """The orchestrator's: whoever acts as it, the person writes for the orchestrator."""
        return System.ORCHESTRATOR.party

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
    def party(self) -> str:
        """Its signature. ``orchestrator`` is also the party of each agent of the ``orchestrator`` role."""
        return self.value

    @property
    def launched(self) -> None:
        return None


def _signed(actor: Actor) -> str:
    return actor.signature


type Actor = Annotated[
    Agent | Person | Service | Orchestrating | System,
    BeforeValidator(_fields),
    PlainSerializer(_signed, return_type=str),
]
type SignedAgent = Annotated[Agent, BeforeValidator(_fields), PlainSerializer(_signed, return_type=str)]
"""An ``Agent`` that is read from and written as its signature, as an ``Actor`` is."""
ACTORS: TypeAdapter[Actor] = TypeAdapter(Actor)


def party_of(signature: str) -> str:
    """The party of the actor that signs ``signature`` (``Agent.party``).

    A signature that names no actor, such as one that an older piloop wrote, is a party of its own.
    """
    try:
        return ACTORS.validate_python(signature).party
    except ValidationError:  # not an actor's signature: nothing else signs as it
        return signature
