"""The ids of rows, the names of roles and tracks, what a row is about, and the states that the API shows and takes.

Each id is a ``NewType`` of ``int``, so an idea's id does not type-check where a candidate's id goes. A name is a
``NewType`` of ``str``. A ``TargetRef`` is a kind and an id, written ``idea#12``.
"""

import re
from enum import StrEnum
from typing import Annotated, Final, NewType

from pydantic import BeforeValidator, PlainSerializer, PositiveInt
from pydantic.dataclasses import dataclass

CampaignId = NewType('CampaignId', int)
IdeaId = NewType('IdeaId', int)
RunId = NewType('RunId', int)
CandidateId = NewType('CandidateId', int)
MeasurementId = NewType('MeasurementId', int)
EvaluatorId = NewType('EvaluatorId', int)
AgentSessionId = NewType('AgentSessionId', int)
CommentId = NewType('CommentId', int)
Revision = NewType('Revision', str)
"""The vcs provider's name for a version of a campaign's files: a commit SHA under ``git``."""

ROLE_NAME = r'^[a-z][a-z0-9_-]{0,19}$'
"""A role's name, which is also its config table, CLI name, signature and pane prefix."""
RoleName = NewType('RoleName', str)
"""A role's name: a role of a package, piloop's included, or a ``[roles.<name>]`` table. It matches ``ROLE_NAME``."""
IMPLEMENTER: Final = RoleName('implementer')
"""The role that run slots fill last, and the role of a run whose row predates its role (``runs.role``)."""

TrackName = NewType('TrackName', str)
"""A track's name: its primary's ``track``, or else the primary's name."""


class TargetKind(StrEnum):
    """What a comment or an event points at."""

    CAMPAIGN = 'campaign'
    IDEA = 'idea'
    RUN = 'run'
    CANDIDATE = 'candidate'
    MEASUREMENT = 'measurement'
    AGENT_SESSION = 'agent_session'
    COMMENT = 'comment'
    INSTANCE = 'instance'
    """A worker that a provisioner started (``worker_instances``)."""
    HOME = 'home'
    """The whole home, for an event about no row: a limit, a worker. Its one id is 1 (``HOME``). No comment is on it."""


@dataclass(frozen=True)
class TargetRef:
    """What a comment or an event is about: ``target_kind`` and ``target_id``, written ``idea#12``.

    A dataclass, so that it maps the two columns as one composite attribute.
    """

    kind: TargetKind
    id: PositiveInt

    def __str__(self) -> str:
        return f'{self.kind}#{self.id}'


HOME: Final = TargetRef(TargetKind.HOME, 1)
"""The target of an event about the whole home."""

REF_PATTERN = re.compile(r'(?P<kind>[a-z_]+)#(?P<id>[0-9]+)')


def _parsed(value: object) -> object:
    if not isinstance(value, str):
        return value
    if (found := REF_PATTERN.fullmatch(value)) is None:
        raise ValueError(f'{value!r} is not a reference like idea#12')
    return {'kind': found['kind'], 'id': int(found['id'])}


type Ref = Annotated[TargetRef, BeforeValidator(_parsed), PlainSerializer(str, return_type=str)]
"""A ``TargetRef`` that a model holds as its text, ``idea#12``."""


class CommentKind(StrEnum):
    """Why a comment was written. An ``escalation`` asks for a decision and is always in the human's inbox."""

    NOTE = 'note'
    FINDING = 'finding'
    DECISION = 'decision'
    ESCALATION = 'escalation'


class CampaignStatus(StrEnum):
    """Whether the daemon still requests work for a campaign. ``stopped`` after a ``[stop]`` condition held."""

    ACTIVE = 'active'
    STOPPED = 'stopped'


class Ending(StrEnum):
    """How the daemon ends the agents of a stop or a cancel that someone requested."""

    WRAP_UP = 'wrap_up'
    """After the wrap-up prompt and its grace period: the agent may commit first."""
    NOW = 'now'
    """At once."""


class IdeaStatus(StrEnum):
    """The lifecycle of an idea. The scheduler picks up ``ready`` ideas in priority order.

    A ``seed`` is a rough idea with pointers. An agent whose role's ``files`` allows it files one (``piloop idea
    seed``). Research develops it into a defined idea (``piloop idea develop``, which makes it ``ready`` or
    ``proposed``), or rejects it.
    A ``proposed`` idea is defined and waits for a human to approve it.
    """

    SEED = 'seed'
    PROPOSED = 'proposed'
    READY = 'ready'
    SCHEDULED = 'scheduled'
    DONE = 'done'
    PARKED = 'parked'
    REJECTED = 'rejected'
