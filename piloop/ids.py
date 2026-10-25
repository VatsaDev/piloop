"""The ids of the rows that agents and the daemon refer to, the names of roles and tracks, and what a row is about.

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


@dataclass(frozen=True)
class TargetRef:
    """What a comment or an event is about: ``target_kind`` and ``target_id``, written ``idea#12``.

    A dataclass, so that it maps the two columns as one composite attribute.
    """

    kind: TargetKind
    id: PositiveInt

    def __str__(self) -> str:
        return f'{self.kind}#{self.id}'


REF_PATTERN = re.compile(r'(?P<kind>[a-z_]+)#(?P<id>[0-9]+)')


def _parsed(value: object) -> object:
    if not isinstance(value, str):
        return value
    if (found := REF_PATTERN.fullmatch(value)) is None:
        raise ValueError(f'{value!r} is not a reference like idea#12')
    return {'kind': found['kind'], 'id': int(found['id'])}


type Ref = Annotated[TargetRef, BeforeValidator(_parsed), PlainSerializer(str, return_type=str)]
"""A ``TargetRef`` that a model holds as its text, ``idea#12``."""
