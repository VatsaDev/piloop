"""Storage models. The database is the contract that every agent reads and writes through ``piloop sql``.

Every NOT NULL column with a default also has a server default, so the raw-SQL inserts in the docs work without
timestamps or statuses. ``docs/reference/schema.mdx`` (the DDL) is generated from these models.
"""

import sys
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, ClassVar, Final, Literal, get_args, get_origin

from covalence import Cid
from pydantic import (
    BaseModel,
    ConfigDict,
    JsonValue,
    PlainSerializer,
    PlainValidator,
    TypeAdapter,
    model_serializer,
)
from sqlalchemy import (
    JSON,
    CheckConstraint,
    Column,
    Computed,
    DateTime,
    Enum,
    FetchedValue,
    ForeignKey,
    Index,
    Integer,
    Select,
    String,
    UniqueConstraint,
    event,
    func,
    inspect,
    select,
    text,
)
from sqlalchemy.engine import Dialect
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, composite, mapped_column, relationship
from sqlalchemy.orm.attributes import set_committed_value
from sqlalchemy.types import TypeDecorator

from piloop.api.contract import Access
from piloop.config import CampaignConfig
from piloop.ids import (
    IMPLEMENTER,
    AgentSessionId,
    CampaignId,
    CampaignStatus,
    CandidateId,
    Cause,
    CommentId,
    CommentKind,
    EvaluatorId,
    IdeaId,
    IdeaStatus,
    LinkKind,
    MeasurementId,
    Revision,
    RoleName,
    RunId,
    TargetKind,
    TargetRef,
    TrackName,
)
from piloop.plugins.host import Slot
from piloop.plugins.verdict_rule.contract import Outcome
from piloop.usage import Usage


def utcnow() -> datetime:
    """The current time, with the UTC time zone."""
    return datetime.now(UTC)


class PydanticJSON[T](TypeDecorator[T]):
    """A JSON column that validates on load and on save, through one ``TypeAdapter``.

    The values are frozen models, so a change in place cannot be lost: assign a new value. None is SQL NULL.
    """

    impl = JSON(none_as_null=True)
    cache_ok = True

    def __init__(self, adapter: TypeAdapter[T]) -> None:
        super().__init__()
        self.adapter = adapter

    def process_bind_param(self, value: T | None, dialect: Dialect) -> object:
        return None if value is None else self.adapter.dump_python(self.adapter.validate_python(value), mode='json')

    def process_result_value(self, value: object, dialect: Dialect) -> T | None:
        return None if value is None else self.adapter.validate_python(value)


class UTCDateTime(TypeDecorator[datetime]):
    """A datetime that is always UTC. SQLite drops the time zone when it stores a value, so this type adds it back."""

    impl = DateTime
    cache_ok = True

    def process_result_value(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        return value.replace(tzinfo=UTC) if value is not None and value.tzinfo is None else value


class CidText(TypeDecorator[Cid]):
    """A covalence ``Cid``, stored as its text."""

    impl = String
    cache_ok = True

    def process_bind_param(self, value: Cid | None, dialect: Dialect) -> str | None:
        return None if value is None else str(value)

    def process_result_value(self, value: str | None, dialect: Dialect) -> Cid | None:
        return None if value is None else Cid(value)


class Pause(StrEnum):
    """What a paused campaign holds (``piloop pause``). Running work goes on either way, and nothing is escalated.

    ``launches``: nothing launches, no run is queued and no role is requested. The daemon still measures again what a
    new evaluator or a request needs. ``all``: those measurements wait too.
    """

    LAUNCHES = 'launches'
    ALL = 'all'


class RunStatus(StrEnum):
    """The lifecycle of one attempt at an idea."""

    QUEUED = 'queued'
    RUNNING = 'running'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'
    TIMED_OUT = 'timed_out'


OPEN_RUNS = (RunStatus.QUEUED, RunStatus.RUNNING)
FAILED_RUNS = (RunStatus.FAILED, RunStatus.TIMED_OUT)
FINISHED_RUNS = (RunStatus.SUCCEEDED, *FAILED_RUNS)


class RunEnd(StrEnum):
    """How a run ended. ``runs.error`` adds the detail, when there is one.

    ``finished``: its implementer left its finding, or committed and exited cleanly. ``failed``: its process failed, it
    made no commit, or only the daemon's commit of what it left uncommitted (the error says which). ``timed_out``: its
    role's timeout. ``awaiting_answer``: its implementer's escalation waits for an answer (the error names it).
    ``cancelled``: someone cancelled it. ``campaign_stopped``, ``daemon_stopped``, ``stale`` (a daemon died while it
    ran) and ``worker_lost``: something else ended it. ``role_removed``: ``piloop init`` removed its role while it was
    queued. ``not_started``: no agent of it started to work. Only the first four are attempts at the idea.
    """

    FINISHED = 'finished'
    FAILED = 'failed'
    TIMED_OUT = 'timed_out'
    AWAITING_ANSWER = 'awaiting_answer'
    CANCELLED = 'cancelled'
    CAMPAIGN_STOPPED = 'campaign_stopped'
    DAEMON_STOPPED = 'daemon_stopped'
    STALE = 'stale'
    ROLE_REMOVED = 'role_removed'
    NOT_STARTED = 'not_started'
    WORKER_LOST = 'worker_lost'

    @property
    def attempt(self) -> bool:
        """Whether a run that ended so was an attempt at its idea: it counts toward ``max_runs`` and ``plateau``."""
        return self in ATTEMPTS

    @property
    def status(self) -> RunStatus:
        """The ``runs.status`` of a run that ended so."""
        return END_STATUS.get(self, RunStatus.FAILED)


ATTEMPTS: Final = frozenset({RunEnd.FINISHED, RunEnd.FAILED, RunEnd.TIMED_OUT, RunEnd.AWAITING_ANSWER})
"""The ends of a run that tried its idea. The others say that the run was stopped, or that no agent started to work."""
END_STATUS: Final = {RunEnd.FINISHED: RunStatus.SUCCEEDED, RunEnd.TIMED_OUT: RunStatus.TIMED_OUT}
"""The status of a run by its end, where it is not ``failed``."""


class Origin(StrEnum):
    """Where a candidate comes from. Only a candidate from the ``baseline`` or a ``run`` is judged.

    ``baseline``: the repo's HEAD when the campaign was registered. ``run``: an implementer's commit. ``reference``: a
    starting point imported with ``piloop candidate import``, measured but never the head or a bar. ``recovered``: the
    daemon's commit of what an implementer left uncommitted when it stopped, or where a daemon stop cut its run, never
    declared ready, so never measured; a new run of the idea starts from it. ``skipped``: a commit marked
    ``Piloop-Eval: skip``, never measured. A ``recovered`` or ``skipped`` tree that a later commit reaches without the
    mark is measured and judged like a new one, and becomes ``run``.
    """

    BASELINE = 'baseline'
    RUN = 'run'
    REFERENCE = 'reference'
    RECOVERED = 'recovered'
    SKIPPED = 'skipped'


JUDGED: Final = frozenset({Origin.BASELINE, Origin.RUN})
"""The origins of the candidates that get a verdict. ``verdict IS NULL`` on one of them means that it is pending."""


class AgentSessionState(StrEnum):
    """The process state of an agent, kept by the daemon. ``requested`` is a request to launch, not a process.

    ``ending``: the daemon ended the agent, for a stop or a cancel, and its process is not gone yet. From then on, a
    finding that the agent writes is refused.
    """

    REQUESTED = 'requested'
    STARTING = 'starting'
    RUNNING = 'running'
    ENDING = 'ending'
    EXITED = 'exited'
    KILLED = 'killed'


LIVE = (AgentSessionState.STARTING, AgentSessionState.RUNNING, AgentSessionState.ENDING)
ENDED = (AgentSessionState.EXITED, AgentSessionState.KILLED)


class LeaseKind(StrEnum):
    """What a lease's job is: an agent session, or an evaluation of the evaluation queue."""

    AGENT = 'agent'
    EVALUATION = 'evaluation'


class LeaseState(StrEnum):
    """The lifecycle of a lease (``piloop.plugins.executor.kit.leases``).

    ``queued`` waits for a worker. ``acquired`` is taken, ``running`` is started. Then ``reported`` (the worker's report
    applied), ``expired`` (the worker went silent past ``toleration_seconds``, or reported its job gone), or
    ``cancelled`` (the job ended before a worker reported it, at a daemon stop for example).
    """

    QUEUED = 'queued'
    ACQUIRED = 'acquired'
    RUNNING = 'running'
    REPORTED = 'reported'
    EXPIRED = 'expired'
    CANCELLED = 'cancelled'


class NoLease(StrEnum):
    """Why a worker got no lease (``piloop.plugins.executor.kit.capacity.admits``). ``piloop worker list`` shows it.

    Declared in order of precedence: of the reasons for each kind of job, the first one declared is the worker's.
    """

    DRAINING = 'draining'
    """The server drained it: it takes no more jobs, and exits once it holds none."""
    MEMORY = 'memory'
    """Its memory available is below ``min_memory_gb``."""
    LOAD = 'load'
    """Its load per CPU is above ``max_load``."""
    SLOTS = 'slots'
    """Its slots of the waiting job's kind are full."""
    NOTHING = 'nothing'
    """Nothing is queued that it can run."""
    UNREACHABLE = 'unreachable'
    """It was silent past ``grace_seconds``."""


class WorkerInstanceState(StrEnum):
    """The lifecycle of a worker that a provisioner starts (``worker_instances``).

    ``starting`` until its worker registers, then ``running``. ``draining`` takes no more jobs: it ends once it holds
    none. ``ended`` says why in ``end``.
    """

    STARTING = 'starting'
    RUNNING = 'running'
    DRAINING = 'draining'
    ENDED = 'ended'


class InstanceEnd(StrEnum):
    """Why a worker instance ended."""

    DRAINED = 'drained'
    """It held no lease when it ended: the pool drained it, or its worker lost its server (``orphan_exit_seconds``)."""
    REGISTER_TIMEOUT = 'register_timeout'
    """Its worker did not register within ``register_seconds``."""
    START_REFUSED = 'start_refused'
    """The provisioner refused to start it: a quota, no capacity, a bad image."""
    LOST = 'lost'
    """Its worker was silent past ``toleration_seconds``."""
    GONE = 'gone'
    """It ended with a lease, or before its worker registered: it crashed, was preempted, or was deleted by hand."""


LIVE_LEASES = (LeaseState.ACQUIRED, LeaseState.RUNNING)
OPEN_LEASES = (LeaseState.QUEUED, *LIVE_LEASES)


def by_value[E: StrEnum](enum: type[E]) -> Enum:
    """Store enum values, not member names, with a CHECK constraint, so that raw SQL cannot write other values."""
    return Enum(
        enum, values_callable=lambda e: [m.value for m in e], native_enum=False, length=16, create_constraint=True
    )


class CommentTarget(TypeDecorator[TargetKind]):
    """A comment's ``TargetKind``, stored by value. Its CHECK leaves out ``home``: no comment is on the whole home."""

    impl = Enum(
        *(k.value for k in TargetKind if k is not TargetKind.HOME),
        name='targetkind',
        native_enum=False,
        length=16,
        create_constraint=True,
    )
    cache_ok = True

    def process_bind_param(self, value: TargetKind | None, dialect: Dialect) -> str | None:
        return None if value is None else TargetKind(value).value

    def process_result_value(self, value: str | None, dialect: Dialect) -> TargetKind | None:
        return None if value is None else TargetKind(value)


def _end_status() -> str:
    """``runs.status`` as a SQL expression of ``runs.end`` (``RunEnd.status``), for the CHECK that keeps them together."""
    cases = ' '.join(f"WHEN '{end}' THEN '{status}'" for end, status in END_STATUS.items())
    return f'CASE "end" {cases} ELSE \'{RunStatus.FAILED}\' END'


NOW = func.now()
OPTIMIZATION = 'optimization'
"""The built-in campaign type: improve the code against its evaluate command, candidate by candidate."""
PROCESS = 'process'
"""The built-in evaluator runner: the evaluate command, run as a process."""
EMPTY_JSON = text("'{}'")


def text_checks(table: str, *columns: str, nullable: tuple[str, ...] = ()) -> tuple[CheckConstraint, ...]:
    """One CHECK for each column that agents write with raw SQL: the value must be text (or NULL, for ``nullable``).

    SQLite stores bound bytes as a BLOB even in a VARCHAR column, and every reader that expects ``str`` then fails.
    The name of the constraint says what it requires, so the error message of every client says it too.
    """
    return tuple(
        CheckConstraint(
            f"typeof({c}) IN ('text', 'null')" if c in nullable else f"typeof({c}) IN ('text')",
            name=f'{table}.{c} must be a text value',
        )
        for c in (*columns, *nullable)
    )


class EventModel(BaseModel):
    """The payload of one kind of event, with its ``kind``: one model for each kind, or for a few that share a shape.

    ``kind`` is a ``Literal`` of the kinds that the model is for. Defining the model registers each of them, and a kind
    that two models register fails at import. Each layer declares the events that it writes, next to the code that
    writes them, and a plugin declares its own the same way. Readers ``match`` on the classes that they import.

    ``kind`` is only the event's own. A field that holds the kind of a row is named after its type, such as
    ``comment_kind: CommentKind`` or ``link_kind: LinkKind``. An agent's kind is its ``role: RoleName``.

    The stored rows keep their shape, and SQL reads them as stored, so a model changes in these ways only:

    - A new field has a default, so the rows before it still load.
    - A field whose meaning changes is a new kind, such as ``run.finished.v2``.
    - A field that goes away, or a rename, is a migration that rewrites the rows, and a line in the upgrade guide.

    A kind that nothing writes any more is retired (``retire``), so that no model takes its name again.
    """

    model_config = ConfigDict(frozen=True, extra='forbid', serialize_by_alias=True, validate_by_name=True)

    kind: str

    _registry: ClassVar[dict[str, type[EventModel]]] = {}
    _retired_kinds: ClassVar[set[str]] = set()

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: object) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        for kind in cls.kinds():
            if kind in EventModel._retired_kinds:
                raise TypeError(f'{cls.__qualname__}: event kind {kind!r} is retired')
            known = EventModel._registry.setdefault(kind, cls)
            if known is not cls:
                raise TypeError(f'{cls.__qualname__}: event kind {kind!r} is {known.__module__}.{known.__qualname__}')

    @classmethod
    def retire(cls, kind: str) -> None:
        """Retire ``kind``: a release wrote it, and nothing writes it now. Its rows load as an ``UnknownEvent``.

        Raises:
            TypeError: a model registers it.
        """
        if (known := EventModel._registry.get(kind)) is not None:
            raise TypeError(f'event kind {kind!r} is {known.__module__}.{known.__qualname__}: it cannot be retired')
        EventModel._retired_kinds.add(kind)

    @classmethod
    def registered(cls) -> dict[str, type[EventModel]]:
        """Every kind that a loaded module registers, with its model."""
        return dict(EventModel._registry)

    @classmethod
    def retired_kinds(cls) -> frozenset[str]:
        """The kinds that are retired."""
        return frozenset(EventModel._retired_kinds)

    @classmethod
    def kinds(cls) -> tuple[str, ...]:
        """The kinds that the model is for: the values of its ``kind`` literal. Empty for a base model."""
        annotation = cls.model_fields['kind'].annotation
        return get_args(annotation) if get_origin(annotation) is Literal else ()


class UnknownEvent(EventModel):
    """An event of a kind that no loaded module registers, such as the event of a plugin that is not installed.

    Its payload is the stored JSON without ``kind``. No reader matches it.
    """

    payload: dict[str, JsonValue]

    @model_serializer
    def _flat(self) -> dict[str, JsonValue]:
        return {'kind': self.kind, **self.payload}


def _event(value: object) -> EventModel:
    match value:
        case EventModel():
            return value
        case {'kind': str(kind)}:
            model = EventModel._registry.get(kind)
            if model is not None:
                return model.model_validate(value)
            return UnknownEvent.model_validate(
                {'kind': kind, 'payload': {k: v for k, v in value.items() if k != 'kind'}}
            )
        case _:
            raise ValueError(f'an event is a JSON object with its kind in "kind", not {value!r}')


def _dumped(event: EventModel) -> dict[str, object]:
    return event.model_dump(mode='json')


EVENT: TypeAdapter[EventModel] = TypeAdapter(Annotated[EventModel, PlainValidator(_event), PlainSerializer(_dumped)])
"""Load the payload of an ``events`` row as the model that registers its kind, or as an ``UnknownEvent``.

A registered kind validates strictly: a payload that does not fit its model raises."""


class Base(DeclarativeBase):
    """The declarative base, with the mappings for JSON, UTC datetimes and enums stored by value."""

    type_annotation_map = {
        datetime: UTCDateTime,
        CampaignId: Integer,
        IdeaId: Integer,
        RunId: Integer,
        CandidateId: Integer,
        MeasurementId: Integer,
        EvaluatorId: Integer,
        AgentSessionId: Integer,
        CommentId: Integer,
        RoleName: String,
        TrackName: String,
        Revision: String,
        dict[str, float]: JSON,
        dict[str, object]: JSON,
        Usage: PydanticJSON(TypeAdapter(Usage)),
        CampaignConfig: PydanticJSON(TypeAdapter(CampaignConfig)),
        EventModel: PydanticJSON(EVENT),
        list[str]: JSON,
        CampaignStatus: by_value(CampaignStatus),
        Pause: by_value(Pause),
        IdeaStatus: by_value(IdeaStatus),
        LinkKind: by_value(LinkKind),
        RunStatus: by_value(RunStatus),
        RunEnd: by_value(RunEnd),
        Outcome: by_value(Outcome),
        Origin: by_value(Origin),
        Cause: by_value(Cause),
        TargetKind: by_value(TargetKind),
        CommentKind: by_value(CommentKind),
        AgentSessionState: by_value(AgentSessionState),
        LeaseKind: by_value(LeaseKind),
        LeaseState: by_value(LeaseState),
        Access: by_value(Access),
        NoLease: by_value(NoLease),
        WorkerInstanceState: by_value(WorkerInstanceState),
        InstanceEnd: by_value(InstanceEnd),
    }


def _pointed_at_already(name: str, _rows: type[Base]) -> str | None:
    return f'comments and events point at {name!r} of piloop already' if name in TargetKind else None


TARGET_KINDS: Slot[type[Base]] = Slot('target_kinds', TypeAdapter(type[Base]), 'target kind', _pointed_at_already)
"""What else comments and events may point at, by name: the table of its rows. The daemon checks them when it
starts, and a ``TargetRef`` points only at a ``TargetKind``."""


class Tracked:
    """Who updated a row last, and when. Triggers keep these columns on every table whose rows change.

    ``server_onupdate=FetchedValue()`` makes the ORM expire both attributes after it sends an UPDATE. An instance then
    reads what the triggers wrote, and does not keep its old value (sessions run with ``expire_on_commit=False``).
    """

    updated_at: Mapped[datetime] = mapped_column(
        default=utcnow, server_default=NOW, server_onupdate=FetchedValue(), sort_order=1
    )
    """Set by a persistent trigger on every UPDATE, so an UPDATE in raw SQL sets it too."""
    updated_by: Mapped[str | None] = mapped_column(server_onupdate=FetchedValue(), sort_order=1)
    """The actor of the connection that made the last UPDATE (``human``, ``daemon``, ``<kind>#<agent session id>``). A TEMP
    trigger of each connection sets it (``piloop.db.sql.install_actor``, which also logs decisions in ``events``).
    NULL until the first UPDATE of the row."""


PAUSE_CHECK = 'campaigns.paused and campaigns.paused_reason go together'


class Campaign(Tracked, Base):
    """A git repository that piloop optimizes. ``piloop init`` creates it."""

    __tablename__ = 'campaigns'
    __table_args__ = (
        CheckConstraint('(paused IS NULL) = (paused_reason IS NULL)', name=PAUSE_CHECK),
        *text_checks('campaigns', nullable=('paused_reason',)),
        Index('ix_campaigns_name', 'name', unique=True, sqlite_where=text(f"status != '{CampaignStatus.RETIRED}'")),
    )

    id: Mapped[CampaignId] = mapped_column(primary_key=True)
    name: Mapped[str]
    """Unique among the campaigns that are not retired: a retired campaign's name is free again."""
    path: Mapped[str]
    """The absolute path of the repository. ``piloop.toml`` is at its root."""
    config: Mapped[CampaignConfig] = mapped_column(deferred=True)
    """The ``piloop.toml`` as the last ``piloop init`` stored it: the config that the daemon schedules from. It loads
    when it is first read, so a copy that no longer validates (a trigger of a package that is not installed) fails
    only that read."""
    evaluator_id: Mapped[EvaluatorId | None]
    """The ``evaluators.id`` that the config describes now. Measurements under older evaluators stay, but no longer
    decide verdicts. A plain integer, not a foreign key, because evaluators reference campaigns."""
    status: Mapped[CampaignStatus] = mapped_column(
        default=CampaignStatus.ACTIVE, server_default=CampaignStatus.ACTIVE.value
    )
    """``stopped`` after a ``[stop]`` condition held: running work finishes and nothing new is requested. To resume,
    set it back to ``active`` and clear ``stopped_reason``. ``piloop init`` does this when the ``[stop]`` table
    changed. ``retired`` after ``piloop campaign retire``, for good."""
    stopped_reason: Mapped[str | None]
    """The condition that ended the campaign, for example ``target: accuracy 0.8531 ≥ 0.85``."""
    paused: Mapped[Pause | None]
    """What ``piloop pause`` holds, NULL while the campaign is not paused. ``piloop resume`` clears it. Each change is a
    ``campaign.paused`` event with ``paused_reason``."""
    paused_reason: Mapped[str | None]
    """Why the campaign is paused, as ``piloop pause --reason`` said it. NULL while it is not paused."""
    repo: Mapped[str | None]
    """The repository that the campaign works on, the same for every clone of it: the URL of its ``origin`` remote
    (else of its first remote) as ``host/owner/name``, else ``path``. ``piloop init`` sets it. Past work on the same
    code is ``WHERE repo = (SELECT repo FROM campaigns WHERE id = <p>)``."""
    type: Mapped[str] = mapped_column(default=OPTIMIZATION, server_default=OPTIMIZATION)
    """What the campaign does: a campaign type, by its name. Every campaign is ``optimization``. Each change is a
    ``campaign.type`` event."""
    parent_id: Mapped[CampaignId | None] = mapped_column(ForeignKey('campaigns.id'))
    """The campaign that this one is under, for example the one that spawned it, NULL for a root. It makes a tree of
    the campaigns of one piloop home. ``piloop.campaign.ops.set_parent`` sets it, and each change is a
    ``campaign.parent_id`` event. The ``campaigns_parent`` triggers refuse a cycle. A raw DELETE of a campaign that has
    children fails on the foreign key. Nothing else reads it yet: each campaign runs on its own."""
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)

    ideas: Mapped[list[Idea]] = relationship(back_populates='campaign', order_by='Idea.id')
    evaluator: Mapped[Evaluator | None] = relationship(
        primaryjoin='foreign(Campaign.evaluator_id) == Evaluator.id', viewonly=True
    )


NOT_RETIRED = Campaign.status != CampaignStatus.RETIRED
"""The campaigns that hold their names, that the daemon schedules, and that a listing shows unless asked."""


class Evaluator(Base):
    """One version of how a campaign is scored: the evaluate command, the judging metrics and the linked inputs.

    ``piloop init`` copies it from ``piloop.toml``, and a change makes a new row. A measurement points at the
    evaluator that produced it, so a number always says what it means, and an old number is never taken for a new one.
    """

    __tablename__ = 'evaluators'
    __table_args__ = (UniqueConstraint('campaign_id', 'key'),)

    id: Mapped[EvaluatorId] = mapped_column(primary_key=True)
    campaign_id: Mapped[CampaignId] = mapped_column(ForeignKey('campaigns.id', ondelete='CASCADE'))
    key: Mapped[str]
    """A short hash of ``definition``, from ``CampaignConfig.evaluator_key``."""
    definition: Mapped[dict[str, object]]
    """``evaluate``, ``link``, and ``judging``: the name and direction of each primary and guardrail. A watch metric is
    not part of it, so adding one measures nothing again. An older row that no current config names has ``metrics``,
    every metric, instead."""
    runner: Mapped[str] = mapped_column(default=PROCESS, server_default=PROCESS)
    """The evaluator runner, by name: how piloop runs the evaluator to measure a candidate. ``process``, built in, runs
    the evaluate command as a process in a clean worktree of the candidate, and is the runner of every evaluator for
    now. ``piloop.scoring.ledger.current_evaluator`` refuses a name that no installed runner has. It is not part of
    ``key`` yet: with one runner, every evaluator has it."""
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)


class Idea(Tracked, Base):
    """Something worth trying. The scheduler turns ready ideas into runs.

    Anyone can insert an idea: a human, the orchestrator or a research agent. Ideas link to ideas of the same campaign
    through ``idea_links`` (``IdeaLink``), many-to-many in both directions.

    ``after``: the idea is blocked (``ready``, but never taken) until every prerequisite is ``done``. Its run then
    starts from the head like any other run, and the head holds every keep on the trunk that the prerequisites made.
    An idea can pin a ``base_candidate_id`` instead, for example the branch keep of a prerequisite. A prerequisite
    that ends without a keep (``parked``, ``rejected``, or ``done`` after runs that kept nothing) never releases its
    dependents: the daemon parks them with an escalation, and the orchestrator or a human deletes the link or sets
    them ready again. Cycles of prerequisites are not detected: the ideas in a cycle never run and show as blocked.

    ``supersedes``: the idea is a sharper version of the targets. A research sweep uses it to refine rejected, parked
    or done ideas instead of proposing near-duplicates. The daemon rejects a superseded idea that never ran (it has
    no run and is still ``seed``, ``proposed`` or ``ready``). A superseded idea that ran keeps its status and results.
    """

    __tablename__ = 'ideas'
    __table_args__ = (
        UniqueConstraint('campaign_id', 'title'),
        # the queue: the pass and the widget read the ``ready`` ideas of one campaign in priority order
        Index('ix_ideas_campaign_status_priority', 'campaign_id', 'status', 'priority'),
        *text_checks(
            'ideas', 'title', 'hypothesis', 'created_by', 'track', nullable=('plan', 'harness', 'provenance', 'family')
        ),
    )

    id: Mapped[IdeaId] = mapped_column(primary_key=True)
    campaign_id: Mapped[CampaignId] = mapped_column(ForeignKey('campaigns.id', ondelete='CASCADE'))
    title: Mapped[str]
    """Unique in a campaign, so two research sweeps that pick the same title fail here and do not duplicate the idea."""
    hypothesis: Mapped[str]
    """Why this idea should improve the primary metric."""
    track: Mapped[TrackName]
    """The track (objective) the idea works on, by its name in ``piloop.toml``: a primary's ``track``, or else the
    primary's name. Its runs are judged on that track's primary, with the primary of every other track as a guardrail,
    and their brief has the track's addendum. The scheduler parks an idea whose track is not in ``piloop.toml``, with
    a note that says why."""
    plan: Mapped[str | None]
    """Concrete steps for the implementer. They are appended to the brief."""
    status: Mapped[IdeaStatus] = mapped_column(default=IdeaStatus.READY, server_default=IdeaStatus.READY.value)
    priority: Mapped[int] = mapped_column(default=2, server_default=text('2'))
    """0 (urgent) to 4 (someday). The scheduler takes the lowest number first."""
    harness: Mapped[str | None]
    """The harness for this idea. None lets the scheduler use the campaign's harnesses in turn."""
    base_candidate_id: Mapped[CandidateId | None]
    """A deliberate branch: the ``candidates.id`` to start from, instead of the head at the time the run starts. NULL
    unless the idea gives a reason for that starting point. Never pin the current head by default: when the idea
    runs, that tree is out of date. The ``run.queued`` event of a pinned run records both bases. A plain integer, as
    on runs."""
    max_runs: Mapped[int] = mapped_column(default=2, server_default=text('2'))
    """The number of attempts allowed. A crashed run is tried again until this number is reached."""
    created_by: Mapped[str]
    """Who proposed it: ``human``, ``orchestrator``, or the agent's signature (``research#12``, ``implementer#40``,
    ``watchdog#7``). The part before ``#`` is the idea's source in the count of keeps by source."""
    provenance: Mapped[str | None]
    """The finding that led to the idea, with ids or a URL. For example ``finding comment#81: timeouts lose the last
    rows``, ``loss table workspace/research/losses.md`` or ``arXiv 2305.11860 via exa``."""
    family: Mapped[str | None]
    """The mechanism family: a short lowercase name for ideas that try the same mechanism (``feature engineering``,
    ``ensembling``, ``test-time training``). The research pack shows what each family produced."""
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)

    campaign: Mapped[Campaign] = relationship(back_populates='ideas')
    runs: Mapped[list[Run]] = relationship(back_populates='idea', order_by='Run.id')
    links: Mapped[list[IdeaLink]] = relationship(
        foreign_keys='IdeaLink.idea_id',
        back_populates='idea',
        order_by='IdeaLink.target_id',
        cascade='all, delete-orphan',
        passive_deletes=True,
    )
    """The links from this idea (it is the ``idea_id``). Query ``IdeaLink.target_id`` for the links to it."""

    def targets(self, kind: LinkKind) -> list[Idea]:
        """The ideas that this one links to with ``kind``, by id."""
        return [link.target for link in self.links if link.kind is kind]

    @property
    def after(self) -> list[Idea]:
        """The prerequisites. Each one must be ``done`` before this idea runs."""
        return self.targets(LinkKind.AFTER)

    @property
    def supersedes(self) -> list[Idea]:
        """The older ideas that this one is a sharper version of."""
        return self.targets(LinkKind.SUPERSEDES)


class IdeaLink(Base):
    """A directed link between two ideas of one campaign: ``idea_id`` comes *after* or *supersedes* ``target_id``.

    Links are many-to-many in both directions. An idea may wait for several prerequisites, several ideas may wait for
    it, and a sharper version may replace several earlier ideas. One pair may have both kinds. Rows are inserted and
    deleted, never updated, so they have no ``updated_at`` or ``updated_by``. A CHECK refuses a link from an idea to
    itself, and a trigger refuses a link across campaigns. When either idea is deleted, the link is deleted too. To add
    a kind of link, add a ``LinkKind`` member (which changes the CHECK) and a rule for it in
    ``piloop/campaign/ideas.py``.
    """

    __tablename__ = 'idea_links'
    __table_args__ = (
        CheckConstraint('idea_id != target_id', name='idea_links_not_self'),
        Index('ix_idea_links_target', 'target_id', 'kind'),
    )

    idea_id: Mapped[IdeaId] = mapped_column(ForeignKey('ideas.id', ondelete='CASCADE'), primary_key=True)
    target_id: Mapped[IdeaId] = mapped_column(ForeignKey('ideas.id', ondelete='CASCADE'), primary_key=True)
    kind: Mapped[LinkKind] = mapped_column(primary_key=True)

    idea: Mapped[Idea] = relationship(foreign_keys=[idea_id], back_populates='links')
    target: Mapped[Idea] = relationship(foreign_keys=[target_id])


class Run(Tracked, Base):
    """One attempt at an idea, by one agent of a role that does runs, with one harness and model."""

    __tablename__ = 'runs'
    __table_args__ = (
        UniqueConstraint('idea_id', 'attempt'),
        # what is running or queued: reap, drain and the agents' "what is running" query select by status only
        Index('ix_runs_status', 'status'),
        CheckConstraint(
            f'scoring = 0 OR status IN ({", ".join(repr(s.value) for s in FINISHED_RUNS)})',
            name='runs.scoring needs a finished status',
        ),
        CheckConstraint(
            f'("end" IS NULL) = (status IN ({", ".join(repr(s.value) for s in OPEN_RUNS)}))',
            name='runs.end is set once a run is finished',
        ),
        CheckConstraint(f'"end" IS NULL OR status = {_end_status()}', name='runs.status follows from runs.end'),
    )

    id: Mapped[RunId] = mapped_column(primary_key=True)
    idea_id: Mapped[IdeaId] = mapped_column(ForeignKey('ideas.id', ondelete='CASCADE'))
    role: Mapped[RoleName] = mapped_column(default=IMPLEMENTER, server_default=IMPLEMENTER)
    """The role that does the run: ``implementer``, or a role of the campaign with ``runs = true``. Its agents have this
    ``agent_sessions.kind``."""
    attempt: Mapped[int]
    base_candidate_id: Mapped[CandidateId]
    """The ``candidates.id`` that the worktree started from. The run's candidates descend from it. A plain integer,
    not a foreign key: candidates reference runs, and SQLite cannot add a foreign key for a cycle afterwards."""
    branch: Mapped[bool] = mapped_column(default=False, server_default=text('0'))
    """Whether the run started from an alternative starting point: its idea pinned a base other than the head when it
    was queued (a reference, an older keep or a note). The run is judged against its own base only, so what it keeps
    is a branch keep, which never moves the head. The scheduler sets it (``explore_every`` of the run's role)."""
    harness: Mapped[str]
    model: Mapped[str]
    status: Mapped[RunStatus] = mapped_column(default=RunStatus.QUEUED, server_default=RunStatus.QUEUED.value)
    scoring: Mapped[bool] = mapped_column(default=False, server_default=text('0'))
    """Whether the daemon scores the run's commits now. It is set when the implementer ends and the run gets its final
    ``status``, and cleared when the commits are judged and the idea moved on, whatever the idea's status. While it is
    set, the run keeps its place under ``[stop] max_runs``, and a restarted daemon resumes the scoring."""
    brief_cid: Mapped[Cid | None] = mapped_column(CidText)
    """The exact prompt that the implementer got, a text blob in the CAS. NULL until the run starts."""
    started_at: Mapped[datetime | None]
    finished_at: Mapped[datetime | None]
    end: Mapped[RunEnd | None]
    """How the run ended. NULL while it is queued or running. ``status`` follows from it."""
    error: Mapped[str | None]
    """The detail of the end, for a human: the process error, the escalation that waits, or why it failed."""
    log_dir: Mapped[str | None]
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)

    idea: Mapped[Idea] = relationship(back_populates='runs')
    base: Mapped[Candidate] = relationship(primaryjoin='foreign(Run.base_candidate_id) == Candidate.id', viewonly=True)
    candidates: Mapped[list[Candidate]] = relationship(
        back_populates='run', foreign_keys='Candidate.run_id', order_by='Candidate.id'
    )


def campaign_runs(campaign_id: CampaignId) -> Select[Run]:
    """Every run of the campaign. A run has no ``campaign_id``, but its idea has one."""
    return select(Run).join(Idea).where(Idea.campaign_id == campaign_id)


def count[*Ts](s: Session, query: Select[*Ts]) -> int:
    """The number of rows that ``query`` selects."""
    return s.scalar(select(func.count()).select_from(query.subquery())) or 0


def latest_agent_session(s: Session, run_id: RunId) -> AgentSession | None:
    """The latest agent row that did run ``run_id``. A relaunch after the agent failed to start is a second row."""
    return s.scalars(
        select(AgentSession).where(AgentSession.run_id == run_id).order_by(AgentSession.id.desc()).limit(1)
    ).first()


class Candidate(Tracked, Base):
    """A distinct state of the campaign's code. The git tree identifies it, so equal contents are one candidate.

    The baseline has no run and no parent. A reference has neither (``piloop candidate import`` makes one from the
    tree of any commit, pinned at ``refs/piloop/<campaign>/reference/<id>``). An implementer committed every other candidate,
    and it descends from the candidate that its run started from. When the last commit of a run reverts to its base,
    the run produces nothing new: the commit resolves to the base candidate. Measurements score candidates, and the
    verdict judges them against a bar.
    """

    __tablename__ = 'candidates'
    __table_args__ = (
        UniqueConstraint('campaign_id', 'tree_sha'),
        Index('ix_candidates_campaign_verdict', 'campaign_id', 'verdict'),
        # the candidates of a run (every history row), and the FK action when a run is deleted
        Index('ix_candidates_run', 'run_id'),
        # the FK check on every delete of a candidate (deleting a campaign deletes thousands)
        Index('ix_candidates_parent', 'parent_id'),
        *text_checks('candidates', nullable=('summary', 'verdict_by', 'verdict_reason')),
        CheckConstraint(
            f'verdict IS NULL OR origin IN ({", ".join(repr(o.value) for o in sorted(JUDGED))})',
            name='candidates.verdict needs a judged origin',
        ),
        CheckConstraint(
            f"cause IS NULL OR verdict = '{Outcome.INTERESTING}'", name='candidates.cause needs interesting'
        ),
    )

    id: Mapped[CandidateId] = mapped_column(primary_key=True)
    campaign_id: Mapped[CampaignId] = mapped_column(ForeignKey('campaigns.id', ondelete='CASCADE'))
    run_id: Mapped[RunId | None] = mapped_column(ForeignKey('runs.id', ondelete='SET NULL'))
    parent_id: Mapped[CandidateId | None] = mapped_column(ForeignKey('candidates.id'))
    """The candidate that this one comes from."""
    tree_sha: Mapped[str]
    """The hash of the contents. Two commits with the same tree are the same candidate."""
    revision: Mapped[Revision]
    """The vcs provider's name for one version that has this tree, which it keeps (``Repository.keep``)."""
    summary: Mapped[str | None]
    """The first line of what the version says about itself."""
    origin: Mapped[Origin] = mapped_column(default=Origin.RUN, server_default=Origin.RUN.value)
    verdict: Mapped[Outcome | None]
    """The judgement against the candidate's bar. NULL while it is pending, and on a candidate whose origin is not
    judged."""
    cause: Mapped[Cause | None]
    verdict_by: Mapped[str | None]
    verdict_reason: Mapped[str | None]
    """The reason of the verdict, for a human."""
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)

    run: Mapped[Run | None] = relationship(back_populates='candidates', foreign_keys=[run_id])
    parent: Mapped[Candidate | None] = relationship(remote_side=[id], foreign_keys=[parent_id])
    measurements: Mapped[list[Measurement]] = relationship(back_populates='candidate', order_by='Measurement.id')


class Measurement(Base):
    """One run of an evaluator on a candidate. A candidate can have many: repeats, or a new version of the evaluator."""

    __tablename__ = 'measurements'
    __table_args__ = (Index('ix_measurements_candidate_evaluator', 'candidate_id', 'evaluator_id'),)

    id: Mapped[MeasurementId] = mapped_column(primary_key=True)
    candidate_id: Mapped[CandidateId] = mapped_column(ForeignKey('candidates.id', ondelete='CASCADE'))
    evaluator_id: Mapped[EvaluatorId] = mapped_column(ForeignKey('evaluators.id'))
    metrics: Mapped[dict[str, float]] = mapped_column(default=dict, server_default=EMPTY_JSON)
    error: Mapped[str | None]
    """Set when the evaluate command failed. ``metrics`` then has what it printed before it failed."""
    seconds: Mapped[float | None]
    pair_id: Mapped[MeasurementId | None] = mapped_column(ForeignKey('measurements.id', ondelete='SET NULL'))
    """The bar's measurement that ran next to this one in a paired evaluation. NULL on any other measurement."""
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)

    candidate: Mapped[Candidate] = relationship(back_populates='measurements')
    evaluator: Mapped[Evaluator] = relationship()
    pair: Mapped[Measurement | None] = relationship(remote_side=[id])


class Evaluation(Base):
    """An evaluation that the daemon needs: it waits for a slot of the executor or runs. The evaluation queue.

    The daemon inserts a row when it needs a measurement, and deletes it when the measurement is recorded, so the table
    holds only what waits or runs. A free slot goes to the campaign with the fewest running rows, and in it to its head
    first, then to the lowest id (``piloop.scoring.queue``). The requests for one candidate under one evaluator share
    one row: a tree is evaluated once. An id is never reused, so a request that waits for a row never mistakes a later
    one for it. A daemon that starts deletes the rows that the previous one left, and asks again for what it still
    needs. ``piloop eval queue`` lists the rows, and ``piloop eval cancel`` cancels one.
    """

    __tablename__ = 'evaluations'
    __table_args__ = (
        UniqueConstraint('candidate_id', 'evaluator_id'),
        *text_checks('evaluations', nullable=('cancelled_by',)),
        {'sqlite_autoincrement': True},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    candidate_id: Mapped[CandidateId] = mapped_column(ForeignKey('candidates.id', ondelete='CASCADE'))
    evaluator_id: Mapped[EvaluatorId] = mapped_column(ForeignKey('evaluators.id'))
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)
    started_at: Mapped[datetime | None]
    """When it got a slot. NULL while it waits."""
    cancelled_by: Mapped[str | None]
    """Who cancelled it while it ran. Their crashed measurement is recorded; the daemon stops the evaluator."""

    candidate: Mapped[Candidate] = relationship()


class Lease(Base):
    """A lease (``piloop.plugins.executor.kit.leases``): a time-limited claim on one job by one worker (``piloop.plugins.executor.kit.leases``).

    The table is in core's schema until a plugin can own tables. ``WorkersExecutor.submit`` inserts a ``queued`` lease that names its job by address (``job_cid``). A worker acquires it, starts the job and
    renews the lease with a heartbeat every ``renew_seconds``. The id is the **fencing token**: it only grows, and
    a job's next lease always has a larger one. A report applies only on the live, newest lease of its job, so a worker
    that was presumed dead cannot overwrite newer work. Rows are kept: a refused report stays as a
    ``lease.stale_report`` event on the job's row.
    """

    __tablename__ = 'leases'
    __table_args__ = (
        CheckConstraint(
            "(kind = 'agent') = (agent_session_id IS NOT NULL) AND (kind = 'evaluation') = (evaluation_id IS NOT NULL)",
            name='leases.a lease has the job of its kind',
        ),
        Index('ix_leases_state', 'state'),
        Index('ix_leases_agent_session', 'agent_session_id'),
        Index('ix_leases_evaluation', 'evaluation_id'),
        Index('ix_leases_acquire_request', 'acquire_request', unique=True),
        *text_checks('leases', nullable=('worker', 'report_request')),
        {'sqlite_autoincrement': True},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    """The fencing token. AUTOINCREMENT: an id is never used again, so a newer lease always has a larger one."""
    kind: Mapped[LeaseKind]
    agent_session_id: Mapped[AgentSessionId | None] = mapped_column(ForeignKey('agent_sessions.id', ondelete='CASCADE'))
    """The job of an ``agent`` lease."""
    evaluation_id: Mapped[int | None]
    """The job of an ``evaluation`` lease: an ``evaluations`` id. No foreign key: the queue row goes when it is
    measured, and the lease stays."""
    state: Mapped[LeaseState] = mapped_column(default=LeaseState.QUEUED, server_default=LeaseState.QUEUED.value)
    worker: Mapped[str | None]
    """The worker that acquired it. NULL while it is queued."""
    job_cid: Mapped[Cid] = mapped_column(CidText)
    """The job description (``AgentJob``, ``EvaluationJob``), a JSON blob in the CAS: what to materialize, what to run,
    and what to return. One job queued again on a new lease is one blob."""
    handle: Mapped[dict[str, object]] = mapped_column(default=dict, server_default=EMPTY_JSON)
    """What the worker needs to find its job again after a restart: the runtime's handle of an agent, or the process
    group of an evaluator."""
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)
    acquired_at: Mapped[datetime | None]
    started_at: Mapped[datetime | None]
    renewed_at: Mapped[datetime | None]
    """The last heartbeat, or the acquire."""
    expires_at: Mapped[datetime | None]
    """``renewed_at`` plus ``toleration_seconds``. Past it, the scheduler expires the lease."""
    revoked_at: Mapped[datetime | None]
    """When the daemon asked the worker to stop the job: a kill or a cancel. The worker still reports it."""
    ended_at: Mapped[datetime | None]
    acquire_request: Mapped[str | None]
    """The request id of the acquire that took it. An acquire sent again with it gets this lease again."""
    report_request: Mapped[str | None]
    """The request id of the report that applied. A report sent again with it gets the same ``receipt``."""
    receipt: Mapped[dict[str, object]] = mapped_column(default=dict, server_default=EMPTY_JSON)
    """What the report applied, or why the lease ended."""


class Token(Base):
    """A bearer token of the server of ``piloop serve`` and ``piloop cas serve`` (``piloop.net.tokens``).

    Only the SHA-256 of the secret is kept: the secret is shown once, when the token is created. ``piloop sql`` cannot
    write this table, so an agent cannot add a token. Revoking a token deletes its row.
    """

    __tablename__ = 'tokens'

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(unique=True)
    """The caller's name, such as a worker's id, or the signature of the agent that the token is bound to."""
    access: Mapped[Access]
    sha256: Mapped[str] = mapped_column(unique=True)
    """The hex SHA-256 of the secret."""
    agent_session_id: Mapped[AgentSessionId | None] = mapped_column(unique=True)
    """The agent session that the token was minted for when it launched: it acts as that agent while the session lives.

    No foreign key: its cascade would write this table, which an agent's ``piloop sql`` may not, so an agent's delete of
    a campaign would fail. A token whose session is gone acts as nobody. A session that reuses its id gets its own token
    at its start (``mint_agent``), in place of the old one.
    """
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)


class Migration(Base):
    """Core or a plugin that migrated this database: one row each (``piloop.db.schema``). Only ``migrate`` writes it.

    ``fingerprint`` is that of its revisions up to the one in its version table (``fingerprints``).
    """

    __tablename__ = 'migrations'

    name: Mapped[str] = mapped_column(primary_key=True)
    """``core``, or a plugin's schema name, such as ``memory/local``."""
    package: Mapped[str]
    """The distribution that declared the schema: what to install again when it is missing."""
    fingerprint: Mapped[int]


class LeaseWorker(Base):
    """A worker that takes leases (``piloop.plugins.executor.kit.leases``): its pool, and what it last sent.

    The table is in core's schema until a plugin can own tables. A worker's row is written when it starts, and on each
    heartbeat. A worker whose ``seen_at`` is older than ``grace_seconds`` is unreachable: its slots count zero.
    """

    __tablename__ = 'lease_workers'
    __table_args__ = (*text_checks('lease_workers', 'id', 'pool', 'version'),)

    id: Mapped[str] = mapped_column(primary_key=True)
    """The worker's name. ``serve`` is the worker inside ``piloop serve``."""
    pool: Mapped[str]
    """Its pool: ``[plugins.executor.pools.<pool>]`` gives its slots and thresholds."""
    version: Mapped[str]
    """The piloop version it runs."""
    runners: Mapped[list[str]] = mapped_column(default=list, server_default=text("'[]'"))
    """The evaluator runners it has. It takes only the evaluations of these."""
    sample: Mapped[dict[str, object]] = mapped_column(default=dict, server_default=EMPTY_JSON)
    """Its last host sample (``HostSample``): load, CPUs, memory available and disk free."""
    seen_at: Mapped[datetime]
    """When a call of it last came in."""
    no_lease: Mapped[NoLease | None]
    """Why its last request got no lease, or that it went silent. NULL after a grant."""
    cursor: Mapped[int] = mapped_column(default=0, server_default=text('0'))
    """The event after which it reads its messages: its ack floor. A new worker starts at the newest event."""
    drained_at: Mapped[datetime | None]
    """When the server drained it (``lease.drained``): it takes no more leases. NULL while it takes them."""


class WorkerInstance(Base):
    """A worker that a provisioner starts for a pool (``piloop.plugins.executor.kit.scaling``).

    The row commits before the provisioner's ``start``, so a daemon that restarts finds every instance that it asked
    for. Its name is the worker's everywhere: its credential, its ``lease_workers`` row, the provider's tag. The table
    is in core's schema until a plugin can own tables, as the leases are.
    """

    __tablename__ = 'worker_instances'
    __table_args__ = (
        CheckConstraint('(state = \'ended\') = ("end" IS NOT NULL)', name='worker_instances.an ended one says why'),
        Index('ix_worker_instances_state', 'state'),
        *text_checks('worker_instances', 'name', 'pool'),
        {'sqlite_autoincrement': True},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(unique=True)
    """The worker's name, which core picks: ``<pool>-<8 hex digits>``."""
    pool: Mapped[str]
    state: Mapped[WorkerInstanceState] = mapped_column(
        default=WorkerInstanceState.STARTING, server_default=WorkerInstanceState.STARTING.value
    )
    handle: Mapped[dict[str, object]] = mapped_column(default=dict, server_default=EMPTY_JSON)
    """The provider's own id of it, as its ``list`` last showed it: a sandbox id, a pod uid."""
    requested_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)
    registered_at: Mapped[datetime | None]
    """When its worker first registered."""
    ended_at: Mapped[datetime | None]
    end: Mapped[InstanceEnd | None]


class FirstTransfer(Base):
    """What a worker's empty clone of a repo of record starts from: its versions up to ``revision``, as one blob.

    The server builds it once per repo (``piloop.plugins.executor.kit.transfers``). The table is in core's schema until
    a plugin can own tables, as the leases are.
    """

    __tablename__ = 'first_transfers'
    __table_args__ = (*text_checks('first_transfers', 'revision', 'blob'),)

    campaign_id: Mapped[CampaignId] = mapped_column(ForeignKey('campaigns.id', ondelete='CASCADE'), primary_key=True)
    """The repo of record (``RepoId``)."""
    revision: Mapped[Revision]
    """The head of the repo of record when it was built."""
    blob: Mapped[Cid] = mapped_column(CidText)


class Limit(Base):
    """A live limit (``piloop limits set``): a value for one ``Live`` key of a provider's parameters, over ``config.toml``.

    The daemon applies the rows at once, without a restart. The table is in core's schema, as the leases are.
    """

    __tablename__ = 'limits'
    __table_args__ = (*text_checks('limits', 'key', 'reason', 'actor'),)

    key: Mapped[str] = mapped_column(primary_key=True)
    """The dotted key: the capability, then the path in its parameters, ``executor.pools.serve.evaluations``."""
    value: Mapped[JsonValue] = mapped_column(JSON, nullable=False)
    """JSON: a null is a value too, as ``agents = null``, no bound."""
    reason: Mapped[str]
    actor: Mapped[str]
    """Who set it."""
    at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)


class Secret(Base):
    """A secret of the ``local`` secrets provider (``piloop secret set``): a value that agents get in their environment.

    ``piloop sql`` cannot write the table, and reads ``value`` as NULL. Who set a secret, and when, is in its
    ``secret.set`` events. The table is in core's schema, as the leases are.
    """

    __tablename__ = 'secrets'
    __table_args__ = (
        UniqueConstraint('campaign_id', 'name'),
        Index('ux_secrets_home_name', 'name', unique=True, sqlite_where=text('campaign_id IS NULL')),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    campaign_id: Mapped[CampaignId | None] = mapped_column(ForeignKey('campaigns.id'))
    """The campaign whose agents get it, over the home's of the same name. NULL: the home's, for every campaign. A
    campaign with secrets is deleted after them: ``piloop sql`` cannot delete them, and a cascade would."""
    name: Mapped[str]
    """An environment variable's name."""
    value: Mapped[str]


class Comment(Tracked, Base):
    """The forum and the human's inbox. Anything with an id can get a comment: ``idea#12``, ``run#7``, ``candidate#30``.

    A comment with a ``priority`` is for the human. It stays in the inbox (``piloop inbox``) until someone acknowledges
    it (``acked_at``, ``acked_by``). An escalation always has a priority (a CHECK makes sure). The orchestrator gives
    a priority to every decision it makes for the human, so that the human can review and change each one later. The
    rest of the forum (implementers that steer each other, research findings) has no priority.
    """

    __tablename__ = 'comments'
    __table_args__ = (
        Index('ix_comments_target', 'target_kind', 'target_id'),
        # the inbox: the widget counts the items not acknowledged on every refresh, and the CLI lists them by priority
        Index('ix_comments_inbox', 'priority', 'id', sqlite_where=text('priority IS NOT NULL AND acked_at IS NULL')),
        CheckConstraint("kind != 'escalation' OR priority IS NOT NULL", name='escalation_needs_priority'),
        CheckConstraint("in_reply_to IS NULL OR target_kind = 'comment'", name='reply_is_on_a_comment'),
        *text_checks('comments', 'author', 'body'),
    )

    id: Mapped[CommentId] = mapped_column(primary_key=True)
    target_kind: Mapped[TargetKind] = mapped_column(CommentTarget())
    target_id: Mapped[int]
    target: Mapped[TargetRef] = composite('target_kind', 'target_id')
    in_reply_to: Mapped[CommentId | None] = mapped_column(ForeignKey('comments.id', ondelete='SET NULL'))
    """The comment that a reply answers, as email's In-Reply-To. The target of a reply stays the root of its thread.

    NULL for a comment that is not a reply, and for a reply written before piloop recorded it.
    """
    author: Mapped[str]
    """Who wrote the comment, for example ``human``, ``orchestrator`` or ``implementer#12``."""
    kind: Mapped[CommentKind] = mapped_column(default=CommentKind.NOTE, server_default=CommentKind.NOTE.value)
    body: Mapped[str]
    priority: Mapped[int | None]
    """When set, the comment is for the human: 0 (urgent, notify now) to 4 (someday), as on ideas. NULL: forum only."""
    acked_at: Mapped[datetime | None]
    """When someone acknowledged the inbox item. NULL while it waits in the inbox."""
    acked_by: Mapped[str | None]
    """Who acknowledged it: ``human`` with ``piloop inbox ack``, or ``orchestrator`` when the orchestrator did."""
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)


class Technique(Base):
    """The technique library: outside techniques that a scout sweep found and did not file as a seed. One row per link.

    A scout reads it before it searches, and checks every technique it finds against it and ``ideas_fts`` (plain
    text, ``LIKE``), so each technique is filed once. When a later loss makes a technique relevant, it becomes a seed
    that cites ``technique#<id>``. Agents write it with raw SQL. Rows are never updated.
    """

    __tablename__ = 'techniques'
    __table_args__ = (
        UniqueConstraint('campaign_id', 'url'),
        *text_checks('techniques', 'title', 'mechanism', 'url', 'created_by'),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    campaign_id: Mapped[CampaignId] = mapped_column(ForeignKey('campaigns.id', ondelete='CASCADE'))
    title: Mapped[str]
    """The technique, in a few words."""
    mechanism: Mapped[str]
    """One line: how it works, and the evidence for it (a number, code, an ablation)."""
    url: Mapped[str]
    """The primary source: a post, repo or paper. Unique in a campaign, so the same link is never filed twice."""
    created_by: Mapped[str]
    """Who filed it: the scout (``scout#<agent session id>``) or someone else."""
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)


class AgentSession(Tracked, Base):
    """An agent process in one of the roles. While ``state`` is ``requested``, the row is a request to start one.

    A request needs only ``kind`` and ``campaign_id``. ``harness`` and ``model`` pin the rotation, and ``reason`` says
    why. The daemon fills in the rest when it launches the row, so the table is also the launch queue of every role.
    """

    __tablename__ = 'agent_sessions'
    __table_args__ = (
        # the launch queue of the pass: the requested and active rows of a campaign for each role, and its latest row
        Index('ix_agent_sessions_campaign_kind_state', 'campaign_id', 'kind', 'state'),
        # the session behind the candidates of a run (every kept-lineage row), and the FK action when a run is deleted
        Index('ix_agent_sessions_run', 'run_id'),
        *text_checks('agent_sessions', 'kind', nullable=('reason', 'harness', 'model')),
    )

    id: Mapped[AgentSessionId] = mapped_column(primary_key=True)
    kind: Mapped[RoleName]
    """The agent's role: a role of its campaign, a built-in one or a ``[roles.<name>]`` table. A trigger refuses other
    names."""
    campaign_id: Mapped[CampaignId | None] = mapped_column(ForeignKey('campaigns.id', ondelete='CASCADE'))
    """The campaign the agent works for. piloop always sets it, but readers must accept NULL."""
    run_id: Mapped[RunId | None] = mapped_column(ForeignKey('runs.id', ondelete='SET NULL'))
    """The run that the row does, for a role that does runs. Sweeping roles have none."""
    harness: Mapped[str | None]
    model: Mapped[str | None]
    runtime: Mapped[str | None]
    """The runtime of the agent (``runtime`` in ``piloop.toml``): how to reach the process."""
    handle: Mapped[dict[str, object]] = mapped_column(default=dict, server_default=EMPTY_JSON)
    """The address of the process in its runtime. Each runtime defines its keys, for example a pid or a pane."""
    state: Mapped[AgentSessionState] = mapped_column(
        default=AgentSessionState.REQUESTED, server_default=AgentSessionState.REQUESTED.value
    )
    reason: Mapped[str | None]
    """Why it was requested: the trigger that fired, or what the requester wrote."""
    log_dir: Mapped[str | None]
    started_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)
    """The time of the request until the launch, then the time of the launch."""
    ended_at: Mapped[datetime | None]
    error: Mapped[str | None]
    session_id: Mapped[str | None]
    """The harness's own session id. piloop creates it at launch when the harness takes it as an argument
    (``{session_id}``), or the runtime reports it after the agent starts."""
    machine: Mapped[str | None]
    """The machine that runs the agent, as its executor names it (a worker of ``piloop worker``). NULL: the daemon's."""
    session_file: Mapped[Cid | None] = mapped_column(CidText)
    """The harness's own session file, as a chunked file in the CAS (``piloop cas get``): a snapshot a few seconds after
    the agent writes, at least every 30 s while it runs, and the whole file once it ended. NULL until the first
    snapshot, and for a harness without a usage spec."""
    usage: Mapped[Usage | None]
    """The tokens the agent spent, added up from the harness's own session file when the agent ended:
    ``{"input", "output", "cache_read", "cache_write", "total", "turns"}``. ``total`` is what the harness itself counts
    as the session's tokens (sum ``json_extract(usage, '$.total')`` for the spend). ``turns`` is the number of model
    responses. For every harness, ``input`` is the uncached input, and ``cache_read`` and ``cache_write`` the input
    read from and written to the prompt cache. NULL while the agent runs, or when no session file was found
    (``agent_session.usage_unknown``)."""


class Event(Base):
    """The append-only feed of what happened. The daemon pushes comments to implementers from it, reading from a cursor.

    It has two writers: ``piloop.db.events.emit``, for what only piloop knows, and the triggers of the change log
    (``piloop.db.triggers.changelog``), for what any connection does to the rows. The authorizer of ``piloop sql``
    denies any other insert. The payload is an ``EventModel``, with its ``kind``.
    """

    __tablename__ = 'events'
    __table_args__ = (
        Index('ix_events_target', 'target_kind', 'target_id', 'id'),
        # the asks of a kind and whether a newer one exists (``piloop.db.requests.pending``)
        Index('ix_events_kind', 'kind', 'target_kind', 'target_id', 'id'),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)
    kind: Mapped[str] = mapped_column(Computed("json_extract(payload, '$.kind')"))
    """``<subject>.<what>``, for example ``run.started``, ``candidate.judged``, ``idea.status``, ``comment.decision``
    or ``idea.deleted``: the payload's ``kind``."""
    actor: Mapped[str]
    target_kind: Mapped[TargetKind]
    target_id: Mapped[int]
    target: Mapped[TargetRef] = composite('target_kind', 'target_id')
    payload: Mapped[EventModel]


_warned: set[tuple[str, str, str]] = set()


@event.listens_for(Base, 'load', propagate=True)
def _blob_placeholders(target: Base, _context: object) -> None:
    """Replace bytes in a text column (a BLOB that raw SQL stored) with a placeholder, and warn once.

    Agents write raw SQL, and one such row must not make every view of its table fail. The value is replaced in memory
    only, never written back. Once per process, stderr says which row it is and how to repair it. ``text_checks``
    keeps BLOBs out of the columns that agents write; this covers the other columns.
    """
    state = inspect(target)
    for attr in state.mapper.column_attrs:
        value, column = target.__dict__.get(attr.key), attr.columns[0]
        if not isinstance(value, bytes) or not isinstance(column, Column) or not isinstance(column.type, String):
            continue
        set_committed_value(target, attr.key, f'<not text: a {len(value)}-byte blob>')
        table, row = column.table.name, ', '.join(map(str, state.identity or ()))
        if (table, row, attr.key) not in _warned:
            _warned.add((table, row, attr.key))
            print(
                f'piloop: warning: {table}#{row} {attr.key} is a {len(value)}-byte blob, not text; shown as a'
                f' placeholder. Repair it: UPDATE {table} SET {attr.key} = CAST({attr.key} AS TEXT) WHERE id = {row}',
                file=sys.stderr,
            )
