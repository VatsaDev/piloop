"""Storage models. The database is the contract that every agent reads and writes through ``piloop sql``.

Every NOT NULL column with a default also has a server default, so the raw-SQL inserts in the docs work without
timestamps or statuses. ``docs/reference/schema.mdx`` (the DDL) is generated from these models.
"""

import sys
from datetime import UTC, datetime
from enum import StrEnum
from typing import TypedDict

from sqlalchemy import (
    JSON,
    CheckConstraint,
    Column,
    DateTime,
    Enum,
    FetchedValue,
    ForeignKey,
    Index,
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
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship
from sqlalchemy.orm.attributes import set_committed_value
from sqlalchemy.types import TypeDecorator


def utcnow() -> datetime:
    """The current time, with the UTC time zone."""
    return datetime.now(UTC)


class UTCDateTime(TypeDecorator[datetime]):
    """A datetime that is always UTC. SQLite drops the time zone when it stores a value, so this type adds it back."""

    impl = DateTime
    cache_ok = True

    def process_result_value(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        return value.replace(tzinfo=UTC) if value is not None and value.tzinfo is None else value


class ProjectStatus(StrEnum):
    """Whether the daemon still requests work for a project. ``stopped`` after a ``[stop]`` condition held."""

    ACTIVE = 'active'
    STOPPED = 'stopped'


class IdeaStatus(StrEnum):
    """The lifecycle of an idea. The scheduler picks up ``ready`` ideas in priority order.

    A ``seed`` is a rough idea with pointers. Any role except the worker can file one (``piloop idea seed``). Research
    develops it into a defined idea (``piloop idea develop``, which makes it ``ready`` or ``proposed``), or rejects it.
    A ``proposed`` idea is defined and waits for a human to approve it.
    """

    SEED = 'seed'
    PROPOSED = 'proposed'
    READY = 'ready'
    SCHEDULED = 'scheduled'
    DONE = 'done'
    PARKED = 'parked'
    REJECTED = 'rejected'


class LinkKind(StrEnum):
    """How one idea relates to another in ``idea_links``: it runs *after* the target, or it *supersedes* the target."""

    AFTER = 'after'
    SUPERSEDES = 'supersedes'


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


class Verdict(StrEnum):
    """The judgement on a candidate, made from its measurements against its bar (see https://piloop.autoresearch.inc/concepts/verdict).

    The daemon sets a verdict when it scores a candidate. Any agent or human may change it, and should say why in a
    comment. ``reference`` is not a judgement: it marks a starting point imported with ``piloop candidate import``.
    The daemon measures it with the current evaluator, and a pinned run may start from it. But it is never the head or
    a bar, and nothing that counts keeps counts it (the status, the sparkline, ``[stop]``). An ``interesting`` verdict
    whose reason starts with ``note:`` is an agent's note (``piloop candidate note``, the ``Piloop-Note`` trailer): the
    result got worse, but the tree is worth starting from.
    """

    PENDING = 'pending'
    KEEP = 'keep'
    INTERESTING = 'interesting'
    DISCARD = 'discard'
    CRASH = 'crash'
    REFERENCE = 'reference'


class TargetKind(StrEnum):
    """What a comment or an event points at."""

    PROJECT = 'project'
    IDEA = 'idea'
    RUN = 'run'
    CANDIDATE = 'candidate'
    MEASUREMENT = 'measurement'
    WORKER = 'worker'
    COMMENT = 'comment'


class CommentKind(StrEnum):
    """Why a comment was written. An ``escalation`` asks for a decision and is always in the human's inbox.

    A ``lesson`` is the standing conclusion about one mechanism family (``ideas.family``) of a project. The closing
    watchdog writes one for each family that a campaign tried. It replaces the findings on that family, and the newest
    lesson for a (project, family) is the current one. It is on its project, and its first line is ``LESSON_FORMAT``
    (CHECKs).
    """

    NOTE = 'note'
    FINDING = 'finding'
    DECISION = 'decision'
    ESCALATION = 'escalation'
    LESSON = 'lesson'


LESSON_FORMAT = 'family: <family> | status: dead|promising|untested | best: <+N metric> (<evaluator/eval>)'
"""The first line of a lesson. The lines below it say why, and cite idea and candidate ids."""
LESSON_STATUSES = ('dead', 'promising', 'untested')


def lesson_checks() -> tuple[CheckConstraint, ...]:
    """A lesson is on a project, and the first line of its body is ``LESSON_FORMAT``.

    Agents write lessons with raw SQL, and the queries that find the current lesson of a family read that line. The
    name of each CHECK says what it requires, so the error that an agent gets says it too.
    """
    first = 'substr(body, 1, instr(body || char(10), char(10)) - 1)'
    line = ' OR '.join(f"{first} GLOB 'family: ?* | status: {status} | best: ?*'" for status in LESSON_STATUSES)
    return (
        CheckConstraint("kind != 'lesson' OR target_kind = 'project'", name='a lesson is on its project'),
        CheckConstraint(f"kind != 'lesson' OR ({line})", name=f'a lesson starts {LESSON_FORMAT}'),
    )


class WorkerKind(StrEnum):
    """The role of an agent process. It is also the config key, CLI name, pane prefix and prompt file of the role."""

    WORKER = 'worker'
    WATCHDOG = 'watchdog'
    RESEARCH = 'research'


class WorkerState(StrEnum):
    """The process state of an agent, kept by the daemon. ``requested`` is a request to launch, not a process."""

    REQUESTED = 'requested'
    STARTING = 'starting'
    RUNNING = 'running'
    EXITED = 'exited'
    KILLED = 'killed'


LIVE = (WorkerState.STARTING, WorkerState.RUNNING)
ENDED = (WorkerState.EXITED, WorkerState.KILLED)


def _values[E: StrEnum](enum: type[E]) -> Enum:
    """Store enum values, not member names, with a CHECK constraint, so that raw SQL cannot write other values."""
    return Enum(
        enum, values_callable=lambda e: [m.value for m in e], native_enum=False, length=16, create_constraint=True
    )


NOW = func.now()
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


class UsageJson(TypedDict):
    """The shape of ``workers.usage``, as ``piloop.usage.Usage.as_json`` writes it."""

    input: int
    output: int
    cache_read: int
    cache_write: int
    total: int
    turns: int
    source: str


class Base(DeclarativeBase):
    """The declarative base, with the mappings for JSON, UTC datetimes and enums stored by value."""

    type_annotation_map = {
        datetime: UTCDateTime,
        dict[str, float]: JSON,
        dict[str, object]: JSON,
        UsageJson: JSON,
        ProjectStatus: _values(ProjectStatus),
        IdeaStatus: _values(IdeaStatus),
        LinkKind: _values(LinkKind),
        RunStatus: _values(RunStatus),
        Verdict: _values(Verdict),
        TargetKind: _values(TargetKind),
        CommentKind: _values(CommentKind),
        WorkerKind: _values(WorkerKind),
        WorkerState: _values(WorkerState),
    }


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
    """The actor of the connection that made the last UPDATE (``human``, ``daemon``, ``<kind>#<worker id>``). A TEMP
    trigger of each connection sets it (``piloop/db/actor_triggers.sql``, which also logs decisions in ``events``).
    NULL until the first UPDATE of the row."""


class Project(Tracked, Base):
    """A git repository that piloop optimizes. ``piloop init`` creates it."""

    __tablename__ = 'projects'

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(unique=True)
    path: Mapped[str]
    """The absolute path of the repository. ``piloop.toml`` is at its root."""
    config: Mapped[dict[str, object]]
    """The ``ProjectConfig`` as the last ``piloop init`` stored it."""
    evaluator_id: Mapped[int | None]
    """The ``evaluators.id`` that the config describes now. Measurements under older evaluators stay, but no longer
    decide verdicts. A plain integer, not a foreign key, because evaluators reference projects."""
    status: Mapped[ProjectStatus] = mapped_column(
        default=ProjectStatus.ACTIVE, server_default=ProjectStatus.ACTIVE.value
    )
    """``stopped`` after a ``[stop]`` condition held: running work finishes and nothing new is requested. To resume,
    set it back to ``active`` and clear ``stopped_reason``. ``piloop init`` does this when the ``[stop]`` table
    changed."""
    stopped_reason: Mapped[str | None]
    """The condition that ended the campaign, for example ``target: accuracy 0.8531 ≥ 0.85``."""
    spend_usd: Mapped[float | None]
    """What ``[spend] command`` printed last: the campaign's outside spend so far in USD. None until it first runs."""
    spend_error: Mapped[str | None]
    """Why the last run of ``[spend] command`` failed. None after a run succeeds. While it is set, nothing launches."""
    spend_at: Mapped[datetime | None]
    """When ``[spend] command`` ran last, whatever the result. Its ``interval`` counts from this time."""
    repo: Mapped[str | None]
    """The repository that the project works on, the same for every clone of it: the URL of its ``origin`` remote
    (else of its first remote) as ``host/owner/name``, else ``path``. ``piloop init`` sets it. Past work on the same
    code is ``WHERE repo = (SELECT repo FROM projects WHERE id = <p>)``."""
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)

    ideas: Mapped[list[Idea]] = relationship(back_populates='project', order_by='Idea.id')
    evaluator: Mapped[Evaluator | None] = relationship(
        primaryjoin='foreign(Project.evaluator_id) == Evaluator.id', viewonly=True
    )


class Evaluator(Base):
    """One version of how a project is scored: the evaluate command, the metric definitions and the linked inputs.

    ``piloop init`` copies it from ``piloop.toml``, and a change makes a new row. A measurement points at the
    evaluator that produced it, so a number always says what it means, and an old number is never taken for a new one.
    """

    __tablename__ = 'evaluators'
    __table_args__ = (UniqueConstraint('project_id', 'key'),)

    id: Mapped[int] = mapped_column(primary_key=True)
    project_id: Mapped[int] = mapped_column(ForeignKey('projects.id', ondelete='CASCADE'))
    key: Mapped[str]
    """A short hash of ``definition``, from ``ProjectConfig.evaluator_key``."""
    definition: Mapped[dict[str, object]]
    """``evaluate``, ``metrics`` and ``link``, as configured."""
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)


class Idea(Tracked, Base):
    """Something worth trying. The scheduler turns ready ideas into runs.

    Anyone can insert an idea: a human, the orchestrator or a research agent. Ideas link to ideas of the same project
    through ``idea_links`` (``IdeaLink``), many-to-many in both directions.

    ``after``: the idea is blocked (``ready``, but never taken) until every prerequisite is ``done``. Its run then
    starts from the head like any other run, and the head holds every keep on the trunk that the prerequisites made.
    An idea can pin a ``base_candidate_id`` instead, for example the branch keep of a prerequisite. A prerequisite
    that ends ``rejected`` or ``parked`` keeps blocking. ``piloop status`` and the history block say so, and the
    orchestrator or a human parks the dependent idea or deletes the link. Cycles of prerequisites are not detected:
    the ideas in a cycle never run and show as blocked.

    ``supersedes``: the idea is a sharper version of the targets. A research sweep uses it to refine rejected, parked
    or done ideas instead of proposing near-duplicates. The daemon rejects a superseded idea that never ran (it has
    no run and is still ``seed``, ``proposed`` or ``ready``). A superseded idea that ran keeps its status and results.
    """

    __tablename__ = 'ideas'
    __table_args__ = (
        UniqueConstraint('project_id', 'title'),
        # the queue: the tick and the widget read the ``ready`` ideas of one project in priority order
        Index('ix_ideas_project_status_priority', 'project_id', 'status', 'priority'),
        *text_checks(
            'ideas', 'title', 'hypothesis', 'created_by', 'track', nullable=('plan', 'harness', 'provenance', 'family')
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    project_id: Mapped[int] = mapped_column(ForeignKey('projects.id', ondelete='CASCADE'))
    title: Mapped[str]
    """Unique in a project, so two research sweeps that pick the same title fail here and do not duplicate the idea."""
    hypothesis: Mapped[str]
    """Why this idea should improve the primary metric."""
    track: Mapped[str]
    """The track (objective) the idea works on, by its name in ``piloop.toml``: a primary's ``track``, or else the
    primary's name. Its runs are judged on that track's primary, with the primary of every other track as a guardrail,
    and their brief has the track's addendum. The scheduler parks an idea whose track is not in ``piloop.toml``, with
    a note that says why."""
    plan: Mapped[str | None]
    """Concrete steps for the worker. They are appended to the brief."""
    status: Mapped[IdeaStatus] = mapped_column(default=IdeaStatus.READY, server_default=IdeaStatus.READY.value)
    priority: Mapped[int] = mapped_column(default=2, server_default=text('2'))
    """0 (urgent) to 4 (someday). The scheduler takes the lowest number first."""
    harness: Mapped[str | None]
    """The harness for this idea. None lets the scheduler use the project's harnesses in turn."""
    base_candidate_id: Mapped[int | None]
    """A deliberate branch: the ``candidates.id`` to start from, instead of the head at the time the run starts. NULL
    unless the idea gives a reason for that starting point. Never pin the current head by default: when the idea
    runs, that tree is out of date. The ``run.queued`` event of a pinned run records both bases. A plain integer, as
    on runs."""
    max_runs: Mapped[int] = mapped_column(default=2, server_default=text('2'))
    """The number of attempts allowed. A crashed run is tried again until this number is reached."""
    created_by: Mapped[str]
    """Who proposed it: ``human``, ``orchestrator``, or the agent's signature (``research#12``, ``worker#40``,
    ``watchdog#7``). The part before ``#`` is the idea's source in the count of keeps by source."""
    provenance: Mapped[str | None]
    """The finding that led to the idea, with ids or a URL. For example ``finding comment#81: timeouts lose the last
    rows``, ``loss table workspace/research/losses.md`` or ``arXiv 2305.11860 via exa``."""
    family: Mapped[str | None]
    """The mechanism family: a short lowercase name for ideas that try the same mechanism (``feature engineering``,
    ``ensembling``, ``test-time training``). The research pack shows what each family produced."""
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)

    project: Mapped[Project] = relationship(back_populates='ideas')
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
    """A directed link between two ideas of one project: ``idea_id`` comes *after* or *supersedes* ``target_id``.

    Links are many-to-many in both directions. An idea may wait for several prerequisites, several ideas may wait for
    it, and a sharper version may replace several earlier ideas. One pair may have both kinds. Rows are inserted and
    deleted, never updated, so they have no ``updated_at`` or ``updated_by``. A CHECK refuses a link from an idea to
    itself, and a trigger refuses a link across projects. When either idea is deleted, the link is deleted too. To add
    a kind of link, add a ``LinkKind`` member (which changes the CHECK) and a rule for it in
    ``piloop/campaign/ideas.py``.
    """

    __tablename__ = 'idea_links'
    __table_args__ = (
        CheckConstraint('idea_id != target_id', name='idea_links_not_self'),
        Index('ix_idea_links_target', 'target_id', 'kind'),
    )

    idea_id: Mapped[int] = mapped_column(ForeignKey('ideas.id', ondelete='CASCADE'), primary_key=True)
    target_id: Mapped[int] = mapped_column(ForeignKey('ideas.id', ondelete='CASCADE'), primary_key=True)
    kind: Mapped[LinkKind] = mapped_column(primary_key=True)

    idea: Mapped[Idea] = relationship(foreign_keys=[idea_id], back_populates='links')
    target: Mapped[Idea] = relationship(foreign_keys=[target_id])


class Run(Tracked, Base):
    """One attempt at an idea, by one worker with one harness and model."""

    __tablename__ = 'runs'
    __table_args__ = (
        UniqueConstraint('idea_id', 'attempt'),
        # what is running or queued: reap, drain and the agents' "what is running" query select by status only
        Index('ix_runs_status', 'status'),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    idea_id: Mapped[int] = mapped_column(ForeignKey('ideas.id', ondelete='CASCADE'))
    attempt: Mapped[int]
    base_candidate_id: Mapped[int]
    """The ``candidates.id`` that the worktree started from. The run's candidates descend from it. A plain integer,
    not a foreign key: candidates reference runs, and SQLite cannot add a foreign key for a cycle afterwards."""
    branch: Mapped[bool] = mapped_column(default=False, server_default=text('0'))
    """Whether the run started from an alternative starting point: its idea pinned a base other than the head when it
    was queued (a reference, an older keep or a note). The run is judged against its own base only, so what it keeps
    is a branch keep, which never moves the head. The scheduler sets it (``[roles.worker] explore_every``)."""
    harness: Mapped[str]
    model: Mapped[str]
    status: Mapped[RunStatus] = mapped_column(default=RunStatus.QUEUED, server_default=RunStatus.QUEUED.value)
    brief: Mapped[str | None]
    """The exact prompt that the worker got."""
    started_at: Mapped[datetime | None]
    finished_at: Mapped[datetime | None]
    error: Mapped[str | None]
    log_dir: Mapped[str | None]
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)

    idea: Mapped[Idea] = relationship(back_populates='runs')
    base: Mapped[Candidate] = relationship(primaryjoin='foreign(Run.base_candidate_id) == Candidate.id', viewonly=True)
    candidates: Mapped[list[Candidate]] = relationship(
        back_populates='run', foreign_keys='Candidate.run_id', order_by='Candidate.id'
    )


def project_runs(project_id: int) -> Select[Run]:
    """Every run of the project. A run has no ``project_id``, but its idea has one."""
    return select(Run).join(Idea).where(Idea.project_id == project_id)


def count[*Ts](s: Session, query: Select[*Ts]) -> int:
    """The number of rows that ``query`` selects."""
    return s.scalar(select(func.count()).select_from(query.subquery())) or 0


def latest_worker(s: Session, run_id: int) -> Worker | None:
    """The latest agent row that did run ``run_id``. A relaunch after the agent failed to start is a second row."""
    return s.scalars(select(Worker).where(Worker.run_id == run_id).order_by(Worker.id.desc()).limit(1)).first()


class Candidate(Tracked, Base):
    """A distinct state of the project's code. The git tree identifies it, so equal contents are one candidate.

    The baseline has no run and no parent. A reference has neither (``piloop candidate import`` makes one from the
    tree of any commit, pinned at ``refs/piloop/<project>/reference/<id>``). A worker committed every other candidate,
    and it descends from the candidate that its run started from. When the last commit of a run reverts to its base,
    the run produces nothing new: the commit resolves to the base candidate. Measurements score candidates, and the
    verdict judges them against a bar.
    """

    __tablename__ = 'candidates'
    __table_args__ = (
        UniqueConstraint('project_id', 'tree_sha'),
        Index('ix_candidates_project_verdict', 'project_id', 'verdict'),
        # the candidates of a run (every history row), and the FK action when a run is deleted
        Index('ix_candidates_run', 'run_id'),
        # the FK check on every delete of a candidate (deleting a project deletes thousands)
        Index('ix_candidates_parent', 'parent_id'),
        *text_checks('candidates', nullable=('summary', 'verdict_by', 'verdict_reason')),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    project_id: Mapped[int] = mapped_column(ForeignKey('projects.id', ondelete='CASCADE'))
    run_id: Mapped[int | None] = mapped_column(ForeignKey('runs.id', ondelete='SET NULL'))
    parent_id: Mapped[int | None] = mapped_column(ForeignKey('candidates.id'))
    """The candidate that this one comes from."""
    tree_sha: Mapped[str]
    """The hash of the contents. Two commits with the same tree are the same candidate."""
    commit_sha: Mapped[str]
    """One commit that has this tree, pinned under ``refs/piloop/``."""
    summary: Mapped[str | None]
    """The first line of the commit message."""
    verdict: Mapped[Verdict] = mapped_column(default=Verdict.PENDING, server_default=Verdict.PENDING.value)
    verdict_by: Mapped[str | None]
    verdict_reason: Mapped[str | None]
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)

    run: Mapped[Run | None] = relationship(back_populates='candidates', foreign_keys=[run_id])
    parent: Mapped[Candidate | None] = relationship(remote_side=[id], foreign_keys=[parent_id])
    measurements: Mapped[list[Measurement]] = relationship(back_populates='candidate', order_by='Measurement.id')


class Measurement(Base):
    """One run of an evaluator on a candidate. A candidate can have many: repeats, or a new version of the evaluator."""

    __tablename__ = 'measurements'
    __table_args__ = (Index('ix_measurements_candidate_evaluator', 'candidate_id', 'evaluator_id'),)

    id: Mapped[int] = mapped_column(primary_key=True)
    candidate_id: Mapped[int] = mapped_column(ForeignKey('candidates.id', ondelete='CASCADE'))
    evaluator_id: Mapped[int] = mapped_column(ForeignKey('evaluators.id'))
    metrics: Mapped[dict[str, float]] = mapped_column(default=dict, server_default=EMPTY_JSON)
    error: Mapped[str | None]
    """Set when the evaluate command failed. ``metrics`` then has what it printed before it failed."""
    seconds: Mapped[float | None]
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)

    candidate: Mapped[Candidate] = relationship(back_populates='measurements')
    evaluator: Mapped[Evaluator] = relationship()


class Comment(Tracked, Base):
    """The forum and the human's inbox. Anything with an id can get a comment: ``idea#12``, ``run#7``, ``candidate#30``.

    A comment with a ``priority`` is for the human. It stays in the inbox (``piloop inbox``) until someone acknowledges
    it (``acked_at``, ``acked_by``). An escalation always has a priority (a CHECK makes sure). The orchestrator gives
    a priority to every decision it makes for the human, so that the human can review and change each one later. The
    rest of the forum (workers that steer each other, research findings) has no priority.
    """

    __tablename__ = 'comments'
    __table_args__ = (
        Index('ix_comments_target', 'target_kind', 'target_id'),
        # the inbox: the widget counts the items not acknowledged on every refresh, and the CLI lists them by priority
        Index('ix_comments_inbox', 'priority', 'id', sqlite_where=text('priority IS NOT NULL AND acked_at IS NULL')),
        CheckConstraint("kind != 'escalation' OR priority IS NOT NULL", name='escalation_needs_priority'),
        *text_checks('comments', 'author', 'body'),
        *lesson_checks(),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    target_kind: Mapped[TargetKind]
    target_id: Mapped[int]
    author: Mapped[str]
    """Who wrote the comment, for example ``human``, ``orchestrator`` or ``worker#12``."""
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
        UniqueConstraint('project_id', 'url'),
        *text_checks('techniques', 'title', 'mechanism', 'url', 'created_by'),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    project_id: Mapped[int] = mapped_column(ForeignKey('projects.id', ondelete='CASCADE'))
    title: Mapped[str]
    """The technique, in a few words."""
    mechanism: Mapped[str]
    """One line: how it works, and the evidence for it (a number, code, an ablation)."""
    url: Mapped[str]
    """The primary source: a post, repo or paper. Unique in a project, so the same link is never filed twice."""
    created_by: Mapped[str]
    """Who filed it: the scout (``scout#<worker id>``) or someone else."""
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)


class Document(Tracked, Base):
    """A text document of a project, for agents to search: a campaign report, an orchestrator log, a design note.

    ``piloop init`` and the stop of a campaign copy in the repo files that ``[memory] documents`` names. Agents may
    insert more with raw SQL: ``hash`` is ``blake3(body)``, a function that every piloop connection has. Each version
    is a row. One path may have several versions, and the newest has the highest ``id``. The same text at the same
    path is one row, however often it is synced. ``documents_fts`` indexes the title and the body. Never a document
    derived from validation or test data: nothing that agents can search may show the scored data.
    """

    __tablename__ = 'documents'
    __table_args__ = (
        UniqueConstraint('project_id', 'path', 'hash'),
        CheckConstraint(
            "length(hash) = 64 AND hash NOT GLOB '*[^0-9a-f]*'", name='documents.hash is the blake3 hex digest of body'
        ),
        *text_checks('documents', 'path', 'title', 'body', 'created_by', nullable=('commit_sha',)),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    project_id: Mapped[int] = mapped_column(ForeignKey('projects.id', ondelete='CASCADE'))
    path: Mapped[str]
    """Where it is: repo-relative for a synced file, or else wherever its writer keeps it."""
    hash: Mapped[str]
    """BLAKE3 of the UTF-8 bytes of the body, in lowercase hex. It makes a sync of unchanged text do nothing."""
    title: Mapped[str]
    """Its first heading, or else the file name."""
    body: Mapped[str]
    commit_sha: Mapped[str | None]
    """The commit that last changed the file, when the sync read a clean, tracked file. NULL otherwise."""
    created_by: Mapped[str]
    """Who inserted it: the actor of ``piloop init``, or the daemon, for a synced file. Else the agent's signature."""
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)


JOURNAL_LINE_BYTES = 280
"""The longest journal line, in UTF-8 bytes: one decision, plan or result, with its ids."""


class Journal(Base):
    """The orchestrator's journal: one line for each decision, plan or result, so a new session can pick up the work.

    A coordinating session can lose its context (a crash, a full window). ``piloop wake`` prints the latest lines
    first, next to the lessons, the inbox and the heads. ``piloop journal "<line>"`` appends a line, signed with the
    actor. Rows are never updated or deleted.
    """

    __tablename__ = 'journal'
    __table_args__ = (
        CheckConstraint(
            f"typeof(line) IN ('text') AND length(CAST(line AS BLOB)) BETWEEN 1 AND {JOURNAL_LINE_BYTES}",
            name=f'journal.line is text of 1 to {JOURNAL_LINE_BYTES} bytes',
        ),
        *text_checks('journal', 'author'),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    author: Mapped[str]
    """Who wrote it, for example ``orchestrator``."""
    line: Mapped[str]
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)


class Worker(Tracked, Base):
    """An agent process in one of the roles. While ``state`` is ``requested``, the row is a request to start one.

    A request needs only ``kind`` and ``project_id``. ``harness`` and ``model`` pin the rotation, and ``reason`` says
    why. The daemon fills in the rest when it launches the row, so the table is also the launch queue of every role.
    """

    __tablename__ = 'workers'
    __table_args__ = (
        # the launch queue of the tick: the requested and active rows of a project for each role, and its latest row
        Index('ix_workers_project_kind_state', 'project_id', 'kind', 'state'),
        # the session behind the candidates of a run (every kept-lineage row), and the FK action when a run is deleted
        Index('ix_workers_run', 'run_id'),
        *text_checks('workers', nullable=('reason', 'harness', 'model')),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    kind: Mapped[WorkerKind]
    project_id: Mapped[int | None] = mapped_column(ForeignKey('projects.id', ondelete='CASCADE'))
    """The project the agent works for. piloop always sets it, but readers must accept NULL."""
    run_id: Mapped[int | None] = mapped_column(ForeignKey('runs.id', ondelete='SET NULL'))
    """The run that a row of kind ``worker`` does. Other roles have none."""
    harness: Mapped[str | None]
    model: Mapped[str | None]
    runtime: Mapped[str | None]
    """``herdr`` or ``subprocess``: how to reach the process."""
    handle: Mapped[dict[str, object]] = mapped_column(default=dict, server_default=EMPTY_JSON)
    """The address of the process in its runtime: the pid and pgid, or the Herdr workspace, pane and agent name."""
    state: Mapped[WorkerState] = mapped_column(
        default=WorkerState.REQUESTED, server_default=WorkerState.REQUESTED.value
    )
    reason: Mapped[str | None]
    """Why it was requested: the trigger that fired, or what the requester wrote."""
    log_dir: Mapped[str | None]
    started_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)
    """The time of the request until the launch, then the time of the launch."""
    ended_at: Mapped[datetime | None]
    error: Mapped[str | None]
    session_id: Mapped[str | None]
    """The harness's own session id. piloop creates it at launch (``--session-id`` for pi and claude), or the runtime
    reports it after the agent starts. ``entire session attach`` takes it."""
    checkpoint_id: Mapped[str | None]
    """The Entire checkpoint of this session (``refs/entire/checkpoints/``). ``entire session attach`` writes it when
    the session ends. NULL when Entire is not installed or the attach failed."""
    usage: Mapped[UsageJson | None]
    """The tokens the agent spent, added up from the harness's own session file when the agent ended:
    ``{"input", "output", "cache_read", "cache_write", "total", "turns", "source"}``. ``total`` is what the harness
    itself counts as the session's tokens (sum ``json_extract(usage, '$.total')`` for the spend). ``turns`` is the
    number of model responses, and ``source`` is the file that was read. The cache fields follow the harness:
    Anthropic's ``input`` does not include cached tokens, and Codex's does. NULL while the agent runs, or when no
    session file was found (``worker.usage_unknown``)."""


class Event(Base):
    """The append-only feed of what happened. The daemon pushes comments to workers from it, reading from a cursor.

    It has two writers. The daemon writes what only it knows: ``run.started``, ``measurement.recorded``,
    ``candidate.judged`` with the score, ``comment.delivered``, ``comment.undelivered`` and ``worker.wrap_up`` on a
    worker, ``<role>.started`` and ``<role>.finished``, and ``anomaly.*`` for faults in the environment such as
    ``anomaly.startup``. Triggers write what any connection does to the rows. The persistent trigger writes
    ``comment.escalation``. The TEMP triggers of each connection (``piloop/db/actor_triggers.sql``, generated from
    ``piloop.db.triggers.TEMP``) write the change log, signed by the connection's actor: ``idea.status``,
    ``idea.priority``, ``idea.track``, ``candidate.verdict``, ``run.brief`` and ``project.status`` as
    ``{old, new}``, ``<kind>.deleted`` for deleted rows, and ``comment.<kind>``.
    """

    __tablename__ = 'events'
    __table_args__ = (Index('ix_events_target', 'target_kind', 'target_id', 'id'),)

    id: Mapped[int] = mapped_column(primary_key=True)
    at: Mapped[datetime] = mapped_column(default=utcnow, server_default=NOW)
    kind: Mapped[str]
    """``<subject>.<what>``, for example ``run.started``, ``candidate.judged``, ``idea.status``, ``comment.decision``,
    ``idea.deleted`` or ``<role>.finished``."""
    actor: Mapped[str]
    target_kind: Mapped[TargetKind]
    target_id: Mapped[int]
    payload: Mapped[dict[str, object]] = mapped_column(default=dict, server_default=EMPTY_JSON)


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
