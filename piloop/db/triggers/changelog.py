"""The change log: the decisions that leave a record in ``events``, whoever makes them.

Every trigger here inserts one ``events`` row through ``event_insert``, with the payload of its event model: the
trigger renders its JSON from the model (``payload``), so the SQL and the model cannot drift. The event kind is
``<table singular>.<column>`` for a change, ``<singular>.deleted`` for a tombstone and ``comment.<kind>`` for a
comment. ``target_kind`` is the table's singular, which is also a ``TargetKind`` value. Payloads never have a large
body.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Literal

from pydantic import JsonValue

from piloop.db.models import EventModel, Origin, Pause
from piloop.db.triggers.ddl import Event, Trigger
from piloop.db.triggers.provenance import DAEMON_ACTOR, NOW, SESSION_ACTOR
from piloop.ids import (
    HOME,
    CampaignId,
    CampaignStatus,
    CandidateId,
    CommentKind,
    IdeaId,
    IdeaStatus,
    LinkKind,
    RoleName,
    TargetKind,
    TrackName,
)
from piloop.plugins.cas.contract import BlobAddress
from piloop.plugins.verdict_rule.contract import Outcome

type Row = Literal['NEW', 'OLD']


@dataclass(frozen=True)
class Sql:
    """The SQL that a trigger writes into a field of its event, when that is not the column of the same name.

    ``{row}`` stands for ``NEW`` or ``OLD``, the row of the trigger.
    """

    expression: str


class Changed[T](EventModel):
    """A change of the column that the kind names: ``idea.status`` is a change of ``ideas.status``.

    Other fields are the columns of the same name in the new row.
    """

    old: T | None
    new: T | None


class IdeaStatusChanged(Changed[IdeaStatus]):
    """An idea's status changed."""

    kind: Literal['idea.status'] = 'idea.status'


class IdeaPriorityChanged(Changed[int]):
    """An idea's priority changed."""

    kind: Literal['idea.priority'] = 'idea.priority'


class IdeaTrackChanged(Changed[TrackName]):
    """An idea moved to another track."""

    kind: Literal['idea.track'] = 'idea.track'


class IdeaBaseChanged(Changed[CandidateId]):
    """An idea's pinned base changed."""

    kind: Literal['idea.base_candidate_id'] = 'idea.base_candidate_id'


class VerdictChanged(Changed[Outcome]):
    """A candidate's verdict changed, by the daemon or by an override."""

    kind: Literal['candidate.verdict'] = 'candidate.verdict'
    verdict_by: str | None
    verdict_reason: str | None


class OriginChanged(Changed[Origin]):
    """A candidate's origin changed: the daemon did not measure a commit, or measured a tree that it had not."""

    kind: Literal['candidate.origin'] = 'candidate.origin'


class CampaignStatusChanged(Changed[CampaignStatus]):
    """A campaign stopped, or was set active again."""

    kind: Literal['campaign.status'] = 'campaign.status'
    stopped_reason: str | None
    """Set in the same statement as the status: a stop says why, and a resume says null."""


class CampaignTypeChanged(Changed[str]):
    """A campaign's type changed."""

    kind: Literal['campaign.type'] = 'campaign.type'


class CampaignParentChanged(Changed[CampaignId]):
    """A campaign moved under another campaign, or to the top."""

    kind: Literal['campaign.parent_id'] = 'campaign.parent_id'


class CampaignPausedChanged(Changed[Pause]):
    """``piloop pause`` and ``piloop resume``: the reason is set with the pause and cleared with it."""

    kind: Literal['campaign.paused'] = 'campaign.paused'
    paused_reason: str | None


class CommentAcked(Changed[datetime]):
    """An inbox item was acknowledged. The actor of the event is who did it, whatever ``acked_by`` says."""

    kind: Literal['comment.acked_at'] = 'comment.acked_at'
    acked_by: str | None


class RunBriefChanged(Changed[BlobAddress]):
    """A brief was written or replaced: a steer. ``old`` and ``new`` are the briefs' addresses: the text is in the CAS."""

    kind: Literal['run.brief_cid'] = 'run.brief_cid'


EventModel.retire('run.brief')  # 0.2.0 logged the lengths of runs.brief; the brief is a blob in the CAS now


class IdeaLinked(EventModel):
    """An ``idea_links`` row was inserted or deleted. The event is on the idea that links."""

    kind: Literal['idea.linked', 'idea.unlinked']
    target: Annotated[IdeaId, Sql('{row}.target_id')]
    link_kind: Annotated[LinkKind, Sql('{row}.kind')]


class IdeaDeleted(EventModel):
    """An idea was deleted."""

    kind: Literal['idea.deleted'] = 'idea.deleted'
    title: str
    status: IdeaStatus
    hypothesis: str


class CommentDeleted(EventModel):
    """A comment was deleted."""

    kind: Literal['comment.deleted'] = 'comment.deleted'
    target_kind: TargetKind
    target_id: int
    author: str
    comment_kind: Annotated[CommentKind, Sql('{row}.kind')]
    body: str


class CandidateDeleted(EventModel):
    """A candidate was deleted."""

    kind: Literal['candidate.deleted'] = 'candidate.deleted'
    tree_sha: str
    revision: str
    origin: Origin
    verdict: Outcome | None


class Commented(EventModel):
    """A comment was written: ``comment.<its kind>``, on the comment, with what the comment is about."""

    kind: Literal['comment.note', 'comment.finding', 'comment.decision', 'comment.escalation']
    target_kind: TargetKind
    target_id: int


class AgentSessionRequested(EventModel):
    """An agent was requested: by a trigger, ``piloop launch``, or an INSERT of a ``requested`` row."""

    kind: Literal['agent_session.requested'] = 'agent_session.requested'
    role: Annotated[RoleName, Sql('{row}.kind')]
    campaign: Annotated[CampaignId | None, Sql('{row}.campaign_id')]
    reason: str | None


class IdeaCreated(EventModel):
    """An idea was added: by hand, by an agent, or as a follow-up seed."""

    kind: Literal['idea.created'] = 'idea.created'


class CampaignConfigChanged(EventModel):
    """A campaign's stored config changed (``piloop init`` read its ``piloop.toml`` again). The payload has no body."""

    kind: Literal['campaign.config'] = 'campaign.config'


class EvaluationCancelled(EventModel):
    """On the candidate: someone cancelled its evaluation while it ran (``piloop eval cancel``)."""

    kind: Literal['evaluation.cancelled'] = 'evaluation.cancelled'
    evaluation: Annotated[int, Sql('{row}.id')]
    by: Annotated[str, Sql('{row}.cancelled_by')]


class LimitSet(EventModel):
    """On the home: a live limit was set (``piloop limits set``)."""

    kind: Literal['limit.set'] = 'limit.set'
    key: str
    value: Annotated[JsonValue, Sql('json({row}.value)')]


class LimitUnset(EventModel):
    """On the home: a live limit was removed (``piloop limits unset``)."""

    kind: Literal['limit.unset'] = 'limit.unset'
    key: str


def plural(subject: str) -> str:
    """``idea`` → ``ideas``: the table of an event kind's subject."""
    return f'{subject}s'


def payload(model: type[EventModel], row: Row, kind: str | None = None, **fields: str) -> str:
    """The ``json_object`` of an event of ``model``, as a SQL expression.

    Args:
        model: The event. Each field is the column of the same name in ``row``, or its ``Sql``.
        row: The row of the trigger.
        kind: The SQL of the kind, when the model is for more than one. Otherwise its one kind.
        fields: The SQL of fields that the caller knows: the ``old`` and ``new`` of a ``Changed``.
    """
    if kind is None:
        (only,) = model.kinds()
        kind = f"'{only}'"
    pairs = [f"'kind', {kind}"]
    for name, field in model.model_fields.items():
        if name == 'kind':
            continue
        sql = next((m.expression for m in field.metadata if isinstance(m, Sql)), f'{{row}}.{name}')
        pairs.append(f"'{field.alias or name}', {fields.get(name, sql).format(row=row)}")
    return f'json_object({", ".join(pairs)})'


def event_insert(actor: str, target_kind: str, target_id: str, payload: str) -> str:
    """The INSERT that every event trigger runs. Each argument is a SQL expression. The kind is in the payload.

    The time has milliseconds, as an event that the ORM writes has microseconds.
    """
    return (
        'INSERT INTO events (at, actor, target_kind, target_id, payload)\n'
        f'VALUES ({NOW}, {actor}, {target_kind}, {target_id}, {payload})'
    )


def changed[T](model: type[Changed[T]]) -> Trigger:
    """TEMP: log a change of the column that ``model``'s kind names, ``<singular>.<column>``.

    ``AFTER UPDATE OF`` fires only when a statement names the column, so the ``updated_at`` and ``updated_by`` triggers
    (which SET their own column) never fire it. The WHEN clause ignores a SET to the value already there.
    """
    (kind,) = model.kinds()
    subject, column = kind.split('.')
    table = plural(subject)
    return Trigger(
        f'{table}_{column}_event',
        table,
        'AFTER UPDATE',
        of=(column,),
        when=f'OLD.{column} IS NOT NEW.{column}',
        body=event_insert(
            SESSION_ACTOR, f"'{subject}'", 'NEW.id', payload(model, 'NEW', old=f'OLD.{column}', new=f'NEW.{column}')
        ),
        temp=True,
    )


def tombstone(model: type[EventModel]) -> Trigger:
    """TEMP: log a deleted row as a ``<singular>.deleted`` event with the columns of the old row that ``model`` has."""
    (kind,) = model.kinds()
    subject = kind.removesuffix('.deleted')
    table = plural(subject)
    return Trigger(
        f'{table}_deleted_event',
        table,
        'AFTER DELETE',
        body=event_insert(SESSION_ACTOR, f"'{subject}'", 'OLD.id', payload(model, 'OLD')),
        temp=True,
    )


COMMENT_KIND = "'comment.' || NEW.kind"


def comment_event() -> Trigger:
    """TEMP: log every comment except an escalation as a ``comment.<kind>`` event, shaped like the escalation event."""
    return Trigger(
        'comments_inserted_event',
        'comments',
        'AFTER INSERT',
        when="NEW.kind != 'escalation'",
        body=event_insert('NEW.author', "'comment'", 'NEW.id', payload(Commented, 'NEW', COMMENT_KIND)),
        temp=True,
    )


def escalation() -> Trigger:
    """Persistent: log ``comment.escalation`` whatever connection wrote the comment."""
    return Trigger(
        'comments_escalation_event',
        'comments',
        'AFTER INSERT',
        when="NEW.kind = 'escalation'",
        body=event_insert('NEW.author', "'comment'", 'NEW.id', payload(Commented, 'NEW', COMMENT_KIND)),
    )


# The one exception to `changed`: when the daemon writes a brief where there was none, the run starts (`run.started`),
# and that is not a steer. Anyone else who writes a brief, or the daemon replacing one, steers the run.
RUN_BRIEF = Trigger(
    'runs_brief_cid_event',
    'runs',
    'AFTER UPDATE',
    of=('brief_cid',),
    when=f"OLD.brief_cid IS NOT NEW.brief_cid AND NOT (OLD.brief_cid IS NULL AND {SESSION_ACTOR} = '{DAEMON_ACTOR}')",
    body=event_insert(
        SESSION_ACTOR, "'run'", 'NEW.id', payload(RunBriefChanged, 'NEW', old='OLD.brief_cid', new='NEW.brief_cid')
    ),
    temp=True,
)


def link_event(row: Row) -> Trigger:
    """TEMP: log an inserted (``idea.linked``) or a deleted (``idea.unlinked``) ``idea_links`` row on its idea.

    The rows have no id, so the event is on the idea that links, with the target and the kind in the payload. A cascade
    (deleting an idea) fires it for each link that goes with the idea.
    """
    event: Event = 'AFTER INSERT' if row == 'NEW' else 'AFTER DELETE'
    kind = 'idea.linked' if row == 'NEW' else 'idea.unlinked'
    return Trigger(
        f'idea_links_{kind.removeprefix("idea.")}_event',
        'idea_links',
        event,
        body=event_insert(SESSION_ACTOR, "'idea'", f'{row}.idea_id', payload(IdeaLinked, row, f"'{kind}'")),
        temp=True,
    )


HOME_TARGET = (f"'{HOME.kind}'", str(HOME.id))


def inserted(
    model: type[EventModel], table: str, target: tuple[str, str], *, actor: str = SESSION_ACTOR, when: str | None = None
) -> Trigger:
    """TEMP: log an inserted row of ``table`` as an event of ``model`` on ``target`` (its kind and id), each as SQL."""
    (kind,) = model.kinds()
    return Trigger(
        f'{table}_{kind.split(".")[1]}_event',
        table,
        'AFTER INSERT',
        when=when,
        body=event_insert(actor, *target, payload(model, 'NEW')),
        temp=True,
    )


OUTBOX = (
    inserted(AgentSessionRequested, 'agent_sessions', ("'agent_session'", 'NEW.id'), when="NEW.state = 'requested'"),
    inserted(IdeaCreated, 'ideas', ("'idea'", 'NEW.id'), actor='NEW.created_by'),
    Trigger(
        'campaigns_config_event',
        'campaigns',
        'AFTER UPDATE',
        of=('config',),
        when='OLD.config IS NOT NEW.config',
        body=event_insert(SESSION_ACTOR, "'campaign'", 'NEW.id', payload(CampaignConfigChanged, 'NEW')),
        temp=True,
    ),
    Trigger(
        'evaluations_cancelled_event',
        'evaluations',
        'AFTER UPDATE',
        of=('cancelled_by',),
        when='OLD.cancelled_by IS NULL AND NEW.cancelled_by IS NOT NULL',
        body=event_insert(SESSION_ACTOR, "'candidate'", 'NEW.candidate_id', payload(EvaluationCancelled, 'NEW')),
        temp=True,
    ),
    inserted(LimitSet, 'limits', HOME_TARGET),
    Trigger(
        'limits_updated_event',
        'limits',
        'AFTER UPDATE',
        of=('value',),
        when='OLD.value IS NOT NEW.value',
        body=event_insert(SESSION_ACTOR, *HOME_TARGET, payload(LimitSet, 'NEW')),
        temp=True,
    ),
    Trigger(
        'limits_unset_event',
        'limits',
        'AFTER DELETE',
        body=event_insert(SESSION_ACTOR, *HOME_TARGET, payload(LimitUnset, 'OLD')),
        temp=True,
    ),
)
"""The writes that the daemon reacts to and that no decision above logs, so that it wakes on the event."""

CHANGES = (
    changed(IdeaStatusChanged),
    changed(IdeaPriorityChanged),
    changed(IdeaTrackChanged),
    changed(IdeaBaseChanged),
    link_event('NEW'),
    link_event('OLD'),
    changed(VerdictChanged),
    changed(OriginChanged),
    RUN_BRIEF,
    changed(CampaignStatusChanged),
    changed(CampaignTypeChanged),
    changed(CampaignParentChanged),
    changed(CampaignPausedChanged),
    changed(CommentAcked),
)
"""The decisions that are logged. To log another column, add its ``Changed`` model here and run ``make docs``."""
TOMBSTONES = (tombstone(IdeaDeleted), tombstone(CommentDeleted), tombstone(CandidateDeleted))
"""The deletes that are logged, with the key columns, so the feed can still say what was there."""
COMMENTS = (comment_event(),)
"""The forum (``comment.note``, ``comment.finding``, ``comment.decision``), which the daemon pushes to implementers."""
