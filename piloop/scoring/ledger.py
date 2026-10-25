"""What the ledger means: the current evaluator, a candidate's score under it, the head, the verdict and the rebase.

All code that reads candidates and measurements to answer "how good is this" uses this module. So the CLI, the
scheduler, the run controller and the history block agree.
"""

import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from statistics import fmean
from typing import Literal, assert_never

from pydantic import Field
from sqlalchemy import ColumnElement, Select, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from piloop.actor import Actor, System
from piloop.config import CampaignConfig, Metric, Track
from piloop.db import requests
from piloop.db.events import emit
from piloop.db.models import (
    OPEN_RUNS,
    PROCESS,
    AgentSession,
    Campaign,
    Candidate,
    Evaluator,
    Event,
    EventModel,
    Idea,
    IdeaLink,
    Measurement,
    Origin,
    Run,
    latest_agent_session,
)
from piloop.ids import (
    CampaignId,
    CampaignStatus,
    CandidateId,
    EvaluatorId,
    IdeaStatus,
    LinkKind,
    MeasurementId,
    RunId,
    TargetKind,
    TargetRef,
    TrackName,
)
from piloop.plugins.vcs.contract import (
    Conflicts,
    RebaseVersion,
    ReferenceVersion,
    Repository,
    Revision,
    UnknownRevision,
    Version,
)
from piloop.plugins.verdict_rule.contract import VERDICT_RULES, Case, Criterion, Outcome, Side, VerdictRule
from piloop.scoring.evaluate import UNITS, Units
from piloop.scoring.verdict import judgeable, label, pending, pending_clause
from piloop.text import metric

type Samples = dict[str, list[float]]
"""The samples of a paired evaluation: metric name → the value of each sample, in the order of the pairs."""


def current_evaluator(s: Session, campaign: Campaign, cfg: CampaignConfig) -> Evaluator:
    """Return the evaluator row that ``cfg`` describes, and make it the campaign's current one. Create it if needed.

    Raises:
        ValueError: the evaluator has a runner that is not installed (``evaluator_runners``).
    """
    key = cfg.evaluator_key
    evaluator = s.scalar(select(Evaluator).where(Evaluator.campaign_id == campaign.id, Evaluator.key == key))
    if evaluator is None:
        evaluator = Evaluator(campaign_id=campaign.id, key=key, definition=cfg.evaluator_definition)
        s.add(evaluator)
        s.flush()
    if evaluator.runner not in (installed := evaluator_runners()):
        raise ValueError(
            f'evaluator#{evaluator.id} of {campaign.name} has the evaluator runner {evaluator.runner!r}, which is not'
            f' installed: {", ".join(installed)}'
        )
    if campaign.evaluator_id != evaluator.id:
        campaign.evaluator_id = evaluator.id
        s.flush()
    return evaluator


def evaluator_runners() -> tuple[str, ...]:
    """Return the names of the installed evaluator runners: the built-in ``process`` for now.

    TODO: add the evaluator-runner plugins from the plugin registry when it lands; nothing else changes here.
    """
    return (PROCESS,)


def metrics(candidate: Candidate, evaluator_id: EvaluatorId | None) -> dict[str, float]:
    """Return the mean of each metric over the candidate's successful measurements under ``evaluator_id``."""
    rows = [m.metrics for m in candidate.measurements if m.evaluator_id == evaluator_id and m.error is None]
    names = {name for row in rows for name in row}
    return {name: fmean(values) for name in names if (values := [r[name] for r in rows if name in r])}


def score(candidate: Candidate, evaluator_id: EvaluatorId | None, primary: Metric) -> float | None:
    """Return the primary metric under ``evaluator_id``, or None when no successful measurement has it."""
    return metrics(candidate, evaluator_id).get(primary.name)


def measured(candidate: Candidate, evaluator_id: EvaluatorId | None) -> bool:
    """Return whether any measurement under ``evaluator_id`` exists. A crashed measurement counts too."""
    return any(m.evaluator_id == evaluator_id for m in candidate.measurements)


def head(s: Session, campaign: Campaign, *, excluding: Candidate | None = None) -> Candidate | None:
    """Return the head: the latest trunk keep, a kept candidate that no branch run made. The baseline is the first.

    Every run that pins no base starts from the head. Research and the watchdog get the head checked out, and
    ``target`` is read on it. "Latest" is by id: the run controller keeps a run's candidate only while its bar is the head, and
    a candidate is always newer than its bar. So each trunk keep is newer than the one before. Scores do not matter:
    after an evaluator change, the head stays where it was.

    ``excluding`` leaves out one candidate: the one being judged, which is never its own bar.
    """
    query = trunk_keeps(campaign)
    if excluding is not None:
        query = query.where(Candidate.id != excluding.id)
    return s.scalars(query.order_by(Candidate.id.desc()).limit(1)).first()


def trunk_keeps(campaign: Campaign) -> Select[Candidate]:
    """Select the campaign's trunk keeps: its kept candidates except those that a branch run (``runs.branch``) made."""
    return keeps(campaign).outerjoin(Run, Candidate.run_id == Run.id).where(Run.id.is_(None) | ~Run.branch)


def bar_of(candidate: Candidate) -> Candidate:
    """Return the bar of a run that starts from ``candidate``.

    The bar is ``candidate`` when it is a keep or a reference, else its nearest ancestor that is one. With neither, it
    is the root of the ancestry. A non-keep is never a bar, pinned or not. Otherwise a run pinned to an ``interesting``
    candidate would be held to that candidate's numbers, which a fix to it could never beat. A run pinned to a
    non-keep starts from its tree and must beat the latest keep of its lineage.
    """
    while not (candidate.verdict is Outcome.KEEP or candidate.origin is Origin.REFERENCE) and candidate.parent:
        candidate = candidate.parent
    return candidate


def raises_bar(candidate: Candidate, side: Lineage) -> bool:
    """Whether a run's commit that is ``candidate`` becomes the bar for its next commits, on the run's ``side``.

    A keep does, and a known keep that the run reached again too. On the trunk, a branch keep does not: a trunk run
    that merges a branch keep onto the head must beat the head, so a branch's numbers are never a trunk run's bar.
    """
    match side:
        case Branch():
            return candidate.verdict is Outcome.KEEP
        case Trunk():
            return candidate.verdict is Outcome.KEEP and (candidate.run is None or not candidate.run.branch)
        case _:
            assert_never(side)


def track_of(cfg: CampaignConfig, candidate: Candidate) -> Track:
    """Return the track of the candidate's run (its idea's track). A renderer shows the candidate's score on it.

    The baseline and a reference get the first track (``fallback_track``).
    """
    return fallback_track(cfg, candidate.run.idea.track if candidate.run is not None else None)


def fallback_track(cfg: CampaignConfig, name: TrackName | None) -> Track:
    """Return the track called ``name``, else the first track. An idea can name a track that ``piloop.toml`` removed."""
    return next((t for t in cfg.all_tracks if t.name == name), cfg.all_tracks[0])


def due(s: Session, campaign: Campaign) -> list[Candidate]:
    """Return what the daemon measures with the current evaluator, most urgent first, each once.

    1. The head: an unpinned run starts from it and is judged against it, so the queue waits only for it
       (``head_unmeasured``).
    2. The bar of each open run (``bar_of`` its base), oldest run first: the run is judged against it.
    3. Each candidate that someone asked to measure again (``requested``), oldest request first, and each that waits
       for a verdict that no run gives (``unjudged``).
    4. The bar of each ``ready`` idea pinned to a base.
    5. Each reference that no evaluator measured: a new import.

    The head and the bars come only while the current evaluator has not measured them. Nothing else is measured again
    unasked: after an evaluator change, an older keep or a reference that no run needs keeps the scores of the old
    evaluator until someone asks (``request_remeasure``). A measurement never changes a keep's verdict. A stopped
    campaign gets only 3: what someone asked for, to review the campaign. The rest is for runs, and none starts.
    """
    open_runs = select(Run).join(Idea).where(Idea.campaign_id == campaign.id, Run.status.in_(OPEN_RUNS))
    pins = select(Candidate).join(Idea, Idea.base_candidate_id == Candidate.id)
    new_references = select(Candidate).where(
        Candidate.campaign_id == campaign.id, Candidate.origin == Origin.REFERENCE, ~Candidate.measurements.any()
    )
    waiting = select(Candidate).join(Run).where(Candidate.campaign_id == campaign.id, pending_clause())
    asked = [c for c, _ in requested(s, campaign)]
    active = campaign.status is CampaignStatus.ACTIVE
    found = [head(s, campaign), *(bar_of(run.base) for run in s.scalars(open_runs.order_by(Run.id)))] if active else []
    found += asked + [c for c in s.scalars(waiting.order_by(Candidate.id)) if unjudged(c)]
    if active:
        ready = (Idea.campaign_id == campaign.id, Idea.status == IdeaStatus.READY)
        found += [bar_of(c) for c in s.scalars(pins.where(*ready).order_by(Idea.id))]
        found += s.scalars(new_references.order_by(Candidate.id))
    evaluator_id = campaign.evaluator_id
    return [c for c in dict.fromkeys(found) if c is not None and (c in asked or not measured(c, evaluator_id))]


def head_unmeasured(s: Session, campaign: Campaign) -> bool:
    """Whether the head has no measurement from the current evaluator: the queue then holds the unpinned ideas."""
    trunk = head(s, campaign)
    return trunk is not None and not measured(trunk, campaign.evaluator_id)


REPEATS = 3
"""The pairs of a remeasure that asks for no number: the 95% bound of 3 pairs is near their worst pair."""


class RemeasureRequested(EventModel):
    """Someone asked the daemon to measure a candidate again (``request_remeasure``)."""

    kind: Literal['candidate.remeasure_requested'] = 'candidate.remeasure_requested'
    repeats: int = Field(default=REPEATS, ge=1)
    """How many samples of the candidate, each next to one of its bar."""


class RemeasureRefused(Exception):
    """``request_remeasure`` cannot ask for the candidate. The message says why."""


def request_remeasure(s: Session, candidate: Candidate, actor: Actor, repeats: int = REPEATS) -> None:
    """Ask the daemon to measure ``candidate`` again with the current evaluator, next to its bar. The caller commits.

    Use it for a measurement that a fault of the host spoiled (a full disk, a lost GPU), or that load made noisy. The
    request (``RemeasureRequested``) is pending until a measurement of the candidate under the campaign's current
    evaluator is recorded (``record``). The daemon runs ``repeats`` pairs of the candidate and its bar (``bar_of`` its
    parent), then judges it again when it is ``rejudgeable`` (``RunController.remeasure``). A candidate without a
    parent, the baseline or a reference, gets ``repeats`` samples alone. A commit that the daemon never measured
    (``skipped`` or ``recovered``) gets its first measurement, to isolate a regression to it, say.

    Raises:
        RemeasureRefused: the candidate is pending: its run measures and judges it.
    """
    if pending(candidate):
        raise RemeasureRefused(f'candidate#{candidate.id} is pending: the daemon measures and judges it already')
    emit(s, RemeasureRequested(repeats=repeats), TargetRef(TargetKind.CANDIDATE, candidate.id), actor)


def remeasure_asked(s: Session, candidate: Candidate) -> RemeasureRequested | None:
    """Return the pending request to measure ``candidate`` again, or None."""
    found = requests.pending(s, RemeasureRequested, Event.target == TargetRef(TargetKind.CANDIDATE, candidate.id))
    return found[0][1] if found else None


def candidates_of(campaign: Campaign) -> ColumnElement[bool]:
    """Select the events on the candidates of ``campaign``."""
    of_campaign = select(Candidate.id).where(Candidate.campaign_id == campaign.id)
    return (Event.target_kind == TargetKind.CANDIDATE) & Event.target_id.in_(of_campaign)


def requested(s: Session, campaign: Campaign) -> list[tuple[Candidate, Event]]:
    """Return the candidates of ``campaign`` that someone asked to measure again, with the open requests, oldest first."""
    return [
        (s.get_one(Candidate, asked.target_id), asked)
        for asked, _ in requests.pending(s, RemeasureRequested, candidates_of(campaign))
    ]


def record(
    s: Session,
    candidate: Candidate,
    evaluator_id: EvaluatorId,
    actor: Actor,
    *,
    metrics: dict[str, float],
    error: str | None,
    seconds: float | None,
    run: RunId | None = None,
    pair: MeasurementId | None = None,
) -> Measurement:
    """Store a measurement of ``candidate``, with ``MeasurementRecorded``. The caller commits.

    The one writer of measurements. ``run`` is the run that the daemon measured it for, and ``pair`` the bar's
    measurement that ran next to it in a paired evaluation. A measurement under the campaign's current evaluator
    answers the pending request to measure the candidate again (``request_remeasure``).
    """
    measurement = Measurement(
        candidate=candidate, evaluator_id=evaluator_id, metrics=metrics, error=error, seconds=seconds, pair_id=pair
    )
    s.add(measurement)
    s.flush()
    recorded = MeasurementRecorded(candidate=candidate.id, metrics=metrics, error=error, run=run, pair=pair)
    emit(s, recorded, TargetRef(TargetKind.MEASUREMENT, measurement.id), actor)
    if evaluator_id == s.get_one(Campaign, candidate.campaign_id).evaluator_id:
        for asked, _ in requests.pending(
            s, RemeasureRequested, Event.target == TargetRef(TargetKind.CANDIDATE, candidate.id)
        ):
            requests.answer(s, asked)
    return measurement


class RejudgeRequested(EventModel):
    """Someone measured a candidate by hand (``piloop measurement add``): the daemon judges it again."""

    kind: Literal['candidate.rejudge_requested'] = 'candidate.rejudge_requested'
    measurement: MeasurementId


class RejudgeFailed(EventModel):
    """The daemon could not judge a candidate again. It answers the request."""

    kind: Literal['candidate.rejudge_failed'] = 'candidate.rejudge_failed'
    error: str


class CandidateJudged(EventModel):
    """The daemon judged a candidate: its verdict, the primary metric's score and the reason."""

    kind: Literal['candidate.judged'] = 'candidate.judged'
    verdict: Outcome
    score: float | None
    reason: str | None


class MeasurementRecorded(EventModel):
    """A measurement of a candidate was recorded: by the daemon, or by hand (``piloop measurement add``)."""

    kind: Literal['measurement.recorded'] = 'measurement.recorded'
    candidate: CandidateId
    metrics: dict[str, float]
    error: str | None
    run: RunId | None
    """The run that the daemon measured it for. None for a measurement by hand or a cancelled evaluation."""
    pair: MeasurementId | None = None
    """The bar's measurement that ran next to it in a paired evaluation."""


def unjudged(candidate: Candidate) -> bool:
    """Return whether ``candidate`` waits for a verdict that no run gives: pending, and its run scores no more.

    That is the merge onto the head of a keep that someone set off the head (``ops.set_verdict``). The daemon measures
    it (``due``) and judges it against the head that it merged onto (``RunController.remeasure``).
    """
    run = candidate.run
    return pending(candidate) and run is not None and run.status not in OPEN_RUNS and not run.scoring


def rejudges(s: Session, campaign: Campaign) -> list[Candidate]:
    """Return the candidates of ``campaign`` that someone asked to judge again (``RejudgeRequested``), oldest first.

    The daemon answers each request when it judges the candidate again, or finds that it cannot (``answer_rejudge``).
    """
    return [
        s.get_one(Candidate, asked.target_id)
        for asked, _ in requests.pending(s, RejudgeRequested, candidates_of(campaign))
    ]


def answer_rejudge(s: Session, candidate_id: CandidateId) -> None:
    """Answer the pending request to judge the candidate again: the daemon judged it, or found that it cannot."""
    for asked, _ in requests.pending(
        s, RejudgeRequested, Event.target == TargetRef(TargetKind.CANDIDATE, candidate_id)
    ):
        requests.answer(s, asked)


def references(s: Session, campaign: Campaign) -> list[Candidate]:
    """Return the campaign's imported starting points (``piloop candidate import``), oldest first."""
    query = select(Candidate).where(Candidate.campaign_id == campaign.id, Candidate.origin == Origin.REFERENCE)
    return list(s.scalars(query.order_by(Candidate.id)))


def keeps(campaign: Campaign) -> Select[Candidate]:
    """Select the campaign's kept candidates. The baseline is one while its verdict is ``keep``."""
    return select(Candidate).where(Candidate.campaign_id == campaign.id, Candidate.verdict == Outcome.KEEP)


def baseline(s: Session, campaign: Campaign) -> Candidate:
    """Return the campaign's first candidate: the repo's HEAD when the campaign was registered."""
    return s.scalars(
        select(Candidate).where(Candidate.campaign_id == campaign.id).order_by(Candidate.id).limit(1)
    ).one()


def checkout_candidate(s: Session, campaign: Campaign) -> Candidate:
    """Return what a sweeping agent gets checked out: the head, or the baseline when its verdict was overridden."""
    return head(s, campaign) or baseline(s, campaign)


def units_file(units_dir: Path, measurement_id: MeasurementId) -> Path:
    """Return the path of a measurement's per-unit results, under ``PILOOP_HOME``.

    The file is outside every worktree and the database. The results are not a column, because every agent can read
    every table through ``piloop sql``, and an implementer must not tune against per-unit results. A file is not a sandbox,
    because an agent's shell can read ``PILOOP_HOME``. But the file is not in the schema, the briefs or any renderer.
    """
    return units_dir / f'{measurement_id}.json'


def save_units(units_dir: Path, measurement_id: MeasurementId, units: Units) -> None:
    """Store a measurement's per-unit results. With none, remove any old file that a reused id could find."""
    path = units_file(units_dir, measurement_id)
    if not units:
        path.unlink(missing_ok=True)
        return
    units_dir.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(units))


def pairs(candidate: Candidate, bar: Candidate, evaluator_id: EvaluatorId) -> tuple[Samples, Samples]:
    """Return the samples that ``candidate`` and ``bar`` measured next to each other under ``evaluator_id``.

    The i-th sample of a metric of the candidate and the i-th of the bar are one pair (``Measurement.pair``). A pair
    counts when both of its measurements succeeded.
    """
    mine: Samples = {}
    theirs: Samples = {}
    for m in candidate.measurements:
        other = m.pair
        if m.evaluator_id != evaluator_id or m.error is not None or other is None:
            continue
        if other.candidate_id != bar.id or other.error is not None:
            continue
        for name in m.metrics.keys() & other.metrics.keys():
            mine.setdefault(name, []).append(m.metrics[name])
            theirs.setdefault(name, []).append(other.metrics[name])
    return mine, theirs


def units(candidate: Candidate, evaluator_id: EvaluatorId, units_dir: Path) -> Units:
    """Return the candidate's per-unit results under ``evaluator_id``: each unit's mean over its measurements."""
    rows = [
        UNITS.validate_json(path.read_bytes())
        for m in candidate.measurements
        if m.evaluator_id == evaluator_id and m.error is None and (path := units_file(units_dir, m.id)).is_file()
    ]
    per_metric: dict[str, dict[str, list[float]]] = {}
    for row in rows:
        for name, values in row.items():
            for unit, value in values.items():
                per_metric.setdefault(name, {}).setdefault(unit, []).append(value)
    return {name: {unit: fmean(v) for unit, v in values.items()} for name, values in per_metric.items()}


def judge(
    candidate: Candidate,
    own: Candidate,
    evaluator_id: EvaluatorId,
    cfg: CampaignConfig,
    track: Track,
    units_dir: Path,
    *,
    head: Candidate | None,
    side: Lineage,
) -> tuple[Outcome, str]:
    """Return the daemon's verdict on ``candidate``, a candidate of ``track``, against ``own``, and the reason.

    ``own`` is the run's bar: its last keep, or its last candidate that kept against its bar and was rebased. It
    starts as the run's base. The run controller keeps a run's candidate on the trunk only while ``own`` is the head. A keep
    against an ``own`` that the head has moved past is rebased onto the head instead (``rebase``).

    On a ``Branch`` (``runs.branch``: its idea pinned another starting point), a run keeps what beats ``own``: a branch
    keep. Its reason also says whether it is above ``head`` on the track's primary.

    The campaign's verdict rule gives the verdict (``decide``): ``[plugins.verdict_rule]``, else ``bootstrap``. When
    the deciding metrics equal the parent's (``same``), the reason says so first: the change did not affect what the
    evaluation measures, whatever the verdict against the bar.
    """
    mine = metrics(candidate, evaluator_id)
    mine_samples, theirs_samples = pairs(candidate, own, evaluator_id)
    verdict, reason = decide(
        VERDICT_RULES.selected(cfg.plugins),
        track,
        Side(f'candidate#{candidate.id}', mine, units(candidate, evaluator_id, units_dir), mine_samples),
        Side(f'candidate#{own.id}', metrics(own, evaluator_id), units(own, evaluator_id, units_dir), theirs_samples),
    )
    primary = track.primary
    leader = metrics(head, evaluator_id).get(primary.name) if head is not None else None
    match side, verdict:
        # a rule from a plugin may keep a candidate that has no primary value
        case Branch(), Outcome.KEEP if head is not None and leader is not None and primary.name in mine:
            where = 'above' if primary.better(mine[primary.name], leader) else 'not above'
            reason += f'; a branch keep, {where} the head candidate#{head.id} ({primary.name} {metric(leader)})'
        case _:
            pass
    parent = candidate.parent
    if parent is not None and same(track, mine, metrics(parent, evaluator_id)):
        which = 'primary and guardrails' if track.guardrails else primary.name
        reason = f'no-op: {which} identical to parent candidate#{parent.id}; {reason}'
    return verdict, reason


def criterion(m: Metric) -> Criterion:
    """Return what a verdict rule reads of a metric of ``piloop.toml``."""
    return Criterion(m.name, m.direction, m.margin, m.floor)


def decide(rule: VerdictRule, track: Track, candidate: Side, bar: Side) -> tuple[Outcome, str]:
    """Return ``rule``'s verdict on ``candidate``, a candidate of ``track``, against ``bar``, and the reason.

    The rule gets the case as data (``piloop.plugins.verdict_rule.contract.Case``): the track's primary and guardrails,
    and both sides' results.
    """
    guardrails = tuple(map(criterion, track.guardrails))
    judged = rule.decide(Case(criterion(track.primary), guardrails, candidate, bar))
    return judged.verdict, judged.reason


def same(track: Track, mine: dict[str, float], theirs: dict[str, float]) -> bool:
    """Return whether ``mine`` has a primary value and equals ``theirs`` on the primary and every guardrail.

    Watch metrics are left out. A timing metric (``eval_seconds``) differs on every evaluation, and a metric that does
    not decide a verdict cannot make a change matter.
    """
    deciding = [track.primary, *track.guardrails]
    return track.primary.name in mine and all(mine.get(m.name) == theirs.get(m.name) for m in deciding)


class Rebased(EventModel):
    """A candidate that kept against its own bar after the head moved on was merged onto the head.

    Only one of ``rebased``, ``known`` and ``conflicts`` is set.
    """

    kind: Literal['candidate.rebased'] = 'candidate.rebased'
    head: CandidateId
    """The head that it was merged onto."""
    rebased: CandidateId | None = None
    """The merged candidate, which is measured and judged against that head next."""
    known: CandidateId | None = None
    """The candidate that the merged tree is already."""
    conflicts: list[str] | None = None
    """The paths that conflict."""


class CandidateImported(EventModel):
    """A reference candidate was imported (``piloop candidate import``)."""

    kind: Literal['candidate.imported'] = 'candidate.imported'
    ref: str
    revision: Revision


REBASED_SUFFIX = re.compile(r' \(rebased onto candidate#\d+\)$')
"""How the subject of a merge commit ends. A merge that is rebased again keeps the run commit's subject, with the
new head."""
MOVED = 'keeps against its own bar, but the head moved on to candidate#{head}: '
"""How the reason of a candidate to rebase starts. The candidate is ``interesting`` (``Cause.MOVED``), never a keep off
the trunk."""


def rebase(s: Session, cfg: CampaignConfig, repo: Repository, candidate: Candidate, onto: Candidate) -> Rebased:
    """Merge ``candidate``'s version onto the head ``onto``. Return how: the merged candidate to measure and judge, if any.

    ``candidate`` kept against its own bar after the head moved on. So it is ``interesting`` (``Cause.MOVED``), and its
    reason ends with the result of the merge. The merge is a version made without a checkout (``Repository.merge``),
    and kept (``RebaseVersion``). A new tree becomes a pending
    candidate of the same run, with ``onto`` as its parent. A merge that conflicts files a seed on the run's track to
    compose the two by hand, unless the campaign stopped. The seed is not pinned, so it runs from the head
    (``follow_up``). It supersedes the compose seeds of the run's earlier candidates that ``candidate`` contains
    (``supersede_composed``). A merged tree that is already a candidate (the head had the change) gives no new
    candidate. Emits ``Rebased``. The ideas that wait for the run's idea wait for the seed instead
    (``wait_for_compose``).
    """
    subject = REBASED_SUFFIX.sub('', candidate.summary or '')
    message = f'{subject} (rebased onto candidate#{onto.id})'
    merged = repo.merge(onto.revision, candidate.revision, message)
    if isinstance(merged, Conflicts):
        paths = ', '.join(merged.paths)
        why = (
            f'it kept against its own bar but its merge onto the head candidate#{onto.id} conflicts in {paths}:'
            f' from the head, merge candidate#{candidate.id} ({candidate.revision}) and resolve the conflicts'
        )
        assert candidate.run is not None  # only a run's candidate is rebased
        stopped = s.get_one(Campaign, candidate.campaign_id).status is CampaignStatus.STOPPED
        seeds = (
            []
            if stopped
            else follow_up(
                s,
                candidate,
                [f'Compose candidate#{candidate.id} onto the head'],
                why,
                System.DAEMON,
                track=candidate.run.idea.track,
                pin=False,
            )
        )
        for seed in seeds:
            supersede_composed(s, repo, candidate, seed)
            wait_for_compose(s, candidate.run.idea, seed)
        filed = f': seed idea#{seeds[0].id} composes it by hand' if seeds else ''
        candidate.verdict_reason = f'{candidate.verdict_reason}; its merge onto the head conflicts in {paths}{filed}'
        rebased = Rebased(head=onto.id, conflicts=list(merged.paths))
    else:
        repo.keep(merged.revision, RebaseVersion(cfg.name, candidate.id))
        found, created = resolve(
            s, candidate.campaign_id, merged, parent=onto, run_id=candidate.run_id, origin=Origin.RUN
        )
        if created:
            candidate.verdict_reason = f'{candidate.verdict_reason}; rebased onto the head as candidate#{found.id}'
            rebased = Rebased(head=onto.id, rebased=found.id)
        else:
            candidate.verdict_reason = (
                f'{candidate.verdict_reason}; its merge onto the head is candidate#{found.id} already ({label(found)})'
            )
            rebased = Rebased(head=onto.id, known=found.id)
    emit(s, rebased, TargetRef(TargetKind.CANDIDATE, candidate.id), System.DAEMON)
    return rebased


def supersede_composed(s: Session, repo: Repository, candidate: Candidate, seed: Idea) -> None:
    """Link ``seed`` as superseding each compose seed of the run's earlier candidates that ``candidate`` contains.

    Composing ``candidate`` composes those too, so ``ideas.supersede`` rejects each one that is still a seed. A run of
    four chained commits that each conflict on the head then leaves one seed, not four.
    """
    earlier = select(Candidate).where(Candidate.run_id == candidate.run_id, Candidate.id != candidate.id)
    by_provenance = {str(TargetRef(TargetKind.CANDIDATE, c.id)): c for c in s.scalars(earlier)}
    composes = select(Idea).where(
        Idea.campaign_id == candidate.campaign_id,
        Idea.status == IdeaStatus.SEED,
        Idea.created_by == System.DAEMON,
        Idea.provenance.in_(by_provenance),
    )
    for old in s.scalars(composes):
        assert old.provenance is not None  # the IN clause matched it
        if repo.contains(candidate.revision, by_provenance[old.provenance].revision):
            s.add(IdeaLink(idea_id=seed.id, target_id=old.id, kind=LinkKind.SUPERSEDES))


NEVER_RAN = (IdeaStatus.SEED, IdeaStatus.PROPOSED, IdeaStatus.READY)
"""The statuses of an idea that has not started: the daemon can reject it when it is superseded and has no run
(``ideas.supersede``), and move its ``after`` links (``wait_for_compose``).

A failed attempt puts an idea back to ``ready`` if it has attempts left. The daemon does not cancel such an idea."""


def wait_for_compose(s: Session, idea: Idea, seed: Idea) -> None:
    """Move the ``after`` links to ``idea`` of the ideas that have not started to ``seed``, the compose of its change.

    Its run's change did not land on the trunk: the merge conflicted. They would start from a head without it, or
    be parked as stranded once ``idea`` is done. The change lands through the compose now, so they wait for it. A
    started idea keeps its link: it began without the change. The link triggers log the move.
    """
    links = select(IdeaLink).join(Idea, Idea.id == IdeaLink.idea_id)
    waiting = links.where(IdeaLink.target_id == idea.id, IdeaLink.kind == LinkKind.AFTER, Idea.status.in_(NEVER_RAN))
    for link in s.scalars(waiting).all():
        s.delete(link)
        s.add(IdeaLink(idea_id=link.idea_id, target_id=seed.id, kind=LinkKind.AFTER))
    s.flush()


def reproposed(candidate: Candidate, run: Run) -> bool:
    """Return whether ``run``'s commit proposes the known ``candidate`` again, so that the run judges it again.

    It is a ``judgeable`` candidate of another run on the same side of the trunk (``runs.branch``). Its verdict came
    from another bar, or from a merge onto the head that conflicted. A compose that reproduces the tree must be able to
    land. A run on the other side never judges it: a branch run's keep of a trunk run's candidate would move the head,
    and a trunk run's keep of a branch run's candidate never would.
    """
    other = candidate.run
    return other is not None and other.id != run.id and other.branch == run.branch and judgeable(candidate)


def resolve(
    s: Session,
    campaign_id: CampaignId,
    version: Version,
    *,
    parent: Candidate | None,
    run_id: RunId | None,
    origin: Origin,
) -> tuple[Candidate, bool]:
    """Return the candidate for the tree of ``version``: the existing one, or a new row of ``origin`` under ``parent``.

    Returns ``(candidate, created)``. An existing candidate is returned unchanged: a version that reverts to a known
    state is that state, whatever run or version reached it. Two runs that reach one new tree at once share it: the
    insert that loses the race on ``UNIQUE(campaign_id, tree_sha)`` returns the winner's row.
    """
    known = select(Candidate).where(Candidate.campaign_id == campaign_id, Candidate.tree_sha == version.tree)
    found = s.scalar(known)
    if found is not None:
        return found, False
    candidate = Candidate(
        campaign_id=campaign_id,
        run_id=run_id,
        parent_id=parent.id if parent else None,
        tree_sha=version.tree,
        revision=version.revision,
        summary=version.summary,
        origin=origin,
    )
    try:
        with s.begin_nested():
            s.add(candidate)
    except IntegrityError:
        return s.scalars(known).one(), False
    return candidate, True


class ImportRefused(Exception):
    """``import_reference`` cannot register the commit. The message says why."""


def import_reference(
    s: Session,
    campaign: Campaign,
    cfg: CampaignConfig,
    repo: Repository,
    ref: str,
    *,
    summary: str | None,
    actor: Actor,
) -> Candidate:
    """Register the tree of ``ref`` (a version of the campaign's repo, in its vcs's terms) as a ``reference`` candidate.

    Like the baseline, it has no run and no parent. The version is kept (``ReferenceVersion``), so it outlives what named it.
    The daemon measures it under the current evaluator (``due``).

    Raises:
        ImportRefused: ``ref`` names no version, or its tree is already a candidate of the campaign.
    """
    try:
        version = repo.version(ref)
    except UnknownRevision as e:
        raise ImportRefused(str(e)) from e
    revision = version.revision
    candidate, created = resolve(s, campaign.id, version, parent=None, run_id=None, origin=Origin.REFERENCE)
    if not created:
        raise ImportRefused(f'the tree of {ref!r} already is candidate#{candidate.id} ({label(candidate)})')
    candidate.summary = summary or candidate.summary
    candidate.verdict_reason = f'imported from {ref} ({revision[:7]})'
    repo.keep(revision, ReferenceVersion(cfg.name, candidate.id))
    imported = CandidateImported(ref=ref, revision=revision)
    emit(s, imported, TargetRef(TargetKind.CANDIDATE, candidate.id), actor)
    return candidate


def follow_up(
    s: Session,
    candidate: Candidate,
    titles: Sequence[str],
    why: str,
    actor: Actor,
    *,
    track: TrackName,
    pin: bool = True,
) -> list[Idea]:
    """File one seed per title on ``track``, pinned to ``candidate`` unless ``pin`` is False. Return the new seeds.

    A title that the campaign already has gets `` (from candidate#<id>)``. A follow-up already filed from this candidate
    with that title, under either name and in any status, is not filed again: a rejected compose seed stays rejected
    when the candidate is judged again. These are seeds, not ready ideas: research develops them before they run. A
    pinned one runs as a branch run when its candidate is not the computed base (``[roles.implementer] explore_every``). An
    unpinned one runs from the head, as a trunk run.
    """
    ideas: list[Idea] = []
    hypothesis = f'Follow-up of candidate#{candidate.id} ({candidate.summary or "no summary"})'
    provenance = f'candidate#{candidate.id}'
    for title in titles:
        names = (title, f'{title} (from {provenance})')
        taken = s.scalars(select(Idea).where(Idea.campaign_id == candidate.campaign_id, Idea.title.in_(names))).all()
        name = next((n for n in names if n not in {i.title for i in taken}), None)
        if name is None or any(i.provenance == provenance for i in taken):
            continue
        idea = Idea(
            campaign_id=candidate.campaign_id,
            title=name,
            hypothesis=f'{hypothesis}: {why}',
            status=IdeaStatus.SEED,
            track=track,
            base_candidate_id=candidate.id if pin else None,
            provenance=provenance,
            created_by=actor.signature,
        )
        s.add(idea)
        s.flush()
        ideas.append(idea)
    return ideas


@dataclass(frozen=True)
class Trunk:
    """The lineage of the head: the keeps that no branch run made, from the baseline."""


@dataclass(frozen=True)
class Branch:
    """A lineage that a branch run started from ``root``: its keeps never move the head."""

    root: Candidate


type Lineage = Trunk | Branch
"""Where a candidate grew, or where a run keeps (``runs.branch``)."""


def side(run: Run) -> Lineage:
    """Return where ``run`` keeps: on a branch from its base, or on the trunk."""
    return Branch(run.base) if run.branch else Trunk()


def lineage(candidate: Candidate) -> Lineage:
    """Return the lineage of ``candidate``: the branch of the nearest branch run among its ancestors, else the trunk.

    A branch from the baseline is a different lineage from the trunk that grew from it.
    """
    while (run := candidate.run) is not None:
        if run.branch:
            return Branch(run.base)
        candidate = run.base
    return Trunk()


@dataclass(frozen=True)
class Line:
    """One lineage of keeps, with its latest keep."""

    lineage: Lineage
    latest: Candidate
    keeps: int


def lineages(s: Session, campaign: Campaign) -> list[Line]:
    """Return every lineage of keeps: the trunk first (its latest keep is the head), then the branches, newest first."""
    found: dict[Lineage, Line] = {}
    for c in s.scalars(keeps(campaign).order_by(Candidate.id)):
        at = lineage(c)
        known = found.get(at)
        found[at] = Line(at, c, (known.keeps if known else 0) + 1)
    return sorted(found.values(), key=lambda line: (isinstance(line.lineage, Branch), -line.latest.id))


def session_agent_session(s: Session, candidate: Candidate) -> AgentSession | None:
    """Return the implementer whose session made ``candidate`` (through ``agent_sessions.run_id``). None for the baseline."""
    return latest_agent_session(s, candidate.run_id) if candidate.run_id is not None else None


def session_tokens(s: Session, candidate: Candidate) -> int | None:
    """Return the tokens that the session that made ``candidate`` spent (``agent_sessions.usage`` total). None when unknown."""
    agent_session = session_agent_session(s, candidate)
    return agent_session.usage.total if agent_session is not None and agent_session.usage else None
