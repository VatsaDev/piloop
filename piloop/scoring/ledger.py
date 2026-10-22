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

from git import Repo
from git.exc import BadName
from sqlalchemy import Select, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from piloop import git
from piloop.actor import Actor, System
from piloop.config import CampaignConfig, Metric, Track
from piloop.db.events import emit
from piloop.db.models import (
    OPEN_RUNS,
    PROCESS,
    AgentSession,
    Campaign,
    CampaignStatus,
    Candidate,
    Evaluator,
    Event,
    Idea,
    IdeaStatus,
    Run,
    TargetKind,
    Verdict,
    latest_agent_session,
)
from piloop.plugins.verdict_rule import contract as rules
from piloop.plugins.verdict_rule.contract import VERDICT_RULES, Case, Criterion, Side, VerdictRule
from piloop.scoring.evaluate import UNITS, Units
from piloop.text import metric

DAEMON = System.DAEMON
"""The ``verdict_by`` of a verdict that the daemon set. The daemon never overwrites a verdict by anyone else."""
SKIPPED = 'not evaluated: Piloop-Eval: skip'
"""The ``verdict_reason`` of the daemon's ``unmeasured`` verdict on a commit that asked not to be evaluated.

When a commit without the trailer reaches the same tree, the daemon measures and judges it (``skipped``).
"""


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


def metrics(candidate: Candidate, evaluator_id: int) -> dict[str, float]:
    """Return the mean of each metric over the candidate's successful measurements under ``evaluator_id``."""
    rows = [m.metrics for m in candidate.measurements if m.evaluator_id == evaluator_id and m.error is None]
    names = {name for row in rows for name in row}
    return {name: fmean(values) for name in names if (values := [r[name] for r in rows if name in r])}


def score(candidate: Candidate, evaluator_id: int, primary: Metric) -> float | None:
    """Return the primary metric under ``evaluator_id``, or None when no successful measurement has it."""
    return metrics(candidate, evaluator_id).get(primary.name)


def measured(candidate: Candidate, evaluator_id: int) -> bool:
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
    while candidate.verdict not in (Verdict.KEEP, Verdict.REFERENCE) and candidate.parent is not None:
        candidate = candidate.parent
    return candidate


def raises_bar(candidate: Candidate, *, branch: bool) -> bool:
    """Whether a run's commit that is ``candidate`` becomes the run's bar for its next commits.

    A keep does, and a known keep that the run reached again too. On a trunk run, a branch keep does not: a trunk run
    that merges a branch keep onto the head must beat the head, so a branch's numbers are never a trunk run's bar.
    """
    trunk = candidate.run is None or not candidate.run.branch
    return candidate.verdict is Verdict.KEEP and (branch or trunk)


def track_of(cfg: CampaignConfig, candidate: Candidate) -> Track:
    """Return the track of the candidate's run (its idea's track). A renderer shows the candidate's score on it.

    The baseline and a reference get the first track (``fallback_track``).
    """
    return fallback_track(cfg, candidate.run.idea.track if candidate.run is not None else None)


def fallback_track(cfg: CampaignConfig, name: str | None) -> Track:
    """Return the track called ``name``, else the first track. An idea can name a track that ``piloop.toml`` removed."""
    return next((t for t in cfg.all_tracks if t.name == name), cfg.all_tracks[0])


def due(s: Session, campaign: Campaign) -> list[Candidate]:
    """Return what the daemon measures with the current evaluator, most urgent first, each once.

    1. The head: an unpinned run starts from it and is judged against it, so the queue waits only for it
       (``head_unmeasured``).
    2. The bar of each open run (``bar_of`` its base), oldest run first: the run is judged against it.
    3. Each candidate that someone asked to measure again (``requested``), oldest request first. A crash that the daemon
       judges again (``judged_again``) comes after the bar of its parent.
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
        Candidate.campaign_id == campaign.id, Candidate.verdict == Verdict.REFERENCE, ~Candidate.measurements.any()
    )
    asked = requested(s, campaign)
    active = campaign.status is CampaignStatus.ACTIVE
    found = [head(s, campaign), *(bar_of(run.base) for run in s.scalars(open_runs.order_by(Run.id)))] if active else []
    for candidate in asked:
        if judged_again(candidate) and candidate.parent is not None:
            found.append(bar_of(candidate.parent))
        found.append(candidate)
    if active:
        ready = (Idea.campaign_id == campaign.id, Idea.status == IdeaStatus.READY)
        found += [bar_of(c) for c in s.scalars(pins.where(*ready).order_by(Idea.id))]
        found += s.scalars(new_references.order_by(Candidate.id))
    evaluator_id = campaign.evaluator_id or 0
    return [c for c in dict.fromkeys(found) if c is not None and (c in asked or not measured(c, evaluator_id))]


def judged_again(candidate: Candidate) -> bool:
    """Whether a new measurement of ``candidate`` judges it again: a ``crash`` of a run that the daemon judged."""
    return candidate.verdict is Verdict.CRASH and candidate.verdict_by == DAEMON and candidate.run_id is not None


def head_unmeasured(s: Session, campaign: Campaign) -> bool:
    """Whether the head has no measurement from the current evaluator: the queue then holds the unpinned ideas."""
    trunk = head(s, campaign)
    return trunk is not None and not measured(trunk, campaign.evaluator_id or 0)


REMEASURE_REQUESTED = 'candidate.remeasure_requested'
"""The event on a candidate that someone asked the daemon to measure again (``request_remeasure``)."""


class RemeasureRefused(Exception):
    """``request_remeasure`` cannot ask for the candidate. The message says why."""


def request_remeasure(s: Session, candidate: Candidate, actor: Actor) -> None:
    """Ask the daemon to measure ``candidate`` again with the current evaluator. The caller commits.

    Use it for a measurement that a fault of the host spoiled (a full disk, a lost GPU), or for one more repeat. The
    request is the ``REMEASURE_REQUESTED`` event, and ``requested`` lists it until a newer measurement exists. The
    daemon judges a ``crash`` again (``RunController.remeasure``). Any other verdict stays.

    Raises:
        RemeasureRefused: the daemon never measures the candidate (``pending``, ``unmeasured`` or ``recovered``).
    """
    if candidate.verdict in (Verdict.PENDING, Verdict.UNMEASURED, Verdict.RECOVERED):
        raise RemeasureRefused(f'candidate#{candidate.id} is {candidate.verdict}: the daemon never measures it again')
    emit(s, REMEASURE_REQUESTED, TargetKind.CANDIDATE, candidate.id, actor=actor)


def requested(s: Session, campaign: Campaign) -> list[Candidate]:
    """Return the candidates that someone asked to measure again, with no measurement since, oldest request first.

    Only a measurement with the current evaluator answers a request.
    """
    evaluator_id = campaign.evaluator_id or 0
    asks = s.execute(
        select(Event.target_id, func.max(Event.at))
        .join(Candidate, Candidate.id == Event.target_id)
        .where(
            Event.kind == REMEASURE_REQUESTED,
            Event.target_kind == TargetKind.CANDIDATE,
            Candidate.campaign_id == campaign.id,
        )
        .group_by(Event.target_id)
        .order_by(func.max(Event.id))
    ).all()
    found: list[Candidate] = []
    for candidate_id, asked in asks:
        candidate = s.get_one(Candidate, candidate_id)
        if all(m.created_at < asked for m in candidate.measurements if m.evaluator_id == evaluator_id):
            found.append(candidate)
    return found


REJUDGE_REQUESTED = 'candidate.rejudge_requested'
"""The event on a candidate that someone measured by hand (``piloop measurement add``): the daemon judges it again."""
REJUDGE_FAILED = 'candidate.rejudge_failed'
"""The daemon's event on a candidate that it could not judge again, with the error. It answers the request."""
REJUDGED = (Verdict.CRASH, Verdict.DISCARD, Verdict.INTERESTING)
"""The daemon's verdicts that a measurement by hand has judged again. A keep stays: runs started from it as a bar."""


def rejudgeable(candidate: Candidate) -> bool:
    """Return whether the daemon judges the candidate again after a measurement by hand.

    Only a candidate of a run, whose verdict is the daemon's and one of ``REJUDGED``. Anyone else's verdict stays.
    """
    return candidate.run_id is not None and candidate.verdict_by == DAEMON and candidate.verdict in REJUDGED


def rejudges(s: Session, campaign: Campaign) -> list[Candidate]:
    """Return the candidates to judge again: still ``rejudgeable``, asked after their last judgement, oldest first.

    A request is answered by a later ``candidate.judged`` event, or by a ``REJUDGE_FAILED`` one.
    """
    asks = s.execute(
        select(Event.target_id, func.max(Event.id))
        .join(Candidate, Candidate.id == Event.target_id)
        .where(
            Event.kind == REJUDGE_REQUESTED,
            Event.target_kind == TargetKind.CANDIDATE,
            Candidate.campaign_id == campaign.id,
        )
        .group_by(Event.target_id)
        .order_by(func.max(Event.id))
    ).all()
    found: list[Candidate] = []
    for candidate_id, asked in asks:
        answered = s.scalar(
            select(func.max(Event.id)).where(
                Event.kind.in_(('candidate.judged', REJUDGE_FAILED)),
                Event.target_kind == TargetKind.CANDIDATE,
                Event.target_id == candidate_id,
            )
        )
        candidate = s.get_one(Candidate, candidate_id)
        if (answered is None or answered < asked) and rejudgeable(candidate):
            found.append(candidate)
    return found


def references(s: Session, campaign: Campaign) -> list[Candidate]:
    """Return the campaign's imported starting points (``piloop candidate import``), oldest first."""
    query = select(Candidate).where(Candidate.campaign_id == campaign.id, Candidate.verdict == Verdict.REFERENCE)
    return list(s.scalars(query.order_by(Candidate.id)))


def keeps(campaign: Campaign) -> Select[Candidate]:
    """Select the campaign's kept candidates. The baseline is one while its verdict is ``keep``."""
    return select(Candidate).where(Candidate.campaign_id == campaign.id, Candidate.verdict == Verdict.KEEP)


def baseline(s: Session, campaign: Campaign) -> Candidate:
    """Return the campaign's first candidate: the repo's HEAD when the campaign was registered."""
    return s.scalars(
        select(Candidate).where(Candidate.campaign_id == campaign.id).order_by(Candidate.id).limit(1)
    ).one()


def checkout_candidate(s: Session, campaign: Campaign) -> Candidate:
    """Return what a sweeping agent gets checked out: the head, or the baseline when its verdict was overridden."""
    return head(s, campaign) or baseline(s, campaign)


def units_file(units_dir: Path, measurement_id: int) -> Path:
    """Return the path of a measurement's per-unit results, under ``PILOOP_HOME``.

    The file is outside every worktree and the database. The results are not a column, because every agent can read
    every table through ``piloop sql``, and an implementer must not tune against per-unit results. A file is not a sandbox,
    because an agent's shell can read ``PILOOP_HOME``. But the file is not in the schema, the briefs or any renderer.
    """
    return units_dir / f'{measurement_id}.json'


def save_units(units_dir: Path, measurement_id: int, units: Units) -> None:
    """Store a measurement's per-unit results. With none, remove any old file that a reused id could find."""
    path = units_file(units_dir, measurement_id)
    if not units:
        path.unlink(missing_ok=True)
        return
    units_dir.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(units))


def units(candidate: Candidate, evaluator_id: int, units_dir: Path) -> Units:
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
    evaluator_id: int,
    cfg: CampaignConfig,
    track: Track,
    units_dir: Path,
    *,
    head: Candidate | None,
    branch: bool,
) -> tuple[Verdict, str]:
    """Return the daemon's verdict on ``candidate``, a candidate of ``track``, against ``own``, and the reason.

    ``own`` is the run's bar: its last keep, or its last candidate that kept against its bar and was rebased. It
    starts as the run's base. The run controller keeps a run's candidate on the trunk only while ``own`` is the head. A keep
    against an ``own`` that the head has moved past is rebased onto the head instead (``rebase``).

    A branch run (``runs.branch``: its idea pinned another starting point) keeps what beats ``own``: a branch keep.
    Its reason also says whether it is above ``head`` on the track's primary.

    The campaign's verdict rule gives the verdict (``decide``): ``[plugins.verdict_rule]``, else ``bootstrap``. When
    the deciding metrics equal the parent's (``same``), the reason says so first: the change did not affect what the
    evaluation measures, whatever the verdict against the bar.
    """
    mine = metrics(candidate, evaluator_id)
    table = cfg.plugins.get('verdict_rule')
    verdict, reason = decide(
        VERDICT_RULES.get(table.provider if table is not None else rules.DEFAULT),
        track,
        cfg.confidence,
        Side(f'candidate#{candidate.id}', mine, units(candidate, evaluator_id, units_dir)),
        Side(f'candidate#{own.id}', metrics(own, evaluator_id), units(own, evaluator_id, units_dir)),
        table.parameters if table is not None else '{}',
    )
    primary = track.primary
    leader = metrics(head, evaluator_id).get(primary.name) if head is not None else None
    # a rule from a plugin may keep a candidate that has no primary value
    if branch and verdict is Verdict.KEEP and head is not None and leader is not None and primary.name in mine:
        where = 'above' if primary.better(mine[primary.name], leader) else 'not above'
        reason += f'; a branch keep, {where} the head candidate#{head.id} ({primary.name} {metric(leader)})'
    parent = candidate.parent
    if parent is not None and same(track, mine, metrics(parent, evaluator_id)):
        which = 'primary and guardrails' if track.guardrails else primary.name
        reason = f'no-op: {which} identical to parent candidate#{parent.id}; {reason}'
    return verdict, reason


def criterion(m: Metric) -> Criterion:
    """Return what a verdict rule reads of a metric of ``piloop.toml``."""
    return Criterion(m.name, m.direction, m.margin, m.floor)


def decide(
    rule: VerdictRule, track: Track, confidence: float, candidate: Side, bar: Side, parameters: str = '{}'
) -> tuple[Verdict, str]:
    """Return ``rule``'s verdict on ``candidate``, a candidate of ``track``, against ``bar``, and the reason.

    The rule gets the case as data (``piloop.plugins.verdict_rule.contract.Case``): the track's primary and guardrails,
    the confidence, both sides' results and its own parameters. Its outcome is the ``Verdict`` of the same value.
    """
    guardrails = tuple(map(criterion, track.guardrails))
    judged = rule.decide(Case(criterion(track.primary), guardrails, confidence, candidate, bar, parameters))
    return Verdict(judged.verdict), judged.reason


def same(track: Track, mine: dict[str, float], theirs: dict[str, float]) -> bool:
    """Return whether ``mine`` has a primary value and equals ``theirs`` on the primary and every guardrail.

    Watch metrics are left out. A timing metric (``eval_seconds``) differs on every evaluation, and a metric that does
    not decide a verdict cannot make a change matter.
    """
    deciding = [track.primary, *track.guardrails]
    return track.primary.name in mine and all(mine.get(m.name) == theirs.get(m.name) for m in deciding)


REBASED = 'candidate.rebased'
"""The event on a candidate that kept against its own bar after the head moved on.

Its payload has ``head``, the head it was merged onto, and one of: ``rebased`` (the merged candidate, which is
measured and judged against that head next), ``known`` (the merged tree is already a candidate) or ``conflicts`` (the
paths).
"""
REBASED_SUFFIX = re.compile(r' \(rebased onto candidate#\d+\)$')
"""How the subject of a merge commit ends. A merge that is rebased again keeps the run commit's subject, with the
new head."""
MOVED = 'keeps against its own bar, but the head moved on to candidate#{head}: '
"""How the reason of a candidate to rebase starts. The candidate is ``interesting``, never a keep off the trunk."""


def rebase(s: Session, cfg: CampaignConfig, repo: Repo, candidate: Candidate, onto: Candidate) -> Candidate | None:
    """Merge ``candidate``'s commit onto the head ``onto``. Return the merged candidate to measure and judge, or None.

    ``candidate`` kept against its own bar after the head moved on. So it is ``interesting`` (``MOVED``), and its
    reason ends with the result of the merge. The merge is a commit with both as parents, made without a worktree
    (``git.merge``) and pinned at ``refs/piloop/<campaign>/rebase/<candidate id>``. A new tree becomes a pending
    candidate of the same run, with ``onto`` as its parent. A merge that conflicts files a seed on the run's track to
    compose the two by hand, unless the campaign stopped. The seed is not pinned, so it runs from the head
    (``follow_up``). A merged tree that is already a candidate (the head had the change) gives no new candidate. Emits
    ``REBASED``.
    """
    subject = REBASED_SUFFIX.sub('', candidate.summary or '')
    merged = git.merge(repo, onto.commit_sha, candidate.commit_sha, f'{subject} (rebased onto candidate#{onto.id})')
    payload: dict[str, object] = {'head': onto.id}
    found: Candidate | None = None
    if isinstance(merged, list):
        paths = ', '.join(merged)
        why = (
            f'it kept against its own bar but its merge onto the head candidate#{onto.id} conflicts in {paths}:'
            f' from the head, git merge {candidate.commit_sha} and resolve the conflicts'
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
                DAEMON,
                track=candidate.run.idea.track,
                pin=False,
            )
        )
        filed = f': seed idea#{seeds[0].id} composes it by hand' if seeds else ''
        candidate.verdict_reason = f'{candidate.verdict_reason}; its merge onto the head conflicts in {paths}{filed}'
        payload['conflicts'] = merged
    else:
        git.pin(repo, f'refs/piloop/{cfg.name}/rebase/{candidate.id}', merged)
        found, created = resolve(s, candidate.campaign_id, repo, merged, parent=onto, run_id=candidate.run_id)
        if created:
            candidate.verdict_reason = f'{candidate.verdict_reason}; rebased onto the head as candidate#{found.id}'
            payload['rebased'] = found.id
        else:
            candidate.verdict_reason = (
                f'{candidate.verdict_reason}; its merge onto the head is candidate#{found.id} already ({found.verdict})'
            )
            payload['known'] = found.id
            found = None
    emit(s, REBASED, TargetKind.CANDIDATE, candidate.id, payload=payload)
    return found


def skipped(candidate: Candidate) -> bool:
    """Return whether the candidate has the daemon's ``unmeasured`` or ``recovered`` verdict.

    A measurement may replace either: a later commit of the same tree is measured and judged like a new one.
    """
    return candidate.verdict_by == DAEMON and candidate.verdict in (Verdict.UNMEASURED, Verdict.RECOVERED)


def judgeable(candidate: Candidate) -> bool:
    """Return whether the daemon may set its verdict on ``candidate`` now: no keep, and nobody else's verdict.

    That is a pending candidate, or a verdict of the daemon that a measurement can change (``REJUDGED``, ``skipped``).
    A keep stays: runs started from it as their bar, and it may be the head. The daemon checks it in the transaction
    that judges, so another run's keep that landed during a measurement is never judged over.
    """
    judgements = (Verdict.PENDING, *REJUDGED, Verdict.UNMEASURED, Verdict.RECOVERED)
    return candidate.verdict_by in (None, DAEMON) and candidate.verdict in judgements


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
    s: Session, campaign_id: int, repo: Repo, sha: str, *, parent: Candidate | None, run_id: int | None
) -> tuple[Candidate, bool]:
    """Return the candidate for the tree of ``sha``: the existing one, or a new row with ``parent`` as its parent.

    Returns ``(candidate, created)``. An existing candidate is returned unchanged: a commit that reverts to a known
    state is that state, whatever run or commit reached it. Two runs that reach one new tree at once share it: the
    insert that loses the race on ``UNIQUE(campaign_id, tree_sha)`` returns the winner's row.
    """
    tree = git.tree_sha(repo, sha)
    known = select(Candidate).where(Candidate.campaign_id == campaign_id, Candidate.tree_sha == tree)
    found = s.scalar(known)
    if found is not None:
        return found, False
    candidate = Candidate(
        campaign_id=campaign_id,
        run_id=run_id,
        parent_id=parent.id if parent else None,
        tree_sha=tree,
        commit_sha=sha,
        summary=git.subject_line(repo, sha),
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
    s: Session, campaign: Campaign, cfg: CampaignConfig, repo: Repo, ref: str, *, summary: str | None, actor: Actor
) -> Candidate:
    """Register the tree of ``ref`` (a commit, branch or tag in the campaign's repo) as a ``reference`` candidate.

    Like the baseline, it has no run and no parent. The commit is pinned at ``refs/piloop/<campaign>/reference/<id>``,
    so it outlives its branch. The daemon measures it under the current evaluator (``unmeasured``).

    Raises:
        ImportRefused: ``ref`` names no commit, or its tree is already a candidate of the campaign.
    """
    try:
        sha = repo.commit(ref).hexsha
    except (BadName, ValueError) as e:
        raise ImportRefused(f'{ref!r} names no commit in {repo.working_dir}: {e}') from e
    candidate, created = resolve(s, campaign.id, repo, sha, parent=None, run_id=None)
    if not created:
        raise ImportRefused(f'the tree of {ref!r} already is candidate#{candidate.id} ({candidate.verdict})')
    candidate.summary = summary or candidate.summary
    candidate.verdict, candidate.verdict_by = Verdict.REFERENCE, actor.signature
    candidate.verdict_reason = f'imported from {ref} ({sha[:7]})'
    git.pin(repo, f'refs/piloop/{cfg.name}/reference/{candidate.id}', sha)
    emit(s, 'candidate.imported', TargetKind.CANDIDATE, candidate.id, actor=actor, payload={'ref': ref, 'commit': sha})
    return candidate


NOTE = 'note: '
"""How the reason of a noted candidate starts. An agent marked a regressed result as a lead (``note``)."""


class NoteRefused(Exception):
    """``note`` cannot mark the candidate. The message says why."""


def note(candidate: Candidate, reason: str, actor: Actor) -> None:
    """Mark a measured ``discard`` or ``interesting`` as a lead: ``interesting``, with ``reason`` before the daemon's.

    The result regressed, but the tree is worth starting from. It stays pinnable and in view. Like any
    ``interesting``, it is not a keep and moves no bar. The verdict is now ``actor``'s, so the daemon never overwrites
    it.

    Raises:
        NoteRefused: the reason is empty, or the candidate is not a measured discard or interesting. A keep needs no
            note. A pending, crashed, unmeasured, recovered or reference candidate has no result to note.
    """
    if not reason.strip():
        raise NoteRefused('a note needs a reason: what the follow-ups from this tree are')
    if candidate.verdict not in (Verdict.DISCARD, Verdict.INTERESTING):
        raise NoteRefused(
            f'candidate#{candidate.id} is {candidate.verdict} ({candidate.verdict_reason or "no reason"}):'
            ' only a measured discard or interesting takes a note'
        )
    candidate.verdict, candidate.verdict_by = Verdict.INTERESTING, actor.signature
    candidate.verdict_reason = (
        NOTE + reason.strip() + (f'; {candidate.verdict_reason}' if candidate.verdict_reason else '')
    )


def follow_up(
    s: Session,
    candidate: Candidate,
    titles: Sequence[str],
    why: str,
    actor: Actor,
    *,
    track: str,
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
        emit(s, 'idea.created', TargetKind.IDEA, idea.id, actor=actor)
        ideas.append(idea)
    return ideas


@dataclass(frozen=True)
class Branch:
    """One lineage of keeps: the starting point it grew from, and its latest keep."""

    root: Candidate
    branched: bool
    """True when a branch run started the lineage at ``root``. False for the trunk."""
    latest: Candidate
    keeps: int


def branch_root(candidate: Candidate) -> tuple[Candidate, bool]:
    """Return the starting point of ``candidate``'s lineage, and whether a branch run started the lineage there.

    The starting point is the base of the nearest branch run among its ancestors. Without one, it is the first
    ancestor without a run: the baseline, or a reference that a trunk run started from. A branch from the baseline is
    a different lineage from the trunk that grew from it, so the flag tells them apart.
    """
    while (run := candidate.run) is not None:
        if run.branch:
            return run.base, True
        candidate = run.base
    return candidate, False


def branches(s: Session, campaign: Campaign) -> list[Branch]:
    """Return every lineage of keeps: the trunk first (its latest keep is the head), then the branches, newest first."""
    found: dict[tuple[int, bool], Branch] = {}
    for c in s.scalars(keeps(campaign).order_by(Candidate.id)):
        root, branched = branch_root(c)
        known = found.get((root.id, branched))
        found[root.id, branched] = Branch(root, branched, c, (known.keeps if known else 0) + 1)
    return sorted(found.values(), key=lambda b: (b.branched, -b.latest.id))


def session_agent_session(s: Session, candidate: Candidate) -> AgentSession | None:
    """Return the implementer whose session made ``candidate`` (through ``agent_sessions.run_id``). None for the baseline."""
    return latest_agent_session(s, candidate.run_id) if candidate.run_id is not None else None


def session_checkpoint(s: Session, candidate: Candidate) -> str | None:
    """Return the Entire checkpoint of the session that made ``candidate`` (``agent_sessions.checkpoint_id``).

    None for the baseline, which has no run, and for a run whose agent session was not attached.
    """
    agent_session = session_agent_session(s, candidate)
    return agent_session.checkpoint_id if agent_session is not None else None


def session_tokens(s: Session, candidate: Candidate) -> int | None:
    """Return the tokens that the session that made ``candidate`` spent (``agent_sessions.usage`` total). None when unknown."""
    agent_session = session_agent_session(s, candidate)
    return agent_session.usage['total'] if agent_session is not None and agent_session.usage else None
