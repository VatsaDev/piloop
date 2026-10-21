"""The spend reading: the daemon polls a campaign's ``[spend] command`` and posts notices and escalations.

piloop does not know any vendor. The campaign's command prints the total that the campaign has spent so far, in USD,
for example a GPU cloud's bill for the campaign's environments. The daemon keeps the latest reading on the campaign
row. The reading is a fact for the stop rule (``Fact.SPEND``), and the built-in rule ``limits`` holds launches while
it is unknown or at ``[spend] limit``. The daemon holds, not an agent, so nothing launches past the limit even when no
agent is active.
"""

import math
import subprocess
from datetime import timedelta
from pathlib import Path
from subprocess import PIPE

from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.config import CampaignConfig, ClosingReview, Spend
from piloop.db.models import (
    Campaign,
    CampaignStatus,
    Comment,
    CommentKind,
    TargetKind,
    Worker,
    WorkerState,
    utcnow,
)
from piloop.db.triggers import DAEMON_ACTOR
from piloop.proc import STDERR_IN_ERROR, child_env, kill_group

TIMEOUT = 60
"""The seconds that the spend command can take. The daemon's tick waits for it, so a hung command fails."""
NOTICE_PRIORITY = 1
ESCALATION_PRIORITY = 0
"""An escalation from the guard stops all launches, so it is the most urgent item in the inbox."""


class SpendError(Exception):
    """The spend command did not print a spend. It failed, timed out or printed something else."""


def due(campaign: Campaign, spend: Spend) -> bool:
    """Return True if the command never ran, or last ran ``interval`` or more seconds ago."""
    return campaign.spend_at is None or utcnow() - campaign.spend_at >= timedelta(seconds=spend.interval)


def read(command: list[str], cwd: Path) -> float:
    """Run the spend command and parse its output as one finite, non-negative number of USD.

    Raises:
        SpendError: The command did not start, timed out, exited non-zero or printed something else.
    """
    try:
        proc = subprocess.Popen(command, cwd=cwd, env=child_env(), stdout=PIPE, stderr=PIPE, start_new_session=True)
    except OSError as e:
        raise SpendError(f'could not start: {e}') from e
    with proc:
        try:
            out, err = proc.communicate(timeout=TIMEOUT)
        except subprocess.TimeoutExpired as e:
            # Kill the children too. A child that holds the pipe open blocks the read below.
            kill_group(proc)
            proc.communicate()
            raise SpendError(f'timed out after {TIMEOUT}s') from e
    stdout, stderr = out.decode(errors='replace'), err.decode(errors='replace')
    if proc.returncode != 0:
        raise SpendError(f'exit {proc.returncode}: {stderr.strip()[-STDERR_IN_ERROR:]}')
    try:
        usd = float(stdout)
    except ValueError as e:
        raise SpendError(f'printed {stdout.strip()[:STDERR_IN_ERROR]!r}, not a number of USD') from e
    if not math.isfinite(usd) or usd < 0:
        raise SpendError(f'printed {usd}, not a spend')
    return usd


def record(s: Session, campaign: Campaign, spend: Spend, reading: float | SpendError) -> None:
    """Store a reading on the campaign row and post the notes and escalations that it causes. The caller commits.

    A reading posts a note for each ``notices`` threshold that it crossed since the previous reading. It posts an
    escalation if it crossed ``limit``. The first reading crosses every threshold below it. Because only a crossing
    posts, a raised limit escalates again when the spend reaches it. A failed reading escalates only if the previous
    reading did not fail.
    """
    campaign.spend_at = utcnow()
    if isinstance(reading, SpendError):
        if campaign.spend_error is None:
            _post(s, campaign, CommentKind.ESCALATION, ESCALATION_PRIORITY, _failed(reading))
        campaign.spend_error = str(reading)
        return
    before = campaign.spend_usd if campaign.spend_usd is not None else -math.inf
    campaign.spend_usd, campaign.spend_error = reading, None
    for threshold in spend.notices:
        if before < threshold <= reading:
            body = f'Spend notice: ${reading:,.2f} spent, past the ${threshold:,.2f} notice. The limit is ${spend.limit:,.2f}.'
            _post(s, campaign, CommentKind.NOTE, NOTICE_PRIORITY, body)
    if before < spend.limit <= reading:
        _post(s, campaign, CommentKind.ESCALATION, ESCALATION_PRIORITY, _limited(reading, spend.limit))


def _failed(error: SpendError) -> str:
    return (
        f'Spend unknown: the [spend] command failed ({error}). Nothing launches and no evaluation starts until it'
        ' prints a spend again. Runs in progress finish. Fix the command or its environment. It runs again every'
        ' interval.'
    )


def _limited(usd: float, limit: float) -> str:
    return (
        f'Spend limit reached: ${usd:,.2f} spent, limit ${limit:,.2f}. Nothing launches and no evaluation starts.'
        ' Runs in progress finish and are scored. To resume, raise [spend] limit in piloop.toml and run `piloop init`.'
    )


def review_waits(s: Session, campaign: Campaign, cfg: CampaignConfig) -> bool:
    """Whether the stopped campaign's closing review waits for the spend hold to end.

    That is ``[spend] closing_review = "wait"``, with an agent still requested. The daemon reads the spend of such a
    campaign, so that the hold can end.
    """
    if campaign.status is not CampaignStatus.STOPPED or cfg.spend is None:
        return False
    if cfg.spend.closing_review is not ClosingReview.WAIT:
        return False
    requested = select(Worker.id).where(Worker.campaign_id == campaign.id, Worker.state == WorkerState.REQUESTED)
    return s.scalar(requested.limit(1)) is not None


def review_skipped(s: Session, campaign: Campaign, held: str) -> None:
    """Post the escalation of a closing review that ``[spend] closing_review = "skip"`` dropped. The caller commits."""
    body = (
        f'Closing review skipped: the campaign stopped while the stop rule held its launches ({held}), and'
        ' [spend] closing_review = "skip".'
    )
    _post(s, campaign, CommentKind.ESCALATION, ESCALATION_PRIORITY, body)


def _post(s: Session, campaign: Campaign, kind: CommentKind, priority: int, body: str) -> None:
    s.add(
        Comment(
            target_kind=TargetKind.CAMPAIGN,
            target_id=campaign.id,
            author=DAEMON_ACTOR,
            kind=kind,
            priority=priority,
            body=body,
        )
    )
