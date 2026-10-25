from datetime import datetime, timedelta

from pydantic import Field

from piloop.db.models import AgentSession, AgentSessionState
from piloop.plugins.trigger.contract import Backlog, Fire, Pass, Trigger
from piloop.plugins.trigger.kit.reasons import futile, news


class LowWaterTrigger(Trigger):
    """The backlog of runnable ideas is shorter than its target, and there is a reason to sweep now.

    The target is one idea per run slot plus ``buffer``. So ideas exist before slots free up, and a free slot does not
    wait a whole sweep for an idea. The ideas that the role's sweeps in flight can insert already count as backlog.
    Nothing is requested while every slot of the role is busy. Runnable ideas count. Ideas blocked only by
    prerequisites in flight also count, because they run when those finish. An idea blocked by a prerequisite that is
    not running does not count. Below the target, a sweep fires for one of these reasons:

    - The campaign has no sweep yet, so there is nothing to wait for.
    - The last batch was killed. Lost prompts, timeouts or a daemon stop say nothing about the backlog, so the batch
      is retried once. A sweep that someone cancelled is not retried: after it, only new results start the next one. This applies to a streak of killed rows no longer than one batch (``max_concurrent``). A second
      killed batch is a systemic fault and counts as a normal last sweep.
    - New results: runs finished since the role's latest row started, which that sweep could not read. A sweep with
      nothing new to read reads the same history and proposes nothing. So each consecutive sweep that defined no idea
      doubles the number of finished runs the next sweep waits for. A sweep that defines an idea resets this. The
      wait is never longer than what can still come: with no run in flight, the finished runs are all there is.
    - The backlog is low. The role keeps sweeping while the backlog is short, not only when results come in or a slot
      is idle. A sweep that defined an idea is followed at once. After empty sweeps, the role rests 2**n − 1 times as
      long as its latest ended sweep took. n is the number of consecutive ended sweeps, killed ones included, that
      defined no idea. An empty queue never deadlocks, and a role that keeps finding nothing uses at most 1/2**n of
      the wall-clock time.

    It requests enough sweeps to fill the shortfall at the role's ``max_ideas`` each, within the role's free slots.
    Nothing is requested while the queue holds runnable ideas for the head to be measured again (``Backlog.on_hold``):
    no idea that a sweep adds could run sooner.
    """

    buffer: int = Field(default=2, ge=0)
    """The number of ready ideas to keep in addition to one for each run slot (the ``max_concurrent`` of every role
    that does runs). With this backlog, a free slot finds an idea at once and does not wait for a sweep. A larger value
    makes implementers wait less for the role, but the ideas they run are older."""

    def fire(self, p: Pass) -> Fire | None:
        backlog = p.backlog(self.buffer)
        if backlog.short <= 0 or futile(p, backlog):
            return None
        batch = min(-(-backlog.short // backlog.per_sweep), p.max_concurrent - backlog.sweeps)
        reason = self._reason(p, backlog)
        return None if reason is None else Fire(reason, count=batch)

    def _reason(self, p: Pass, backlog: Backlog) -> str | None:
        if (reason := news(p, str(backlog))) is not None:
            return reason
        assert p.last is not None  # Without a row, every backlog is news.
        if p.cancel(p.last) is not None:
            return None
        empty = p.empty_sweeps(ENDED)
        if not empty:
            return str(backlog)
        rest, rested = _rest(empty)
        if p.now < rested:
            return None
        return f'{backlog}; rested {rest.total_seconds():.0f}s after {len(empty)} sweeps that defined no idea'

    def due_in(self, p: Pass) -> timedelta | None:
        """While the role rests after empty sweeps and the backlog is short: when the rest ends."""
        if p.last is None or p.last.state not in ENDED:
            return None
        empty = p.empty_sweeps(ENDED)
        if not empty:
            return None
        _, rested = _rest(empty)
        return rested - p.now if rested > p.now else None


ENDED = (AgentSessionState.EXITED, AgentSessionState.KILLED)


def _rest(empty: list[AgentSession]) -> tuple[timedelta, datetime]:
    """How long the role rests after ``empty``, and until when. ``empty``: its latest ended sweeps without an idea."""
    latest = empty[0]
    assert latest.ended_at is not None  # An exited or killed row has ended.
    rest = (2 ** len(empty) - 1) * (latest.ended_at - latest.started_at)
    return rest, latest.ended_at + rest
