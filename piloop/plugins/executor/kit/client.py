"""The worker's half of the leases library: the calls a worker makes, whatever carries them.

After the GitHub Actions runner (a session, one long-poll for messages, then acquire, renew and finish) and Buildkite
agent v3 (register, ping, accept, start, finish). ``Local`` makes the calls on ``Leases`` in this process: the worker
inside ``piloop serve`` takes that path. Another transport implements ``LeaseClient`` the same way, and the worker
does not change.
"""

from collections.abc import Mapping, Sequence
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict

from piloop.db.models import NoLease
from piloop.plugins.executor.contract import AgentReport, EvaluationReport
from piloop.plugins.executor.kit.capacity import HostSample
from piloop.plugins.executor.kit.leases import (
    Answer,
    Grant,
    JobHandle,
    Leases,
    PromptMessage,
    Revoke,
    Timing,
    Work,
)


class WorkerInfo(BaseModel):
    """What a worker says of itself when it starts."""

    model_config = ConfigDict(frozen=True)

    protocol: Literal[1] = 1
    version: str
    """The piloop version that it runs."""
    pool: str
    runners: list[str]
    """The evaluator runners that it has."""
    sample: HostSample


class Registration(BaseModel):
    """What a worker learns when it registers: its name, its timing, and the leases that it holds."""

    model_config = ConfigDict(frozen=True)

    worker: str
    """The caller's name: the transport's principal, never read from what the worker sends."""
    timing: Timing
    held: list[Grant]
    """The worker's live leases, with each handle: what a restarted worker finds again."""
    cursor: int
    """Read the messages after this event: the worker's ack floor, or the newest event for a new worker."""


class LeaseClient(Protocol):
    """The calls of a worker on its leases.

    Each call that names a lease raises ``StaleLease`` when it is not the worker's live, newest lease: then the worker
    stops the job.
    """

    def register(self, info: WorkerInfo) -> Registration:
        """Start a worker's session: record it, and return its timing and its live leases."""
        ...

    def acquire(self, sample: HostSample) -> Grant | NoLease:
        """Take a queued job, or say why there is none."""
        ...

    def started(self, lease: int, handle: JobHandle, session_id: str | None) -> None:
        """The job started, and runs at ``handle``."""
        ...

    def heartbeat(self, leases: Mapping[int, str | None], sample: HostSample) -> dict[int, Answer]:
        """Renew the leases that the worker runs, each with the session id that its agent reported."""
        ...

    def report(self, lease: int, request_id: str, report: AgentReport | EvaluationReport) -> dict[str, object]:
        """Apply the job's result, and return its receipt. Sent again with its request id, it gets the same one."""
        ...

    def lost(self, lease: int, reason: str) -> None:
        """The worker cannot find its job again."""
        ...

    def messages(self, after: int, wait: float) -> Sequence[Work | PromptMessage | Revoke]:
        """The worker's messages after ``after``, its ack floor. A long-poll: it waits up to ``wait`` seconds for one."""
        ...

    def delivered(self, message: int, error: str | None) -> None:
        """Answer a ``PromptMessage``: the prompt reached the agent, or why it did not. Raises ``NotAddressed``."""
        ...


class Local:
    """``LeaseClient`` on ``Leases`` in this process, as one worker: ``worker`` is its name."""

    def __init__(self, leases: Leases, worker: str) -> None:
        self._leases = leases
        self._worker = worker

    def register(self, info: WorkerInfo) -> Registration:
        held, cursor = self._leases.register(self._worker, info.pool, info.version, info.runners, info.sample)
        return Registration(worker=self._worker, timing=self._leases.timing, held=held, cursor=cursor)

    def acquire(self, sample: HostSample) -> Grant | NoLease:
        return self._leases.acquire(self._worker, sample)

    def started(self, lease: int, handle: JobHandle, session_id: str | None) -> None:
        self._leases.started(lease, self._worker, handle, session_id)

    def heartbeat(self, leases: Mapping[int, str | None], sample: HostSample) -> dict[int, Answer]:
        return self._leases.heartbeat(self._worker, dict(leases), sample)

    def report(self, lease: int, request_id: str, report: AgentReport | EvaluationReport) -> dict[str, object]:
        return self._leases.report(lease, self._worker, request_id, report)

    def lost(self, lease: int, reason: str) -> None:
        self._leases.lost(lease, self._worker, reason)

    def messages(self, after: int, wait: float) -> Sequence[Work | PromptMessage | Revoke]:
        return self._leases.messages(self._worker, after, wait)

    def delivered(self, message: int, error: str | None) -> None:
        self._leases.delivered(self._worker, message, error)
