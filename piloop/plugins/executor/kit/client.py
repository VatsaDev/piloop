"""The worker's half of the leases library: the calls a worker makes, whatever carries them.

After the GitHub Actions runner (a session, one long-poll for messages, then acquire, renew and finish) and Buildkite
agent v3 (register, ping, accept, start, finish). ``Local`` makes the calls on ``Leases`` in this process: the worker
inside ``piloop serve`` takes that path. Another transport implements ``LeaseClient`` the same way, and the worker
does not change.
"""

from collections.abc import Mapping, Sequence
from typing import Protocol

from pydantic import BaseModel, ConfigDict, SecretStr

from piloop.db.models import NoLease
from piloop.mcp import EnvName
from piloop.plugins.cas.contract import BlobAddress
from piloop.plugins.executor.contract import AgentJob, AgentReport, EvaluationReport
from piloop.plugins.executor.kit.capacity import HostSample
from piloop.plugins.executor.kit.leases import (
    Answer,
    Drain,
    Grant,
    JobHandle,
    Leases,
    PromptMessage,
    Revoke,
    Timing,
    UnknownPool,
    Work,
)

PROTOCOL = 7
"""The version of the ``LeaseClient`` calls on the wire."""


class Dismissed(Exception):
    """The control plane refuses this worker for good: a revoked token, another protocol, an unknown pool.

    The worker stops its jobs, keeps their reports, and exits with this message.
    """


class WorkerInfo(BaseModel):
    """What a worker says of itself when it starts."""

    model_config = ConfigDict(frozen=True)

    protocol: int = PROTOCOL
    """The version of the calls that it makes. A server of another one refuses it (``Dismissed``)."""
    version: str
    """The piloop version that it runs."""
    pool: str
    runners: list[str]
    """The evaluator runners that it has."""
    sample: HostSample
    cas_probe: BlobAddress
    """A tiny blob, unique to this start, that the worker just put in its CAS. A server whose CAS lacks it refuses the
    worker: the two must share one CAS. Nothing deletes it: one blob of about 50 bytes per start."""


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
    stops the job. Any call raises ``Dismissed`` when the control plane refuses the worker itself. ``report`` raises
    ``MissingBlobs`` when the CAS lacks what the report names: then the worker loses the job.
    """

    def register(self, info: WorkerInfo) -> Registration:
        """Start a worker's session: record it, and return its timing and its live leases."""
        ...

    def acquire(self, request_id: str, sample: HostSample) -> Grant | NoLease:
        """Take a queued job, or say why there is none. Sent again with its request id, it gets the same lease."""
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

    def messages(self, after: int, wait: float) -> Sequence[Work | PromptMessage | Revoke | Drain]:
        """The worker's messages after ``after``, its ack floor. A long-poll: it waits up to ``wait`` seconds for one."""
        ...

    def queued(self, message: int, error: str | None) -> None:
        """Answer a ``PromptMessage``: the prompt waits for the agent in its runtime, or why it does not.

        Raises ``NotAddressed``.
        """
        ...

    def secrets(self, lease: int) -> dict[EnvName, SecretStr]:
        """The values of the secrets of the agent's job on ``lease``, by name: what it starts with."""
        ...


class Local:
    """``LeaseClient`` on ``Leases`` in this process, as one worker: ``worker`` is its name."""

    def __init__(self, leases: Leases, worker: str) -> None:
        self._leases = leases
        self._worker = worker

    def register(self, info: WorkerInfo) -> Registration:
        try:
            held, cursor = self._leases.register(self._worker, info.pool, info.version, info.runners, info.sample)
        except UnknownPool as e:
            raise Dismissed(str(e)) from e
        return Registration(worker=self._worker, timing=self._leases.timing, held=held, cursor=cursor)

    def acquire(self, request_id: str, sample: HostSample) -> Grant | NoLease:
        return self._leases.acquire(self._worker, request_id, sample)

    def started(self, lease: int, handle: JobHandle, session_id: str | None) -> None:
        self._leases.started(lease, self._worker, handle, session_id)

    def heartbeat(self, leases: Mapping[int, str | None], sample: HostSample) -> dict[int, Answer]:
        return self._leases.heartbeat(self._worker, dict(leases), sample)

    def report(self, lease: int, request_id: str, report: AgentReport | EvaluationReport) -> dict[str, object]:
        return self._leases.report(lease, self._worker, request_id, report)

    def lost(self, lease: int, reason: str) -> None:
        self._leases.lost(lease, self._worker, reason)

    def messages(self, after: int, wait: float) -> Sequence[Work | PromptMessage | Revoke | Drain]:
        return self._leases.messages(self._worker, after, wait)

    def queued(self, message: int, error: str | None) -> None:
        self._leases.queued(self._worker, message, error)

    def secrets(self, lease: int) -> dict[EnvName, SecretStr]:
        return self._leases.secrets(lease, self._worker)

    def relayed(self, lease: int) -> AgentJob:
        """The agent's job of a live lease of this worker, whose calls it relays (``Leases.relayed``)."""
        return self._leases.relayed(lease, self._worker)
