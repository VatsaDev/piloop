"""Where agent processes run and how piloop talks to them.

The daemon and the CLI depend only on the ``Runtime`` protocol, and never on the name of a runtime. To add a runtime
(a container, a remote host), subclass ``RuntimeConfig``, whose ``open`` returns your ``Runtime``, and register the
subclass with an entry point in the group of ``RUNTIMES``. Its fields are its settings, from ``[runtimes.<name>]`` of
``config.toml``: they depend on the machine, not on the campaign.
"""

import fcntl
import json
from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Protocol

from pydantic import BeforeValidator

from piloop import deprecated
from piloop.plugins.harness.contract import fill
from piloop.plugins.host import Capability, Parameters


class ExitStatus(StrEnum):
    """How an agent process ended (``AgentExit``). A runtime maps its own ends onto these, and the error says why."""

    EXITED = 'exited'
    TIMED_OUT = 'timed_out'
    """The runtime killed the agent because ``wait`` ran out of time."""


@dataclass(frozen=True)
class AgentStart:
    """What starts one agent, on any machine.

    ``args`` and ``resume_args`` can hold ``{cwd}`` and ``{log_dir}``: ``at`` fills in where it runs.
    """

    executable: str
    """The program to run (``Harness.executable``)."""
    args: list[str]
    """Arguments after the executable, the first prompt included."""
    env: dict[str, str] = field(default_factory=dict)
    session_id_pattern: str | None = None
    """The regex that finds the harness's session id in its stdout (``Harness.session_id_pattern``)."""
    resume_args: list[str] = field(default_factory=list)
    """Arguments after the executable that continue the agent's session with a new prompt (``Harness.resume``).
    ``{prompt}`` is still in them, and ``{session_id}`` when the harness reports its id. Empty when the harness cannot
    continue a session: the agent then takes no message while it runs."""

    def at(self, cwd: Path, log_dir: Path, env: dict[str, str]) -> AgentSpec:
        """The spec of this start in ``cwd``, writing to ``log_dir``, with ``env`` added to its environment."""
        places = {'cwd': str(cwd), 'log_dir': str(log_dir)}
        return AgentSpec(
            executable=self.executable,
            args=[fill(a, **places) for a in self.args],
            env=self.env | env,
            session_id_pattern=self.session_id_pattern,
            resume_args=[fill(a, **places) for a in self.resume_args],
            cwd=cwd,
            log_dir=log_dir,
        )


@dataclass(frozen=True, kw_only=True)
class AgentSpec(AgentStart):
    """Everything needed to start one agent: its start, where it runs and where its logs go."""

    cwd: Path
    log_dir: Path


@dataclass(frozen=True)
class Handle:
    """The opaque address of a started agent. It is stored as JSON in ``agent_sessions.handle``."""

    runtime: str
    data: dict[str, object]


@dataclass(frozen=True)
class AgentExit:
    """How an agent process ended: what ``Runtime.wait`` returns."""

    status: ExitStatus
    seconds: float
    exit_code: int | None = None
    error: str | None = None


class NotSteerable(RuntimeError):
    """The runtime cannot deliver a message to this agent now. The message says why."""


class Runtime(Protocol):
    """Start, watch, steer and stop agents. ``RuntimeConfig.open`` makes one."""

    def start(self, spec: AgentSpec) -> Handle:
        """Start the agent from a ``AgentSpec`` (argv with its first prompt, working directory) and return once it runs.

        A failure leaves nothing running.
        """
        ...

    def wait(self, handle: Handle, timeout: float) -> AgentExit:
        """Block until the agent is finished or gone. After ``timeout`` seconds, kill it and return ``timed_out``."""
        ...

    def session_id(self, handle: Handle) -> str | None:
        """Return the harness's session id once the agent reports it, or None when the runtime cannot learn it."""
        ...

    def prompt(self, handle: Handle, text: str) -> None:
        """Queue a message for the agent's next turn. It never cuts the turn in progress short.

        Raise ``NotSteerable`` when the runtime cannot deliver it (a harness that cannot continue its session, an ended
        agent).
        """
        ...

    def read(self, handle: Handle, lines: int = 80) -> str:
        """Return the recent output as plain text."""
        ...

    def kill(self, handle: Handle) -> None:
        """Stop the agent and what it left running, and release its resources.

        ``piloop.plugins.runtime.kit.tags`` finds what the agent left, also in sessions of their own: start the agent with
        ``TAG_ENV`` set to a value of its own, and end them with ``kill_tagged``.
        """
        ...


TAG_ENV = 'PILOOP_PROCESS_TAG'
"""The variable that holds an agent's process tag."""
MESSAGES_ENV = 'PILOOP_MESSAGES'
"""The variable that names the agent's message queue (``QUEUE``), so that a hook of its harness can take the messages
during a turn (``piloop agent messages``). A file, because ``prompt`` can run in another
runtime instance, or another process, than the turn that takes them."""
QUEUE = 'queued-messages'
"""Under the agent's log directory: the messages queued for its next turn, one JSON string per line."""
READ = 'read-messages'
"""Under the agent's log directory: the messages that the agent took from its queue (``take``), as ``QUEUE`` holds them."""


def put(log_dir: Path, text: str) -> None:
    """Append ``text`` to the agent's queue, under its lock."""
    with locked(log_dir), (log_dir / QUEUE).open('a') as queue:
        queue.write(json.dumps(text) + '\n')


def take(queue: Path) -> list[str]:
    """Remove and return the messages in ``queue``, oldest first, under its lock. Add them to the agent's ``READ``."""
    with locked(queue.parent):
        lines = [line for line in queue.read_text().splitlines() if line] if queue.is_file() else []
        if lines:
            with (queue.parent / READ).open('a') as read:
                read.writelines(f'{line}\n' for line in lines)
            queue.unlink()
    return [json.loads(line) for line in lines]


def taken(log_dir: Path) -> tuple[int, int]:
    """How many messages the agent took from its queue (``READ``), and how many still wait in it, oldest first."""
    return _count(log_dir / READ), _count(log_dir / QUEUE)


def _count(path: Path) -> int:
    return sum(1 for line in path.read_text().splitlines() if line) if path.is_file() else 0


@contextmanager
def locked(log_dir: Path) -> Generator[None]:
    """Hold the agent's queue lock: ``put`` appends and ``take`` drains under it, across processes."""
    with (log_dir / f'{QUEUE}.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


class RuntimeConfig(Parameters, ABC):
    """A runtime, with its settings: ``[runtimes.<name>]`` of ``config.toml``.

    Its name is ``runtime`` in ``piloop.toml``, and ``agent_sessions.runtime``.
    """

    @abstractmethod
    def open(self) -> Runtime:
        """Make the runtime. Raise when it cannot run on this machine."""


RUNTIMES = Capability('runtime', RuntimeConfig)
"""Every runtime by name."""


def _not_removed(name: object) -> object:
    if isinstance(name, str) and (removed := deprecated.removed_runtime(name)) is not None:
        raise ValueError(removed)
    return name


RuntimeName = Annotated[str, RUNTIMES.named, BeforeValidator(_not_removed)]
"""The name of a registered runtime (``RUNTIMES``): ``subprocess``, or one that a package adds."""
