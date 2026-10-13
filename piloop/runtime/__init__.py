"""Where agent processes run and how the daemon talks to them.

The daemon depends only on the ``Runtime`` protocol. ``herdr`` and ``subprocess`` implement it (see ``factory``). To add
a runtime (tmux, a remote host), implement the protocol, add a ``RuntimeKind`` and add one ``factory.RUNTIMES`` entry.
"""

from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Protocol


class WorkerStatus(StrEnum):
    """The state a runtime can report for a worker."""

    WORKING = 'working'
    IDLE = 'idle'
    BLOCKED = 'blocked'
    EXITED = 'exited'
    TIMED_OUT = 'timed_out'
    """The runtime killed the worker because ``wait`` ran out of time."""


@dataclass(frozen=True)
class WorkerSpec:
    """Everything needed to start one worker."""

    name: str
    """A unique name that matches ``[a-z][a-z0-9_-]{0,31}`` (Herdr's rule for agent names), e.g. ``r12-claude``."""
    label: str
    """What a visible runtime shows for the worker (Herdr's tab label): its name and what it works on."""
    executable: str
    """The program to run (``Harness.executable``). Herdr also uses it as the agent's ``--kind``."""
    args: list[str]
    """Arguments after the executable. For the subprocess runtime, they already include the prompt."""
    prompt: str
    """The first prompt, which names the brief's file (``launcher.PROMPT``).

    Herdr delivers it with ``agent prompt`` after the start. The subprocess runtime has it in ``args``.
    """
    cwd: Path
    log_dir: Path
    env: dict[str, str] = field(default_factory=dict)
    startup_dialogs: list[tuple[str, list[str]]] = field(default_factory=list)
    """First-launch dialogs to answer in order before the agent is ready: the text to find and the keys to send."""
    session_id_pattern: str | None = None
    """The regex that finds the harness's session id in its stdout (``Harness.session_id_pattern``)."""


@dataclass(frozen=True)
class Handle:
    """The opaque address of a started worker. It is stored as JSON in ``workers.handle``."""

    runtime: str
    data: dict[str, object]


@dataclass(frozen=True)
class Outcome:
    """How a worker finished."""

    status: WorkerStatus
    seconds: float
    exit_code: int | None = None
    error: str | None = None
    startup_failed: bool = False
    """The agent did not start to work: its start failed, or it ended within ``launcher.STARTUP_SECONDS`` without
    spending a token. Only the launcher sets it."""
    handed_over: bool = False
    """The daemon restarted in place before it closed this agent's row. The next daemon re-attaches to the agent and
    finishes its run in its worktree. Only the launcher sets it."""


class StartFailed(RuntimeError):
    """``Runtime.start`` failed. ``screen`` is what the worker's terminal showed, read before the terminal closed."""

    def __init__(self, error: str, screen: str) -> None:
        super().__init__(error)
        self.screen = screen


class NotSteerable(RuntimeError):
    """The runtime cannot deliver a message to a running worker, for example a one-shot subprocess."""


class Runtime(Protocol):
    """Start, observe, steer and stop workers."""

    kind: str

    def start(self, spec: WorkerSpec) -> Handle:
        """Start the worker and return once it runs. For Herdr, this is when the agent is detected and has its brief.

        A failure leaves nothing running. A runtime that can show what the worker was stuck on raises ``StartFailed``.
        """
        ...

    def wait(self, handle: Handle, timeout: float) -> Outcome:
        """Block until the worker is finished or gone. After ``timeout`` seconds, kill it and return ``TIMED_OUT``."""
        ...

    def status(self, handle: Handle) -> WorkerStatus:
        """Return the current state without waiting."""
        ...

    def session_id(self, handle: Handle) -> str | None:
        """Return the harness's session id once the agent reports it, or None when the runtime cannot learn it."""
        ...

    def jobs(self, handle: Handle) -> list[str]:
        """Return ``<pid> <command line>`` for each job the worker started that still runs. Empty when unknown."""
        ...

    def prompt(self, handle: Handle, text: str) -> None:
        """Deliver a steering message. Raise ``NotSteerable`` when the runtime cannot (a one-shot process, a dialog)."""
        ...

    def read(self, handle: Handle, lines: int = 80) -> str:
        """Return the recent output as plain text."""
        ...

    def kill(self, handle: Handle) -> None:
        """Stop the worker and what it left running, and release its resources."""
        ...
