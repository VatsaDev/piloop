"""Where agent processes run and how piloop talks to them.

The daemon and the CLI depend only on the ``Runtime`` protocol, and never on the name of a runtime. To add a runtime
(tmux, a remote host), implement the protocol in a class and register the class with an entry point in
the group of ``factory.RUNTIMES``.
"""

from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import ClassVar, Protocol

from piloop.harnesses import Invocation, QueueKey


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
    busy_screens: tuple[str, ...] = ()
    """Texts at the bottom of a still screen that show that the agent works (``Harness.busy_screens``)."""
    failed_turn_screens: tuple[str, ...] = ()
    """Texts at the bottom of the screen that show that a turn failed on an API error (``Harness.failed_turn_screens``)."""
    queue_key: QueueKey | None = None
    """How to queue a message for the end of the agent's turn (``Harness.queue_key``)."""
    composer: str | None = None
    """The marker at the start of the agent's input line (``Harness.composer``)."""
    session_id_pattern: str | None = None
    """The regex that finds the harness's session id in its stdout (``Harness.session_id_pattern``)."""
    resume_args: list[str] = field(default_factory=list)
    """Arguments after the executable that continue the agent's session with a new prompt (``Harness.resume``), for
    the subprocess runtime. ``{prompt}`` is still in them, and ``{session_id}`` when the harness reports its id. Empty
    when the harness cannot continue a session: the agent then takes no message while it runs."""


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
    """The agent did not start to work: its start failed, or it ended within ``launcher.STARTUP_SECONDS`` and left
    nothing (``Launcher._never_worked``). Only the launcher sets it."""
    handed_over: bool = False
    """The daemon restarted in place before it closed this agent's row. The next daemon re-attaches to the agent and
    finishes its run in its worktree. Only the launcher sets it."""


@dataclass(frozen=True)
class Check:
    """One line of the report of ``piloop doctor``: what was checked, what was found and, for a failure, the fix."""

    name: str
    found: str
    fix: str | None = None
    """None when the check passed."""


class RuntimeFailure(RuntimeError):
    """A runtime could not do what it was asked. The message says what failed. The CLI shows it."""


class StartFailed(RuntimeError):
    """``Runtime.start`` failed. ``screen`` is what the worker's terminal showed, read before the terminal closed."""

    def __init__(self, error: str, screen: str) -> None:
        super().__init__(error)
        self.screen = screen


class NotSteerable(RuntimeError):
    """The runtime cannot deliver a message to this agent now. The message says why."""


class NotAttachable(RuntimeError):
    """The runtime cannot show this agent. The message says where to look instead, for example a file of its output."""


class Runtime(Protocol):
    """Start, watch, steer, show and stop workers.

    The registry (``factory.RUNTIMES``) holds the classes. The class attributes say what the runtime is before it is
    constructed: config validation reads them, and a constructor can fail when the runtime is not installed.
    """

    kind: ClassVar[str]
    """The name of the runtime: ``runtime`` in ``piloop.toml``, and ``workers.runtime``."""
    invocation: ClassVar[Invocation]
    """Which argument list of a harness the runtime starts: ``interactive`` or ``headless``."""

    def __init__(self, project: str, project_path: Path) -> None:
        """Make the runtime of one project. A runtime can name what it makes after the project."""
        ...

    def start(self, spec: WorkerSpec) -> Handle:
        """Start the agent from a ``WorkerSpec`` (argv, first prompt, working directory) and return once it runs.

        A failure leaves nothing running. A runtime that can show what the worker was stuck on raises ``StartFailed``.
        """
        ...

    def wait(self, handle: Handle, timeout: float) -> Outcome:
        """Block until the agent is finished or gone. After ``timeout`` seconds, kill it and return ``timed_out``."""
        ...

    def status(self, handle: Handle) -> WorkerStatus:
        """Return the current state without waiting."""
        ...

    def session_id(self, handle: Handle) -> str | None:
        """Return the harness's session id once the agent reports it, or None when the runtime cannot learn it."""
        ...

    def jobs(self, handle: Handle) -> list[str]:
        """Return ``<pid> <command line>`` for each job the agent started that still runs. Empty when unknown."""
        ...

    def prompt(self, handle: Handle, text: str) -> None:
        """Queue a message for the agent's next turn. It never cuts the turn in progress short.

        An idle agent takes it at once. Raise ``NotSteerable`` when the runtime cannot deliver it (a harness that cannot
        continue its session, an ended agent, a dialog).
        """
        ...

    def read(self, handle: Handle, lines: int = 80) -> str:
        """Return the recent output as plain text."""
        ...

    def kill(self, handle: Handle) -> None:
        """Stop the agent and what it left running, and release its resources."""
        ...

    def focus(self, handle: Handle) -> str:
        """Bring the agent to the front of the runtime's own view, and return what was done.

        ``enter`` in ``piloop watch`` calls it. Raise ``NotAttachable`` when the runtime has no view of its own, and ``RuntimeFailure`` when it fails.
        """
        ...

    def attach(self, handle: Handle) -> str:
        """Show the agent in this terminal, and return what was done. ``piloop attach`` calls it.

        It may replace this process, for example with the client of a terminal multiplexer. Raise ``NotAttachable``
        when the runtime cannot show the agent, and ``RuntimeFailure`` when it fails.
        """
        ...

    @classmethod
    def view(cls, handle: Handle) -> str | None:
        """Return where you can see the agent, in a few words (``tab w7:t3``), or None when there is no such place.

        ``piloop status`` and ``piloop watch`` show it. It reads only the handle, so it runs nothing.
        """
        ...

    @classmethod
    def checks(cls, executables: set[str]) -> list[Check]:
        """Check that the runtime can run agents on this machine. ``piloop doctor`` shows the result.

        ``executables`` are the programs of the harnesses that the projects of this runtime launch. Read only: start
        nothing.
        """
        ...
