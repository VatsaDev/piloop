"""The daemon's jobs: the consumers that providers and capabilities contribute, their loop, and their lifecycles.

Core names none of them. The executor and the archiver that the daemon opened, the machinery of each capability
(``piloop.contributors``) and each installed provider that a campaign may choose contribute consumers
(``piloop.plugins.contribution``). The loop runs them by phase (``piloop.daemon.loop``). This module starts and stops
their lifecycles with the daemon, and runs their threads.
"""

import sys
import threading
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, wait
from datetime import datetime
from pathlib import Path
from queue import SimpleQueue

from piloop.daemon.loop import Consumer, Loop, Turn, in_turn_order
from piloop.daemon.run_controller import RunController
from piloop.db.models import utcnow
from piloop.db.session import Database
from piloop.otel import Tracer
from piloop.plugins.archive.contract import Archiver
from piloop.plugins.campaign_type.contract import CAMPAIGN_TYPES, CAMPAIGNS, TypeHost
from piloop.plugins.contribution import DaemonHost, Leaving, collect, contributions_of, installed, machinery
from piloop.runtime.factory import RuntimeFactory, make_runtime
from piloop.settings import AgentSettings, DaemonSettings


class Scheduler:
    """The daemon's jobs, as the consumers of its loop. There is one instance per daemon."""

    def __init__(
        self,
        db: Database,
        controller: RunController,
        tracer: Tracer,
        home: Path,
        runtime_factory: RuntimeFactory = make_runtime,
        archiver: Archiver | None = None,
        now: Callable[[], datetime] = utcnow,
        daemon: DaemonSettings | None = None,
        agents: AgentSettings | None = None,
    ) -> None:
        """Collect the daemon's jobs.

        ``archiver``: the home's archive provider, opened, which the daemon always passes. A test without one archives
        nothing. ``now``: the clock. ``daemon`` and ``agents``: the home's timings, ``[daemon]`` and ``[agents]`` in its
        ``config.toml``, by default their defaults.
        """
        self._db, self._now, self._tracer, self._controller = db, now, tracer, controller
        daemon = daemon if daemon is not None else DaemonSettings()
        # No bound: a run's thread waits in the evaluation queue while it scores, and a bound would let those waits
        # hold back agent launches. The roles' max_concurrent and the executor's slots bound the work itself.
        self._pool = ThreadPoolExecutor(max_workers=sys.maxsize, thread_name_prefix='agent')
        self._stop = threading.Event()
        self._wake: SimpleQueue[None] = SimpleQueue()
        """Ends the loop (``interrupt``): a signal handler may put to it, where it may not set an ``Event``."""
        typed = TypeHost(tracer, home, controller, self._submit)
        self._planners = {
            name: provider.model().planner(typed) for name, provider in CAMPAIGN_TYPES.providers().items()
        }
        """Each installed campaign type at work, by its name (``campaigns.type``)."""
        host = DaemonHost(
            db,
            tracer,
            home,
            controller,
            runtime_factory,
            now,
            lambda name: self.loop.wake(name),
            daemon,
            agents if agents is not None else AgentSettings(),
            self._planners,
            self._thread,
        )
        opened = (controller.executor,) if archiver is None else (controller.executor, archiver)
        self.contributions = collect([*contributions_of(opened), *machinery(host), *contributions_of(installed())], [])
        """What the providers and the capabilities add: the executor and the archiver that the daemon opened, then the
        machinery of each capability, then each installed provider that a campaign may choose."""
        self.loop = Loop(db, self.consumers(), now, daemon.feed_page)

    def consumers(self) -> tuple[Consumer, ...]:
        """The daemon's jobs, in the order that a turn of its loop runs them, with the events that wake each one."""
        return in_turn_order(self.contributions.consumers)

    def run_all(self, now: datetime | None = None) -> None:
        """Run every consumer once, as the loop does when it starts."""
        self.loop.run_all(Turn(now or self._now()))

    def reap(self) -> None:
        """Take over what a previous daemon process left: each lifecycle, in order (``Lifecycle.start``)."""
        for lifecycle in self.contributions.lifecycles:
            lifecycle.start()

    def _thread(self, body: Callable[[], None]) -> Future[None]:
        """Run ``body`` in a thread of the daemon."""
        return self._pool.submit(body)

    def _submit(self, body: Callable[[], None]) -> Future[None]:
        """Run ``body`` in a thread of the daemon: its end wakes the campaigns' pass."""
        future = self._thread(body)
        future.add_done_callback(lambda _: self.loop.wake(CAMPAIGNS))
        return future

    def _in_flight(self) -> dict[str, Future[None]]:
        """The threads in flight, by what they work on: the agents' and the campaign types'."""
        found: dict[str, Future[None]] = {}
        for each in (*self.contributions.lifecycles, *self._planners.values()):
            found |= each.in_flight()
        return found

    def serve(self) -> None:
        """Run the loop until ``stop`` or ``interrupt`` is called."""
        self.loop.serve(self._stopped)

    def _stopped(self) -> bool:
        return self._stop.is_set() or not self._wake.empty()

    def interrupt(self) -> None:
        """Make ``serve`` return after its current turn, or at once from its wait. Safe in a signal handler.

        A handler runs on the main thread, also inside the loop's wait. ``Event.set`` there could wait forever for the
        lock that an interrupted ``Event.wait`` holds. ``SimpleQueue.put`` is reentrant, and so is the database's
        notify, whose condition has a reentrant lock.
        """
        self._wake.put(None)
        self._db.notify()

    def wait(self, timeout: float) -> None:
        """Wait for the threads in flight to finish. It returns as soon as they have.

        Raises:
            TimeoutError: some are still in flight after ``timeout`` seconds. The message names them.
        """
        wait(self._in_flight().values(), timeout=timeout)
        if left := self._in_flight():
            raise TimeoutError(f'still in flight after {timeout:g} s: {", ".join(left)}')

    def hand_over(self, timeout: float) -> None:
        """Stop the loop and leave every agent running for the next daemon process. This is an in-place restart.

        As with ``stop``, nothing starts after this and the evaluators are killed. But no agent is killed and no row is
        written. The next daemon's ``reap`` re-attaches to the agents and resumes the scoring that stops here. This
        waits up to ``timeout`` for the threads that do not watch an agent. If an agent ends meanwhile, the next daemon
        also handles it.
        """
        self._stop.set()
        self.interrupt()
        self._controller.hand_over()
        wait(self._in_flight().values(), timeout=timeout)
        self._stop_lifecycles(Leaving.HAND_OVER, timeout)

    def stop(self, timeout: float) -> None:
        """Stop the loop, kill the agents in flight, and write what is still open.

        The executor stops first: no job is submitted after it, and it stops every job. Each job still reports, which
        closes its agent's row as stopped and lets its run keep what it made. ``timeout`` limits the wait for them.
        Then each lifecycle writes what is still open (``Lifecycle.stop``).
        """
        self._stop.set()
        self.interrupt()
        self._controller.stop(timeout)
        wait(self._in_flight().values(), timeout=timeout)
        self._stop_lifecycles(Leaving.STOP, timeout)

    def _stop_lifecycles(self, leaving: Leaving, timeout: float) -> None:
        """Stop each lifecycle, the latest contributed first, then release the threads.

        The archiver stops after the campaigns' pass wrote what is open: its job prunes and evicts, so the next daemon
        must not start under it.
        """
        for lifecycle in reversed(self.contributions.lifecycles):
            lifecycle.stop(leaving, timeout)
        self._pool.shutdown(wait=False, cancel_futures=True)
