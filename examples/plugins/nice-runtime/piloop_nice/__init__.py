"""``nice``: a piloop runtime that starts each agent as ``subprocess`` does, under ``nice``.

The agents of a long campaign then leave the CPU to your own work first. ``[runtimes.nice] niceness`` in
``$PILOOP_HOME/config.toml`` sets how much lower (default 10).
"""

from dataclasses import replace
from typing import ClassVar

from piloop.capabilities import AgentSpec, Handle, Invocation, SubprocessRuntime, runtime_settings


class NiceRuntime(SubprocessRuntime):
    """``nice``: background processes, like ``subprocess``, at a lower CPU priority."""

    kind: ClassVar[str] = 'nice'
    invocation: ClassVar[Invocation] = Invocation.HEADLESS

    def start(self, spec: AgentSpec) -> Handle:
        niceness = str(runtime_settings(self.kind).get('niceness', 10))
        args = ['-n', niceness, spec.executable, *spec.args]
        return super().start(replace(spec, executable='nice', args=args))
