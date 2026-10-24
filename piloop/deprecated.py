"""Compat code that piloop keeps until a set release: deprecated ``piloop.toml`` keys, old names and old behavior.

Mark each piece of compat code with ``until``: the release that deletes it and what to delete. A test finds every
marker and fails once ``pyproject.toml`` reaches its release, so the code goes before that release ships.

A deprecated key still loads, with a warning, until that release. To deprecate a key, add a bridge to ``BRIDGES``. A
bridge is a function that takes the file path and the parsed TOML, changes the old key to the new form in place and
calls ``warn``. Mark it with ``until``. Its docstring is its entry under "Deprecated keys" in the generated
configuration reference (``docs/reference/configuration.mdx``). The release notes list it under ``Deprecated``.

When its marker fails, delete the bridge's function, its ``BRIDGES`` entry and its tests, and run ``make docs``. The
release notes list the key under ``Removed``. The old key then fails as an unknown key.
"""

import warnings
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Protocol

from piloop.actor import Agent
from piloop.version import Version

Bridge = Callable[[Path, dict[str, object]], None]


class Marked(Protocol):
    """What ``until`` returns: a decorator that returns what it decorates."""

    def __call__[T](self, obj: T, /) -> T: ...


def _same[T](obj: T, /) -> T:
    return obj


def until(version: str, reason: str) -> Marked:
    """Mark compat code that piloop keeps until release ``version``. ``reason`` says what to delete and what replaces it.

    Call it as a statement where the code is (a branch, an alias, a table entry), or put it on a function or a class as
    a decorator. It only parses ``version`` (``Version.parse``), so a malformed one fails when the code runs.
    ``tests/test_deprecated.py`` reads every call in ``piloop/`` and fails once ``pyproject.toml`` is at ``version``.
    Write both arguments as literal strings, ``version`` as ``X.Y.Z``, and call it as ``deprecated.until`` (here,
    ``until``), so that the test can read it.

    Raises:
        ValueError: ``version`` is not ``X.Y.Z``.
    """
    Version.parse(version)
    return _same


# Where the ``default`` action records the warnings it showed, so a file loaded twice in one process warns once.
_shown: dict[str | tuple[str, type[Warning], int], int] = {}


class PiloopDeprecationWarning(DeprecationWarning):
    """A deprecated piloop key that still works until the release that removes it. The CLI shows each one once."""


REMOVED_IN = Version(0, 3, 0)
"""The release that ``warn`` names for the keys that 0.2.0 deprecates."""
MAX_EVALUATIONS_REMOVED_IN = Version(0, 4, 0)
"""The release that ``warn`` names for ``max_evaluations``, which 0.2.0 configs set."""
SCOUT_SHARED = ('runtime', 'harnesses', 'model', 'timeout', 'mcp', 'addendum')
"""The keys of ``[roles.research]`` that a scout sweep used before the scout was a role of its own."""


def _table(raw: dict[str, object], *path: str) -> dict[str, object] | None:
    """Return the table at ``path`` in the parsed file, or None. A value that is not a table is left to validation."""
    table: object = raw
    for key in path:
        table = table.get(key) if isinstance(table, dict) else None
    return table if isinstance(table, dict) else None


@until('0.3.0', '[roles.research.scout] is [roles.scout]: delete the research_scout bridge')
def research_scout(toml: Path, raw: dict[str, object]) -> None:
    """``[roles.research.scout]`` is ``[roles.scout]``, a role of its own.

    The keys of ``[roles.research]`` that the scout used (``runtime``, ``harnesses``, ``model``, ``timeout``, ``mcp``
    and ``addendum``) are copied into it, unless ``[roles.scout]`` sets them.
    """
    research = _table(raw, 'roles', 'research')
    if research is None or (scout := research.pop('scout', None)) is None:
        return
    roles = _table(raw, 'roles')
    assert roles is not None  # research is in it
    shared = {k: research[k] for k in SCOUT_SHARED if k in research}
    roles['scout'] = shared | (scout if isinstance(scout, dict) else {}) | (_table(roles, 'scout') or {})
    warn(
        toml,
        'roles.research.scout',
        '[roles.research.scout] is deprecated; write [roles.scout], a role of its own, with the keys of'
        ' [roles.research] that it needs (mcp, harnesses, …)',
        removed_in=REMOVED_IN,
    )


@until('0.3.0', 'the scout trigger of [roles.research] is gone: delete the scout_trigger bridge')
def scout_trigger(toml: Path, raw: dict[str, object]) -> None:
    """The ``scout`` trigger of ``[roles.research] triggers`` is gone: the scout is a role with its own ``cron``."""
    research = _table(raw, 'roles', 'research')
    triggers = research.get('triggers') if research is not None else None
    if research is None or not isinstance(triggers, list) or not any(_is_scout(t) for t in triggers):
        return
    research['triggers'] = [t for t in triggers if not _is_scout(t)]
    lines = toml.read_text().splitlines()
    header = next((n for n, line in enumerate(lines) if line.strip() == '[roles.research]'), len(lines))
    nth = sum(line.partition('=')[0].strip() == 'triggers' for line in lines[:header])
    warn(
        toml,
        'roles.research.triggers',
        'the scout trigger is deprecated; delete it: the scout is [roles.scout], with its own cron trigger',
        removed_in=REMOVED_IN,
        nth=nth,
    )


def _is_scout(trigger: object) -> bool:
    return isinstance(trigger, dict) and trigger.get('kind') == 'scout'


RESEARCH_TRIGGERS = ({'kind': 'low_water'}, {'kind': 'seed'}, {'kind': 'stalled'})
"""The default triggers of research."""


@until('0.3.0', '[roles.research] buffer is a low_water trigger buffer: delete the research_buffer bridge')
def research_buffer(toml: Path, raw: dict[str, object]) -> None:
    """``[roles.research] buffer`` is the ``buffer`` of research's ``low_water`` trigger.

    Without ``triggers`` in the table, research gets its default triggers with that ``buffer``.
    """
    research = _table(raw, 'roles', 'research')
    if research is None or 'buffer' not in research:
        return
    buffer = research.pop('buffer')
    triggers = research.get('triggers', RESEARCH_TRIGGERS)
    if isinstance(triggers, list | tuple):
        research['triggers'] = [
            t | {'buffer': buffer} if isinstance(t, dict) and t.get('kind') == 'low_water' else t for t in triggers
        ]
    warn(
        toml,
        'roles.research.buffer',
        '[roles.research] buffer is deprecated; write triggers ='
        ' [{ kind = "low_water", buffer = … }, { kind = "seed" }, { kind = "stalled" }]',
        removed_in=REMOVED_IN,
    )


@until('0.3.0', '[roles.research] context is the top-level context: delete the research_context bridge')
def research_context(toml: Path, raw: dict[str, object]) -> None:
    """``[roles.research] context`` is the top-level ``context``: every sweep's research pack lists the files."""
    research = _table(raw, 'roles', 'research')
    if research is None or 'context' not in research:
        return
    old = research.pop('context')
    top = raw.get('context', [])
    raw['context'] = [*top, *old] if isinstance(top, list) and isinstance(old, list) else old
    warn(
        toml,
        'roles.research.context',
        '[roles.research] context is deprecated; write context = [...] at the top of the file',
        removed_in=REMOVED_IN,
    )


USAGE_PLACEHOLDERS = {
    '{claude_config_dir}': '{env:CLAUDE_CONFIG_DIR:~/.claude}',
    '{codex_home}': '{env:CODEX_HOME:~/.codex}',
}


@until('0.3.0', '{claude_config_dir} and {codex_home} are {env:…} placeholders: delete the usage_dirs bridge')
def usage_dirs(toml: Path, raw: dict[str, object]) -> None:
    """``{claude_config_dir}`` and ``{codex_home}`` in ``[harness.<name>.usage] file`` are ``{env:…}`` placeholders.

    ``{claude_config_dir}`` is ``{env:CLAUDE_CONFIG_DIR:~/.claude}``, and ``{codex_home}`` is
    ``{env:CODEX_HOME:~/.codex}``.
    """
    harnesses = _table(raw, 'harness') or {}
    for name in harnesses:
        usage = _table(raw, 'harness', name, 'usage')
        glob = usage.get('file') if usage is not None else None
        if usage is None or not isinstance(glob, str) or not any(old in glob for old in USAGE_PLACEHOLDERS):
            continue
        for old, new in USAGE_PLACEHOLDERS.items():
            glob = glob.replace(old, new)
        usage['file'] = glob
        warn(
            toml,
            f'harness.{name}.usage.file',
            f'{{claude_config_dir}} and {{codex_home}} are deprecated; write {glob!r}',
            removed_in=REMOVED_IN,
        )


@until('0.3.0', '[roles.worker] is [roles.implementer]: delete the worker_role bridge')
def worker_role(toml: Path, raw: dict[str, object]) -> None:
    """``[roles.worker]`` is ``[roles.implementer]``: the role that runs one idea in its own worktree.

    A key that ``[roles.implementer]`` sets too keeps its value there.
    """
    roles = _table(raw, 'roles')
    if roles is None or (old := roles.pop('worker', None)) is None:
        return
    roles['implementer'] = (old if isinstance(old, dict) else {}) | (_table(roles, 'implementer') or {})
    warn(toml, 'roles.worker', '[roles.worker] is deprecated; write [roles.implementer]', removed_in=REMOVED_IN)


BRIDGES: tuple[Bridge, ...] = (
    worker_role,
    research_scout,
    scout_trigger,
    research_buffer,
    research_context,
    usage_dirs,
)


@until('0.3.0', '[storage.remote] in config.toml is [storage.backend]: delete the storage_remote bridge')
def storage_remote(toml: Path, raw: dict[str, object]) -> None:
    """In ``config.toml``, ``[storage.remote]`` is ``[storage.backend]``: the storage backend that mirrors the store.

    A key that ``[storage.backend]`` sets too keeps its value there.
    """
    storage = _table(raw, 'storage')
    if storage is None or (old := storage.pop('remote', None)) is None:
        return
    storage['backend'] = (old if isinstance(old, dict) else {}) | (_table(storage, 'backend') or {})
    warn(toml, 'storage.remote', '[storage.remote] is deprecated; write [storage.backend]', removed_in=REMOVED_IN)


@until('0.4.0', '[storage.backend] and its eviction keys are [plugins.cas] cached: delete the storage_backend bridge')
def storage_backend(toml: Path, raw: dict[str, object]) -> None:
    """In ``config.toml``, ``[storage.backend]`` is the ``remote`` of ``[plugins.cas] provider = "cached"``.

    Its ``kind`` is the remote's ``provider``. ``evict_after_days`` and ``local_max_gb`` of ``[storage]`` move with
    it. A ``[plugins.cas]`` table of the file wins: the old keys then go, with the warning, and change nothing.
    """
    storage = _table(raw, 'storage')
    if storage is None or (backend := storage.pop('backend', None)) is None:
        return
    remote = dict(backend) if isinstance(backend, dict) else {}
    cached: dict[str, object] = {'provider': 'cached', 'remote': {'provider': remote.pop('kind', None), **remote}}
    for key in ('evict_after_days', 'local_max_gb'):
        if key in storage:
            cached[key] = storage.pop(key)
    plugins = raw.setdefault('plugins', {})
    if isinstance(plugins, dict):
        plugins.setdefault('cas', cached)
    warn(
        toml,
        'storage.backend',
        '[storage.backend] is deprecated; write [plugins.cas] provider = "cached" and remote = { provider = <its kind>,'
        ' <its keys> }, with evict_after_days and local_max_gb there too',
        removed_in=Version(0, 4, 0),
    )


@until(
    '0.4.0',
    'max_evaluations in config.toml is [plugins.executor.pools.serve] evaluations: delete the max_evaluations bridge',
)
def max_evaluations(toml: Path, raw: dict[str, object]) -> None:
    """In ``config.toml``, ``max_evaluations`` is ``evaluations`` in ``[plugins.executor.pools.serve]``.

    A value that ``[plugins.executor.pools.serve]`` sets too wins.
    """
    if (old := raw.pop('max_evaluations', None)) is None:
        return
    _slots(raw, old)
    warn(
        toml,
        'max_evaluations',
        'max_evaluations is deprecated; write evaluations in [plugins.executor.pools.serve]',
        removed_in=MAX_EVALUATIONS_REMOVED_IN,
    )


OLD_MAX_EVALUATIONS_ENV = 'PILOOP_MAX_EVALUATIONS'


@until(
    '0.4.0',
    'PILOOP_MAX_EVALUATIONS is PILOOP_PLUGINS__EXECUTOR__POOLS__SERVE__EVALUATIONS: delete max_evaluations_env',
)
def max_evaluations_env(environ: Mapping[str, str], raw: dict[str, object]) -> dict[str, object]:
    """Return ``raw``, the parsed ``config.toml``, with ``PILOOP_MAX_EVALUATIONS`` in it, with a warning.

    It becomes ``[plugins.executor.pools.serve] evaluations``, unless the file sets that.
    """
    if (old := environ.get(OLD_MAX_EVALUATIONS_ENV)) is None:
        return raw
    _slots(raw, old)
    warnings.warn(
        f'{OLD_MAX_EVALUATIONS_ENV} is deprecated; set PILOOP_PLUGINS__EXECUTOR__POOLS__SERVE__EVALUATIONS;'
        f' removed in piloop {MAX_EVALUATIONS_REMOVED_IN}',
        PiloopDeprecationWarning,
        stacklevel=2,
    )
    return raw


WORKERS = 'workers'
"""The executor whose slots ``max_evaluations`` was in 0.2.0, the only one there was."""


def _slots(raw: dict[str, object], old: object) -> None:
    """Put ``old`` in ``raw`` as ``[plugins.executor.pools.serve] evaluations``, if it selects ``workers`` or nothing.

    Another executor has no such key: the old one goes, with its warning, and changes nothing.
    """
    plugins = raw.setdefault('plugins', {})
    executor = plugins.setdefault('executor', {'provider': WORKERS}) if isinstance(plugins, dict) else None
    if isinstance(executor, dict) and executor.setdefault('provider', WORKERS) == WORKERS:
        pools = executor.setdefault('pools', {})
        serve = pools.setdefault('serve', {}) if isinstance(pools, dict) else None
        if isinstance(serve, dict):
            serve.setdefault('evaluations', old)


CONFIG_BRIDGES: tuple[Bridge, ...] = (storage_remote, storage_backend, max_evaluations)
"""The bridges of ``config.toml``, the global settings file (``piloop.settings.config_file``)."""


HERDR = 'herdr'
"""The name of the runtime that 0.3.0 removed."""
HERDR_UPGRADE = 'https://piloop.autoresearch.inc/guides/upgrading#the-herdr-runtime'
"""What to do about the removed herdr runtime."""


@until('0.4.0', 'runtime = "herdr" of a 0.2 piloop.toml: delete removed_runtime, HERDR and HERDR_UPGRADE')
def removed_runtime(name: str) -> str | None:
    """Why ``runtime = <name>`` does not load when piloop removed that runtime, else None."""
    if name != HERDR:
        return None
    return f'the herdr runtime is removed in piloop 0.3.0: write runtime = "subprocess", or delete the key ({HERDR_UPGRADE})'


WORKDIR_UPGRADE = 'https://piloop.autoresearch.inc/guides/upgrading#workdir'
"""What to do about the removed ``[roles.<name>] workdir``."""


@until('0.4.0', 'workdir of a 0.2 piloop.toml: delete removed_workdir and WORKDIR_UPGRADE')
def removed_workdir() -> str:
    """Why a role table with ``workdir`` does not load: every agent works in a worktree of its own now."""
    return (
        'workdir is removed in piloop 0.3.0: every agent works in a worktree of its own, never in the campaign'
        f' checkout. Delete the key ({WORKDIR_UPGRADE})'
    )


ENTIRE_UPGRADE = 'https://piloop.autoresearch.inc/guides/upgrading#entire'
"""What to do about ``entire_agent``, which 0.3.0 removed with Entire."""


@until('0.4.0', 'entire_agent of a 0.2 piloop.toml: delete removed_entire_agent, ENTIRE_UPGRADE and its call')
def removed_entire_agent(harness: Mapping[str, object]) -> str | None:
    """Why a ``[harness.<name>]`` table does not load when it sets ``entire_agent``, else None."""
    if 'entire_agent' not in harness:
        return None
    return f'entire_agent is removed in piloop 0.3.0, with Entire: delete the key ({ENTIRE_UPGRADE})'


OLD_ENV_PREFIX = 'PILOOP_PROJECT_'
"""The prefix of the environment variables that override ``piloop.toml`` before 0.2.0. ``PILOOP_CAMPAIGN_`` now."""


@until('0.3.0', 'PILOOP_PROJECT_<KEY> is PILOOP_CAMPAIGN_<KEY>: delete old_env and OLD_ENV_PREFIX')
def old_env(toml: Path, environ: Mapping[str, str]) -> None:
    """Warn for each ``PILOOP_PROJECT_<KEY>`` variable: it still overrides its key, after ``PILOOP_CAMPAIGN_<KEY>``."""
    for name in sorted(n for n in environ if n.upper().startswith(OLD_ENV_PREFIX)):
        new = 'PILOOP_CAMPAIGN_' + name[len(OLD_ENV_PREFIX) :]
        warn(toml, name, f'{name} is deprecated; set {new}', removed_in=REMOVED_IN)


OLD_SIGNATURE = 'worker#'
"""The signature of an implementer before 0.2.0, ``worker#<id>``. An agent that an older daemon started outlives an
upgrade with it: in its ``PILOOP_ACTOR``, and in the SQL that its brief told it to write (``'<you>'``)."""


@until('0.3.0', 'a 0.1.0 implementer signs worker#<id>: delete signatures and OLD_SIGNATURE, match <kind>#<id>')
def signatures(agent: Agent) -> tuple[str, ...]:
    """Return the signatures of an agent session's rows: ``<kind>#<id>``, and an implementer's ``worker#<id>`` too.

    The checks that find an agent's own rows (its work at startup, its finding, its open escalation, its own comments,
    its silence) match authors against these, so a row that a 0.1.0 agent signs after the upgrade still counts.
    """
    return (agent.signature, f'{OLD_SIGNATURE}{agent.session}') if agent.role == 'implementer' else (agent.signature,)


@until('0.3.0', 'PILOOP_ACTOR=worker#<id> of a 0.1.0 agent: delete old_actor and its call in Settings.actor')
def old_actor(actor: str) -> str:
    """Return ``actor`` with the implementer's signature for ``worker#<id>``, with a warning; else ``actor`` as it is."""
    if not actor.startswith(OLD_SIGNATURE):
        return actor
    new = 'implementer#' + actor.removeprefix(OLD_SIGNATURE)
    warnings.warn(
        f'PILOOP_ACTOR={actor} is deprecated; the daemon signs an implementer {new}; removed in piloop {REMOVED_IN}',
        PiloopDeprecationWarning,
        stacklevel=2,
    )
    return new


def show_in_cli() -> None:
    """Show each deprecation once. By default, Python hides a ``DeprecationWarning`` outside ``__main__``."""
    warnings.simplefilter('default', PiloopDeprecationWarning)


def warn(toml: Path, key: str, message: str, *, removed_in: Version, nth: int = 0) -> None:
    """Warn at the line of ``toml`` that sets ``key`` for the ``nth`` time, or at line 0 if no line sets it.

    A dotted ``key`` (``roles.research.scout``) is set by its table header, or by a line that sets its last part. The
    output looks like a compiler error: ``piloop.toml:12: PiloopDeprecationWarning: …``, then the line.
    """
    last = key.rpartition('.')[2]
    lines = [
        n
        for n, line in enumerate(toml.read_text().splitlines(), 1)
        if line.strip() == f'[{key}]' or line.partition('=')[0].strip() == last
    ]
    lineno = lines[nth] if nth < len(lines) else 0
    text = f'{message}; removed in piloop {removed_in}'
    warnings.warn_explicit(text, PiloopDeprecationWarning, str(toml), lineno, registry=_shown)


def bridge(toml: Path, raw: dict[str, object], bridges: tuple[Bridge, ...] = BRIDGES) -> None:
    """Run every bridge on the parsed file. ``config.load_campaign_config`` calls this before it validates the file."""
    for b in bridges:
        b(toml, raw)
