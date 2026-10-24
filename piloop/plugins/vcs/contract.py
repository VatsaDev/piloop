"""The vcs capability: how a campaign saves versions of its files, and how agents and jobs work on them.

A **version** is one saved state of the campaign's files, with its metadata (``Version``). The provider names it by its
**revision**: a commit SHA under ``git``. Core stores, compares and ships revisions, and never parses one.

Host tier: a provider runs processes and writes files. ``[plugins.vcs] provider`` in ``piloop.toml`` selects one
(``git`` by default). A provider is its parameters, a ``Vcs`` whose ``open`` returns the ``Repository`` of a checkout.
A job names the campaign's provider with its parameters (``Source.vcs``), so a worker uses the same one. Versions travel
between machines through the CAS (``Transfer``).

Like Jujutsu's ``Backend`` and Sapling's storage: core asks the contract for versions, never the tool behind it.
"""

import re
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import NewType, Protocol

from covalence import Cid
from pydantic import BaseModel, ConfigDict

from piloop.ids import CandidateId, RunId
from piloop.plugins.cas.contract import BlobAddress, Cas
from piloop.plugins.host import Capability, Check, Choice, Parameters, Scope

Revision = NewType('Revision', str)
"""The provider's name for a version: a commit SHA under ``git``."""


@dataclass(frozen=True)
class Version:
    """A saved version, as the provider reads it."""

    revision: Revision
    tree: str
    """The provider's name for its files: two versions with equal files have equal trees, whatever their history."""
    summary: str
    """The first line of what the version says about itself."""
    skip: bool = False
    """Its author asked not to measure it."""
    notes: tuple[str, ...] = ()
    """What its author noted on it, to apply when it is judged."""
    recovered: bool = False
    """It holds what an agent left unsaved, which ``Repository.capture`` saved for it."""


@dataclass(frozen=True)
class Conflicts:
    """A merge that cannot be made without a human: the paths that both sides changed."""

    paths: tuple[str, ...]


class Transfer(BaseModel):
    """Versions in the CAS, in the provider's own format: what ``head`` has beyond the version that a job started from."""

    model_config = ConfigDict(frozen=True)

    blob: BlobAddress
    head: Revision


@dataclass(frozen=True)
class RunVersion:
    """The ``index``-th version of a run, oldest first."""

    campaign: str
    run: RunId
    index: int


@dataclass(frozen=True)
class RebaseVersion:
    """The merge of a candidate onto the head (``ledger.rebase``)."""

    campaign: str
    candidate: CandidateId


@dataclass(frozen=True)
class ReferenceVersion:
    """A version that a human imported as a reference candidate."""

    campaign: str
    candidate: CandidateId


type Keep = RunVersion | RebaseVersion | ReferenceVersion
"""Why a version must outlive whatever named it first (a branch, an agent's worktree): ``Repository.keep``."""

type Fetch = Callable[[Sequence[Revision], Sequence[Revision]], Cid | None]
"""Ask the repo of record to put what ``revisions`` have beyond the revisions that a clone has into the CAS, and return
its address (``Repository.send``). None: nothing is missing."""


class Part(StrEnum):
    """A part of the built-in prompts that the vcs provider writes, where a prompt has ``{vcs:<part>}``."""

    REPO = 'repo'
    """What a campaign's files are kept in."""
    CHECKOUT = 'checkout'
    """What an implementer works in."""
    SAVE = 'save'
    """The implementer's section on how to save its work, and what of it is scored."""
    VERSIONS = 'versions'
    """How saved versions become candidates."""
    SKIP = 'skip'
    """How a version asks not to be measured, and what happens to unsaved work."""
    EARLIER = 'earlier'
    """The implementer's section on how to read earlier candidates."""
    OLD_FILES = 'old_files'
    """How to read an older file without copying the tree."""
    SHOW = 'show'
    """How to read a candidate of another campaign, at ``<path>``."""
    READ = 'read'
    """How the research agent reads a candidate against its base."""
    PORT = 'port'
    """How to read a candidate's change against its parent, to port it."""
    CODE = 'code'
    """The tool line on how to read a candidate's code."""


PART = re.compile(r'\{vcs:(\w+)\}')


class VcsError(Exception):
    """The provider could not do what it was asked. The message says what failed."""


class NotARepository(VcsError):
    """A path is not a checkout of the provider."""


class UnknownRevision(VcsError):
    """A name of a version names none in the repository."""


class Repository(Protocol):
    """A checkout and the versions that it holds: the repo of record of a campaign, or a worker's clone of one."""

    def head(self) -> Version:
        """The version that the checkout is at."""
        ...

    def version(self, native: str) -> Version:
        """The version that ``native`` names in the provider's own terms: a revision, or under ``git`` a branch or tag.

        Raises:
            UnknownRevision: ``native`` names no version. The message says why.
        """
        ...

    def receive(self, cas: Cas, transfer: Transfer, base: Revision) -> list[Version]:
        """Take the versions of ``transfer`` from ``cas``, and return those from ``base`` to its head, oldest first.

        They are the versions that the work made on top of ``base``: not the ones that it merged in from elsewhere.
        """
        ...

    def send(self, cas: Cas, revisions: Sequence[Revision], have: Sequence[Revision]) -> Cid | None:
        """Put what ``revisions`` have beyond ``have`` into ``cas``, as a ``Transfer``'s blob, or None when nothing is.

        ``revisions`` and ``have`` may name revisions that the repository lacks: they are left out.
        """
        ...

    def fetch(self, cas: Cas, revisions: Sequence[Revision], ask: Fetch) -> None:
        """Take those of ``revisions`` that the repository lacks: ``ask`` puts them into ``cas``.

        One that the repo of record lacks too stays missing.
        """
        ...

    def merge(self, onto: Revision, other: Revision, message: str) -> Version | Conflicts:
        """Save the merge of ``other`` onto ``onto`` as a version, without a checkout, or return what conflicts."""
        ...

    def contains(self, revision: Revision, ancestor: Revision) -> bool:
        """Whether ``revision`` has all of ``ancestor``: it descends from it, or merging it in changes nothing.

        Raises:
            VcsError: The repository lacks one of them.
        """
        ...

    def keep(self, revision: Revision, why: Keep) -> None:
        """Keep ``revision`` for ``why``, also after whatever named it is gone."""
        ...

    def kept(self, campaign: str, run: RunId) -> list[Version]:
        """The versions of ``run`` that ``keep`` kept (``RunVersion``), in their order."""
        ...

    def checkout(self, revision: Revision, path: Path) -> None:
        """Put the files of ``revision`` at ``path``, where an agent or an evaluator works on them.

        A directory left at ``path`` is replaced.
        """
        ...

    def discard(self, path: Path) -> None:
        """Remove what ``checkout`` put at ``path``. Nothing fails if it is gone, or a directory of something else."""
        ...

    def checkouts(self, root: Path) -> list[Path]:
        """The paths under ``root`` that ``checkout`` made and ``discard`` did not remove yet."""
        ...

    def untracked(self, revision: Revision, names: Sequence[str]) -> list[str]:
        """Those of ``names`` that ``revision`` does not hold: what was put in its checkout from elsewhere."""
        ...

    def capture(self, cas: Cas, base: Revision, summary: str, run: RunId, ignore: Sequence[str]) -> Transfer | None:
        """Put the versions that an agent saved in this checkout of ``base`` into ``cas``, or None when it saved none.

        What it left unsaved, outside ``ignore`` (the paths that are not its work), is saved first as a ``recovered``
        version of ``run``. A version that the agent saved is never changed: it is scored as the agent made it.
        """
        ...


class Vcs(Parameters, ABC):
    """A vcs provider: its parameters, the keys of ``[plugins.vcs]`` besides ``provider``, and its operations."""

    @abstractmethod
    def init(self, path: Path, template: str, owned: Sequence[str]) -> None:
        """Make ``path`` a repository if it is not one, and save its files for ``piloop init``.

        A repository with no version yet saves all its files, as the baseline of ``template``. Else it saves the paths
        in ``owned`` that changed, and nothing else: other changes in a shared checkout are someone's work.
        """

    @abstractmethod
    def open(self, path: Path) -> Repository:
        """The repository of the checkout at ``path``.

        Raises:
            NotARepository: ``path`` is no checkout of the provider.
        """

    @abstractmethod
    def clone(self, path: Path) -> Repository:
        """A worker's clone of a repo of record at ``path``, made empty if none is there: ``Repository.fetch`` fills it."""

    @abstractmethod
    def identity(self, path: Path) -> str:
        """The project that the checkout at ``path`` is, the same for each clone of it: ``campaigns.repo``."""

    @classmethod
    @abstractmethod
    def root(cls, path: Path) -> Path | None:
        """The main checkout that ``path`` is in, also from a checkout that ``Repository.checkout`` made. None: none."""

    @abstractmethod
    def changed(self, path: Path, ignore: Sequence[str]) -> bool:
        """Whether an agent changed the checkout at ``path``: a saved version, or a changed file outside ``ignore``.

        False when there is no checkout there.
        """

    @abstractmethod
    def prompt(self, part: Part) -> str:
        """What the built-in prompts say about ``part`` for this provider (``prompted``)."""

    @classmethod
    @abstractmethod
    def checks(cls) -> list[Check]:
        """Check that the provider can work on this machine. ``piloop doctor`` shows the result. Read only."""


def prompted(text: str, vcs: Vcs) -> str:
    """``text`` with each ``{vcs:<part>}`` replaced by what ``vcs`` writes for that part (``Vcs.prompt``).

    Raises:
        ValueError: ``text`` names a part that there is not.
    """

    def part(found: re.Match[str]) -> str:
        return vcs.prompt(Part(found[1]))

    return PART.sub(part, text)


VCS = Capability('vcs', Vcs, choice=Choice(Scope.CAMPAIGN, default='git'), group='piloop.vcs', noun='vcs provider')
"""Every vcs provider by name. ``git``, built in and the default, keeps versions as commits of a git repository."""
