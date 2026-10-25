"""An agent's harness session file, put into the CAS as it grows: what ``agent_sessions.session_file`` names.

The job code looks at the file every ``POLL_SECONDS`` while the agent runs, on whatever machine runs it. A snapshot is
due once the file grew and then was quiet for ``QUIET_SECONDS``, and at least every ``FLUSH_SECONDS`` while it keeps
growing; one more comes when the agent ends. A snapshot is a chunked file (``piloop.store.chunks``): it stores only the
chunks that are new since the one before, so all the snapshots of a session take about the size of its last one.
"""

from dataclasses import dataclass, field
from pathlib import Path

from covalence import Cid

from piloop.plugins.cas.contract import Cas
from piloop.store.chunks import GROUP, Chunked, cuts
from piloop.usage import UsageReader

POLL_SECONDS = 1.0
"""How often the job code looks at a running agent's session file."""
QUIET_SECONDS = 2.0
"""How long a file that grew stays unchanged before its snapshot: a turn writes several records at once."""
FLUSH_SECONDS = 30.0
"""The longest a file that keeps growing waits for its snapshot: what a lost worker loses, at most."""


@dataclass
class SessionFile:
    """The session file of one agent's job, found as its harness's usage spec says, and its latest snapshot.

    A harness appends to its file. The chunks before the open last one stay as they were stored, and the next snapshot
    reads only the bytes after them. A file that got shorter is read again from its start.
    """

    reader: UsageReader
    cas: Cas
    cwd: Path
    session_id: str | None = None
    """The harness's session id, which finds the file. None until the agent reports it."""
    latest: Cid | None = None
    """The latest snapshot in the CAS: a chunked file's manifest. None until there is one."""
    _path: Path | None = None
    _groups: list[Cid] = field(default_factory=list)
    """Full groups of closed chunks, each a chunked file, in order."""
    _closed: list[Cid] = field(default_factory=list)
    """The chunks that end at a cut after the groups, in order: every later snapshot starts with the groups and them."""
    _offset: int = 0
    """The bytes that ``_groups`` and ``_closed`` hold."""
    _whole: int = 0
    """The bytes of whole lines in ``latest``."""
    _read: int = 0
    """The size of the file that the latest snapshot read."""
    _size: int = 0
    """The size of the file at the last look, and when it changed to that."""
    _grew_at: float = 0.0
    _saved_at: float | None = None
    """When the latest snapshot was taken, or the first look before one."""

    def due(self, now: float) -> bool:
        """Look at the file: whether it grew since the latest snapshot and was quiet since, or the flush is due."""
        path = self._find()
        if path is None:
            return False
        try:
            size = path.stat().st_size
        except FileNotFoundError:  # it went: find it again at the next look, and the last snapshot says what failed
            self._path = None
            return False
        if size != self._size:
            self._size, self._grew_at = size, now
        if self._saved_at is None:
            self._saved_at = now
        return size != self._read and (now - self._grew_at >= QUIET_SECONDS or now - self._saved_at >= FLUSH_SECONDS)

    def snapshot(self, now: float) -> Cid | None:
        """Put the new whole lines of the file into the CAS, as the chunks after the closed ones; the latest snapshot.

        The file is read once: what the harness appends meanwhile goes into the next snapshot. A live file can end in
        the middle of a line, which waits for the next one too.

        Raises:
            OSError: the file went between the look and the read.
        """
        path = self._find()
        if path is None:
            return self.latest
        with path.open('rb') as file:
            if file.seek(0, 2) < self._offset:  # rewritten, not appended to
                self._groups, self._closed, self._offset, self._whole = [], [], 0, 0
            file.seek(self._offset)
            new = file.read()
        self._read, self._saved_at = self._offset + len(new), now
        whole = new[: new.rfind(b'\n') + 1]
        if self._offset + len(whole) == self._whole:
            return self.latest
        start = 0
        for end in cuts(whole):
            self._closed.append(self.cas.put(whole[start:end]))
            start = end
            if len(self._closed) == GROUP:
                self._groups.append(Chunked(chunks=tuple(self._closed)).put(self.cas))
                self._closed = []
        self._offset += start
        tail = [self.cas.put(whole[start:])] if start < len(whole) else []
        parts = (*self._groups, *self._closed, *tail)
        self.latest, self._whole = Chunked(chunks=parts).put(self.cas), self._offset + len(whole) - start
        return self.latest

    def _find(self) -> Path | None:
        if self._path is None and self.session_id is not None:
            self._path = self.reader.session_file(self.session_id, self.cwd)
        return self._path
