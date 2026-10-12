"""``piloop watch``: the campaign on one screen, with keys to act on it. A Textual app with a layout like lazygit.

The first screen lists every project, one row each. ``enter`` opens a project, and ``piloop watch <project>`` starts
there. A project screen has a header (head, spend, progress against ``[stop]``), four numbered panels on the left
(inbox, agents, ideas, candidates), and on the right the detail of the selected row of the focused panel. Each panel
has its own keys, which the key bar and ``?`` show. ``/`` filters the focused panel.

A key that writes calls the same functions as the CLI (``inbox.ack``, ``inbox.reply``, ``ideas.review``,
``ideas.reprioritise``, the runtime's ``prompt``). Each write is one short session signed by the watcher's actor, so
the triggers stamp it and the events feed carries it like the write of any other client.

``piloop/watch/panels.py`` gives the data. This module draws it and maps the keys. It reads again when the database
files change (``piloop.db`` and its WAL, checked every ``POLL_SECONDS``), and at least every ``REFRESH_SECONDS``.
"""

from collections.abc import Callable
from functools import partial
from pathlib import Path
from time import monotonic
from typing import ClassVar

from rich.text import Text
from sqlalchemy.orm import Session
from textual.app import App, ComposeResult
from textual.binding import Binding, BindingType
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.markup import escape
from textual.screen import ModalScreen, Screen
from textual.widgets import DataTable, Footer, HelpPanel, Input, Label, Select, Static

from piloop.campaign import inbox
from piloop.campaign.ideas import reprioritise, review
from piloop.campaign.project import find
from piloop.config import config_of
from piloop.db.models import Idea, IdeaStatus, Worker, WorkerState, utcnow
from piloop.db.session import Database
from piloop.reports.overview import tab_of
from piloop.runtime import NotSteerable
from piloop.runtime.factory import handle_for, runtime_for
from piloop.runtime.herdr import HerdrError, focus_tab
from piloop.watch.panels import PANELS, PROJECT_COLUMNS, Cell, Context, Item, Panel, header, projects

POLL_SECONDS = 0.5
"""How often to check the database files for a change."""
REFRESH_SECONDS = 5
"""Read again at least this often without a change, so that the ages and the agents' output stay current."""
REPLY_PRIORITY = 2
"""The default priority of a reply. It puts the reply in the inbox, so it wakes the orchestrator, but sends no push."""
CSS = """
#header { height: 2; padding: 0 1; }
#body { height: 1fr; }
#left { width: 45%; }
PanelTable { height: 1fr; border: round $panel; border-title-align: left; scrollbar-size-horizontal: 0; }
PanelTable:focus { border: round $accent; }
#main { width: 1fr; border: round $panel; padding: 0 1; }
#main:focus { border: round $accent; }
#filter { dock: bottom; display: none; }
#filter.shown { display: block; }
#projects { height: 1fr; }
ModalScreen { align: center middle; }
#dialog { width: 80%; height: auto; border: round $accent; padding: 0 1; background: $surface; }
"""


class Ask(ModalScreen[str | None]):
    """A dialog for one line of text, such as a reason or a steering message.

    ``enter`` sends it. ``esc`` drops it, and so does ``enter`` on an empty line.
    """

    BINDINGS: ClassVar[list[BindingType]] = [Binding('escape', 'drop', 'drop')]

    def __init__(self, prompt: str) -> None:
        super().__init__()
        self.prompt = prompt

    def compose(self) -> ComposeResult:
        with Vertical(id='dialog'):
            yield Label(escape(self.prompt))
            yield Input(id='text')

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.dismiss(event.value.strip() or None)

    def action_drop(self) -> None:
        self.dismiss(None)


class ReplyBox(ModalScreen[tuple[str, int] | None]):
    """A dialog for a reply and its priority.

    ``enter`` in the text sends both, ``tab`` moves to the priority, and ``esc`` drops the reply.
    """

    BINDINGS: ClassVar[list[BindingType]] = [Binding('escape', 'drop', 'drop')]

    def __init__(self, prompt: str) -> None:
        super().__init__()
        self.prompt = prompt

    def compose(self) -> ComposeResult:
        with Vertical(id='dialog'):
            yield Label(escape(self.prompt))
            yield Input(id='text', placeholder='your reply; enter sends it')
            yield Select([(f'p{p}', p) for p in range(5)], value=REPLY_PRIORITY, allow_blank=False, id='priority')

    def on_input_submitted(self, event: Input.Submitted) -> None:
        text = event.value.strip()
        self.dismiss((text, self.query_one(Select).selection or REPLY_PRIORITY) if text else None)

    def action_drop(self) -> None:
        self.dismiss(None)


class PanelTable(DataTable[Text]):
    """One numbered panel. Its rows come from ``panel``, and the cursor stays on its key when the rows change."""

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding('j', 'cursor_down', 'down', show=False),
        Binding('k', 'cursor_up', 'up', show=False),
    ]

    def __init__(self, number: int, name: str, panel: Panel) -> None:
        super().__init__(cursor_type='row', show_header=False, id=name)
        self.number, self.panel, self.query_text = number, panel, ''

    def fill(self, items: list[Item], title: Callable[[list[Item]], str]) -> None:
        """Replace the rows with the ``items`` that match the filter, and title the panel by what it shows.

        The cursor stays on its key, else near its index.
        """
        shown = [i for i in items if self.query_text.lower() in ' '.join(map(str, i.cells)).lower()]
        current, index = self.key(), self.cursor_row
        self.clear(columns=True)
        if shown:
            self.add_columns(*(str(n) for n in range(len(shown[0].cells))))
            for item in shown:
                self.add_row(*plain(item.cells), key=str(item.key))
        keys = [i.key for i in shown]
        self.move_cursor(row=keys.index(current) if current in keys else min(index, len(keys) - 1), animate=False)
        query = f' /{self.query_text}' if self.query_text else ''
        self.border_title = escape(f'[{self.number}] {title(shown)}{query}')

    def key(self) -> int | None:
        """The id of the selected row; None in an empty panel."""
        if not self.row_count:
            return None
        return int(self.coordinate_to_cell_key(self.cursor_coordinate).row_key.value or 0)

    def keys(self) -> list[int]:
        """The ids of every row shown, filter applied."""
        return [int(row.value or 0) for row in self.rows]

    @property
    def campaign(self) -> CampaignScreen:
        screen = self.screen
        assert isinstance(screen, CampaignScreen)
        return screen


class InboxTable(PanelTable):
    """Panel 1. ``a`` acknowledges the item, ``A`` every item shown, and ``r`` replies to its thread with a priority."""

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding('a', 'ack', 'ack'),
        Binding('A', 'ack_shown', 'ack all shown'),
        Binding('r', 'reply', 'reply'),
    ]

    def action_ack(self) -> None:
        if (key := self.key()) is not None:
            self.campaign.act(lambda s: '; '.join(inbox.ack(s, [key], self.campaign.actor)))

    def action_ack_shown(self) -> None:
        if keys := self.keys():
            self.campaign.act(lambda s: '; '.join(inbox.ack(s, keys, self.campaign.actor)))

    def action_reply(self) -> None:
        if (key := self.key()) is None:
            return

        def send(answer: tuple[str, int] | None) -> None:
            if answer is None:
                self.campaign.say('reply dropped')
                return
            text, priority = answer

            def write(s: Session) -> str:
                sent = inbox.reply(s, key, text, author=self.campaign.actor, priority=priority)
                return f'sent comment#{sent.id} (p{priority}) in reply to comment#{sent.target_id}'

            self.campaign.act(write)

        self.app.push_screen(ReplyBox(f'reply to comment#{key}, as {self.campaign.actor}'), send)


class AgentsTable(PanelTable):
    """Panel 2. ``enter`` focuses the agent's Herdr tab, and ``s`` steers it with a prompt typed into its pane."""

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding('enter', 'focus_tab', 'focus tab'),
        Binding('s', 'steer', 'steer'),
    ]

    def action_focus_tab(self) -> None:
        if (key := self.key()) is None:
            return
        with self.campaign.db.session() as s:
            tab = tab_of(s.get_one(Worker, key))
        if tab is None:
            self.campaign.say(f'worker#{key} has no Herdr tab (headless runtime): nothing to focus')
            return
        try:
            focus_tab(tab)
        except HerdrError as e:
            self.campaign.say(f'could not focus worker#{key}: {e}')
            return
        self.campaign.say(f'focused worker#{key} (tab {tab})')

    def action_steer(self) -> None:
        if (key := self.key()) is None:
            return

        def send(text: str | None) -> None:
            if text is not None:
                self.campaign.say(steer(self.campaign.db, key, text, self.campaign.home))

        self.app.push_screen(Ask(f'steer worker#{key}: typed into its pane as a prompt'), send)


def steer(db: Database, worker_id: int, text: str, home: Path) -> str:
    """Prompt a running agent as ``piloop prompt`` does, and return a note that says what happened."""
    with db.session() as s:
        worker = s.get_one(Worker, worker_id)
        if worker.state is not WorkerState.RUNNING:
            return f'worker#{worker_id} is {worker.state}, not running: nothing to steer'
        handle, runtime = handle_for(worker), runtime_for(s, worker, home)
    try:
        runtime.prompt(handle, text)
    except NotSteerable as e:
        return f'not steered: {e}'
    except HerdrError as e:
        return f'could not steer worker#{worker_id}: {e}'
    return f'steered worker#{worker_id}'


class IdeasTable(PanelTable):
    """Panel 3. ``y`` approves (to ``ready``), ``p`` parks and ``x`` rejects, each with a reason.

    ``+`` and ``-`` change the priority. A seed cannot be approved here. Research develops it into a defined idea first (``piloop idea develop``).
    """

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding('y', 'review("ready")', 'approve'),
        Binding('p', 'review("parked")', 'park'),
        Binding('x', 'review("rejected")', 'reject'),
        Binding('plus', 'priority(-1)', 'more urgent'),
        Binding('minus', 'priority(1)', 'less urgent'),
    ]

    def action_review(self, status: str) -> None:
        if (key := self.key()) is None:
            return
        to = IdeaStatus(status)
        with self.campaign.db.session() as s:
            now = s.get_one(Idea, key).status
        if now is to:
            self.campaign.say(f'idea#{key} is {to} already')
            return
        if now is IdeaStatus.SEED and to is IdeaStatus.READY:
            self.campaign.say(f'idea#{key} is a seed: research defines it first (piloop idea develop {key} …)')
            return

        def send(reason: str | None) -> None:
            if reason is None:
                self.campaign.say(f'idea#{key} left {now}: no reason given')
                return
            self.campaign.act(
                lambda s: f'idea#{key} {now} → {review(s, key, to, reason, author=self.campaign.actor).status}'
            )

        self.app.push_screen(Ask(f'idea#{key} {now} → {to}: why? (a note on the idea)'), send)

    def action_priority(self, by: int) -> None:
        if (key := self.key()) is not None:
            self.campaign.act(lambda s: f'idea#{key} priority {reprioritise(s, key, by).priority}')


class CandidatesTable(PanelTable):
    """Panel 4, read-only: the candidates, newest first, under the sparkline of the head of each track."""


TABLES: dict[str, type[PanelTable]] = {
    'inbox': InboxTable,
    'agents': AgentsTable,
    'ideas': IdeasTable,
    'candidates': CandidatesTable,
}


class FilterInput(Input):
    """The filter of the focused panel, opened with ``/``. ``enter`` applies it and ``esc`` clears it."""

    BINDINGS: ClassVar[list[BindingType]] = [Binding('escape', 'clear', 'clear filter')]

    def action_clear(self) -> None:
        self.value = ''
        self.post_message(self.Submitted(self, ''))


class ProjectsScreen(Screen[None]):
    """Every project, one row each, active projects first. ``enter`` opens one."""

    BINDINGS: ClassVar[list[BindingType]] = [Binding('enter', 'open', 'open', priority=True)]

    def compose(self) -> ComposeResult:
        yield Static(id='header')
        table: DataTable[Text] = DataTable(cursor_type='row', id='projects')
        table.add_columns(*PROJECT_COLUMNS)
        yield table
        yield Footer()

    def on_mount(self) -> None:
        self.reload()

    def reload(self) -> None:
        app = self.app
        assert isinstance(app, Watch)
        table = self.query_one(DataTable)
        with app.db.session() as s:
            rows = projects(s, utcnow(), app.actor)
        index = table.cursor_row
        table.clear()
        for item in rows:
            table.add_row(*plain(item.cells), key=str(item.cells[0]))
        table.move_cursor(row=min(index, len(rows) - 1), animate=False)
        self.query_one('#header', Static).update(Text(f'piloop: {len(rows)} project{"" if len(rows) == 1 else "s"}'))

    def action_open(self) -> None:
        table = self.query_one(DataTable)
        if table.row_count:
            name = table.coordinate_to_cell_key(table.cursor_coordinate).row_key.value
            self.app.push_screen(CampaignScreen(str(name)))


class CampaignScreen(Screen[None]):
    """One project: the header, the four panels and the selected row's detail."""

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding('1', 'panel("inbox")', 'inbox', show=False),
        Binding('2', 'panel("agents")', 'agents', show=False),
        Binding('3', 'panel("ideas")', 'ideas', show=False),
        Binding('4', 'panel("candidates")', 'candidates', show=False),
        Binding('0', 'main', 'detail', show=False),
        Binding('slash', 'filter', 'filter'),
        Binding('escape', 'projects', 'projects'),
    ]

    def __init__(self, ref: str) -> None:
        super().__init__()
        self.ref = ref
        self.note = ''
        """The last action's outcome, as the toast said it."""
        self.shown: tuple[str | None, int | None] = (None, None)
        """The panel and row that the main pane shows. A read that leaves the cursor there skips the pane."""

    @property
    def main_app(self) -> Watch:
        """The ``Watch`` app. Not ``watch``: that name is Textual's method for watching a reactive attribute."""
        app = self.app
        assert isinstance(app, Watch)
        return app

    @property
    def db(self) -> Database:
        return self.main_app.db

    @property
    def actor(self) -> str:
        return self.main_app.actor

    @property
    def home(self) -> Path:
        return self.main_app.db_file.parent

    def compose(self) -> ComposeResult:
        yield Static(id='header')
        with Horizontal(id='body'):
            with Vertical(id='left'):
                for number, (name, panel) in enumerate(PANELS.items(), start=1):
                    yield TABLES[name](number, name, panel)
            with VerticalScroll(id='main'):
                yield Static(id='detail')
        yield FilterInput(id='filter', placeholder='filter the focused panel; enter applies, esc clears')
        yield Footer()

    def on_mount(self) -> None:
        self.reload()
        self.query_one(InboxTable).focus()

    def tables(self) -> list[PanelTable]:
        return list(self.query(PanelTable))

    def reload(self) -> None:
        """Read the project, and fill every panel and the detail, in one session."""
        with self.db.session() as s:
            ctx = self.context(s)
            if ctx is None:
                self.query_one('#header', Static).update(
                    Text(f'no project {self.ref!r} yet: `piloop init` registers it, and this view follows')
                )
                return
            self.query_one('#header', Static).update(header(s, ctx.project, ctx.cfg, ctx.now))
            for table in self.tables():
                table.fill(table.panel.items(s, ctx), partial(table.panel.title, s, ctx))
            self.show(s, ctx)

    def context(self, s: Session) -> Context | None:
        project = find(s, self.ref)
        if project is None:
            return None
        return Context(project, config_of(project), utcnow(), self.actor)

    def focused_table(self) -> PanelTable:
        focused = self.focused
        return focused if isinstance(focused, PanelTable) else self.query_one(InboxTable)

    def show(self, s: Session, ctx: Context) -> None:
        """Show the selected row of the focused panel in the main pane. While the pane has focus, use the last panel."""
        table = self.focused_table() if not isinstance(self.focused, VerticalScroll) else self.shown_table
        self.shown_table = table
        key = table.key()
        self.shown = (table.id, key)
        detail = self.query_one('#detail', Static)
        detail.update(table.panel.detail(s, ctx, key) if key is not None else Text('nothing here'))
        self.query_one('#main', VerticalScroll).border_title = escape(f'[0] {table.id}')

    shown_table: PanelTable

    def refresh_detail(self) -> None:
        with self.db.session() as s:
            ctx = self.context(s)
            if ctx is not None:
                self.show(s, ctx)

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        table = event.data_table
        if table is self.focused and isinstance(table, PanelTable) and (table.id, table.key()) != self.shown:
            self.refresh_detail()

    def on_descendant_focus(self) -> None:
        if isinstance(self.focused, PanelTable):
            self.refresh_detail()

    def act(self, write: Callable[[Session], str]) -> None:
        """Run one write in its own session, say what it did, and read the screen again."""
        with self.db.session() as s:
            note = write(s)
        self.say(note)
        self.reload()

    def say(self, note: str) -> None:
        self.note = note
        self.notify(note, timeout=6)

    def action_panel(self, name: str) -> None:
        self.query_one(f'#{name}', PanelTable).focus()

    def action_main(self) -> None:
        self.query_one('#main', VerticalScroll).focus()

    def action_filter(self) -> None:
        box = self.query_one(FilterInput)
        self.filtering = self.focused_table()
        box.value = self.filtering.query_text
        box.add_class('shown')
        box.focus()

    filtering: PanelTable

    def on_input_submitted(self, event: Input.Submitted) -> None:
        event.input.remove_class('shown')
        self.filtering.query_text = event.value.strip()
        self.filtering.focus()
        self.reload()

    def action_projects(self) -> None:
        self.app.pop_screen()


class Watch(App[None]):
    """The app: the projects screen at the bottom of the stack, and a project screen on top when one is open."""

    CSS = CSS
    TITLE = 'piloop watch'
    ENABLE_COMMAND_PALETTE = False
    BINDINGS: ClassVar[list[BindingType]] = [
        Binding('q', 'quit', 'quit'),
        Binding('question_mark', 'keys', 'keys'),
    ]

    def __init__(self, db: Database, db_file: Path, ref: str | None, actor: str) -> None:
        super().__init__()
        self.db, self.db_file, self.ref, self.actor = db, db_file, ref, actor
        self._stamp: tuple[int, ...] | None = None
        self._read = monotonic()

    def on_mount(self) -> None:
        self._stamp = stamp(self.db_file)
        self.push_screen(ProjectsScreen())
        if self.ref is not None:
            self.push_screen(CampaignScreen(self.ref))
        self.set_interval(POLL_SECONDS, self.poll)

    def poll(self) -> None:
        """Read the top screen again when the database changed or ``REFRESH_SECONDS`` passed. Not under a dialog."""
        now = stamp(self.db_file)
        if now == self._stamp and monotonic() - self._read < REFRESH_SECONDS:
            return
        self._stamp, self._read = now, monotonic()
        if isinstance(self.screen, ProjectsScreen | CampaignScreen):
            self.screen.reload()

    def action_keys(self) -> None:
        """Show or hide the keys of the focused panel beside the screen (``?``)."""
        if self.screen.query(HelpPanel):
            self.action_hide_help_panel()
        else:
            self.action_show_help_panel()


def plain(cells: tuple[Cell, ...]) -> list[Text]:
    """Return cells as ``Text``: a ``DataTable`` reads a ``str`` as markup, and ``[wip]`` in a summary is data."""
    return [Text(c) if isinstance(c, str) else c for c in cells]


def stamp(db_file: Path) -> tuple[int, ...]:
    """Return the mtimes of the database and its WAL. Every write by any connection changes one of them."""
    files = (db_file, db_file.with_name(f'{db_file.name}-wal'))
    return tuple(f.stat().st_mtime_ns for f in files if f.exists())
