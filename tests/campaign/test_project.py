import json
import re
from pathlib import Path
from typing import get_args, get_origin

from git import Repo
from pydantic import BaseModel
from pytest_mock import MockerFixture

from piloop.agents.capture import ENTIRE_DIR
from piloop.campaign import project as project_module
from piloop.campaign.project import BLANK, TEMPLATES, create_project_repo, register, registered, stub_untouched
from piloop.config import ProjectConfig, load_project_config
from piloop.db.session import Database
from tests.conftest import HARNESS_APPEND, write_toml

ENTIRE_FILES = ['.entire/.gitignore', '.entire/settings.json']


def test_create_project_repo(tmp_path: Path, mocker: MockerFixture) -> None:
    template = tmp_path / 'template'
    template.mkdir()
    (template / 'solve.py').write_text('print(1)\n')
    (template / 'pyproject.toml').write_text('[project]\nname = "t"\nversion = "0"\nrequires-python = ">=3.14"\n')
    (template / 'data').mkdir()
    (template / 'data' / 'ignored.csv').write_text('x')
    get = mocker.patch('piloop.campaign.project.httpx.get')
    get.return_value.raise_for_status.return_value.content = b'a,b\n'
    dest = tmp_path / 'dest'
    written = create_project_repo(dest, template, {'data/train.csv': 'http://example/train.csv'})
    assert written == [*ENTIRE_FILES, '.gitignore', 'pyproject.toml', 'solve.py']
    assert (dest / 'data' / 'train.csv').read_bytes() == b'a,b\n' and not (dest / 'data' / 'ignored.csv').exists()
    repo = Repo(dest)
    assert repo.head.commit.summary == 'template: baseline'
    assert repo.git.ls_files().splitlines() == [*ENTIRE_FILES, '.gitignore', 'pyproject.toml', 'solve.py', 'uv.lock']
    assert (dest / '.venv').is_dir(), 'uv sync ran because the template has a pyproject'

    plain = tmp_path / 'plain'
    plain.mkdir()
    (plain / 'x.txt').write_text('x')
    create_project_repo(tmp_path / 'plain-dest', plain, {})
    assert Repo(tmp_path / 'plain-dest').head.commit.summary == 'plain: baseline'
    # A second run downloads and overwrites nothing. The user's edit is committed with its own message.
    (dest / 'solve.py').write_text('print(2)\n')
    assert create_project_repo(dest, template, {'data/train.csv': 'http://example/train.csv'}) == []
    assert get.call_count == 1 and (dest / 'solve.py').read_text() == 'print(2)\n'
    assert Repo(dest).head.commit.summary == 'piloop init: solve.py'
    assert create_project_repo(dest, template, {}) == [] and Repo(dest).head.commit.summary == 'piloop init: solve.py'


def test_blank_template_into_an_existing_repo(tmp_path: Path) -> None:
    dest = tmp_path / 'my repo!'
    dest.mkdir()
    (dest / 'main.py').write_text('x\n')
    (dest / '.gitignore').write_text('*.log\n')
    repo = Repo.init(dest, initial_branch='main')
    repo.git.add('-A')
    repo.git.commit('-qm', 'first')
    assert create_project_repo(dest, BLANK, {}) == [*ENTIRE_FILES, 'brief.md', 'evaluate.py', 'piloop.toml']
    assert (dest / '.gitignore').read_text() == '*.log\n', 'an existing ignore file is kept'
    assert load_project_config(dest).name == 'my-repo', 'the directory name, made ref-safe'
    assert (
        repo.head.commit.summary
        == 'piloop init: .entire/.gitignore, .entire/settings.json, brief.md, evaluate.py, piloop.toml'
    ), 'not a baseline: the repo had one'
    assert not repo.is_dirty(untracked_files=True)
    assert stub_untouched(dest)
    toml = dest / 'piloop.toml'
    toml.write_text(toml.read_text().replace('"uv", "run", "python", "evaluate.py"', '"make", "score"'))
    assert not stub_untouched(dest), 'the stub is untouched but piloop.toml points elsewhere'
    toml.write_text(toml.read_text().replace('"make", "score"', '"uv", "run", "python", "evaluate.py"'))
    (dest / 'evaluate.py').write_text('print("METRIC score=1")\n')
    assert not stub_untouched(dest)


def test_a_project_that_predates_session_capture_gets_the_entire_files_and_nothing_else(tmp_path: Path) -> None:
    dest = tmp_path / 'older'
    create_project_repo(dest, BLANK, {})
    for rel in ENTIRE_FILES:
        (dest / rel).unlink()
    (dest / ENTIRE_DIR).rmdir()
    Repo(dest).git.commit('-qam', 'before session capture')
    assert create_project_repo(dest, BLANK, {}) == ENTIRE_FILES, 'a project (it has piloop.toml) is not re-templated'
    assert Repo(dest).head.commit.summary == 'piloop init: .entire/.gitignore, .entire/settings.json'
    assert create_project_repo(dest, BLANK, {}) == [], 'idempotent'


def test_example_templates_are_projects_with_entire_settings() -> None:
    for template in TEMPLATES.iterdir():
        cfg = load_project_config(template)
        assert cfg.name == template.name or template == BLANK, template
        for rel in ENTIRE_FILES:
            assert (template / rel).read_bytes() == (BLANK / rel).read_bytes(), (template, rel)
    settings = json.loads((BLANK / ENTIRE_DIR / 'settings.json').read_text())
    assert settings == {
        'enabled': True,
        'strategy_options': {'push_sessions': False},
        'telemetry': False,
        'checkpoints': {'primary': {'type': 'git-refs'}},
    }, 'checkpoints as refs in the repo, nothing pushed, nothing phoned home'
    assert (
        BLANK / ENTIRE_DIR / '.gitignore'
    ).read_text() == 'tmp/\nsettings.local.json\nmetadata/\nlogs/\nredactors/local/\n'


HEADER = re.compile(r'^#?\s*\[\[?([^\]]+)\]')
"""A table header line, commented or not: ``[stop]``, ``# [stop]``, ``[[metrics]]``."""


def model_in(annotation: object) -> tuple[type[BaseModel], bool] | None:
    """The model that a field holds, and whether it holds one per name (a dict, so one table for each name)."""
    if get_origin(annotation) is dict:
        value = get_args(annotation)[1]
        return (value, True) if isinstance(value, type) and issubclass(value, BaseModel) else None
    for arg in (annotation, *get_args(annotation)):
        if isinstance(arg, type) and issubclass(arg, BaseModel):
            return arg, False
    return None


def unnamed_keys(template: str) -> list[str]:
    """The keys of ``ProjectConfig``, at every depth, that a ``piloop.toml`` text does not name.

    A key is named by an assignment (``key =`` or ``# key =``) in its table, or by a header (``[table]``,
    ``# [table]``) at its path. A table that lists its keys in a ``keys:`` comment may name a key there instead. A
    nested table with no header of its own names its keys in the table above it.
    """
    sections: dict[str, str] = {}
    name = ''
    for line in template.splitlines():
        if header := HEADER.match(line):
            name = header.group(1)
        sections[name] = sections.get(name, '') + line + '\n'

    def walk(model: type[BaseModel], path: str, label: str, text: str) -> list[str]:
        missing = []
        for key, field in model.model_fields.items():
            full = f'{path}\\.{re.escape(key)}' if path else re.escape(key)
            where = f'{label}.{key}' if label else key
            headed = any(re.fullmatch(rf'{full}(\..+)?', n) for n in sections)
            named = (
                headed
                or re.search(rf'^#?\s*{re.escape(key)}\s*=', text, re.MULTILINE) is not None
                or ('keys:' in text and re.search(rf'\b{re.escape(key)}\b', text) is not None)
            )
            if not named:
                missing.append(where)
            if (nested := model_in(field.annotation)) is not None:
                table = full + (r'\.[^.]+' if nested[1] else '')
                own = ''.join(t for n, t in sections.items() if re.fullmatch(table, n))
                missing += walk(nested[0], table, where + ('.<name>' if nested[1] else ''), own or text)
        return missing

    return walk(ProjectConfig, '', '', sections.get('', ''))


def test_the_project_template_names_every_key_of_the_config() -> None:
    template = (BLANK / 'piloop.toml').read_text()
    assert template.startswith('# piloop project config. This file lists every key.')
    assert unnamed_keys(template) == [], 'the template says that it lists every key; add the missing ones to it'
    gaps = {'explore_every = 4': 'every = 4', 'plateau': 'p', 'bearer_token_env': 'x', ' tally;': ';'}
    without = template
    for old, new in gaps.items():
        without = without.replace(old, new)
    assert unnamed_keys(without) == [
        'harness.<name>.usage.tally',
        'roles.worker.explore_every',
        'mcp.<name>.bearer_token_env',
        'stop.plateau',
    ], 'the check finds a key missing from a table, from a keys: comment, and from a nested table without a header'


def test_the_baseline_keeps_its_per_unit_results_under_the_daemon_home(db: Database, project_path: Path) -> None:
    script = project_path.parent / 'evaluate.sh'
    script.write_text(
        'echo METRIC score=1\necho METRIC size=5\nprintf \'{"score": {"only": 1}}\' > "$PILOOP_UNITS_FILE"\n'
    )
    write_toml(project_path, HARNESS_APPEND, evaluate=['sh', str(script)])
    with db.session() as s:
        _, measurement = register(s, project_path)
    units_dir = project_module.settings.units_dir
    assert units_dir == project_module.settings.home / 'units'
    assert [p.name for p in units_dir.iterdir()] == [f'{measurement.id}.json']
    assert json.loads((units_dir / f'{measurement.id}.json').read_text()) == {'score': {'only': 1}}


def test_the_baseline_is_scored_without_locking_the_database(db: Database, project_path: Path, tmp_path: Path) -> None:
    script = project_path.parent / 'evaluate.py'
    script.write_text(
        'import sqlite3\n'
        f'con = sqlite3.connect({str(tmp_path / "piloop.db")!r}, timeout=0)\n'
        "con.execute('BEGIN IMMEDIATE')\n"
        'con.rollback()\n'
        "print('METRIC score=1')\n"
        "print('METRIC size=5')\n"
    )
    write_toml(project_path, HARNESS_APPEND, evaluate=['python3', str(script)])
    with db.session() as s:
        assert registered(s, project_path) is None, 'the CLI reads first, in the same session'
        project, measurement = register(s, project_path)
        assert measurement.metrics == {'score': 1.0, 'size': 5.0}, 'another client could write meanwhile'
    projects_dir = project_module.projects_dir(project_module.settings.home)
    assert [p.name for p in projects_dir.iterdir()] == [str(project.id)], 'the staging directory became the project'
    assert (
        projects_dir / str(project.id) / 'baseline' / 'eval.stdout'
    ).read_text() == 'METRIC score=1\nMETRIC size=5\n'
