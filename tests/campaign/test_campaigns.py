import re
from pathlib import Path
from typing import get_args, get_origin

import pytest
from git import Repo
from pydantic import BaseModel, RootModel, ValidationError
from pytest_mock import MockerFixture
from sqlalchemy import select

from piloop.campaign.campaigns import (
    BLANK,
    TEMPLATES,
    create_campaign_repo,
    stub_untouched,
    sync,
)
from piloop.config import CampaignConfig, ConfigError, load_campaign_config
from piloop.db.models import Campaign
from piloop.db.session import Database
from piloop.ids import CampaignStatus
from piloop.plugins.host import Scope, declared
from piloop.plugins.role.contract import ROLES
from tests.conftest import store_config


def test_create_campaign_repo(tmp_path: Path, mocker: MockerFixture) -> None:
    template = tmp_path / 'template'
    template.mkdir()
    (template / 'solve.py').write_text('print(1)\n')
    (template / 'pyproject.toml').write_text('[project]\nname = "t"\nversion = "0"\nrequires-python = ">=3.14"\n')
    (template / 'data').mkdir()
    (template / 'data' / 'ignored.csv').write_text('x')
    get = mocker.patch('piloop.campaign.campaigns.httpx.get')
    get.return_value.raise_for_status.return_value.content = b'a,b\n'
    dest = tmp_path / 'dest'
    written = create_campaign_repo(dest, template, {'data/train.csv': 'http://example/train.csv'})
    assert written == ['.gitignore', 'pyproject.toml', 'solve.py']
    assert (dest / 'data' / 'train.csv').read_bytes() == b'a,b\n' and not (dest / 'data' / 'ignored.csv').exists()
    repo = Repo(dest)
    assert repo.head.commit.summary == 'template: baseline'
    assert repo.git.ls_files().splitlines() == ['.gitignore', 'pyproject.toml', 'solve.py', 'uv.lock']
    assert (dest / '.venv').is_dir(), 'uv sync ran because the template has a pyproject'

    plain = tmp_path / 'plain'
    plain.mkdir()
    (plain / 'x.txt').write_text('x')
    create_campaign_repo(tmp_path / 'plain-dest', plain, {})
    assert Repo(tmp_path / 'plain-dest').head.commit.summary == 'plain: baseline'
    # A second run downloads and overwrites nothing. The user's edit is committed with its own message.
    (dest / 'solve.py').write_text('print(2)\n')
    assert create_campaign_repo(dest, template, {'data/train.csv': 'http://example/train.csv'}) == []
    assert get.call_count == 1 and (dest / 'solve.py').read_text() == 'print(2)\n'
    assert Repo(dest).head.commit.summary == 'piloop init: solve.py'
    assert create_campaign_repo(dest, template, {}) == [] and Repo(dest).head.commit.summary == 'piloop init: solve.py'


def test_blank_template_into_an_existing_repo(tmp_path: Path) -> None:
    dest = tmp_path / 'my repo!'
    dest.mkdir()
    (dest / 'main.py').write_text('x\n')
    (dest / '.gitignore').write_text('*.log\n')
    repo = Repo.init(dest, initial_branch='main')
    repo.git.add('-A')
    repo.git.commit('-qm', 'first')
    assert create_campaign_repo(dest, BLANK, {}) == ['brief.md', 'evaluate.py', 'piloop.toml']
    assert (dest / '.gitignore').read_text() == '*.log\n', 'an existing ignore file is kept'
    assert load_campaign_config(dest).name == 'my-repo', 'the directory name, made ref-safe'
    assert repo.head.commit.summary == 'piloop init: brief.md, evaluate.py, piloop.toml', (
        'not a baseline: the repo had one'
    )
    assert not repo.is_dirty(untracked_files=True)
    assert stub_untouched(dest)
    toml = dest / 'piloop.toml'
    toml.write_text(toml.read_text().replace('"uv", "run", "python", "evaluate.py"', '"make", "score"'))
    assert not stub_untouched(dest), 'the stub is untouched but piloop.toml points elsewhere'
    toml.write_text(toml.read_text().replace('"make", "score"', '"uv", "run", "python", "evaluate.py"'))
    (dest / 'evaluate.py').write_text('print("METRIC score=1")\n')
    assert not stub_untouched(dest)


def test_a_campaign_is_not_templated_again(tmp_path: Path) -> None:
    dest = tmp_path / 'older'
    create_campaign_repo(dest, BLANK, {})
    (dest / 'brief.md').unlink()
    Repo(dest).git.commit('-qam', 'no brief')
    assert create_campaign_repo(dest, BLANK, {}) == [], 'it has piloop.toml: nothing of the template comes back'
    assert Repo(dest).head.commit.summary == 'no brief'


def test_example_templates_are_campaigns() -> None:
    for template in TEMPLATES.iterdir():
        cfg = load_campaign_config(template)
        assert cfg.name == template.name or template == BLANK, template


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
    """The keys of ``CampaignConfig``, at every depth, and of each default provider, that a ``piloop.toml`` text does not name.

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
            if (nested := model_in(field.annotation)) is not None and not issubclass(nested[0], RootModel):
                table = full + (r'\.[^.]+' if nested[1] else '')
                own = ''.join(t for n, t in sections.items() if re.fullmatch(table, n))
                missing += walk(nested[0], table, where + ('.<name>' if nested[1] else ''), own or text)
        return missing

    missing = walk(CampaignConfig, '', '', sections.get('', ''))
    for name, role in ROLES.builtins().items():
        table = f'roles.{name}'
        missing += walk(role, re.escape(table), table, sections.get(table, ''))
    for capability in declared():
        if capability.choice is None or capability.choice.scope is not Scope.CAMPAIGN:
            continue
        # a capability without a default provider names the keys of its built-in ones
        default = capability.choice.default
        models = [capability.get(default).model] if default is not None else capability.builtins().values()
        table = f'plugins.{capability.name}'
        for model in models:
            missing += walk(model, re.escape(table), table, sections.get(table, ''))
    return missing


def test_the_campaign_template_names_every_key_of_the_config() -> None:
    template = (BLANK / 'piloop.toml').read_text()
    assert template.startswith('# piloop campaign config. This file lists every key.')
    assert unnamed_keys(template) == [], 'the template says that it lists every key; add the missing ones to it'
    gaps = {'explore_every = 4': 'every = 4', 'plateau': 'p', 'bearer_token_env': 'x', ' tally;': ';'}
    without = template
    for old, new in gaps.items():
        without = without.replace(old, new)
    assert unnamed_keys(without) == [
        'harness.<name>.usage.tally',
        'mcp.<name>.bearer_token_env',
        'roles.implementer.explore_every',
        'plugins.stop_rule.plateau',
    ], 'the check finds a key missing from a table, from a keys: comment, and from a nested table without a header'


def test_init_commits_only_the_files_it_owns_never_another_agents_work(tmp_path: Path) -> None:
    """A finding on the campaign build: in a shared checkout, init committed another agent's uncommitted evaluator
    work as "piloop init: …". It commits only the template's files and the ones it wrote; the rest stays as it was."""
    dest = tmp_path / 'shared'
    create_campaign_repo(dest, BLANK, {})
    repo = Repo(dest)
    (dest / 'tools').mkdir()
    (dest / 'tools' / 'score.py').write_text('print(1)\n')
    (dest / 'staged.py').write_text('a = 1\n')
    repo.git.add('-A')
    repo.git.commit('-qm', 'tools')
    (dest / 'tools' / 'score.py').write_text('print(2)  # another agent, mid-edit\n')
    (dest / 'staged.py').write_text('a = 2\n')
    repo.git.add('staged.py')
    (dest / 'evaluate.py').write_text('print("METRIC score=3")\n')
    assert create_campaign_repo(dest, BLANK, {}) == []
    assert repo.head.commit.summary == 'piloop init: evaluate.py'
    assert repo.git.show('--name-only', '--format=', 'HEAD').splitlines() == ['evaluate.py']
    assert repo.git.diff('--name-only').splitlines() == ['tools/score.py'], 'still dirty, not committed'
    assert repo.git.diff('--cached', '--name-only').splitlines() == ['staged.py'], 'still staged, not committed'


def test_init_leaves_out_an_owned_file_that_the_repos_ignore_file_ignores(tmp_path: Path) -> None:
    dest = tmp_path / 'ignoring'
    dest.mkdir()
    (dest / '.gitignore').write_text('brief.md\n')
    (dest / 'main.py').write_text('x\n')
    repo = Repo.init(dest, initial_branch='main')
    repo.git.add('-A')
    repo.git.commit('-qm', 'first')
    assert create_campaign_repo(dest, BLANK, {}) == ['brief.md', 'evaluate.py', 'piloop.toml']
    assert repo.head.commit.summary == 'piloop init: evaluate.py, piloop.toml', 'git refuses to add it'


def test_init_commits_no_config_that_is_not_valid(tmp_path: Path) -> None:
    """A finding on the campaign build: init committed piloop.toml, then said it was not a valid config."""
    dest = tmp_path / 'invalid'
    create_campaign_repo(dest, BLANK, {})
    repo = Repo(dest)
    before = repo.head.commit.hexsha
    toml = dest / 'piloop.toml'
    toml.write_text(toml.read_text() + '\nnot_a_key = 1\n')
    with pytest.raises(ConfigError, match='not a valid piloop config'):
        create_campaign_repo(dest, BLANK, {})
    assert repo.head.commit.hexsha == before and repo.git.diff('--name-only').splitlines() == ['piloop.toml']


def test_init_takes_a_staged_rename_between_owned_files(tmp_path: Path) -> None:
    """``git status`` would report ``R  brief.md -> evaluate.py``: init reads paths, not rename records."""
    dest = tmp_path / 'renamed'
    create_campaign_repo(dest, BLANK, {})
    repo = Repo(dest)
    repo.git.rm('-q', 'evaluate.py')
    repo.git.commit('-qm', 'no evaluate yet')
    repo.git.mv('brief.md', 'evaluate.py')
    assert create_campaign_repo(dest, BLANK, {}) == []
    assert repo.head.commit.summary == 'piloop init: brief.md, evaluate.py'
    assert not repo.is_dirty()


def test_a_stored_config_loads_typed_and_one_that_no_longer_validates_fails_only_its_read(
    db: Database, campaign: Campaign
) -> None:
    with db.session() as s:
        assert s.get_one(Campaign, campaign.id).config == load_campaign_config(Path(campaign.path))
    store_config(db, campaign.id, {'name': 'demo'})
    with db.session() as s:
        row = s.scalars(select(Campaign)).one()
        assert row.name == 'demo', 'the row loads without its config'
        with pytest.raises(ValidationError, match='evaluate'):
            _ = row.config


def test_init_replaces_a_stored_config_that_no_longer_validates_and_a_stopped_campaign_stays_stopped(
    db: Database, campaign: Campaign
) -> None:
    store_config(db, campaign.id, {'name': 'demo'})
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        row.status, row.stopped_reason = CampaignStatus.STOPPED, 'target: score 3.0000 ≥ 3'
        cfg = sync(s, row)
        s.commit()
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        assert row.config == cfg
        assert (row.status, row.stopped_reason) == (CampaignStatus.STOPPED, 'target: score 3.0000 ≥ 3'), (
            'an old [stop] that no longer loads is not a changed one'
        )
