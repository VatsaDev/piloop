"""Live limits: a ``Live`` key of a provider's parameters changes while the daemon runs, from its next tick."""

from pathlib import Path
from typing import Annotated

import pytest
from pydantic import BaseModel
from pytest_mock import MockerFixture
from sqlalchemy import select

from piloop.daemon.run_controller import RunController
from piloop.db.models import Campaign, Limit
from piloop.db.session import Database
from piloop.otel import Tracer
from piloop.plugins import limits
from piloop.plugins.executor.contract import EXECUTORS, Capacity
from piloop.plugins.executor.kit.capacity import NoLease, Pool
from piloop.plugins.executor.kit.client import Local
from piloop.plugins.executor.kit.leases import Grant, Leases
from piloop.plugins.executor.workers import WorkersParameters
from piloop.plugins.host import Live, PluginTable
from tests.conftest import ScriptRuntime, factory, serving, workers_of
from tests.test_lease import HOST, MEASURED, TIMING, Recorder, evaluation_job, join, queue

KEY = 'executor.pools.serve.evaluations'
GPU = {'executor': PluginTable.model_validate({'provider': 'workers', 'pools': {'gpu': {'evaluations': 4}}})}


def test_only_a_live_key_that_there_is_takes_a_limit_and_only_a_value_that_its_parameters_accept(
    db: Database,
) -> None:
    with db.session() as s:
        with pytest.raises(limits.Refused, match='executor.renew_seconds is not a live key; the live keys are'):
            limits.set_limit(s, {}, 'executor.renew_seconds', 5, reason='r', actor='human')
        with pytest.raises(limits.Refused, match='executor.pools.gpu.evaluations is not a live key'):
            limits.set_limit(s, {}, 'executor.pools.gpu.evaluations', 1, reason='a limit adds no pool', actor='human')
        with pytest.raises(limits.Refused, match='greater than or equal to 0'):
            limits.set_limit(s, {}, KEY, -1, reason='r', actor='human')
        with pytest.raises(limits.Refused, match=r'no provider of the home that has parameters is \[plugins.nope\]'):
            limits.set_limit(s, {}, 'nope.x', 1, reason='r', actor='human')
        limits.set_limit(s, GPU, 'executor.pools.gpu.evaluations', 1, reason='the GPU is shared', actor='human')
        limits.set_limit(s, GPU, KEY, 0, reason='nothing on the server', actor='orchestrator')
        limits.set_limit(s, GPU, KEY, 1, reason='one on the server', actor='human')
        s.commit()
        assert [(row.key, row.value, row.actor) for row in s.scalars(select(Limit).order_by(Limit.key))] == [
            ('executor.pools.gpu.evaluations', 1, 'human'),
            (KEY, 1, 'human'),
        ], 'set again, a limit has its last value'
        sources = {k: (v, o, row.reason if row else None) for k, v, o, row in limits.sources(s, GPU)}
        assert sources[KEY] == (1, limits.Origin.LIMIT, 'one on the server')
        assert sources['executor.pools.gpu.agents'] == (None, limits.Origin.DEFAULT, None)
        assert limits.unset_limit(s, 'executor.pools.gpu.evaluations') and not limits.unset_limit(s, 'nope')
        s.flush()
        assert {k: o for k, _, o, _ in limits.sources(s, GPU)}['executor.pools.gpu.evaluations'] is limits.Origin.CONFIG


def test_the_daemon_applies_a_changed_limit_on_its_next_tick_and_keeps_its_parameters_when_one_is_refused(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime)))
    assert ex.executor.capacity() == Capacity(None, 2)
    with db.session() as s:
        limits.set_limit(s, {}, KEY, 1, reason='the GPU is busy', actor='human')
        s.commit()
    assert ex.executor.capacity() == Capacity(None, 2), 'until the next tick'
    ex.configure()
    assert ex.executor.capacity() == Capacity(None, 1)
    with db.session() as s:
        s.get_one(Limit, KEY).value = 'many'  # a limit that the parameters no longer accept
        s.commit()
    ex.configure()
    assert ex.executor.capacity() == Capacity(None, 1), 'the last parameters hold'
    assert workers_of(ex).leases.pools['serve'] == Pool(evaluations=1)


def test_with_two_evaluations_running_a_lowered_limit_starts_no_third_until_none_runs(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    """#108's second capacity gate: ``evaluations`` lowered from 2 to 1 while 2 run."""
    leases = Leases(db, Recorder(db), TIMING, {'serve': Pool(evaluations=2)})
    worker = Local(leases, 'serve')
    join(leases, 'serve', ['process'], pool='serve')
    job = evaluation_job(db, campaign, tmp_path)
    for n in range(3):
        queue(db, job.model_copy(update={'evaluation': job.evaluation + n}))
    first, second = worker.acquire(HOST), worker.acquire(HOST)
    assert isinstance(first, Grant) and isinstance(second, Grant) and worker.acquire(HOST) is NoLease.SLOTS
    applied = limits.applied(None, [Limit(key=KEY, value=1)], WorkersParameters, 'executor')
    leases.pools = applied.pools  # what ``WorkersExecutor.configure`` does on the next tick
    worker.report(first.lease, 'r1', MEASURED)
    assert worker.acquire(HOST) is NoLease.SLOTS, 'one runs, and one is the limit'
    worker.report(second.lease, 'r2', MEASURED)
    assert isinstance(worker.acquire(HOST), Grant)


class Nested(BaseModel):
    flags: dict[str, int] = {}
    pools: dict[str, Pool] = {}
    serve: Pool = Pool()
    window: Annotated[int, Live] = 3


def test_the_live_keys_are_found_in_models_and_in_dicts_of_models_by_their_path() -> None:
    nested = Nested(flags={'a': 1}, pools={'gpu': Pool(agents=1)})
    keys = [key for key, _ in limits.live(nested, 'x')]
    assert keys == [
        *(f'x.pools.gpu.{k}' for k in ('agents', 'evaluations', 'min_memory_gb', 'max_load')),
        *(f'x.serve.{k}' for k in ('agents', 'evaluations', 'min_memory_gb', 'max_load')),
        'x.window',
    ]
    other = Limit(key='store.window', value=9)
    assert limits.applied(None, [other, Limit(key='x.window', value=5)], Nested, 'x').window == 5, 'only its own'


def test_a_provider_without_parameters_takes_no_limit(mocker: MockerFixture) -> None:
    mocker.patch.object(EXECUTORS, 'get', return_value=object)
    assert limits.selected({}) == {}
