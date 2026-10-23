"""A worker's capacity: the gates of its pool on its host sample, and the slots that the executor sums."""

from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from piloop.db.models import AgentSession, Campaign, LeaseKind, LeaseWorker, NoLease, utcnow
from piloop.db.session import Database
from piloop.ids import RESEARCH
from piloop.plugins.executor.contract import Capacity
from piloop.plugins.executor.kit.capacity import GB, HostSample, Pool, admits, sample
from piloop.plugins.executor.kit.leases import SERVE, Grant, Leases
from piloop.plugins.executor.workers import Workers
from tests.test_lease import TIMING, Recorder, agent_job, evaluation_job, queue

HOST = HostSample(at=utcnow(), cpus=4, load1=2.0, memory_available=8 * GB, disk_free=100 * GB)
IDLE = Capacity(0, 0)


@pytest.mark.parametrize(
    ('pool', 'host', 'running', 'kind', 'reason'),
    [
        (Pool(), HOST, IDLE, LeaseKind.AGENT, None),
        (Pool(), HOST, Capacity(50, 0), LeaseKind.AGENT, None),
        (Pool(agents=2), HOST, Capacity(2, 0), LeaseKind.AGENT, NoLease.SLOTS),
        (Pool(agents=2), HOST, Capacity(1, 9), LeaseKind.AGENT, None),
        (Pool(agents=0), HOST, IDLE, LeaseKind.AGENT, NoLease.SLOTS),
        (Pool(), HOST, Capacity(9, 1), LeaseKind.EVALUATION, None),
        (Pool(), HOST, Capacity(0, 2), LeaseKind.EVALUATION, NoLease.SLOTS),
        (Pool(evaluations=0), HOST, IDLE, LeaseKind.EVALUATION, NoLease.SLOTS),
        (Pool(min_memory_gb=8), HOST, IDLE, LeaseKind.AGENT, None),
        (Pool(min_memory_gb=8.5), HOST, IDLE, LeaseKind.AGENT, NoLease.MEMORY),
        (Pool(max_load=0.5), HOST, IDLE, LeaseKind.EVALUATION, None),
        (Pool(max_load=0.4), HOST, IDLE, LeaseKind.EVALUATION, NoLease.LOAD),
        (Pool(agents=0, min_memory_gb=9, max_load=0.1), HOST, IDLE, LeaseKind.AGENT, NoLease.MEMORY),
        (Pool(agents=0, max_load=0.1), HOST, IDLE, LeaseKind.AGENT, NoLease.LOAD),
    ],
)
def test_admits_gives_the_first_gate_that_refuses(
    pool: Pool, host: HostSample, running: Capacity, kind: LeaseKind, reason: NoLease | None
) -> None:
    """Memory, then load (per CPU), then the slots of the job's kind; None takes the job."""
    assert admits(pool, host, running, kind) is reason


def test_a_sample_reads_this_host(tmp_path: Path) -> None:
    host = sample(tmp_path)
    assert host.cpus >= 1 and host.memory_available > 0 and host.disk_free > 0 and host.load1 >= 0


def test_pools_keep_serve_with_its_defaults_unless_the_table_names_it() -> None:
    assert Workers().pools == {SERVE: Pool()}
    gpu = Workers.model_validate({'pools': {'gpu': {'evaluations': 1}}}).pools
    assert gpu == {SERVE: Pool(), 'gpu': Pool(evaluations=1)}
    assert Workers.model_validate({'pools': {'serve': {'agents': 2}}}).pools == {SERVE: Pool(agents=2)}
    with pytest.raises(ValueError, match='Extra inputs'):
        Workers.model_validate({'evaluations': 2})


def test_a_worker_takes_no_job_over_its_slots_or_under_pressure_and_its_row_says_why(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    pools = {'small': Pool(agents=1, evaluations=1, min_memory_gb=4)}
    leases = Leases(db, Recorder(db), TIMING, pools)
    with pytest.raises(ValueError, match="no pool 'big'"):
        leases.register('w1', 'big', '0.2.0', ['process'], HOST)
    leases.register('w1', 'small', '0.2.0', ['process'], HOST)
    low = HOST.model_copy(update={'memory_available': 3 * GB})
    assert leases.acquire('w1', uuid4().hex, HOST) is NoLease.NOTHING
    assert leases.acquire('w1', uuid4().hex, low) is NoLease.MEMORY, 'pressure first, also with nothing queued'
    for _ in range(2):
        queue(db, agent_job(db, campaign, tmp_path / 'a'))
    queue(db, evaluation_job(db, campaign, tmp_path))
    assert isinstance(leases.acquire('w1', uuid4().hex, HOST), Grant)
    assert isinstance(grant := leases.acquire('w1', uuid4().hex, HOST), Grant) and grant.job.kind == 'evaluation', (
        'agents full'
    )
    assert leases.acquire('w1', uuid4().hex, HOST) is NoLease.SLOTS
    with db.session() as s:
        assert s.get_one(LeaseWorker, 'w1').no_lease == NoLease.SLOTS
    assert leases.acquire('w1', uuid4().hex, low) is NoLease.MEMORY


def test_a_worker_whose_slots_are_full_says_slots_while_core_holds_back_a_job_for_it(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    """Core submits a job only to a free slot, so a full worker sees no queued lease: the jobs wait in core."""
    leases = Leases(db, Recorder(db), TIMING, {'full': Pool(agents=0, evaluations=0)})
    leases.register('w1', 'full', '0.2.0', ['process'], HOST)
    leases.register('w2', 'full', '0.2.0', ['gpu'], HOST)
    assert leases.acquire('w1', uuid4().hex, HOST) is NoLease.NOTHING
    evaluation_job(db, campaign, tmp_path)  # waits in the evaluation queue, not submitted
    assert leases.acquire('w1', uuid4().hex, HOST) is NoLease.SLOTS
    assert leases.acquire('w2', uuid4().hex, HOST) is NoLease.NOTHING, 'w2 has not the runner of the waiting evaluation'
    with db.session() as s:
        s.add(AgentSession(kind=RESEARCH, campaign_id=campaign.id))  # a requested agent, not launched
        s.commit()
    assert leases.acquire('w2', uuid4().hex, HOST) is NoLease.SLOTS


def test_capacity_sums_the_slots_of_the_reachable_workers_that_are_not_under_pressure(db: Database) -> None:
    pools = {SERVE: Pool(agents=2), 'gpu': Pool(agents=0, evaluations=1, max_load=1.0)}
    leases = Leases(db, Recorder(db), TIMING, pools)
    assert leases.capacity() == Capacity(0, 0), 'no worker yet'
    leases.register(SERVE, SERVE, '0.2.0', ['process'], HOST)
    assert leases.capacity() == Capacity(2, 2)
    for name in ('gpu1', 'gpu2'):
        leases.register(name, 'gpu', '0.2.0', ['process'], HOST)
    assert leases.capacity() == Capacity(2, 4)
    leases.heartbeat('gpu2', {}, HOST.model_copy(update={'load1': 4.5}))
    assert leases.capacity() == Capacity(2, 3), 'under pressure: zero'
    with db.session() as s:
        s.get_one(LeaseWorker, 'gpu1').seen_at = utcnow() - timedelta(seconds=TIMING.grace + 1)
        s.commit()
    assert leases.capacity() == Capacity(2, 2), 'unreachable: zero'
    unbounded = Leases(db, Recorder(db), TIMING, {SERVE: Pool(), 'gpu': Pool()})
    assert unbounded.capacity() == Capacity(None, 4)
    without_gpu = Leases(db, Recorder(db), TIMING, {SERVE: Pool()})
    assert without_gpu.capacity() == Capacity(None, 2), 'a pool that the configuration no longer has counts zero'


def test_capacity_is_the_free_slots_and_a_worker_under_pressure_starves_no_other(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    """Two workers of two agent slots. The jobs of one take nothing from the other's slots, also under pressure."""
    leases = Leases(db, Recorder(db), TIMING, {'pair': Pool(agents=2, min_memory_gb=4)})
    for name in ('w1', 'w2'):
        leases.register(name, 'pair', '0.2.0', ['process'], HOST)
    assert leases.capacity().agents == 4
    for _ in range(2):
        queue(db, agent_job(db, campaign, tmp_path / 'a'))
    assert leases.capacity().agents == 2, 'a queued lease takes a slot'
    for _ in range(2):
        assert isinstance(leases.acquire('w1', uuid4().hex, HOST), Grant)
    assert leases.capacity().agents == 2, 'w1 has no slot left, w2 has two'
    low = HOST.model_copy(update={'memory_available': 1 * GB})
    leases.heartbeat('w1', {}, low)
    assert leases.capacity().agents == 2, 'w1 counts zero, and its two jobs take nothing from w2'
    for _ in range(2):
        queue(db, agent_job(db, campaign, tmp_path / 'a'))
    assert leases.capacity().agents == 0
    assert leases.acquire('w1', uuid4().hex, low) is NoLease.MEMORY
    for _ in range(2):
        assert isinstance(leases.acquire('w2', uuid4().hex, HOST), Grant), 'the healthy worker still gets jobs'
    assert leases.capacity().agents == 0
