"""A worker in a process of its own, on the same database: ``worker_process.py <db> <home> <id> <renew> <grace> <toleration>``.

The fault-injection tests kill it (``kill -9``), stop it (``SIGSTOP``) and let the control plane in the test process
expire its leases. It applies its reports with core's operations (``CampaignResults``), as a transport would on the
server. It prints ``ready`` once it takes jobs.
"""

import sys
import time
from pathlib import Path

from piloop.actor import System
from piloop.agents.results import CampaignResults
from piloop.db.session import Database
from piloop.otel import Tracer
from piloop.plugins.executor.kit.leases import Leases, Timing
from piloop.plugins.executor.workers.worker import Worker


def main() -> None:
    url, home, worker_id = sys.argv[1], Path(sys.argv[2]), sys.argv[3]
    timing = Timing(*(float(a) for a in sys.argv[4:7]))
    db = Database(url, actor=System.DAEMON)
    results = CampaignResults(db, Tracer(None), home / 'units', lambda: False)
    worker = Worker(Leases(db, results, timing), Tracer(None), worker_id=worker_id)
    worker.start()
    print('ready', flush=True)
    while True:
        worker.kick()  # nothing in this process enqueues: look for jobs on every heartbeat
        time.sleep(timing.renew)


if __name__ == '__main__':
    main()
