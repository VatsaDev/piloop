"""A worker in a process of its own, on the same database: ``worker_process.py <db> <home> <id> <renew> <grace> <toleration>``.

The fault-injection tests kill it (``kill -9``), stop it (``SIGSTOP``) and let the control plane in the test process
expire its leases. It applies its reports with core's operations (``CampaignResults``), as a transport would on the
server. It prints ``ready`` once it takes jobs.
"""

import sys
import threading
from functools import partial
from pathlib import Path

from piloop.actor import System
from piloop.agents.results import CampaignResults
from piloop.campaign.campaigns import checkout_of, record_of
from piloop.db.session import Database
from piloop.otel import Tracer
from piloop.plugins.cas.contract import CAS
from piloop.plugins.executor.kit.client import Local
from piloop.plugins.executor.kit.jobs import local_site
from piloop.plugins.executor.kit.leases import Leases, Timing
from piloop.plugins.executor.workers.worker import Worker


def main() -> None:
    url, home, worker_id = sys.argv[1], Path(sys.argv[2]), sys.argv[3]
    timing = Timing(*(float(a) for a in sys.argv[4:7]))
    db = Database(url, actor=System.DAEMON)
    cas = CAS.selected({}).open(home)
    results = CampaignResults(db, Tracer(None), home, cas, lambda: False)
    site = local_site(home, cas, partial(checkout_of, db), partial(record_of, db), reserve=0)
    worker = Worker(Local(Leases(db, results, timing), worker_id), Tracer(None), site)
    worker.start()
    print('ready', flush=True)
    threading.Event().wait()  # the worker's loop looks for jobs on every heartbeat


if __name__ == '__main__':
    main()
