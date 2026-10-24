"""The lease routes of ``piloop serve --listen`` in a process of their own: ``server_process.py <db> <home> <parameters>``.

``<parameters>`` is the JSON of ``[plugins.executor]`` for ``workers``. The remote fault tests kill this process
(``kill -9``) and start it again, while core runs in the test process. It renews every live lease first, as a daemon
that starts does, then serves on a free loopback port. It applies reports with core's operations (``CampaignResults``),
and prints ``ready <port>`` once it listens. Its CAS is the ``disk`` under ``<home>``, which the worker shares.
"""

import sys
import threading
from functools import partial
from pathlib import Path

from piloop.actor import System
from piloop.agents.results import CampaignResults
from piloop.campaign.campaigns import checkout_of, record_of
from piloop.db.session import Database
from piloop.net.server import Server, app
from piloop.net.tokens import Tokens
from piloop.otel import Tracer
from piloop.plugins.cas.contract import CAS
from piloop.plugins.executor.kit.jobs import send_of_record
from piloop.plugins.executor.kit.leases import Leases, Timing
from piloop.plugins.executor.kit.relay import Runner
from piloop.plugins.executor.kit.remote import lease_routes
from piloop.plugins.executor.workers import Workers
from piloop.plugins.network.contract import NetworkHost
from piloop.plugins.network.https import Https


def main() -> None:
    url, home = sys.argv[1], Path(sys.argv[2])
    p = Workers.model_validate_json(sys.argv[3])
    db = Database(url, actor=System.DAEMON)
    cas = CAS.selected({}).open(home)
    results = CampaignResults(db, Tracer(None), home, cas, lambda: False)
    leases = Leases(db, results, Timing(p.renew_seconds, p.grace_seconds, p.toleration_seconds), p.pools)
    leases.renew_all()
    network = Https.model_validate({'listen': '127.0.0.1:0'}).open(NetworkHost(Tokens(db)))
    listener = network.listener()
    assert listener is not None
    bundle = partial(send_of_record, cas, partial(record_of, db))
    cli = Runner(db, home, partial(checkout_of, db))
    Server(app(network, lease_routes(leases, db.changed, cas, bundle, cli)), listener).start()
    print(f'ready {listener.socket.getsockname()[1]}', flush=True)
    threading.Event().wait()


if __name__ == '__main__':
    main()
