"""Everything that a plugin imports from piloop: each capability, its contract types and its built-in providers."""

# ruff: noqa: F401  # this module only re-exports: every import is unused here

from piloop.plugins.host import Capability, Tier, Parameters, Choice, Scope, Check, Severity
from piloop.plugins.host import MissingProvider, Provider, PythonSource, ContractBroken
from piloop.plugins.host import Live
from piloop.plugins.host import Entry, Named, Tables

from piloop.plugins.runtime.contract import RUNTIMES
from piloop.plugins.runtime.contract import AgentSpec, AgentStart, Handle
from piloop.plugins.runtime.contract import NotSteerable
from piloop.plugins.runtime.contract import AgentExit, ExitStatus
from piloop.plugins.runtime.contract import Runtime, RuntimeConfig
from piloop.plugins.runtime.subprocess import Subprocess, SubprocessRuntime

from piloop.plugins.cas.contract import CAS, Cas, CasProvider, BlobAddress, Writer
from piloop.plugins.cas.contract import Upkeep, TierUsage, Flushed, Integrity, integrity, UNBOUNDED
from piloop.plugins.cas.contract import Held, Evictable, LocalCas, LocalProvider
from piloop.plugins.cas.kit.conformance import check_cas
from piloop.plugins.cas.kit.objects import Objects, ObjectsCas, StreamCas, Dir
from piloop.plugins.cas.disk import Disk, DiskCas
from piloop.plugins.cas.cached import Cached, CachedCas
from piloop.plugins.cas.s3 import S3
from piloop.plugins.cas.gcs import Gcs
from piloop.plugins.cas.http import Http

from piloop.plugins.trigger.contract import TRIGGERS, Trigger, TriggerEntry, Pass, Fire
from piloop.plugins.trigger.contract import Backlog, TrackBacklog, Incident
from piloop.plugins.trigger.anomaly import AnomalyTrigger
from piloop.plugins.trigger.cron import CronTrigger
from piloop.plugins.trigger.seed import SeedTrigger
from piloop.plugins.trigger.low_water import LowWaterTrigger
from piloop.plugins.trigger.sql import SqlTrigger
from piloop.plugins.trigger.stalled import StalledTrigger
from piloop.plugins.trigger.stopped import StoppedTrigger

from piloop.plugins.role.contract import ROLES, ImplementerRole, SweepRole
from piloop.plugins.role.contract import Files
from piloop.plugins.role.contract import RoleConfig
from piloop.plugins.role.research import ResearchRole
from piloop.plugins.role.scout import ScoutRole
from piloop.plugins.role.watchdog import WatchdogRole

from piloop.plugins.verdict_rule.contract import VERDICT_RULES, VerdictRule, Case
from piloop.plugins.verdict_rule.contract import Criterion, Side
from piloop.plugins.metric import Direction
from piloop.plugins.verdict_rule.contract import Judgement, Outcome
from piloop.plugins.metric import number
from piloop.plugins.verdict_rule.bootstrap import Bootstrap

from piloop.plugins.stop_rule.contract import STOP_RULES, StopRule, StopCase
from piloop.plugins.stop_rule.contract import Progress, Fact
from piloop.plugins.stop_rule.contract import Go, GO, Hold, Stop
from piloop.plugins.stop_rule.limits import Limits

from piloop.db.models import EventModel
from piloop.plugins.executor.contract import EXECUTORS, Executor, ExecutorConfig, Host
from piloop.plugins.executor.contract import Results, Settled
from piloop.plugins.executor.contract import AgentJob, EvaluationJob, LeftJob
from piloop.plugins.executor.contract import AgentReport, EvaluationReport, Sample
from piloop.plugins.executor.contract import AgentFinished, AgentNotStarted
from piloop.plugins.executor.contract import AgentKey, EvaluationKey, JobKey
from piloop.plugins.executor.contract import key_of, Capacity, Prompt
from piloop.plugins.executor.contract import Source, Bar, RepoId, MissingBlobs, Workspace
from piloop.plugins.executor.kit.jobs import Control, Site, local_site, run_agent, remove
from piloop.plugins.executor.kit.jobs import run_evaluation
from piloop.plugins.executor.kit.leases import Leases, Timing, Grant, Answer
from piloop.plugins.executor.kit.leases import StaleLease
from piloop.proc import kill_group
from piloop.plugins.executor.workers import Workers, WorkersExecutor
from piloop.otel import Tracer, Span, ConsoleProcessor
from piloop.runtime.factory import RuntimeFactory
from piloop.mcp import McpServer
from piloop.usage import Usage, UsageSpec, UsageReader
from piloop.net import Route

from piloop.plugins.network.contract import NETWORKS, Network, NetworkHost
from piloop.plugins.network.contract import NetworkConfig, Address
from piloop.plugins.network.contract import Listener, NoCredential, Peer, Refused
from piloop.plugins.network.contract import Enrollment, NotEnrolled
from piloop.net.tokens import Caller, Tokens
from piloop.plugins.network.https import Https, HttpsNetwork

from piloop.ids import Revision
from piloop.plugins.vcs.contract import VCS, Vcs, Repository, Version, Conflicts, Transfer, Fetch
from piloop.plugins.vcs.contract import Keep, RunVersion, RebaseVersion, ReferenceVersion
from piloop.plugins.vcs.contract import VcsError, NotARepository, UnknownRevision
from piloop.plugins.vcs.contract import Part
from piloop.plugins.vcs.git import Git, GitRepository

from piloop.plugins.provisioner.contract import PROVISIONERS, ProvisionerConfig, ProvisionerHost, Provisioning
from piloop.plugins.provisioner.contract import Launch, Instance, InstanceState
from piloop.plugins.provisioner.contract import StartRefused
from piloop.plugins.provisioner.kit.conformance import check_provisioning
from piloop.plugins.provisioner.modal import Modal
