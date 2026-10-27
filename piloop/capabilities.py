"""Everything that a plugin imports from piloop: each capability, its contract types and its built-in providers."""

# ruff: noqa: F401  # this module only re-exports: every import is unused here

from piloop.plugins.host import Capability, Tier, Parameters, One, Many, Use, Pick, Instances, Scope, Check, Severity
from piloop.plugins.host import MissingProvider, Provider, PythonSource, ContractBroken
from piloop.plugins.host import Live
from piloop.plugins.host import Entry, Ref
from piloop.plugins.contribution import Contribution, Contributes, Lifecycle, Leaving
from piloop.daemon.loop import Consumer, Phase, Turn

from piloop.plugins.runtime.contract import RUNTIMES
from piloop.plugins.runtime.contract import AgentSpec, AgentStart, Handle
from piloop.plugins.runtime.contract import NotSteerable
from piloop.plugins.runtime.contract import AgentExit, ExitStatus
from piloop.plugins.runtime.contract import Runtime, RuntimeConfig
from piloop.plugins.runtime.subprocess import Subprocess, SubprocessRuntime

from piloop.plugins.harness.contract import HARNESSES, Harness, SecretRef, UsageReader
from piloop.settings import Settings, StorageSettings, TracingSettings, EvaluationSettings
from piloop.settings import DaemonSettings, AgentSettings
from piloop.plugins.harness.contract import McpLaunch, McpArgs, McpError, McpRenderer
from piloop.plugins.harness.kit.jsonl import JsonlSessions, Counts, Record, Tally, Said
from piloop.plugins.harness.pi import Pi
from piloop.plugins.harness.claude import Claude
from piloop.plugins.harness.codex import Codex

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

from piloop.plugins.trigger.contract import TRIGGERS, Trigger, TriggerEntry, Pass, Fire, Steer, Quiet
from piloop.plugins.trigger.contract import Backlog, TrackBacklog
from piloop.plugins.trigger.run_failed import RunFailed
from piloop.plugins.trigger.dead_at_startup import DeadAtStartup
from piloop.plugins.trigger.leaking_funnel import LeakingFunnel
from piloop.plugins.trigger.cron import CronTrigger
from piloop.plugins.trigger.seed import SeedTrigger
from piloop.plugins.trigger.low_water import LowWaterTrigger
from piloop.plugins.trigger.sql import SqlTrigger
from piloop.plugins.trigger.stalled import StalledTrigger
from piloop.plugins.trigger.stopped import StoppedTrigger
from piloop.plugins.trigger.news import NewsTrigger

from piloop.plugins.section.contract import SECTIONS, Section, SectionEntry, Rendering, Surface
from piloop.api.views import Rendered, Block, Text, Lines, Figures, Figure, Table, Value
from piloop.plugins.section.headline import Headline
from piloop.plugins.section.goal import Goal
from piloop.plugins.section.tracks import Tracks
from piloop.plugins.section.trajectory import Trajectory
from piloop.plugins.section.runs import Runs
from piloop.plugins.section.queue import Queue
from piloop.plugins.section.agents import Agents
from piloop.plugins.section.evaluations import Evaluations
from piloop.plugins.section.funnel import RunFunnel
from piloop.plugins.section.cost import Cost
from piloop.plugins.section.candidates import Candidates
from piloop.plugins.section.idle_queue import IdleQueue
from piloop.plugins.section.same_as_parent import SameAsParent
from piloop.plugins.section.reported import Reported
from piloop.plugins.section.inbox import Inbox
from piloop.plugins.section.events import Events
from piloop.plugins.section.tools import Tools
from piloop.plugins.section.incidents import Incidents
from piloop.plugins.section.pack import Pack
from piloop.plugins.section.history import History
from piloop.plugins.section.lessons import Lessons

from piloop.plugins.role.contract import ROLES, ImplementerRole, SweepRole
from piloop.plugins.role.contract import Files, Continuity
from piloop.plugins.role.contract import RoleConfig, Sweep
from piloop.plugins.role.research import ResearchRole
from piloop.plugins.role.scout import ScoutRole
from piloop.plugins.role.watchdog import WatchdogRole
from piloop.plugins.role.orchestrator import OrchestratorRole

from piloop.plugins.verdict_rule.contract import VERDICT_RULES, VerdictRule, Case
from piloop.plugins.verdict_rule.contract import Criterion, Ratio, Side, Stopped
from piloop.plugins.metric import Direction
from piloop.plugins.verdict_rule.contract import Judgement, Outcome
from piloop.plugins.verdict_rule.contract import Node, Trunk, Branch, Imported
from piloop.plugins.verdict_rule.contract import Moved, Rebase, Seed, Leave
from piloop.plugins.metric import number
from piloop.plugins.verdict_rule.bootstrap import Bootstrap

from piloop.plugins.stop_rule.contract import STOP_RULES, StopRule, StopCase, Shape, WhileHeld
from piloop.plugins.fact_source.contract import FACT_SOURCES, FactSource, FactSourceEntry, FactReadError
from piloop.plugins.fact_source.kit.readings import Readings
from piloop.plugins.fact_source.facts import Fact, Facts, FactValue, Funnel
from piloop.plugins.fact_source.facts import HEAD, RUNS, UNSETTLED, HOURS, TOKENS, WITHOUT_KEEP, KEEPS, EVALUATIONS
from piloop.plugins.fact_source.facts import METRIC_SUMS, ATTEMPTS, FUNNEL, CAMPAIGN, OPTIMIZATION, reading, since_keep
from piloop.plugins.fact_source.facts import per_keep, per_evaluation, worker_spend, per_run
from piloop.plugins.fact_source.campaign import CampaignFacts
from piloop.plugins.fact_source.optimization import OptimizationFacts
from piloop.plugins.fact_source.command import Command
from piloop.plugins.stop_rule.contract import Go, GO, Hold, Stop, Escalate
from piloop.plugins.stop_rule.limits import Limits

from piloop.db.models import EventModel, Base
from piloop.db.schema import Schema

from piloop.plugins.campaign_type.contract import (
    CAMPAIGN_TYPES,
    CampaignType,
    CannotLaunch,
    Planner,
    Steering,
    Take,
    TypeHost,
)
from piloop.plugins.campaign_type.optimization import Optimization

from piloop.plugins.memory.contract import MEMORY, MemoryProvider, Synced
from piloop.plugins.memory.local import LocalMemory

from piloop.plugins.archive.contract import ARCHIVES, ArchiveConfig, ArchiveHost, Archiver
from piloop.plugins.archive.contract import Restored, Summary, CampaignArchived
from piloop.ids import TargetKind, TargetRef
from piloop.store.tree import Materialised, Copy
from piloop.plugins.archive.local import LocalArchive
from piloop.plugins.executor.contract import EXECUTORS, Executor, ExecutorConfig, Host
from piloop.plugins.executor.contract import Results, Settled
from piloop.plugins.executor.contract import AgentJob, EvaluationJob, LeftJob, Resume
from piloop.plugins.executor.contract import AgentReport, EvaluationReport, Sample
from piloop.config import Rung, Tolerance
from piloop.scoring.ladder import Gate
from piloop.plugins.executor.contract import AgentFinished, AgentNotStarted, HarnessSession
from piloop.plugins.executor.contract import AgentKey, EvaluationKey, JobKey
from piloop.plugins.executor.contract import key_of, Capacity, Prompt
from piloop.plugins.executor.contract import JobKind, JobSecrets, job_secrets
from piloop.plugins.executor.contract import Source, Bar, RepoId, MissingBlobs, Workspace
from piloop.plugins.executor.kit.jobs import Control, Site, local_site, run_agent, remove
from piloop.plugins.executor.kit.jobs import run_evaluation
from piloop.plugins.executor.kit.leases import Leases, Timing, Grant, Answer
from piloop.plugins.executor.kit.leases import StaleLease
from piloop.proc import Channel, kill_group
from piloop.plugins.executor.workers import Workers, WorkersExecutor
from piloop.otel import Tracer, Span, ConsoleProcessor
from piloop.runtime.factory import RuntimeFactory
from piloop.mcp import McpServer
from piloop.usage import Usage, UsageError
from piloop.net import Route
from piloop.net.server import executor_router

from piloop.plugins.network.contract import NETWORKS, Network, NetworkHost
from piloop.plugins.network.contract import NetworkConfig, Address
from piloop.plugins.network.contract import Listener, NoCredential, Peer, Refused
from piloop.plugins.network.contract import Enrollment, NotEnrolled
from piloop.net.tokens import Caller, Tokens
from piloop.api.contract import Access
from piloop.daemon.api import REFUSALS_DOCUMENTED, Acting, Addressed, Writing, actor_access, reader
from piloop.actor import Agent, Person, Service, CallerKind, Orchestrating, System
from piloop.plugins.network.https import Https, HttpsNetwork
from piloop.plugins.network.proxy import Proxy

from piloop.ids import Revision
from piloop.plugins.vcs.contract import VCS, Vcs, Repository, Version, Conflicts, Transfer, Fetch, Captured, Unsaved
from piloop.plugins.vcs.contract import Keep, RunVersion, RebaseVersion, ReferenceVersion
from piloop.plugins.vcs.contract import VcsError, NotARepository, UnknownRevision, CloneFailed
from piloop.plugins.vcs.contract import Part
from piloop.plugins.vcs.git import Git, GitRepository

from piloop.plugins.provisioner.contract import PROVISIONERS, ProvisionerConfig, ProvisionerHost, Provisioning
from piloop.plugins.provisioner.contract import Launch, Instance, InstanceState
from piloop.plugins.provisioner.contract import StartRefused
from piloop.plugins.provisioner.kit.conformance import check_provisioning
from piloop.plugins.provisioner.modal import Modal

from piloop.plugins.secrets.contract import SECRETS, SecretsProvider, SecretKey, Owner, HomeSecrets
from piloop.plugins.secrets.local import LocalSecrets
