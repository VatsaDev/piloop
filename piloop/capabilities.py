"""Everything that a plugin imports from piloop: each capability, its contract types and its built-in providers."""

from piloop.plugins.host import Capability as Capability, PureCapability as PureCapability, Tier as Tier
from piloop.plugins.host import MissingProvider as MissingProvider, Provider as Provider
from piloop.plugins.host import Selection as Selection, Source as Source

from piloop.plugins.contracts.runtime import RUNTIMES as RUNTIMES
from piloop.runtime import AgentSpec as AgentSpec, AgentStatus as AgentStatus, Check as Check, Handle as Handle
from piloop.runtime import NotAttachable as NotAttachable, NotSteerable as NotSteerable
from piloop.runtime import AgentExit as AgentExit
from piloop.runtime import Runtime as Runtime, RuntimeFailure as RuntimeFailure, StartFailed as StartFailed
from piloop.harnesses import Invocation as Invocation, QueueKey as QueueKey
from piloop.settings import runtime_settings as runtime_settings
from piloop.runtime.herdr import HerdrRuntime as HerdrRuntime
from piloop.runtime.subprocess import SubprocessRuntime as SubprocessRuntime

from piloop.plugins.contracts.store import STORAGE_BACKENDS as STORAGE_BACKENDS
from piloop.store import StorageBackend as StorageBackend, StorageBackendConfig as StorageBackendConfig
from piloop.store.contract import check_storage_backend as check_storage_backend
from piloop.store.gcs import GcsConfig as GcsConfig
from piloop.store.s3 import S3Config as S3Config

from piloop.triggers import TRIGGERS as TRIGGERS, Trigger as Trigger, Tick as Tick, Fire as Fire
from piloop.triggers import Backlog as Backlog, TrackBacklog as TrackBacklog, Incident as Incident
from piloop.triggers import AnomalyTrigger as AnomalyTrigger, CronTrigger as CronTrigger, SeedTrigger as SeedTrigger
from piloop.triggers import LowWaterTrigger as LowWaterTrigger, SqlTrigger as SqlTrigger
from piloop.triggers import StoppedTrigger as StoppedTrigger

from piloop.config import ROLES as ROLES, ImplementerRole as ImplementerRole, SweepRole as SweepRole
from piloop.config import Files as Files, Workdir as Workdir
from piloop.plugins.builtin.roles import IMPLEMENTER as IMPLEMENTER, RESEARCH as RESEARCH
from piloop.plugins.builtin.roles import SCOUT as SCOUT, WATCHDOG as WATCHDOG

from piloop.plugins.contracts.verdict import VERDICT_RULES as VERDICT_RULES, VerdictRule as VerdictRule, Case as Case
from piloop.plugins.contracts.verdict import Criterion as Criterion, Direction as Direction, Side as Side
from piloop.plugins.contracts.verdict import Judgement as Judgement, Outcome as Outcome, number as number
from piloop.plugins.builtin.verdict import BOOTSTRAP as BOOTSTRAP

from piloop.plugins.contracts.stop import STOP_RULES as STOP_RULES, StopRule as StopRule, StopCase as StopCase
from piloop.plugins.contracts.stop import Limits as Limits, Progress as Progress, Fact as Fact
from piloop.plugins.contracts.stop import Go as Go, GO as GO, Hold as Hold, Stop as Stop
from piloop.plugins.builtin.stop import LIMITS as LIMITS

from piloop.plugins.contracts.executor import EXECUTORS as EXECUTORS, Executor as Executor, Host as Host
from piloop.plugins.contracts.executor import Results as Results, Settled as Settled, NoParameters as NoParameters
from piloop.plugins.contracts.executor import AgentJob as AgentJob, EvaluationJob as EvaluationJob, LeftJob as LeftJob
from piloop.plugins.contracts.executor import AgentReport as AgentReport, EvaluationReport as EvaluationReport
from piloop.plugins.contracts.executor import AgentKey as AgentKey, EvaluationKey as EvaluationKey, JobKey as JobKey
from piloop.plugins.contracts.executor import key_of as key_of
from piloop.plugins.contracts.executor import GitTree as GitTree, Checkout as Checkout, Commits as Commits
from piloop.plugins.kit.executor.jobs import Control as Control, run_agent as run_agent, remove as remove
from piloop.plugins.kit.executor.jobs import run_evaluation as run_evaluation
from piloop.plugins.kit.executor.leases import Leases as Leases, Timing as Timing, Grant as Grant, Answer as Answer
from piloop.plugins.kit.executor.leases import StaleLease as StaleLease
from piloop.proc import kill_group as kill_group
from piloop.plugins.builtin.executor.workers import WorkersExecutor as WorkersExecutor
from piloop.otel import Tracer as Tracer, Span as Span, ConsoleProcessor as ConsoleProcessor
from piloop.runtime.factory import RuntimeFactory as RuntimeFactory
from piloop.mcp import McpServer as McpServer
from piloop.usage import Usage as Usage, UsageSpec as UsageSpec, UsageReader as UsageReader
