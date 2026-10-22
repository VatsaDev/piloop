"""Everything that a plugin imports from piloop: each capability, its contract types and its built-in providers."""

# ruff: noqa: F401  # this module only re-exports: every import is unused here

from piloop.plugins.host import Capability, PureCapability, Tier
from piloop.plugins.host import MissingProvider, Provider
from piloop.plugins.host import Selection, Source

from piloop.plugins.runtime.contract import RUNTIMES
from piloop.plugins.runtime.contract import AgentSpec, AgentStatus, Check, Handle
from piloop.plugins.runtime.contract import NotAttachable, NotSteerable
from piloop.plugins.runtime.contract import AgentExit, ExitStatus
from piloop.plugins.runtime.contract import Runtime, RuntimeFailure, StartFailed
from piloop.harnesses import Invocation, QueueKey
from piloop.settings import runtime_settings
from piloop.plugins.runtime.herdr import HerdrRuntime
from piloop.plugins.runtime.subprocess import SubprocessRuntime

from piloop.plugins.storage_backend.contract import STORAGE_BACKENDS
from piloop.plugins.storage_backend.contract import StorageBackend, StorageBackendConfig
from piloop.plugins.storage_backend.kit.conformance import check_storage_backend
from piloop.plugins.storage_backend.gcs import GcsConfig
from piloop.plugins.storage_backend.s3 import S3Config

from piloop.plugins.trigger.contract import TRIGGERS, Trigger, Tick, Fire
from piloop.plugins.trigger.contract import Backlog, TrackBacklog, Incident
from piloop.plugins.trigger.anomaly import AnomalyTrigger
from piloop.plugins.trigger.cron import CronTrigger
from piloop.plugins.trigger.seed import SeedTrigger
from piloop.plugins.trigger.low_water import LowWaterTrigger
from piloop.plugins.trigger.sql import SqlTrigger
from piloop.plugins.trigger.stalled import StalledTrigger
from piloop.plugins.trigger.stopped import StoppedTrigger

from piloop.plugins.role.contract import ROLES, ImplementerRole, SweepRole
from piloop.plugins.role.contract import Files, Workdir
from piloop.plugins.role.implementer import IMPLEMENTER
from piloop.plugins.role.research import RESEARCH
from piloop.plugins.role.scout import SCOUT
from piloop.plugins.role.watchdog import WATCHDOG

from piloop.plugins.verdict_rule.contract import VERDICT_RULES, VerdictRule, Case
from piloop.plugins.verdict_rule.contract import Criterion, Side
from piloop.plugins.metric import Direction
from piloop.plugins.verdict_rule.contract import Judgement, Outcome
from piloop.plugins.metric import number
from piloop.plugins.verdict_rule.bootstrap import BOOTSTRAP

from piloop.plugins.stop_rule.contract import STOP_RULES, StopRule, StopCase
from piloop.plugins.stop_rule.contract import Limits, Progress, Fact
from piloop.plugins.stop_rule.contract import Go, GO, Hold, Stop
from piloop.plugins.stop_rule.limits import LIMITS

from piloop.plugins.executor.contract import EXECUTORS, Executor, Host
from piloop.plugins.executor.contract import Results, Settled, NoParameters
from piloop.plugins.executor.contract import AgentJob, EvaluationJob, LeftJob
from piloop.plugins.executor.contract import AgentReport, EvaluationReport
from piloop.plugins.executor.contract import AgentFinished, AgentNotStarted
from piloop.plugins.executor.contract import AgentKey, EvaluationKey, JobKey
from piloop.plugins.executor.contract import key_of
from piloop.plugins.executor.contract import GitTree, Checkout, Commits
from piloop.plugins.executor.kit.jobs import Control, run_agent, remove
from piloop.plugins.executor.kit.jobs import run_evaluation
from piloop.plugins.executor.kit.leases import Leases, Timing, Grant, Answer
from piloop.plugins.executor.kit.leases import StaleLease
from piloop.proc import kill_group
from piloop.plugins.executor.workers import WorkersExecutor
from piloop.otel import Tracer, Span, ConsoleProcessor
from piloop.runtime.factory import RuntimeFactory
from piloop.mcp import McpServer
from piloop.usage import Usage, UsageSpec, UsageReader
