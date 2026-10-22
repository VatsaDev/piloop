"""Everything that a plugin imports from piloop: each capability, its contract types and its built-in providers."""

# ruff: noqa: F401  # this module only re-exports: every import is unused here

from piloop.plugins.host import Capability, PureCapability, Tier
from piloop.plugins.host import MissingProvider, Provider
from piloop.plugins.host import Selection, Source

from piloop.plugins.contracts.runtime import RUNTIMES
from piloop.runtime import AgentSpec, AgentStatus, Check, Handle
from piloop.runtime import NotAttachable, NotSteerable
from piloop.runtime import AgentExit
from piloop.runtime import Runtime, RuntimeFailure, StartFailed
from piloop.harnesses import Invocation, QueueKey
from piloop.settings import runtime_settings
from piloop.runtime.herdr import HerdrRuntime
from piloop.runtime.subprocess import SubprocessRuntime

from piloop.plugins.contracts.store import STORAGE_BACKENDS
from piloop.store import StorageBackend, StorageBackendConfig
from piloop.store.contract import check_storage_backend
from piloop.store.gcs import GcsConfig
from piloop.store.s3 import S3Config

from piloop.triggers import TRIGGERS, Trigger, Tick, Fire
from piloop.triggers import Backlog, TrackBacklog, Incident
from piloop.triggers import AnomalyTrigger, CronTrigger, SeedTrigger
from piloop.triggers import LowWaterTrigger, SqlTrigger
from piloop.triggers import StoppedTrigger

from piloop.config import ROLES, ImplementerRole, SweepRole
from piloop.config import Files, Workdir
from piloop.plugins.builtin.roles import IMPLEMENTER, RESEARCH
from piloop.plugins.builtin.roles import SCOUT, WATCHDOG

from piloop.plugins.contracts.verdict import VERDICT_RULES, VerdictRule, Case
from piloop.plugins.contracts.verdict import Criterion, Direction, Side
from piloop.plugins.contracts.verdict import Judgement, Outcome, number
from piloop.plugins.builtin.verdict import BOOTSTRAP

from piloop.plugins.contracts.stop import STOP_RULES, StopRule, StopCase
from piloop.plugins.contracts.stop import Limits, Progress, Fact
from piloop.plugins.contracts.stop import Go, GO, Hold, Stop
from piloop.plugins.builtin.stop import LIMITS

from piloop.plugins.contracts.executor import EXECUTORS, Executor, Host
from piloop.plugins.contracts.executor import Results, Settled, NoParameters
from piloop.plugins.contracts.executor import AgentJob, EvaluationJob, LeftJob
from piloop.plugins.contracts.executor import AgentReport, EvaluationReport
from piloop.plugins.contracts.executor import AgentKey, EvaluationKey, JobKey
from piloop.plugins.contracts.executor import key_of
from piloop.plugins.contracts.executor import GitTree, Checkout, Commits
from piloop.plugins.kit.executor.jobs import Control, run_agent, remove
from piloop.plugins.kit.executor.jobs import run_evaluation
from piloop.plugins.kit.executor.leases import Leases, Timing, Grant, Answer
from piloop.plugins.kit.executor.leases import StaleLease
from piloop.proc import kill_group
from piloop.plugins.builtin.executor.workers import WorkersExecutor
from piloop.otel import Tracer, Span, ConsoleProcessor
from piloop.runtime.factory import RuntimeFactory
from piloop.mcp import McpServer
from piloop.usage import Usage, UsageSpec, UsageReader
