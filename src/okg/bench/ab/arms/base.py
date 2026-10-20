"""Arm-driver interface + run context.

An arm driver runs ONE task in a provisioned workdir and returns a
TaskResult with real measured metrics. The three protocol arms (fresh /
native / okg) share the headless Claude Code driver with different state
policies; ``echo`` exercises the loop without a model.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..model import TaskInstance, TaskResult


@dataclass
class RunContext:
    """Everything a driver may need for a run. One per run, shared across
    tasks in sequence (drivers keep their cross-task state here)."""

    run_dir: Path
    data_root: Path
    model: str
    task_timeout_s: float
    permission_mode: str
    native_memory_mode: str | None = None
    claude_bin: str = "claude"
    corpus: dict[str, Any] | None = None  # okg arm: deployment name/dsn/mcp config
    state: dict[str, Any] = field(default_factory=dict)


class ArmDriver(ABC):
    #: registry key
    name: str = ""

    def setup_run(self, ctx: RunContext) -> None:
        """Called once before the first task. Default: nothing."""

    def teardown_run(self, ctx: RunContext) -> None:
        """Called once after the last task (also on failure). Default:
        nothing. The okg arm stops its corpus MCP server here."""

    @abstractmethod
    def run_task(
        self, task: TaskInstance, ctx: RunContext, workdir: Path
    ) -> TaskResult:
        """Run one task to completion (or timeout/error) and account for it."""

    def between_tasks(
        self, task: TaskInstance, result: TaskResult, ctx: RunContext
    ) -> dict[str, Any]:
        """Called after each task. The okg arm ingests the completed session
        and publishes a new generation here. Returns a record for the task
        log (e.g. the published generation id). Default: nothing."""
        return {}
