"""Echo arm — exercises the full run loop with zero model spend.

Writes no answer content beyond an echo marker; reports honestly measured
(trivial) metrics. Exists so the framework is testable end-to-end offline.
"""

from __future__ import annotations

import time
from pathlib import Path

from ..model import TaskInstance, TaskMetrics, TaskResult
from .base import ArmDriver, RunContext


class EchoDriver(ArmDriver):
    name = "echo"

    def run_task(
        self, task: TaskInstance, ctx: RunContext, workdir: Path
    ) -> TaskResult:
        start = time.monotonic()
        answer = f"[echo] {task.instance_id}: prompt received ({len(task.prompt)} chars)"
        return TaskResult(
            instance_id=task.instance_id,
            arm=self.name,
            status="completed",
            answer=answer,
            patch=None,
            metrics=TaskMetrics(
                input_tokens=0,
                output_tokens=0,
                cache_read_tokens=0,
                cache_creation_tokens=0,
                tool_calls=0,
                wall_s=time.monotonic() - start,
            ),
            detail={"workdir": str(workdir)},
        )
