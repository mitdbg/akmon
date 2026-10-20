"""Run ingest source jobs in the test process.

Production runs every source in a ``spawn`` child process
(`okg.substrate.ingest.source_jobs`). A test that replaces an
``ingest.service`` internal -- ``_run_source`` above all -- with a stub
cannot reach a spawned interpreter: the child imports the real module. Such
tests install this runtime, which calls ``ingest_service._run_source`` in the
test process and admits every start. It exists only under ``tests/``; the
service has one production runtime.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

import pytest

from okg.substrate.ingest import service as ingest_service
from okg.substrate.ingest import source_job_runtime


class InlineSourceJobRuntime:
    """Coordinator runtime that runs each source synchronously, in-process."""

    def __init__(self, options: Any, plan: Any) -> None:
        self._options = options
        self._plan = plan
        self.launched: list[str] = []

    def launch(self, name: str) -> dict[str, Any]:
        self.launched.append(name)
        # Resolved at call time so a test's monkeypatch applies.
        return ingest_service._run_source(
            self._options,
            self._plan.runnable[name],
            name,
        )

    def connection_state(self) -> tuple[int, int]:
        return 0, 1_000_000

    def recorded_peak(self, name: str) -> int | None:
        return None

    def memory_limit(self, name: str) -> int:
        return 1

    def is_heavy(self, name: str) -> bool:
        return False

    def sample_memory(self, pid: int) -> int | None:
        return None

    def memory_budget(self) -> int:
        return 1 << 62

    def connection_budget_exhausted(self, name: str, **_: Any) -> dict[str, Any]:
        raise AssertionError("the inline runtime admits every start")

    def start_failed(self, name: str, **_: Any) -> dict[str, Any]:
        raise AssertionError("the inline runtime admits every start")


@contextmanager
def _open_inline_runtime(options: Any, plan: Any) -> Iterator[InlineSourceJobRuntime]:
    yield InlineSourceJobRuntime(options, plan)


def use_inline_source_jobs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make ``run_ingest`` run its sources in this process for one test."""
    monkeypatch.setattr(
        source_job_runtime,
        "open_source_job_runtime",
        _open_inline_runtime,
    )
