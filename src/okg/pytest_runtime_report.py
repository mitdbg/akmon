"""Low-overhead pytest/xdist runtime instrumentation for required CI lanes.

This module lives in the installed package because pytest's ``-p`` option
imports a module before repository-root helper directories are importable.
"""

from __future__ import annotations

import json
import os
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pytest


REPORT_ENV = "OKG_PYTEST_RUNTIME_REPORT"
SCHEMA = "okg.ci.pytest_runtime.v1"
_state: dict[str, Any] | None = None


def _path() -> Path | None:
    raw = os.environ.get(REPORT_ENV)
    return Path(raw) if raw else None


def _write(*, force: bool = False) -> None:
    path = _path()
    if path is None or _state is None:
        return
    now = time.monotonic()
    if not force and now - _state["_last_write"] < 5.0:
        return
    payload = dict(_state)
    payload["elapsed_seconds"] = round(now - _state["_started"], 3)
    payload.pop("_started", None)
    payload.pop("_last_write", None)
    payload.pop("collection_by_worker", None)
    payload.pop("collection_seconds", None)
    payload["worker_phase_seconds"] = {
        worker: {phase: round(seconds, 3) for phase, seconds in phases.items()}
        for worker, phases in sorted(_state["worker_phase_seconds"].items())
    }
    payload["worker_report_counts"] = {
        worker: dict(sorted(counts.items()))
        for worker, counts in sorted(_state["worker_report_counts"].items())
    }
    payload["collection"] = {
        "status": "available" if _state["collection_by_worker"] else "pending",
        "seconds": _state["collection_seconds"],
        "node_count_by_worker": dict(sorted(_state["collection_by_worker"].items())),
    }
    collection_counts = set(_state["collection_by_worker"].values())
    payload["unit_test_count"] = (
        {"status": "available", "value": next(iter(collection_counts))}
        if len(collection_counts) == 1
        else {
            "status": "unavailable",
            "reason": (
                "collection is incomplete"
                if not collection_counts
                else "xdist workers collected different test counts"
            ),
        }
    )
    worker_work = {
        worker: round(sum(_state["worker_phase_seconds"][worker].values()), 3)
        for worker in sorted(_state["collection_by_worker"])
    }
    positive_work = [seconds for seconds in worker_work.values() if seconds > 0]
    if worker_work and len(positive_work) == len(worker_work):
        minimum = min(positive_work)
        utilization: dict[str, Any] = {
            "status": "available",
            "work_seconds_by_worker": worker_work,
            "max_to_min_work_ratio": round(max(positive_work) / minimum, 3),
        }
    else:
        utilization = {
            "status": "unavailable",
            "reason": (
                "collection is incomplete"
                if not worker_work
                else "at least one collected worker has no reported test work"
            ),
            "work_seconds_by_worker": worker_work,
        }
    payload["worker_utilization"] = utilization
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)
    _state["_last_write"] = now


def pytest_configure(config: pytest.Config) -> None:
    global _state
    if hasattr(config, "workerinput") or _path() is None:
        return
    _state = {
        "schema": SCHEMA,
        "status": "running",
        "_started": time.monotonic(),
        "_last_write": 0.0,
        "collection_seconds": None,
        "collection_by_worker": {},
        "first_test_seconds": None,
        "worker_phase_seconds": defaultdict(lambda: defaultdict(float)),
        "worker_report_counts": defaultdict(Counter),
        "reports_seen": 0,
        "slowest_reports": [],
    }
    _write(force=True)


@pytest.hookimpl(optionalhook=True)
def pytest_xdist_node_collection_finished(node: Any, ids: list[str]) -> None:
    if _state is None:
        return
    worker = str(getattr(node, "gateway", node).id)
    _state["collection_by_worker"][worker] = len(ids)
    _state["collection_seconds"] = round(time.monotonic() - _state["_started"], 3)
    _write()


def pytest_runtest_logstart(nodeid: str, location: tuple[str, int | None, str]) -> None:
    del nodeid, location
    if _state is not None and _state["first_test_seconds"] is None:
        _state["first_test_seconds"] = round(time.monotonic() - _state["_started"], 3)
        if _state["collection_seconds"] is None:
            _state["collection_seconds"] = _state["first_test_seconds"]
        _write(force=True)


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    if _state is None:
        return
    worker = str(getattr(report, "worker_id", "controller"))
    _state["worker_phase_seconds"][worker][report.when] += float(report.duration)
    _state["worker_report_counts"][worker][report.outcome] += 1
    _state["reports_seen"] += 1
    slow = {
        "nodeid": report.nodeid,
        "phase": report.when,
        "worker": worker,
        "seconds": round(float(report.duration), 3),
    }
    _state["slowest_reports"].append(slow)
    _state["slowest_reports"] = sorted(
        _state["slowest_reports"],
        key=lambda item: item["seconds"],
        reverse=True,
    )[:20]
    _write()


def pytest_sessionfinish(
    session: pytest.Session, exitstatus: int | pytest.ExitCode
) -> None:
    del session
    if _state is None:
        return
    _state["status"] = "complete"
    _state["exit_code"] = int(exitstatus)
    _write(force=True)
