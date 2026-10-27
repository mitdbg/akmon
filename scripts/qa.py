#!/usr/bin/env python3
"""Run OKG QA lanes from ``tests/qa_lanes.yaml``.

The script is deliberately thin: resolve a lane, print/run the underlying
pytest or shell command, and write a small JSON report under ``.scratch/qa``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import shlex
import shutil
import signal
import statistics
import subprocess
import sys
import threading
import time
import tomllib
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, TextIO
from urllib.parse import urlsplit, urlunsplit

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = REPO_ROOT / "tests" / "qa_lanes.yaml"
DEFAULT_POSTGRES_TEST_DSN_TEMPLATE = (
    "postgres://postgres:okg@127.0.0.1:5433/okg_test_{workerid}"
)
TRUST_EPHEMERAL_ENV = "OKG_TEST_DSN_TRUST_AS_EPHEMERAL"
XDIST_MAX_WORKERS_ENV = "OKG_QA_XDIST_MAX_WORKERS"
QA_COST_REPORT_DIR_ENV = "OKG_QA_COST_REPORT_DIR"
QA_PYTEST_DIST_ENV = "OKG_QA_PYTEST_DIST"
FAST_PARTITION_COUNT = 5
FAST_PARTITION_SEED = 1945
FAST_PARTITION_DISTRIBUTION = "loadfile"
FAST_PARTITION_PLUGIN = "okg.pytest_fast_partition"
# The substrate-db core-db-contracts step runs as DB_PARTITION_COUNT CI jobs
# (substrate-db-linux, substrate-db-linux-2, substrate-db-linux-3), each one
# shard of the same selection, placed by okg.pytest_db_partition from the
# committed weights table. Unlike the fast lane it keeps its own --dist: a
# shard is a whole-file subset, so loadscope's grouping is unchanged.
DB_PARTITION_LANE = "substrate-db"
DB_PARTITION_STEP = "core-db-contracts"
DB_PARTITION_COUNT = 3
DB_PARTITION_PLUGIN = "okg.pytest_db_partition"
DB_PARTITION_WEIGHTS = Path("tests") / "db_partition_weights.json"
DB_PARTITION_JOBS = ("substrate-db-linux", "substrate-db-linux-2", "substrate-db-linux-3")
# The nightly substrate-db-full sweep runs as DB_FULL_PARTITION_COUNT legs of
# one matrix job (PACT nightly-time-limits): its parallel-db step is split by
# the same plugin, and every other step of the lane (serial-db) runs whole in
# leg DB_FULL_WHOLE_STEPS_SHARD only, so it runs once a night. It has its own
# weights table (PACT db-full-partition-weights): the per-PR table measures
# only the core-db-contracts files, so the ~450 files only the sweep runs all
# weighed its median and one leg ran past the job limit. The table reserves
# leg 0's time for serial-db (174 s in CI, 2026-10-16) and the synth golden
# pins step (263 s), both run without xdist: 2 workers x 437 s = 874
# worker-seconds.
#
# FOUR legs, not three (PACT db-full-four-legs, issue #2554). The sweep's
# parallel-db step grew from 22,321 worker-seconds (the 2026-10-08 table) to
# 27,644 (run 39218760132, 2026-10-19), and every leg took 80-89 of its 90
# minutes; leg 0 was cancelled at 90 on 10-15 and 10-18. Three perfectly
# balanced legs would still need about 86 minutes each, so a fresh table alone
# cannot restore headroom; four legs need about 66 (70 at the worst
# measured-vs-predicted ratio, x1.07).
DB_FULL_PARTITION_LANE = "substrate-db-full"
DB_FULL_PARTITION_WEIGHTS = Path("tests") / "db_full_partition_weights.json"
DB_FULL_PARTITION_STEP = "parallel-db"
DB_FULL_PARTITION_COUNT = 4
DB_FULL_WHOLE_STEPS_SHARD = 0
DB_FULL_PARTITION_JOBS = tuple(
    f"substrate-db-full (shard {index})" for index in range(DB_FULL_PARTITION_COUNT)
)
# The nightly coverage-full lane runs as COVERAGE_FULL_PARTITION_COUNT legs of
# the nightly-full-ci matrix job (PACT nightly-coverage-full-shards). As one
# job it never finished: the 2026-10-13 four-leg dispatch run 38334063060
# measured about 55,000 worker-seconds of cov-parallel, 7.6 hours on two xdist
# workers, and the job limit was 4h48m, then 6h. Its cov-parallel step is
# split by okg.pytest_db_partition with its own weights table; cov-serial runs
# whole in leg 0 only, which also runs the perf lane, and the table reserves
# that time so leg 0 takes fewer files. Five legs, not four: the four-leg run's
# complete legs took 139 and 128 minutes of tests against a projected 106, so
# five legs of about 95 minutes (about 107 at the new table's own x1.1
# under-prediction) keep the worst projection error seen (x1.31) under 140
# minutes. A leg writes coverage DATA only (pytest-cov's .coverage
# file): its own numbers cover a fifth of the tests, so its JSON report and
# the area floors it would trigger in tests/conftest.py are dropped, and the
# nightly-coverage-full-combine job combines all five legs' data, writes the
# one coverage.json and checks the floors (scripts/ci/coverage_full_combine.py).
COVERAGE_FULL_PARTITION_LANE = "coverage-full"
COVERAGE_FULL_PARTITION_STEP = "cov-parallel"
COVERAGE_FULL_PARTITION_COUNT = 5
COVERAGE_FULL_PARTITION_WEIGHTS = Path("tests") / "coverage_full_partition_weights.json"
COVERAGE_FULL_PARTITION_JOBS = tuple(
    f"nightly-full-ci (shard {index})" for index in range(COVERAGE_FULL_PARTITION_COUNT)
)
# The JSON report argument a coverage-full leg drops (see above).
COVERAGE_JSON_REPORT_PREFIX = "--cov-report=json:"
# Loaded into every coverage-full leg step: it turns an xdist worker that
# returned no coverage data into exit status 3, which the leg's completion
# receipt counts as incomplete (src/okg/pytest_cov_workers.py).
COVERAGE_LEG_WORKERS_PLUGIN = "okg.pytest_cov_workers"
# Added to every coverage-full leg step that runs under xdist: a crashed
# worker is not replaced, so the leg ends with that test failed instead of
# going on with a fresh worker. In the 2026-10-13 dispatch run 38334063060
# both legs that replaced a crashed worker hung at 99% until the job limit.
# The crashed worker's lost coverage data then turns the exit status into 3
# (okg.pytest_cov_workers), and the JUnit "crashed while running" line marks
# the leg's receipt incomplete, so the combine refuses the leg.
COVERAGE_LEG_NO_WORKER_RESTART = "--max-worker-restart=0"
# Lane -> the one step a db partition splits.
DB_PARTITION_STEPS = {
    DB_PARTITION_LANE: DB_PARTITION_STEP,
    DB_FULL_PARTITION_LANE: DB_FULL_PARTITION_STEP,
    COVERAGE_FULL_PARTITION_LANE: COVERAGE_FULL_PARTITION_STEP,
}
# Lane -> the weights table its db partition is balanced by.
DB_PARTITION_WEIGHTS_BY_LANE = {
    DB_PARTITION_LANE: DB_PARTITION_WEIGHTS,
    DB_FULL_PARTITION_LANE: DB_FULL_PARTITION_WEIGHTS,
    COVERAGE_FULL_PARTITION_LANE: COVERAGE_FULL_PARTITION_WEIGHTS,
}
# Lane -> the shard count its db partition requires.
DB_PARTITION_COUNT_BY_LANE = {
    DB_PARTITION_LANE: DB_PARTITION_COUNT,
    DB_FULL_PARTITION_LANE: DB_FULL_PARTITION_COUNT,
    COVERAGE_FULL_PARTITION_LANE: COVERAGE_FULL_PARTITION_COUNT,
}
# Whole-lane partitions: the split step is divided across the legs, and every
# other step of the lane runs whole in leg 0 only, so it runs once a night.
WHOLE_LANE_PARTITION_LANES = (DB_FULL_PARTITION_LANE, COVERAGE_FULL_PARTITION_LANE)
# Lane -> the CI job name of each leg, for the budget summary.
DB_PARTITION_JOBS_BY_LANE = {
    DB_PARTITION_LANE: DB_PARTITION_JOBS,
    DB_FULL_PARTITION_LANE: DB_FULL_PARTITION_JOBS,
    COVERAGE_FULL_PARTITION_LANE: COVERAGE_FULL_PARTITION_JOBS,
}
PERF_MARKER_EXPRESSION = "(slow or perf) and not live_ablation and not chat_live"
PERF_ALLOWED_ARGS = ("-q", "--no-cov", "--tb=short", "--durations=50")
# A lane budget does TWO jobs, and conflating them is why budget-only reds
# rotate around the lanes (#1058). Split them:
#
#   budget_seconds       -- the HARD CEILING, sized from the lane's measured
#                           runs. Blocking (when enforce_budget is true).
#                           Runaway protection only: a hung fixture, an
#                           infinite loop, a pathological regression.
#                           Most lanes below still set it at 2x their
#                           measured baseline; the partitioned fast lane is
#                           DERIVED instead, by
#                           scripts/ci/fast_linux_partition_budget.py.
#   warn threshold       -- BUDGET_WARN_FRACTION of the ceiling, i.e. the
#                           measured baseline itself. NEVER blocking. This is
#                           where the regression signal lives: a lane drifting
#                           past its measured normal is said out loud on every
#                           run instead of being invisible until it crosses a
#                           hard line and reds a pull request that did nothing.
#
# The warn threshold is DERIVED from the ceiling so the two cannot drift. A
# lane may pin it explicitly with `budget_warn_seconds` when the derived value
# would be wrong -- see the substrate-db comment in tests/qa_lanes.yaml, whose
# ceiling is held below its own measured baseline by a separate contract.
BUDGET_WARN_FRACTION = 0.5

# Per-test stack-dump threshold for partitioned fast-lane runs: pytest's own
# faulthandler prints every thread's stack once one test runs this long.
FAST_PARTITION_FAULTHANDLER_TIMEOUT_SECONDS = 300

# THE JOB-DEADLINE STACK DUMP (PACT ci-measured-timeouts). A lane has one time
# limit, its CI job's timeout-minutes; there is no in-step kill. So that a hang
# still names its test, every pytest step loads okg.pytest_job_deadline, which
# prints every thread's stack (a) at OKG_QA_STACK_DUMP_AT and (b) on SIGUSR1,
# which _run_step sends when qa.py itself is interrupted or terminated.
#
# How the job's limit reaches this process: the workflow sets
# OKG_JOB_TIMEOUT_MINUTES at job level to the same number as the job's
# timeout-minutes (tests/ci/test_lane_timeout_headroom.py checks they agree).
# The job's start is OKG_JOB_STARTED_AT when set, otherwise the start time of
# the GitHub runner's per-job `Runner.Worker` process, found among this
# process's ancestors in /proc. The dump fires STACK_DUMP_MARGIN_SECONDS
# before start + limit (at most a quarter of the limit before it). When either
# input is missing the lane says so on stderr and runs without the timed dump;
# the SIGUSR1 dump still works.
JOB_TIMEOUT_MINUTES_ENV = "OKG_JOB_TIMEOUT_MINUTES"
JOB_STARTED_AT_ENV = "OKG_JOB_STARTED_AT"
STACK_DUMP_AT_ENV = "OKG_QA_STACK_DUMP_AT"
STACK_DUMP_PLUGIN = "okg.pytest_job_deadline"
STACK_DUMP_MARGIN_SECONDS = 180.0
COUNT_RE = re.compile(
    r"(?P<count>\d+)\s+"
    r"(?P<kind>passed|failed|skipped|deselected|errors?|xfailed|xpassed)"
)
DURATION_RE = re.compile(
    r"^\s*(?P<seconds>\d+(?:\.\d+)?)s\s+"
    r"(?P<phase>setup|call|teardown)\s+"
    r"(?P<nodeid>.+?)\s*$"
)
QA_LOG_TAIL_MAX_BYTES = 12_000
QA_LOG_DSN_RE = re.compile(
    r"(?i)\b(?:postgres(?:ql)?|mysql|redshift)(?:\+[a-z0-9_.-]+)?://"
    r"[^\s`'\"<>]+"
)
QA_LOG_SECRET_LINE_RE = re.compile(
    r"(?i)(?P<prefix>[\"']?[A-Z0-9_.-]*"
    r"(?:TOKEN|SECRET|PASSWORD|PASSWD|API[_-]?KEY|CREDENTIAL|ACCESS[_-]?KEY)"
    r"[A-Z0-9_.-]*[\"']?\s*[:=]\s*).*$",
    re.MULTILINE,
)
QA_LOG_AUTH_LINE_RE = re.compile(
    r"(?i)(?P<prefix>[\"']?"
    r"(?:authorization|private-token|x-api-key)[\"']?\s*[:=]\s*).*$",
    re.MULTILINE,
)


class QALaneError(RuntimeError):
    """Raised for invalid lane configuration or execution requests."""


@dataclass(frozen=True)
class ResolvedStep:
    name: str
    kind: str
    command: list[str]
    allow_empty: bool = False
    runtime_report: bool = False


def _selector_covers(selector: str, candidate: str) -> bool:
    """Return whether one pytest selector also selects another selector."""
    selector_file = selector.split("::", 1)[0]
    candidate_file = candidate.split("::", 1)[0]
    if selector_file != candidate_file:
        return False
    if "::" not in selector:
        return True
    return candidate == selector or candidate.startswith(f"{selector}::")


def validate_perf_registry(
    manifest: dict[str, Any], *, repo_root: Path = REPO_ROOT
) -> tuple[str, ...]:
    """Validate perf's explicit selectors before constructing its command.

    The live lane-coverage guard proves semantic parity against pytest's final
    marker inventory. This cheaper boundary rejects command shapes that must
    never reach pytest: broad paths, globs, parameter leaves, stale files,
    duplicate or overlapping selectors, and selection-softening arguments.
    """
    lanes = manifest.get("lanes")
    perf = lanes.get("perf") if isinstance(lanes, dict) else None
    if not isinstance(perf, dict):
        raise QALaneError("manifest must declare a perf lane")
    steps = perf.get("steps")
    if not isinstance(steps, list):
        raise QALaneError("perf lane must declare one pytest step")
    pytest_steps = [step for step in steps if step.get("kind") == "pytest"]
    if len(steps) != 1 or len(pytest_steps) != 1:
        raise QALaneError("perf lane must declare exactly one pytest step")

    step = pytest_steps[0]
    if step.get("markers") != PERF_MARKER_EXPRESSION:
        raise QALaneError(
            "perf lane must retain the exact marker expression "
            f"{PERF_MARKER_EXPRESSION!r}"
        )
    args = step.get("args") or []
    if not isinstance(args, list) or any(not isinstance(arg, str) for arg in args):
        raise QALaneError("perf lane args must be a list of strings")
    if tuple(args) != PERF_ALLOWED_ARGS:
        raise QALaneError(
            "perf lane must use the exact allowed args tuple "
            f"{PERF_ALLOWED_ARGS!r}, found {tuple(args)!r}"
        )

    raw_paths = step.get("paths")
    if not isinstance(raw_paths, list) or not raw_paths:
        raise QALaneError("perf lane selector registry must be a non-empty list")
    if any(not isinstance(selector, str) or not selector for selector in raw_paths):
        raise QALaneError("perf lane selector registry contains an invalid entry")
    selectors = tuple(raw_paths)
    duplicates = sorted(
        selector for selector in set(selectors) if selectors.count(selector) > 1
    )
    if duplicates:
        raise QALaneError(
            "perf lane selector registry contains duplicate entries: "
            + ", ".join(duplicates)
        )

    root = repo_root.resolve()
    for selector in selectors:
        if any(character in selector for character in "*?[]"):
            detail = (
                "parameterized leaf IDs are forbidden"
                if "[" in selector or "]" in selector
                else "globs are forbidden"
            )
            raise QALaneError(f"invalid perf selector {selector!r}: {detail}")
        parts = selector.split("::")
        file_part = parts[0]
        if (
            not file_part.startswith("tests/")
            or Path(file_part).is_absolute()
            or Path(file_part).as_posix() != file_part
            or Path(file_part).suffix != ".py"
        ):
            raise QALaneError(
                f"invalid perf selector {selector!r}: expected a repository-relative "
                "test file or base node ID"
            )
        if any(not part or not part.isidentifier() for part in parts[1:]):
            raise QALaneError(
                f"invalid perf selector {selector!r}: malformed base node ID"
            )
        target = (root / file_part).resolve()
        try:
            target.relative_to((root / "tests").resolve())
        except ValueError as error:
            raise QALaneError(
                f"invalid perf selector {selector!r}: expected a repository-relative "
                "test file"
            ) from error
        if not target.is_file():
            raise QALaneError(
                f"invalid perf selector {selector!r}: test file does not exist"
            )

    overlaps: list[tuple[str, str]] = []
    for index, selector in enumerate(selectors):
        for candidate in selectors[index + 1 :]:
            if _selector_covers(selector, candidate) or _selector_covers(
                candidate, selector
            ):
                overlaps.append((selector, candidate))
    if overlaps:
        rendered = ", ".join(f"{left!r} / {right!r}" for left, right in overlaps)
        raise QALaneError(
            "perf lane selector registry contains overlapping entries: " + rendered
        )
    return selectors


def load_manifest(path: Path = DEFAULT_MANIFEST) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise QALaneError(f"{path} must contain a mapping")
    if data.get("version") != 1:
        raise QALaneError(f"{path} must declare version: 1")
    lanes = data.get("lanes")
    if not isinstance(lanes, dict) or not lanes:
        raise QALaneError(f"{path} must declare lanes")
    for lane_name, lane in lanes.items():
        if not isinstance(lane, dict):
            raise QALaneError(f"lane {lane_name!r} must be a mapping")
        if "composed_of" in lane:
            children = lane["composed_of"]
            if not isinstance(children, list) or not children:
                raise QALaneError(f"lane {lane_name!r} has invalid composed_of")
            missing = [child for child in children if child not in lanes]
            if missing:
                raise QALaneError(
                    f"lane {lane_name!r} references unknown children {missing}"
                )
            continue
        steps = lane.get("steps")
        if not isinstance(steps, list) or not steps:
            raise QALaneError(f"lane {lane_name!r} must declare steps")
        for step in steps:
            if not isinstance(step, dict):
                raise QALaneError(f"lane {lane_name!r} step must be a mapping")
            if "name" not in step or "kind" not in step:
                raise QALaneError(f"lane {lane_name!r} step must declare name and kind")
    # Custom manifests are supported by the CLI and may define only the lane
    # under test.  The strict perf registry contract applies whenever a perf
    # lane is present; the checked-in manifest separately proves that perf is
    # one of the repository's required lanes.
    if "perf" in lanes:
        validate_perf_registry(data)
    return data


def report_dir(manifest: dict[str, Any], override: Path | None = None) -> Path:
    if override is not None:
        return override if override.is_absolute() else REPO_ROOT / override
    raw = manifest.get("report_dir", ".scratch/qa")
    path = Path(str(raw))
    return path if path.is_absolute() else REPO_ROOT / path


def _default_xdist_max_workers() -> int:
    return max(1, (os.cpu_count() or 2) // 2)


def _xdist_max_workers() -> int:
    raw = os.environ.get(XDIST_MAX_WORKERS_ENV)
    if raw is None:
        return _default_xdist_max_workers()
    try:
        workers = int(raw)
    except ValueError as exc:
        raise QALaneError(
            f"{XDIST_MAX_WORKERS_ENV} must be a positive integer"
        ) from exc
    if workers < 1:
        raise QALaneError(f"{XDIST_MAX_WORKERS_ENV} must be a positive integer")
    return workers


def _resolve_xdist_workers(raw_xdist: Any) -> str:
    max_workers = _xdist_max_workers()
    requested = str(raw_xdist)
    if requested in {"auto", "logical"}:
        return str(max_workers)
    try:
        workers = int(requested)
    except ValueError:
        return requested
    return str(min(workers, max_workers))


def _shard_only_paths(
    paths: list[str], weights_table: Path, index: int, count: int
) -> tuple[list[str], str | None]:
    """The paths pytest gets for this shard and the whole step's file list.

    PACT lane-ci-shard-only-collection. Every shard used to collect the whole
    step and deselect two thirds at collection, ~281 s per xdist worker. Here
    the runner places the step's files with the plugin's own function and
    hands pytest only the shard's files; the plugin still filters, over the
    same whole-step list (``--okg-db-partition-universe``).

    Fails closed: if the shard's list cannot be computed (unreadable table, a
    path that is not a plain test file, an empty share) this prints a loud
    warning, including a GitHub ``::warning::`` annotation, and returns every
    path: the shard collects everything and deselects as before. The whole-step
    list is still returned whenever it could be computed, so every shard places
    over the same files whether or not it fell back. The lane's result is
    unchanged either way.
    """

    universe: str | None = None
    try:
        from okg import pytest_db_partition as plugin

        missing = [
            path for path in paths if not (REPO_ROOT / path.split("::", 1)[0]).is_file()
        ]
        if missing:
            raise ValueError(f"not plain test files: {', '.join(missing[:5])}")
        table = REPO_ROOT / weights_table
        weights = plugin.load_weights(table)
        reserved = plugin.load_reserved(table, count)
        universe = plugin.universe_option(paths)
        owned = plugin.owned_entries(paths, weights, count, index, reserved)
        if not owned:
            raise ValueError(f"shard {index} owns no files")
        return owned, universe
    except (ImportError, ValueError, OSError) as error:
        print(
            f"::warning title=db shard {index}/{count} collects all files::"
            f"qa: WARNING db shard {index}/{count} cannot pre-select its files "
            f"({error}); FALLING BACK to collect-all-then-deselect for this shard "
            "(slower, every test still runs exactly once)",
            file=sys.stderr,
        )
        return paths, universe


def _pytest_command(
    step: dict[str, Any],
    *,
    serial: bool,
    partition_index: int | None = None,
    partition_count: int | None = None,
    partition_kind: str = "fast",
    db_partition_weights: Path = DB_PARTITION_WEIGHTS,
    presplit: bool = False,
) -> list[str]:
    command = [sys.executable, "-m", "pytest"]
    xdist = step.get("xdist")
    if xdist and not serial:
        command.extend(["-n", _resolve_xdist_workers(xdist)])
    command.extend(str(arg) for arg in step.get("args", []))
    distribution = os.environ.get(QA_PYTEST_DIST_ENV)
    fast_partitioned = (
        partition_kind == "fast"
        and partition_index is not None
        and partition_count is not None
    )
    db_partitioned = (
        partition_kind == "db"
        and partition_index is not None
        and partition_count is not None
    )
    if fast_partitioned:
        if distribution not in {None, FAST_PARTITION_DISTRIBUTION}:
            raise QALaneError(
                f"partitioned fast lane requires --dist={FAST_PARTITION_DISTRIBUTION}; "
                f"{QA_PYTEST_DIST_ENV}={distribution!r} would weaken file isolation"
            )
        distribution = FAST_PARTITION_DISTRIBUTION
    if distribution:
        allowed = {"load", "loadfile", "loadscope", "worksteal"}
        if distribution not in allowed:
            raise QALaneError(
                f"{QA_PYTEST_DIST_ENV} must be one of {sorted(allowed)}, "
                f"not {distribution!r}"
            )
        replaced = False
        for index, arg in enumerate(command):
            if arg.startswith("--dist="):
                command[index] = f"--dist={distribution}"
                replaced = True
        if not replaced:
            raise QALaneError(
                f"{QA_PYTEST_DIST_ENV} cannot override a step without --dist="
            )
    markers = step.get("markers")
    if markers:
        command.extend(["-m", str(markers)])
    paths = [str(path) for path in step.get("paths", [])]
    universe: str | None = None
    if db_partitioned and presplit:
        paths, universe = _shard_only_paths(
            paths, db_partition_weights, partition_index, partition_count
        )
    command.extend(paths)
    if db_partitioned:
        command.extend(
            [
                "-p",
                DB_PARTITION_PLUGIN,
                "--okg-db-partition-index",
                str(partition_index),
                "--okg-db-partition-count",
                str(partition_count),
                "--okg-db-partition-weights",
                db_partition_weights.as_posix(),
            ]
        )
        if universe is not None:
            command.append(f"--okg-db-partition-universe={universe}")
    if fast_partitioned:
        command.extend(
            [
                "-p",
                FAST_PARTITION_PLUGIN,
                "--okg-fast-partition-index",
                str(partition_index),
                "--okg-fast-partition-count",
                str(partition_count),
                # A partition that hangs is killed at its process timeout with
                # no JUnit and no test name. pytest's faulthandler plugin dumps
                # every thread's stack once one test exceeds this, naming the
                # stuck test in the job log well before the kill.
                "-o",
                f"faulthandler_timeout={FAST_PARTITION_FAULTHANDLER_TIMEOUT_SECONDS}",
            ]
        )
    return command


def _coverage_leg_step(step: dict[str, Any]) -> dict[str, Any]:
    """One coverage-full leg's copy of ``step``: coverage data, no JSON report.

    A leg measures a fifth of the tests, so a JSON report of it would trip
    the area floors tests/conftest.py checks on any JSON report. The combine
    job reports the combined data instead. Refuses a step with no JSON report
    to drop: the lane's report argument moved, and dropping nothing would leave
    the floors checking a partial number again.
    """
    args = [str(arg) for arg in step.get("args", [])]
    if not any(arg.startswith(COVERAGE_JSON_REPORT_PREFIX) for arg in args):
        raise QALaneError(
            f"{COVERAGE_FULL_PARTITION_LANE} step {step.get('name')!r} has no "
            f"{COVERAGE_JSON_REPORT_PREFIX}... argument for a leg to drop"
        )
    leg = dict(step)
    leg["args"] = [
        "--cov-report=" if arg.startswith(COVERAGE_JSON_REPORT_PREFIX) else arg
        for arg in args
    ] + ["-p", COVERAGE_LEG_WORKERS_PLUGIN]
    if step.get("xdist"):
        leg["args"].append(COVERAGE_LEG_NO_WORKER_RESTART)
    return leg


def resolve_steps(
    manifest: dict[str, Any],
    lane_name: str,
    *,
    serial: bool = False,
    partition_index: int | None = None,
    partition_count: int | None = None,
    partition_kind: str = "fast",
) -> list[ResolvedStep]:
    lanes = manifest["lanes"]
    if lane_name not in lanes:
        raise QALaneError(f"unknown lane {lane_name!r}")
    lane = lanes[lane_name]
    if "composed_of" in lane:
        resolved: list[ResolvedStep] = []
        for child in lane["composed_of"]:
            resolved.extend(
                resolve_steps(
                    manifest,
                    child,
                    serial=serial,
                    partition_index=partition_index,
                    partition_count=partition_count,
                    partition_kind=partition_kind,
                )
            )
        return resolved

    steps: list[ResolvedStep] = []
    for step in lane["steps"]:
        kind = step["kind"]
        if (
            partition_kind == "db"
            and lane_name in WHOLE_LANE_PARTITION_LANES
            and partition_index is not None
            and partition_index != DB_FULL_WHOLE_STEPS_SHARD
            and step.get("name") != DB_PARTITION_STEPS[lane_name]
        ):
            # A whole-lane partition's unsplit steps run in one leg only.
            continue
        if (
            partition_kind == "db"
            and lane_name == COVERAGE_FULL_PARTITION_LANE
            and partition_index is not None
        ):
            step = _coverage_leg_step(step)
        if kind == "pytest":
            # A database partition belongs to its one step; the lane's other
            # steps (serial-db-pact) run whole wherever they run.
            partitions_step = (
                partition_kind != "db"
                or step.get("name") == DB_PARTITION_STEPS.get(lane_name)
            )
            command = _pytest_command(
                step,
                serial=serial,
                partition_index=partition_index if partitions_step else None,
                partition_count=partition_count if partitions_step else None,
                partition_kind=partition_kind,
                db_partition_weights=(
                    DB_PARTITION_WEIGHTS_BY_LANE[lane_name]
                    if partition_kind == "db"
                    else DB_PARTITION_WEIGHTS
                ),
                presplit=(partition_kind == "db" and lane_name == DB_PARTITION_LANE),
            )
        elif kind == "shell":
            command = [str(part) for part in step.get("command", [])]
            if not command:
                raise QALaneError(
                    f"lane {lane_name!r} step {step['name']!r} has no command"
                )
        else:
            raise QALaneError(
                f"lane {lane_name!r} step {step['name']!r} has unknown kind {kind!r}"
            )
        steps.append(
            ResolvedStep(
                name=str(step["name"]),
                kind=kind,
                command=command,
                allow_empty=bool(step.get("allow_empty", False)),
                runtime_report=bool(step.get("runtime_report", False)),
            )
        )
    return steps


def _summarize_counts(text: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for match in COUNT_RE.finditer(text):
        kind = match.group("kind")
        if kind == "error":
            kind = "errors"
        counts[kind] = counts.get(kind, 0) + int(match.group("count"))
    return counts


def _summarize_durations(text: str) -> list[dict[str, Any]]:
    durations: list[dict[str, Any]] = []
    for line in text.splitlines():
        match = DURATION_RE.match(line)
        if not match:
            continue
        durations.append(
            {
                "seconds": float(match.group("seconds")),
                "phase": match.group("phase"),
                "nodeid": match.group("nodeid"),
            }
        )
    return durations


def _duration_phase_summary(
    durations: list[dict[str, Any]],
) -> dict[str, dict[str, float | int]]:
    summary: dict[str, dict[str, float | int]] = {}
    for phase in ("setup", "call", "teardown"):
        values = sorted(
            float(item["seconds"]) for item in durations if item.get("phase") == phase
        )
        if not values:
            continue
        p95_index = min(len(values) - 1, int(len(values) * 0.95))
        summary[phase] = {
            "count": len(values),
            "median_seconds": round(statistics.median(values), 4),
            "p95_seconds": round(values[p95_index], 4),
            "max_seconds": round(values[-1], 4),
        }
    return summary


def _tail(text: str, *, max_lines: int = 80) -> str:
    if max_lines <= 0:
        return ""
    lines = text.splitlines()
    return "\n".join(lines[-max_lines:])


def _redact_log_text(text: str) -> str:
    """Apply the job-log redaction (paths, DSNs, secret and auth lines).

    Shared by the bounded tail and the uncapped failure list, so the list
    cannot print a credential the tail would have hidden.
    """
    from okg.substrate.support.bug_report import redact_text

    redacted, _markers = redact_text(text, repo_root=REPO_ROOT)
    redacted = QA_LOG_DSN_RE.sub("<dsn:redacted>", redacted)
    redacted = QA_LOG_SECRET_LINE_RE.sub(
        lambda match: f"{match.group('prefix')}<secret:redacted>",
        redacted,
    )
    return QA_LOG_AUTH_LINE_RE.sub(
        lambda match: f"{match.group('prefix')}<secret:redacted>",
        redacted,
    )


def _safe_log_tail(
    text: str,
    *,
    max_lines: int = 80,
    max_bytes: int = QA_LOG_TAIL_MAX_BYTES,
) -> str:
    """Return a redacted, line- and byte-bounded diagnostic tail."""
    redacted = _redact_log_text(_tail(text, max_lines=max_lines))
    payload = redacted.encode("utf-8")
    if len(payload) <= max_bytes:
        return redacted
    if max_bytes <= 0:
        return ""
    # Keep the marker on the first retained line so byte truncation never
    # turns an N-line tail into N+1 lines.
    marker = "[... earlier diagnostic bytes truncated ...] "
    marker_bytes = marker.encode("utf-8")
    if max_bytes <= len(marker_bytes):
        return marker_bytes[:max_bytes].decode("utf-8", errors="ignore")
    keep = max_bytes - len(marker_bytes)
    suffix = payload[-keep:].decode("utf-8", errors="ignore")
    return marker + suffix


def _first_failure_summary(text: str) -> str | None:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("FAILED "):
            return stripped
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("ERROR "):
            return stripped
    return None


def _optional_deployments(lane: dict[str, Any] | list[str]) -> dict[str, Any]:
    if isinstance(lane, dict):
        names = [str(name) for name in lane.get("optional_deployments", [])]
    else:
        names = [str(name) for name in lane]
    absent = [name for name in names if not (REPO_ROOT / "deployments" / name).exists()]
    present = [name for name in names if name not in absent]
    skipped = [
        {
            "deployment": name,
            "reason": f"deployments/{name} is not present in this checkout",
            "count": None,
        }
        for name in absent
    ]
    return {
        "declared": names,
        "present": present,
        "absent": absent,
        "skipped": skipped,
    }


def _lane_optional_deployment_names(
    manifest: dict[str, Any],
    lane_name: str,
    *,
    _seen: set[str] | None = None,
) -> list[str]:
    seen = _seen or set()
    if lane_name in seen:
        raise QALaneError(f"cycle in composed lane graph at {lane_name!r}")
    seen.add(lane_name)
    lane = manifest["lanes"][lane_name]
    names: list[str] = []
    for name in lane.get("optional_deployments", []):
        value = str(name)
        if value not in names:
            names.append(value)
    for child in lane.get("composed_of", []):
        for name in _lane_optional_deployment_names(
            manifest,
            str(child),
            _seen=seen.copy(),
        ):
            if name not in names:
                names.append(name)
    return names


def _lane_requires_postgres(
    manifest: dict[str, Any],
    lane_name: str,
    *,
    _seen: set[str] | None = None,
) -> bool:
    seen = _seen or set()
    if lane_name in seen:
        raise QALaneError(f"cycle in composed lane graph at {lane_name!r}")
    seen.add(lane_name)
    lane = manifest["lanes"][lane_name]
    requires = lane.get("requires") or {}
    if isinstance(requires, dict) and requires.get("postgres"):
        return True
    return any(
        _lane_requires_postgres(manifest, str(child), _seen=seen.copy())
        for child in lane.get("composed_of", [])
    )


def _lane_env(
    lane: dict[str, Any],
    *,
    requires_postgres: bool | None = None,
) -> dict[str, str]:
    env = dict(os.environ)
    # Scheduler overrides are consumed while resolving this QA command. They
    # must not leak into tests that import qa.py and resolve a different lane.
    env.pop(QA_PYTEST_DIST_ENV, None)
    if requires_postgres is None:
        requires = lane.get("requires") or {}
        requires_postgres = bool(
            isinstance(requires, dict) and requires.get("postgres")
        )
    if requires_postgres:
        if "OKG_TEST_DSN" not in env and "OKG_TEST_DSN_TEMPLATE" not in env:
            env["OKG_TEST_DSN_TEMPLATE"] = DEFAULT_POSTGRES_TEST_DSN_TEMPLATE
            env.setdefault(TRUST_EPHEMERAL_ENV, "1")
    if lane.get("live_output"):
        # The selected child process writes to pipes so the QA runner can retain
        # the same report tails and summaries. Force Python children to flush
        # progress promptly enough for a CI log to identify a stuck test.
        env.setdefault("PYTHONUNBUFFERED", "1")
    return env


def _proc_start_epoch(pid: int) -> tuple[str, int, float] | None:
    """(command name, parent pid, start time) of a Linux process, from /proc."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        boot = next(
            float(line.split()[1])
            for line in Path("/proc/stat").read_text(encoding="utf-8").splitlines()
            if line.startswith("btime ")
        )
    except (OSError, StopIteration, ValueError, IndexError):
        return None
    name = stat[stat.index("(") + 1 : stat.rindex(")")]
    fields = stat[stat.rindex(")") + 2 :].split()
    # fields[0] is field 3 (state): ppid is field 4, starttime field 22.
    parent = int(fields[1])
    started = boot + int(fields[19]) / os.sysconf("SC_CLK_TCK")
    return name, parent, started


def _job_started_at(env: dict[str, str]) -> tuple[float | None, str]:
    raw = env.get(JOB_STARTED_AT_ENV)
    if raw:
        return float(raw), JOB_STARTED_AT_ENV
    pid = os.getpid()
    for _ in range(32):
        info = _proc_start_epoch(pid)
        if info is None:
            break
        name, parent, started = info
        if name == "Runner.Worker":
            return started, f"Runner.Worker pid {pid}"
        if parent <= 1:
            break
        pid = parent
    return None, "no Runner.Worker ancestor and no " + JOB_STARTED_AT_ENV


def _job_stack_dump_at(env: dict[str, str]) -> tuple[float | None, str]:
    """When to dump stacks: a margin before the job's limit, or (None, why)."""
    raw_limit = env.get(JOB_TIMEOUT_MINUTES_ENV)
    if not raw_limit:
        return None, f"{JOB_TIMEOUT_MINUTES_ENV} is not set"
    limit_seconds = float(raw_limit) * 60
    if limit_seconds <= 0:
        raise QALaneError(f"{JOB_TIMEOUT_MINUTES_ENV} must be positive, not {raw_limit!r}")
    started, source = _job_started_at(env)
    if started is None:
        return None, source
    margin = min(STACK_DUMP_MARGIN_SECONDS, limit_seconds / 4)
    return started + limit_seconds - margin, (
        f"job start from {source}, limit {raw_limit} min, margin {margin:g}s"
    )


def _platform_context() -> dict[str, Any]:
    context: dict[str, Any] = {
        "system": platform.system(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
    }
    try:
        disk = shutil.disk_usage(REPO_ROOT)
        context["disk"] = {
            "status": "available",
            "total_bytes": disk.total,
            "free_bytes": disk.free,
        }
    except OSError as exc:
        context["disk"] = {"status": "unavailable", "reason": str(exc)}

    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        values: dict[str, int] = {}
        try:
            for line in meminfo.read_text(encoding="utf-8").splitlines():
                name, separator, raw = line.partition(":")
                if separator and raw.strip().endswith(" kB"):
                    values[name] = int(raw.strip().split()[0]) * 1024
            context["memory"] = {
                "status": "available",
                "total_bytes": values["MemTotal"],
                "available_bytes": values["MemAvailable"],
            }
        except (KeyError, OSError, ValueError) as exc:
            context["memory"] = {
                "status": "unavailable",
                "reason": str(exc),
            }
    else:
        context["memory"] = {
            "status": "unavailable",
            "reason": "/proc/meminfo is not available on this platform",
        }
    try:
        one, five, fifteen = os.getloadavg()
        context["load_average"] = {
            "status": "available",
            "one_minute": one,
            "five_minutes": five,
            "fifteen_minutes": fifteen,
        }
    except (AttributeError, OSError) as exc:
        context["load_average"] = {
            "status": "unavailable",
            "reason": str(exc),
        }
    return context


# ---------------------------------------------------------------------------
# What a killed lane was waiting on, asked of the SERVER (#substrate-db kills).
#
# The substrate-db lane is killed by its process timeout in roughly one run in
# five, and every killed run so far shows a 2-CPU runner at ~1.2 load for the
# whole window against ~2.4 on a clean run: one of the two xdist workers is
# BLOCKED, not busy. A blocked worker leaves no trace in pytest's own output --
# the blocking wait is inside Postgres. So before the child is signalled, ask
# the server directly which backends exist, what each is waiting on, and which
# lock requests are ungranted, and keep the answer in the uploaded artifact.
#
# THIS IS DIAGNOSTIC ONLY. It never changes a verdict, never raises, and never
# delays the kill by more than its own bounded budget: a 5s connect timeout, a
# 5s server-side statement timeout, a row cap, and a single attempt per
# candidate DSN. Every failure is RECORDED in the snapshot rather than
# swallowed -- an unreachable server produces a file that says so, which is
# itself evidence, not a default standing in for a measurement.
PG_SNAPSHOT_CONNECT_TIMEOUT_SECONDS = 5
PG_SNAPSHOT_STATEMENT_TIMEOUT = "5s"
PG_SNAPSHOT_ROW_LIMIT = 50
PG_SNAPSHOT_SCHEMA = "okg.qa.timeout_postgres_activity.v1"
PG_ACTIVITY_SQL = """
SELECT
    pid,
    datname,
    state,
    wait_event_type,
    wait_event,
    query_start,
    state_change,
    pg_blocking_pids(pid) AS blocking_pids,
    left(query, 200) AS query
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
ORDER BY query_start NULLS LAST
LIMIT %s
"""
PG_BLOCKED_LOCKS_SQL = """
SELECT
    pid,
    locktype,
    mode,
    granted,
    relation::regclass::text AS relation,
    transactionid::text AS transactionid,
    -- classid/objid/objsubid are the ONLY way to tell one advisory lock from
    -- another, and a contended per-alias advisory lock is this lane's
    -- documented blocking mode. Without them the artifact says "an advisory
    -- lock" and names no key, which does not narrow anything.
    classid,
    objid,
    objsubid
FROM pg_locks
WHERE NOT granted
LIMIT %s
"""


def _jsonable(value: Any) -> Any:
    """Render one psycopg cell as JSON without guessing at its meaning."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        # pg_blocking_pids() returns an int[]. Rendered with str() it becomes
        # the STRING "[69]", which a reader has to parse back out of JSON that
        # could have held the array itself.
        return [_jsonable(item) for item in value]
    return str(value)


def _replace_dsn_database(dsn: str, database: str) -> str | None:
    """Point a DSN at another database on the SAME server, or give up."""
    try:
        parts = urlsplit(dsn)
    except ValueError:
        return None
    if not parts.scheme or not parts.netloc:
        return None
    return urlunsplit(
        (parts.scheme, parts.netloc, f"/{database}", parts.query, parts.fragment)
    )


def _postgres_snapshot_dsn_candidates(env: dict[str, str]) -> list[tuple[str, str]]:
    """Ordered (source, dsn) pairs to try for a cluster-wide activity read.

    ``pg_stat_activity`` and ``pg_locks`` are CLUSTER-wide views, so any
    database on the server answers for every worker. The per-worker template
    is therefore usable even though the lane's own worker databases are named
    per worker: substitute one, and fall back to the maintenance database on
    the same server when that one has already been dropped.
    """
    candidates: list[tuple[str, str]] = []

    def add(source: str, dsn: str | None) -> None:
        if dsn and all(dsn != known for _, known in candidates):
            candidates.append((source, dsn))

    add("OKG_CI_POSTGRES_DSN", env.get("OKG_CI_POSTGRES_DSN"))
    add("OKG_TEST_DSN", env.get("OKG_TEST_DSN"))
    template = env.get("OKG_TEST_DSN_TEMPLATE")
    if template:
        add("OKG_TEST_DSN_TEMPLATE", template.replace("{workerid}", "gw0"))
        add(
            "OKG_TEST_DSN_TEMPLATE:maintenance",
            _replace_dsn_database(template.replace("{workerid}", "gw0"), "postgres"),
        )
    return candidates


def _postgres_activity_snapshot(env: dict[str, str]) -> dict[str, Any]:
    """Return a bounded server-side snapshot. Never raises."""
    snapshot: dict[str, Any] = {
        "schema": PG_SNAPSHOT_SCHEMA,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "status": "unavailable",
        "reason": "the lane environment names no Postgres DSN",
        "dsn_source": None,
        "attempts": [],
        "activity": [],
        "blocked_locks": [],
    }
    try:
        import psycopg
    except ImportError as exc:
        snapshot["reason"] = f"psycopg is not importable: {exc}"
        return snapshot
    candidates = _postgres_snapshot_dsn_candidates(env)
    for source, dsn in candidates:
        try:
            with psycopg.connect(
                dsn,
                connect_timeout=PG_SNAPSHOT_CONNECT_TIMEOUT_SECONDS,
                autocommit=True,
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"SET statement_timeout = '{PG_SNAPSHOT_STATEMENT_TIMEOUT}'"
                    )
                    # Collected into LOCALS and published only once both
                    # reads have succeeded. Writing them into the snapshot as
                    # they arrive would leave a half-read attempt's rows in a
                    # file whose status says "unavailable" -- rows a reader
                    # would take for the state of the server at the kill.
                    collected: dict[str, list[dict[str, Any]]] = {}
                    for key, sql in (
                        ("activity", PG_ACTIVITY_SQL),
                        ("blocked_locks", PG_BLOCKED_LOCKS_SQL),
                    ):
                        cursor.execute(sql, (PG_SNAPSHOT_ROW_LIMIT,))
                        columns = [column.name for column in cursor.description or []]
                        collected[key] = [
                            {
                                name: _jsonable(value)
                                for name, value in zip(columns, row)
                            }
                            for row in cursor.fetchall()
                        ]
        except Exception as exc:  # noqa: BLE001 - recorded, never swallowed
            snapshot["attempts"].append(
                {"source": source, "error": f"{type(exc).__name__}: {exc}"}
            )
            continue
        snapshot.update(collected)
        snapshot["status"] = "available"
        snapshot["dsn_source"] = source
        snapshot["reason"] = None
        return snapshot
    if candidates:
        snapshot["reason"] = "every candidate Postgres DSN failed; see attempts"
    return snapshot


def _postgres_activity_summary(snapshot: dict[str, Any]) -> str:
    """One bounded, log-safe paragraph naming what the server was doing."""
    if snapshot.get("status") != "available":
        attempts = snapshot.get("attempts") or []
        detail = "; ".join(
            f"{attempt.get('source')}: {attempt.get('error')}" for attempt in attempts
        )
        return _safe_log_tail(
            f"postgres activity unavailable: {snapshot.get('reason')}"
            + (f" ({detail})" if detail else ""),
            max_lines=4,
        )
    activity = snapshot.get("activity") or []
    blocked = snapshot.get("blocked_locks") or []
    lines = [
        f"postgres activity via {snapshot.get('dsn_source')}: "
        f"{len(activity)} backend(s), {len(blocked)} ungranted lock request(s)"
    ]
    for row in activity[:20]:
        lines.append(
            f"  pid={row.get('pid')} db={row.get('datname')} "
            f"state={row.get('state')} "
            f"wait={row.get('wait_event_type')}/{row.get('wait_event')} "
            f"since={row.get('query_start')} "
            f"blocked_by={row.get('blocking_pids')} "
            f"query={row.get('query')!r}"
        )
    for row in blocked[:20]:
        lines.append(
            f"  BLOCKED pid={row.get('pid')} {row.get('locktype')} "
            f"{row.get('mode')} relation={row.get('relation')} "
            f"key={row.get('classid')}/{row.get('objid')}/{row.get('objsubid')}"
        )
    return _safe_log_tail("\n".join(lines), max_lines=48)


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    """Replace a report atomically so a killed child cannot truncate it."""
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _available_identity(value: str | None, *, reason: str) -> dict[str, Any]:
    if value:
        return {"status": "available", "value": value}
    return {"status": "unavailable", "reason": reason}


def _partition_kind(lane_name: str, selected_step: str | None) -> str:
    """Which partitioned run ``lane_name`` / ``selected_step`` is: fast or db.

    Refuses every other combination: a partition flag on a lane with no
    partition plugin would run the whole lane and report it as one shard.
    """
    if lane_name == "fast":
        return "fast"
    if lane_name == DB_PARTITION_LANE and selected_step == DB_PARTITION_STEP:
        return "db"
    if lane_name in WHOLE_LANE_PARTITION_LANES and selected_step is None:
        return "db"
    raise QALaneError(
        "partition arguments are only valid for the fast lane, for "
        f"{DB_PARTITION_LANE} --step {DB_PARTITION_STEP}, and for the whole "
        f"{DB_FULL_PARTITION_LANE} and {COVERAGE_FULL_PARTITION_LANE} lanes"
    )


def _db_partition_identity(
    index: int | None, count: int | None, lane_name: str = DB_PARTITION_LANE
) -> dict[str, Any]:
    """The runtime-identity record of one substrate-db shard.

    The weights table's digest is part of it: two shards that read different
    tables could each be complete and still overlap or leave a file out.
    """
    table = DB_PARTITION_WEIGHTS_BY_LANE[lane_name]
    weights = REPO_ROOT / table
    try:
        digest = f"sha256:{hashlib.sha256(weights.read_bytes()).hexdigest()}"
    except OSError as error:
        raise QALaneError(f"substrate-db weights table unreadable: {error}") from error
    return {
        "index": index,
        "count": count,
        "xdist_distribution": "step",
        "ownership": {
            "algorithm": "weighted-largest-first",
            "weights": table.as_posix(),
            "weights_digest": digest,
            "unmeasured_file_weight": "median",
            "input": "normalized-test-file-path",
        },
    }


def _runtime_identity(
    lane_name: str,
    lane: dict[str, Any],
    *,
    partition_index: int | None = None,
    partition_count: int | None = None,
    selected_step: str | None = None,
) -> dict[str, Any]:
    partitioned = partition_index is not None and partition_count is not None
    partition_kind = _partition_kind(lane_name, selected_step) if partitioned else None

    def selector_step(step: dict[str, Any]) -> dict[str, Any]:
        selected = {
            key: step.get(key)
            for key in ("kind", "paths", "markers", "args", "xdist")
            if key in step
        }
        if partition_kind == "fast" and "args" in selected:
            selected["args"] = [
                (
                    f"--dist={FAST_PARTITION_DISTRIBUTION}"
                    if str(arg).startswith("--dist=")
                    else arg
                )
                for arg in selected["args"]
            ]
        return selected

    selector_steps = [
        step
        for step in lane.get("steps", [])
        if selected_step is None or str(step.get("name")) == selected_step
    ]
    if (
        partition_kind == "db"
        and lane_name in WHOLE_LANE_PARTITION_LANES
        and partition_index != DB_FULL_WHOLE_STEPS_SHARD
    ):
        selector_steps = [
            step
            for step in selector_steps
            if str(step.get("name")) == DB_PARTITION_STEPS[lane_name]
        ]
    if partition_kind == "db" and lane_name == COVERAGE_FULL_PARTITION_LANE:
        selector_steps = [_coverage_leg_step(step) for step in selector_steps]
    selector = {
        "lane": lane_name,
        "selected_step": selected_step,
        "partition": (
            {
                "index": partition_index,
                "count": partition_count,
                "xdist_distribution": FAST_PARTITION_DISTRIBUTION,
                "ownership": {
                    "algorithm": "blake2b",
                    "seed": FAST_PARTITION_SEED,
                    "key_encoding": "uint64-big-endian",
                    "digest_bytes": 8,
                    "input": "normalized-test-file-path",
                },
            }
            if partition_kind == "fast"
            else _db_partition_identity(partition_index, partition_count, lane_name)
            if partition_kind == "db"
            else None
        ),
        "steps": [selector_step(step) for step in selector_steps],
    }
    encoded = json.dumps(selector, sort_keys=True, separators=(",", ":")).encode()
    return {
        "partition": selector["partition"],
        "head_sha": _available_identity(
            os.environ.get("GITHUB_SHA"),
            reason="GITHUB_SHA is unavailable outside GitHub Actions",
        ),
        "runner_identity": _available_identity(
            os.environ.get("RUNNER_NAME"),
            reason="RUNNER_NAME is unavailable outside GitHub Actions",
        ),
        "selector_digest": f"sha256:{hashlib.sha256(encoded).hexdigest()}",
        "selector": selector,
        # The count floor is MEASURED from the base tree by
        # scripts/ci/prove_fast_linux_selection.py and written down nowhere.
        # This is the reviewed POLICY around it -- how many collected tests
        # one change may remove unremarked -- so a report explains its own
        # admission bound.
        "baseline_drop_allowance": resolve_baseline_drop_allowance(lane),
    }


DEFAULT_BASELINE_DROP_ALLOWANCE = 0
#: The largest removal budget a lane may declare. The budget is the one blind
#: spot of the base-tree floor, and nothing else bounds it now that the
#: committed count is gone. 250 is about 1% of the fast lane's ~23,300
#: collected tests (2026-09-26), and it is a fixed number rather than a share
#: of the base count so the blind spot does not widen as the suite grows. A
#: deliberate removal larger than this raises the cap here, where a reviewer
#: sees it.
MAX_BASELINE_DROP_ALLOWANCE = 250
#: `enforce_baseline_test_count`'s source when no base-tree count exists.
BASELINE_UNAVAILABLE_SOURCE = "unavailable"


def resolve_baseline_drop_allowance(lane: dict[str, Any]) -> int:
    """Resolve how many collected tests one change may remove unremarked.

    This is reviewed POLICY, not measurement, and it does not move as the tree
    grows: it is the deliberate-removal budget a single change gets against
    the count its own base collects. Absence means ZERO.
    """
    declared = lane.get("baseline_drop_allowance")
    if declared is None:
        return DEFAULT_BASELINE_DROP_ALLOWANCE
    if isinstance(declared, bool) or not isinstance(declared, int) or declared < 0:
        raise QALaneError(
            f"baseline_drop_allowance must be a non-negative integer, got {declared!r}"
        )
    if declared > MAX_BASELINE_DROP_ALLOWANCE:
        raise QALaneError(
            f"baseline_drop_allowance {declared} exceeds the cap of "
            f"{MAX_BASELINE_DROP_ALLOWANCE} (scripts/qa.py::MAX_BASELINE_DROP_ALLOWANCE); "
            "a larger deliberate removal raises the cap in the same change"
        )
    return declared


def enforce_baseline_test_count(
    lane: dict[str, Any],
    collected: int,
    *,
    lane_name: str,
    derived_floor: int | None = None,
    derived_floor_source: str | None = None,
    manifest_path: str = "tests/qa_lanes.yaml",
) -> dict[str, Any]:
    """Refuse a collection that lost tests against the change's own base tree.

    The hazard is tests DISAPPEARING from the lane. The reference is DERIVED:
    the caller measures what the change's base tree collects, in the same
    job, and passes it as `derived_floor`. The count is not in the pull
    request diff, so no branch edits it and it cannot go stale (issue #1396).
    The removal budget IS in the diff: a change that widens it widens its own
    blind spot, which is why it is a small reviewed number and why the
    refusal names it.

        collected <  derived - drop_allowance  -> REFUSE
        anything at or above that              -> pass (growth never refuses)

    Without a derived floor there is nothing to compare against: the result's
    `source` is `BASELINE_UNAVAILABLE_SOURCE`. The caller decides what that
    means; the selection proof refuses on every event but a push. A committed `baseline_test_count` used to stand in on that path; it
    was removed by PACT ci-one-fast-lane-completeness because it read 17633
    while about 23,300 tests ran, so it guarded almost nothing.

    This check was never identity-level: a same-size swap passes any count
    comparison. Completeness -- every collected test runs in exactly one
    partition -- is the same-run partition proof (union equality and
    pairwise disjointness), which this does not touch.
    """
    drop_allowance = resolve_baseline_drop_allowance(lane)
    if derived_floor is None:
        if derived_floor_source is not None:
            raise QALaneError(
                "a derived baseline source was named without a derived floor: "
                f"{derived_floor_source!r}"
            )
        return {
            "collected": collected,
            "effective_floor": None,
            "derived_floor": None,
            "source": BASELINE_UNAVAILABLE_SOURCE,
            "drop_allowance": drop_allowance,
        }
    if (
        isinstance(derived_floor, bool)
        or not isinstance(derived_floor, int)
        or derived_floor <= 0
    ):
        raise QALaneError(
            f"derived baseline floor must be a positive integer, got {derived_floor!r}"
        )
    if (
        not isinstance(derived_floor_source, str)
        or not derived_floor_source
        or derived_floor_source == BASELINE_UNAVAILABLE_SOURCE
    ):
        raise QALaneError(
            "a derived baseline floor must name the measurement it came from"
        )
    effective_floor = derived_floor - drop_allowance
    if collected < effective_floor:
        raise QALaneError(
            f"{lane_name} collected fewer tests than its base tree allows: "
            f"base={derived_floor}, collected={collected}, "
            f"drop_allowance={drop_allowance}, floor={effective_floor}. "
            f"The base count was measured from {derived_floor_source}. Tests "
            "disappeared from the lane; find and restore them, or -- if the "
            "removal is deliberate and larger than the allowance -- raise "
            f"lanes.{lane_name}.baseline_drop_allowance in {manifest_path} in "
            "this change so a reviewer sees the loss."
        )
    return {
        "collected": collected,
        "effective_floor": effective_floor,
        "derived_floor": derived_floor,
        "source": derived_floor_source,
        "drop_allowance": drop_allowance,
    }


def _budget_thresholds(
    lane_name: str,
    lane: dict[str, Any],
) -> tuple[Any, float | None]:
    """Resolve (hard ceiling, warn threshold) for a lane.

    Absence is never silently converted into a passing budget: a lane that
    declares enforce_budget without a budget_seconds is a configuration error,
    not a lane that passes every duration.
    """
    budget = lane.get("budget_seconds")
    declared_warn = lane.get("budget_warn_seconds")
    if budget is None:
        if lane.get("enforce_budget"):
            raise QALaneError(
                f"lane {lane_name!r} sets enforce_budget without a "
                "budget_seconds ceiling; an absent budget must not be "
                "treated as a budget every duration passes."
            )
        if declared_warn is not None:
            raise QALaneError(
                f"lane {lane_name!r} sets budget_warn_seconds without a "
                "budget_seconds ceiling; the warn threshold is defined "
                "relative to the ceiling."
            )
        return None, None
    ceiling = float(budget)
    # Checked BEFORE the derived-warn early return below. A lane that takes
    # the derived warn threshold -- which is most of them, `fast` included --
    # would otherwise skip this entirely, which is how the whole-lane half of
    # #1341 stayed invisible.
    _require_kill_above_ceiling(
        lane_name,
        ceiling=ceiling,
        raw_timeout=lane.get("process_timeout_seconds"),
        ceiling_key="budget_seconds",
        timeout_key="process_timeout_seconds",
        raw_ceiling=budget,
    )
    if declared_warn is None:
        return budget, ceiling * BUDGET_WARN_FRACTION
    warn = float(declared_warn)
    if warn <= 0:
        raise QALaneError(
            f"lane {lane_name!r} declares a non-positive "
            f"budget_warn_seconds ({declared_warn!r})."
        )
    if warn > ceiling:
        raise QALaneError(
            f"lane {lane_name!r} declares budget_warn_seconds "
            f"({declared_warn!r}) above its budget_seconds ceiling "
            f"({budget!r}); the warn threshold must sit at or below the "
            "blocking threshold."
        )
    return budget, warn


def _require_kill_above_ceiling(
    lane_name: str,
    *,
    ceiling: float,
    raw_timeout: Any,
    ceiling_key: str,
    timeout_key: str,
    raw_ceiling: Any,
) -> float | None:
    """Reject a process timeout that would preempt its own budget gate.

    Shared by the whole-lane and partitioned resolvers so the two cannot
    drift. #1341 was the partitioned instance (kill 530 under a 540
    ceiling); the whole-lane `fast` config carried the same defect in its
    milder equal-valued form, where a run killed at exactly the ceiling can
    only ever be reported ``status: timeout`` and never as a clean budget
    overrun. A ceiling the kill can preempt is not a ceiling.
    """
    if raw_timeout is None:
        return None
    process_timeout = float(raw_timeout)
    if process_timeout <= 0:
        raise QALaneError(f"{timeout_key} must be positive")
    if process_timeout <= ceiling:
        raise QALaneError(
            f"lane {lane_name!r} declares {timeout_key} ({raw_timeout!r}) at "
            f"or below its {ceiling_key} ceiling ({raw_ceiling!r}). The kill "
            "would preempt the budget gate, which could then never fire: "
            "every overrun would be reported as an opaque 'timeout' instead "
            "of as a budget overrun (issue #1341). The process timeout must "
            "sit STRICTLY ABOVE the ceiling."
        )
    return process_timeout


def _partition_budget_thresholds(
    lane_name: str,
    lane: dict[str, Any],
) -> tuple[float, float, float | None]:
    """Resolve (ceiling, warn, process timeout) for a PARTITIONED lane run.

    The partitioned path used to read ``partition_budget_seconds`` /
    ``partition_budget_warn_seconds`` / ``partition_process_timeout_seconds``
    straight out of the manifest, which meant none of the coherence checks
    ``_budget_thresholds`` performs for whole-lane budgets applied to them.
    That is how issue #1341's inversion shipped and survived: the kill
    (530s) sat BELOW the ceiling it was meant to protect (540s), so the
    budget gate was unreachable -- a partition could never be reported as
    "over budget", because pytest was always killed first and the report came
    back ``status: timeout`` instead.

    The ordering invariant is therefore enforced here rather than left to
    review:

        warn <= ceiling < process_timeout

    A run that crosses the ceiling must be allowed to FINISH so it is
    classified as a budget overrun with a complete report and a JUnit file.
    The process timeout is the runaway backstop for a genuinely hung
    process, and it only makes sense strictly above the ceiling.
    """
    budget = lane.get("partition_budget_seconds")
    warn = lane.get("partition_budget_warn_seconds")
    if budget is None or warn is None:
        raise QALaneError(
            "partitioned fast lane requires partition budget and warn thresholds"
        )
    ceiling = float(budget)
    warn_seconds = float(warn)
    if warn_seconds <= 0:
        raise QALaneError(
            f"lane {lane_name!r} declares a non-positive "
            f"partition_budget_warn_seconds ({warn!r})."
        )
    if warn_seconds > ceiling:
        raise QALaneError(
            f"lane {lane_name!r} declares partition_budget_warn_seconds "
            f"({warn!r}) above its partition_budget_seconds ceiling "
            f"({budget!r}); the warn threshold must sit at or below the "
            "blocking threshold."
        )
    process_timeout = _require_kill_above_ceiling(
        lane_name,
        ceiling=ceiling,
        raw_timeout=lane.get("partition_process_timeout_seconds"),
        ceiling_key="partition_budget_seconds",
        timeout_key="partition_process_timeout_seconds",
        raw_ceiling=budget,
    )
    return ceiling, warn_seconds, process_timeout


def _budget_status(
    duration: float,
    budget: Any,
    warn_seconds: float | None,
) -> str:
    """Classify a measured duration against the two thresholds."""
    if budget is None:
        return "no_budget"
    if duration > float(budget):
        return "exceeded"
    if warn_seconds is not None and duration > warn_seconds:
        return "warn"
    return "ok"


def _emit_budget_annotation(
    lane_name: str,
    *,
    status: str,
    duration: float,
    budget: Any,
    warn_seconds: float | None,
    enforce_budget: bool,
    timed_out: bool = False,
    overshoot_kind: str = "none",
) -> None:
    """Emit a GitHub workflow annotation for a crossed budget threshold.

    A warning that lives only in a log line is worth very little, so the
    crossing is written in the ``::warning``/``::error`` form the Actions UI
    surfaces on the job and in the run summary.

    ``timed_out`` changes BOTH crossing branches, because a killed run is a
    different event from a slow one and neither stock message describes it.

    In the ``warn`` branch the stock text asserts a NON-FATAL outcome, and
    #1341 printed "This does NOT fail the lane" directly above the errors
    where it very much did. That branch is now unreachable for a killed run
    under a correctly ordered config (a kill lands past the ceiling, so it
    classifies as ``exceeded``), but it is kept as a belt-and-braces guard:
    the annotation must never claim non-fatality for a run that was killed.

    The ``exceeded`` branch is the one an operator actually sees on a kill,
    and its stock text explains a budget verdict on a COMPLETED run -- which
    is wrong in the one way that matters, because a killed run wrote no
    JUnit XML and therefore caused every downstream "report is missing"
    error too. Naming that is the difference between one legible timeout and
    six unattributed failures.
    """
    if status not in {"warn", "exceeded"}:
        return
    warn_text = "unset" if warn_seconds is None else f"{warn_seconds:g}s"
    if status == "warn":
        if timed_out:
            print(
                f"::error title=QA lane killed below its ceiling ({lane_name})::"
                f"[qa:{lane_name}] duration {duration:.2f}s crossed the warn "
                f"threshold {warn_text} but the process was KILLED by the "
                "process timeout before reaching the hard ceiling "
                f"{budget}s. This DOES fail the lane: the run exits 124, its "
                "QA report says 'timeout', and no JUnit XML is produced. The "
                "process timeout is configured at or below the ceiling, which "
                "makes the budget gate unreachable -- fix the ordering rather "
                "than reading this as a test failure.",
                flush=True,
            )
            return
        print(
            f"::warning title=QA lane budget warning ({lane_name})::"
            f"[qa:{lane_name}] duration {duration:.2f}s crossed the warn "
            f"threshold {warn_text} (hard ceiling {budget}s). This does NOT "
            "fail the lane. It reports that the lane is running past its "
            "measured baseline, which is the regression signal a raised "
            "ceiling would otherwise hide.",
            flush=True,
        )
        return
    if timed_out:
        # The kill is the ONLY outcome that produces no JUnit XML, so it is
        # the only one whose downstream "report is missing" errors are
        # consequences rather than independent failures. #1341 was expensive
        # to diagnose precisely because nothing said so: the reader saw six
        # errors with no common cause. Say it here, where the cause is known.
        print(
            f"::error title=QA lane process timeout ({lane_name})::"
            f"[qa:{lane_name}] duration {duration:.2f}s reached the process "
            "timeout and pytest was KILLED (exit 124). Its QA report says "
            "'timeout' rather than 'complete' and NO JUnit XML was written, "
            "so any downstream 'report is missing' errors and the "
            "test-results-gate failure are CONSEQUENCES of this kill, not "
            "separate problems. Treat this as one timeout, not as a suite of "
            "test failures.",
            flush=True,
        )
        return
    if overshoot_kind == "runner_variance":
        # #899: the lane completed with zero failing tests. Duration alone
        # cannot separate that from a slow pool, so it is reported, not
        # failed. A ::warning, deliberately, not a ::error.
        print(
            f"::warning title=QA lane runner_variance ({lane_name})::"
            f"[qa:{lane_name}] duration {duration:.2f}s crossed the ceiling "
            f"{budget}s (warn threshold {warn_text}), but the lane RAN TO "
            "COMPLETION with zero failing tests. This is classified "
            "runner_variance and does NOT fail the lane: a completed run "
            "cannot be told apart from a slow runner pool by its duration. "
            "Runaway protection is the process timeout, which kills and "
            "fails a lane that never finishes.",
            flush=True,
        )
        return
    if enforce_budget:
        print(
            f"::error title=QA lane budget ceiling exceeded ({lane_name})::"
            f"[qa:{lane_name}] duration {duration:.2f}s exceeded the hard "
            f"ceiling {budget}s (warn threshold {warn_text}), AND the run "
            "did not complete cleanly. The lane FAILS. The run wrote its "
            "JUnit XML, so this is a verdict on real output rather than a "
            "truncated run.",
            flush=True,
        )
        return
    print(
        f"::warning title=QA lane budget ceiling exceeded ({lane_name})::"
        f"[qa:{lane_name}] duration {duration:.2f}s exceeded the hard ceiling "
        f"{budget}s (warn threshold {warn_text}). This lane is advisory "
        "(enforce_budget is not enabled), so it reports the overrun and does "
        "NOT fail.",
        flush=True,
    )


def _failed_test_count(step_reports: list[dict[str, Any]]) -> int:
    """Total failed + errored cases across a lane's steps.

    Read from each step's parsed pytest counts rather than from exit codes,
    because the question the job summary has to answer is "did any TEST
    fail", and a lane can exit non-zero for reasons that are not a test
    failure at all.
    """
    total = 0
    for step in step_reports:
        counts = step.get("counts") or {}
        # _summarize_counts normalises "error" to "errors", so only these
        # two keys are ever produced.
        for key in ("failed", "errors"):
            total += int(counts.get(key, 0) or 0)
    return total


def _passed_test_count(step_reports: list[dict[str, Any]]) -> int:
    """Total passed cases across a lane's steps.

    Needed so the summary can distinguish "every test passed" from "no test
    ran". A step that collected nothing is coerced to exit 0 by
    ``allow_empty`` (pytest's exit 5), and claiming every test passed on a
    run where none did is the one sentence the reader is meant to trust.
    """
    total = 0
    for step in step_reports:
        counts = step.get("counts") or {}
        for key in ("passed", "xpassed"):
            total += int(counts.get(key, 0) or 0)
    return total


def _budget_summary_markdown(
    lane_name: str,
    *,
    status: str,
    duration: float,
    budget: Any,
    warn_seconds: float | None,
    enforce_budget: bool,
    timed_out: bool,
    failed_tests: int,
    steps_exit_code: int,
    passed_tests: int = 0,
    overshoot_kind: str = "none",
    partition_index: int | None = None,
    partition_count: int | None = None,
) -> str | None:
    """Render the job-summary block for a crossed budget threshold.

    WHY THIS EXISTS (#899). A budget verdict used to be reported ONLY as a
    ``::error`` annotation and a stderr line. Annotations are collapsed in
    the Actions UI and absent from the job summary, so the reader of a red
    required lane saw a red check with no visible reason and reasonably
    concluded that a test had failed. A lane can be red here with every
    test passing and every step exiting 0 -- see the ``exit_code == 0``
    guard on the budget bump in ``run_lane`` -- and that outcome was the
    one the job page described least.

    The rule this enforces: a lane that ran zero failing tests is never red
    on the budget alone without the reason being written HERE, in the job
    summary, where it is the first thing the reader sees. The block names
    the overshoot in seconds AND percent, states the failing-test count
    explicitly, and says what the ceiling is for.

    Returns ``None`` when there is nothing to report -- the summary is not
    padded on an ordinary passing run.
    """
    if status not in {"warn", "exceeded"} and not timed_out:
        return None
    unit = lane_name
    if partition_index is not None and partition_count is not None:
        # Index, NOT a 1-based position. Partition 2 of 5 is the job named
        # `fast-linux-2`; printing "3/5" sent a reader to the wrong job.
        if lane_name in DB_PARTITION_JOBS_BY_LANE:
            job = DB_PARTITION_JOBS_BY_LANE[lane_name][partition_index]
        else:
            job = "fast-linux" if partition_index == 0 else f"fast-linux-{partition_index}"
        unit = (
            f"{lane_name} partition {partition_index} of {partition_count} (job {job})"
        )
    warn_text = "unset" if warn_seconds is None else f"{warn_seconds:g}s"
    ceiling_text = "unset" if budget is None else f"{float(budget):g}s"
    rows = [
        f"| measured duration | {duration:.2f}s |",
        f"| hard ceiling | {ceiling_text} |",
        f"| warn threshold | {warn_text} |",
        f"| passing tests | {passed_tests if not timed_out else 'unknown'} |",
        f"| failing tests | {failed_tests if not timed_out else 'unknown'} |",
        f"| step exit code before the budget verdict | {steps_exit_code} |",
    ]
    if budget is not None and duration > float(budget):
        overshoot = duration - float(budget)
        # A zero ceiling has no meaningful percentage, and dividing by it
        # would turn a REPORTING path into a crash that erases the verdict
        # it exists to explain. Report the seconds and say the rest is
        # undefined rather than inventing a number.
        percent = (
            f"{100.0 * overshoot / float(budget):.2f}% over the ceiling"
            if float(budget) > 0
            else "percentage undefined against a zero ceiling"
        )
        rows.insert(2, f"| overshoot | {overshoot:.2f}s ({percent}) |")
    table = "\n".join(["| | |", "| --- | --- |", *rows])

    if timed_out:
        headline = f"### QA lane `{unit}`: KILLED by the process timeout"
        body = (
            "pytest was killed (exit 124), so this run wrote **no JUnit XML**. "
            "Every downstream `report is missing` error and the "
            "`test-results-gate` failure are CONSEQUENCES of this kill, not "
            "separate problems. Treat this as one timeout, not as a suite of "
            "test failures."
        )
    elif overshoot_kind == "runner_variance":
        headline = (
            f"### QA lane `{unit}`: over the ceiling, NOT failed (runner_variance)"
        )
        first = (
            f"**All {passed_tests} tests passed and every step exited 0.**"
            if passed_tests > 0
            else "**No test failed -- but note that NO TEST RAN either; this "
            "lane reported zero passing cases.**"
        )
        body = (
            f"{first} The lane ran past its ceiling and **this does not fail "
            "it**. A run that completed cannot be told apart from a slow "
            "runner pool by its duration alone, so the overshoot is reported "
            "rather than blocking.\n\n"
            "Runaway protection is the process timeout: a lane that never "
            "finishes is killed and DOES fail. If this overshoot is large or "
            "persistent, it is a real slowdown -- check the warn threshold "
            "and the recorded p95 in `.github/ci/` rather than treating this "
            "as a pull-request failure."
        )
    elif status == "exceeded" and enforce_budget:
        if failed_tests == 0 and steps_exit_code == 0:
            headline = (
                f"### QA lane `{unit}`: red on the TIME BUDGET only -- no test failed"
            )
            # "Every test passed" is FALSE when none ran, and a lane that
            # collected nothing is coerced to exit 0 by allow_empty. Say
            # which of the two actually happened.
            first = (
                f"**All {passed_tests} tests passed and every step exited 0.**"
                if passed_tests > 0
                else "**No test failed -- but note that NO TEST RAN either; "
                "this lane reported zero passing cases.**"
            )
            body = (
                f"{first} This lane is "
                "red solely because its measured duration crossed the hard "
                "ceiling. Nothing in the diff failed a test.\n\n"
                "The ceiling is a time budget, not a performance target. "
                "Where a lane has a measured record under `.github/ci/`, "
                "re-derive its ceiling from that record rather than "
                "hand-editing the pin in `tests/qa_lanes.yaml`; where it has "
                "none, the pin is a hand-set number and moving it needs a "
                "measurement first."
            )
        else:
            headline = f"### QA lane `{unit}`: over the hard ceiling AND tests failed"
            body = (
                "This run crossed the hard ceiling and also has failing tests. "
                "Fix the failures first; the budget verdict is not the cause "
                "of them."
            )
    elif status == "exceeded":
        headline = f"### QA lane `{unit}`: over the hard ceiling (advisory)"
        body = (
            "`enforce_budget` is not enabled for this lane, so the overrun is "
            "reported and does NOT fail the lane."
        )
    else:
        headline = f"### QA lane `{unit}`: past its warn threshold"
        body = (
            "This does **not** fail the lane. It reports that the lane is "
            "running past its measured baseline, which is the regression "
            "signal a raised ceiling would otherwise hide."
        )
    return f"{headline}\n\n{body}\n\n{table}\n"


def _write_job_summary(block: str | None) -> None:
    """Append a rendered block to ``$GITHUB_STEP_SUMMARY``.

    Silent outside Actions, where the variable is unset. A summary that
    cannot be written must never change the lane's verdict, so an I/O error
    is reported on stderr and swallowed rather than turning a budget verdict
    into a crash.
    """
    if not block:
        return
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if not target:
        return
    try:
        with open(target, "a", encoding="utf-8") as handle:
            handle.write(f"\n{block}\n")
    except OSError as exc:  # pragma: no cover - defensive
        print(f"[qa] could not write job summary: {exc}", file=sys.stderr)


def _performance_context(
    lane_name: str,
    *,
    budget_seconds: Any,
    budget_passed: bool | None,
    warn_seconds: float | None = None,
    warn_passed: bool | None = None,
    budget_status: str = "no_budget",
    duration_seconds: float | None,
    steps: list[ResolvedStep],
    dry_run: bool,
    platform: dict[str, Any],
) -> dict[str, Any]:
    """``platform`` is the report's ONE observation of the machine, and it is
    REQUIRED: there is deliberately no default that would let this function
    sample again. Sampling it again here made `performance_context.platform` and
    the report's own `platform` two independent reads of a LIVE load average,
    which disagree whenever load moves between them -- a flake that only looks
    stable on an idle machine. A caller that forgets the snapshot must fail
    loudly with a TypeError, not silently reopen the flake."""
    if lane_name != "perf":
        return {"enabled": False}
    return {
        "enabled": True,
        "dry_run": dry_run,
        "budget_name": "perf lane wallclock",
        "observed_value": duration_seconds,
        "unit": "seconds",
        "threshold": budget_seconds,
        "passed": budget_passed,
        "warn_threshold": warn_seconds,
        "warn_passed": warn_passed,
        "status": budget_status,
        "platform": platform,
        "commands": [
            {
                "name": step.name,
                "kind": step.kind,
                "command": step.command,
            }
            for step in steps
        ],
    }


def _command_uses_xdist(command: list[str]) -> bool:
    return "-n" in command or "--numprocesses" in command


def _validate_lane_environment(
    lane: dict[str, Any],
    steps: list[ResolvedStep],
    *,
    serial: bool,
    requires_postgres: bool | None = None,
) -> None:
    if requires_postgres is None:
        requires = lane.get("requires") or {}
        requires_postgres = bool(
            isinstance(requires, dict) and requires.get("postgres")
        )
    if not requires_postgres:
        return
    if serial or not any(_command_uses_xdist(step.command) for step in steps):
        return
    if "OKG_TEST_DSN" in os.environ:
        raise QALaneError(
            "Postgres lane uses pytest-xdist, but OKG_TEST_DSN is a single "
            "shared database for all workers. Unset OKG_TEST_DSN and use "
            "OKG_TEST_DSN_TEMPLATE=postgres://postgres:okg@127.0.0.1:5433/"
            "okg_test_{workerid}, or rerun with --serial."
        )


JUNIT_SHARD_DIRNAME = "qa-junit-shards"
JUNIT_GATE_PATH = REPO_ROOT / "scripts" / "ci" / "junit_gate.py"
FAILURE_LIST_BEGIN = "[qa:{lane}] ===== failing tests from {junit} (begin) ====="
FAILURE_LIST_END = "[qa:{lane}] ===== failing tests (end) ====="
# A junit file whose mtime is older than the lane's start (less this slack for
# one-second filesystem timestamps) was not written by this run.
JUNIT_STALE_SLACK_SECONDS = 1.0


def _load_junit_gate() -> Any:
    """Load ``scripts/ci/junit_gate.py`` by path (it is a script, not a package)."""
    import importlib.util

    name = "_okg_qa_junit_gate"
    loaded = sys.modules.get(name)
    if loaded is not None:
        return loaded
    spec = importlib.util.spec_from_file_location(name, JUNIT_GATE_PATH)
    if spec is None or spec.loader is None:
        raise QALaneError(f"cannot load {JUNIT_GATE_PATH}")
    module = importlib.util.module_from_spec(spec)
    # Registered before exec: dataclasses resolve their module through
    # sys.modules while the class body runs.
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(name, None)
        raise
    return module


def _report_junit_failures(
    lane_name: str, junit_target: Path, *, lane_started_wall: float
) -> None:
    """Print every failing test id from the lane's own junit (issue #2554).

    The per-step log tail is capped at QA_LOG_TAIL_MAX_BYTES, and a lane that
    never runs junit_gate.py (the nightly substrate-db-full) had no other
    surface naming its failures once artifact uploads stopped. This list is
    printed OUTSIDE that cap, between marker lines, and appended to the job
    summary. It only reports: the lane's exit code, verdict and test selection
    are unchanged, and any error here is printed rather than raised.
    """
    try:
        path = junit_target if junit_target.is_absolute() else REPO_ROOT / junit_target
        shown = _redact_log_text(str(path))
        if not path.exists():
            print(
                f"[qa:{lane_name}] failure list unavailable: junit {shown} "
                "was not written",
                file=sys.stderr,
            )
            return
        mtime = path.stat().st_mtime
        if mtime < lane_started_wall - JUNIT_STALE_SLACK_SECONDS:
            print(
                f"[qa:{lane_name}] failure list unavailable: junit {shown} is "
                "older than this lane run (stale file from an earlier run)",
                file=sys.stderr,
            )
            return
        gate = _load_junit_gate()
        result, missing, corrupt = gate.gate_files([path])
        if corrupt or missing:
            print(
                f"[qa:{lane_name}] failure list unavailable: junit {shown} "
                "could not be parsed",
                file=sys.stderr,
            )
            return
        if not result.problems:
            return
        # Only the REASONS are redacted: the secret-line pattern would eat
        # the name of any test whose module mentions a token or secret.
        lines = gate.failure_list_lines(result.problems, redact=_redact_log_text)
        failed = sum(1 for p in result.problems if p.kind != "error")
        errored = len(result.problems) - failed
        print(
            _redact_log_text(FAILURE_LIST_BEGIN.format(lane=lane_name, junit=path)),
            file=sys.stderr,
        )
        for line in lines:
            print(line, file=sys.stderr)
        print(FAILURE_LIST_END.format(lane=lane_name), file=sys.stderr)
        _write_job_summary(
            f"### Failing tests — {lane_name} ({failed} failed, {errored} errors)\n\n"
            + gate.fenced_text_block(gate.cap_lines_to_bytes(lines))
        )
    except Exception as exc:  # the report must never change the verdict
        print(
            f"[qa:{lane_name}] failure list could not be produced: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )


def _addopts_junit_path(addopts: str) -> Path | None:
    """The ``--junitxml`` target inside a ``PYTEST_ADDOPTS`` string, if any.

    Parsed with ``shlex`` rather than a regex so a quoted path, a path with
    spaces, and the ``--junitxml <path>`` spelling all resolve the way pytest
    resolves them. The result is anchored at ``REPO_ROOT``, because a lane's
    child processes run there and a relative option must not follow whatever
    directory the runner happened to be invoked from.

    pytest lets the LAST ``--junitxml`` win. Two of them make "which file is
    the receipt" ambiguous for sharding, so this refuses instead of guessing.
    """

    try:
        tokens = shlex.split(addopts or "")
    except ValueError as exc:
        raise QALaneError(f"PYTEST_ADDOPTS is not parseable: {exc}") from exc
    found: list[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token.startswith("--junitxml="):
            found.append(token.split("=", 1)[1])
        elif token == "--junitxml" and index + 1 < len(tokens):
            found.append(tokens[index + 1])
            index += 1
        index += 1
    if not found:
        return None
    if len(found) > 1:
        raise QALaneError(
            "PYTEST_ADDOPTS declares more than one --junitxml "
            f"({', '.join(found)}); a lane receipt must name exactly one file"
        )
    target = Path(found[0])
    return target if target.is_absolute() else REPO_ROOT / target


def _replace_addopts_junit(addopts: str, target: Path) -> str:
    """Point an addopts string at ``target``, leaving every other option alone."""

    try:
        tokens = shlex.split(addopts or "")
    except ValueError as exc:
        raise QALaneError(f"PYTEST_ADDOPTS is not parseable: {exc}") from exc
    rebuilt: list[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token.startswith("--junitxml="):
            rebuilt.append(f"--junitxml={target}")
        elif token == "--junitxml" and index + 1 < len(tokens):
            rebuilt.append(f"--junitxml={target}")
            index += 1
        else:
            rebuilt.append(token)
        index += 1
    return shlex.join(rebuilt)


def _merge_junit_shards(target: Path, shards: list[Path]) -> bool:
    """Combine per-step JUnit shards into the one declared authoritative file.

    WHY (issue #1474 / #1473). ``PYTEST_ADDOPTS`` is one string applied to every
    pytest child a lane starts. The substrate-db lane runs two pytest steps --
    ``core-db-contracts`` (433 tests) and ``serial-db-pact`` (40) -- so both
    children wrote ``.scratch/junit/substrate-db-linux.xml`` and the second
    TRUNCATED the first. The authoritative receipt then held 40 of 473 rows,
    and `test-results-gate` refused any change whose classified nodes lived in
    the missing 433. Worse, the loss appeared only when the lane fully passed:
    a failing first step ends the lane, so the file kept that step's rows and
    the bug hid behind the red it caused.

    Returns whether a merged file was written. Missing shards are skipped
    rather than invented: a step that never ran has no rows, and a step that
    ran must not have its rows replaced by an empty stand-in.
    """

    suites: list[ET.Element] = []
    for shard in shards:
        if not shard.exists():
            continue
        try:
            root = ET.parse(shard).getroot()
        except ET.ParseError:
            # A shard pytest could not finish writing is evidence of a broken
            # run, not of an empty one. Leave it out of the merge and say so;
            # the classified-node check downstream then reports the rows it
            # cannot find instead of accepting a silently short receipt.
            print(
                f"[qa] junit shard is unparseable and was not merged: {shard}",
                file=sys.stderr,
            )
            continue
        suites.extend(root.iter("testsuite"))
    if not suites:
        return False
    merged = ET.Element("testsuites")
    totals = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    elapsed = 0.0
    for suite in suites:
        for key in totals:
            try:
                totals[key] += int(suite.get(key) or 0)
            except ValueError:
                pass
        try:
            elapsed += float(suite.get("time") or 0.0)
        except ValueError:
            pass
        merged.append(suite)
    for key, value in totals.items():
        merged.set(key, str(value))
    merged.set("time", f"{elapsed:.3f}")
    target.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(merged).write(target, encoding="utf-8", xml_declaration=True)
    return True


def _run_step(
    step: ResolvedStep,
    *,
    env: dict[str, str],
    live_output: bool = False,
    timeout_seconds: float | None = None,
    progress: Any = None,
    runtime_report_path: Path | None = None,
    on_timeout: Callable[[], None] | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    child_env = dict(env)
    if runtime_report_path is not None:
        child_env["OKG_PYTEST_RUNTIME_REPORT"] = str(runtime_report_path)
    child = subprocess.Popen(
        step.command,
        cwd=REPO_ROOT,
        env=child_env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1 if live_output else -1,
        start_new_session=os.name != "nt",
    )
    restore = _forward_cancel_to(child)
    try:
        return _await_step(
            step,
            child,
            started=started,
            live_output=live_output,
            timeout_seconds=timeout_seconds,
            progress=progress,
            on_timeout=on_timeout,
            runtime_report_path=runtime_report_path,
        )
    finally:
        restore()


def _forward_cancel_to(child: subprocess.Popen) -> Callable[[], None]:
    """On SIGINT/SIGTERM: dump the child's stacks, stop it, then exit.

    GitHub cancels a job that reaches its limit with SIGINT, then SIGTERM.
    The child runs in its own session and would otherwise outlive qa.py with
    its stacks unprinted. SIGUSR1 makes every process that loaded
    okg.pytest_job_deadline print every thread's stack; the pause lets the
    live-output drain threads copy it into the job log before the group is
    terminated. Only the main thread can install handlers; elsewhere this is
    a no-op.
    """
    if os.name == "nt" or threading.current_thread() is not threading.main_thread():
        return lambda: None

    def _on_cancel(signum: int, _frame: Any) -> None:
        print(
            f"[qa] received {signal.Signals(signum).name}; asking the test "
            "processes for their stacks, then stopping them",
            file=sys.stderr,
            flush=True,
        )
        for sig, pause in ((signal.SIGUSR1, 2.0), (signal.SIGTERM, 0.0)):
            try:
                os.killpg(child.pid, sig)
            except ProcessLookupError:
                break
            time.sleep(pause)
        raise SystemExit(128 + signum)

    previous = {
        sig: signal.signal(sig, _on_cancel) for sig in (signal.SIGINT, signal.SIGTERM)
    }

    def _restore() -> None:
        for sig, handler in previous.items():
            signal.signal(sig, handler)

    return _restore


def _await_step(
    step: ResolvedStep,
    child: subprocess.Popen,
    *,
    started: float,
    live_output: bool,
    timeout_seconds: float | None,
    progress: Any,
    on_timeout: Callable[[], None] | None,
    runtime_report_path: Path | None,
) -> dict[str, Any]:
    timed_out = False

    def _stop_child() -> None:
        if child.poll() is not None:
            return
        # Ask for the evidence BEFORE the first signal. SIGTERM already begins
        # tearing the worker processes down, and a backend that has gone away
        # cannot say what it was waiting on. The hook is bounded by its own
        # budget and its failures are recorded inside the snapshot it writes,
        # so it can only ever delay the kill, never prevent it.
        if on_timeout is not None:
            try:
                on_timeout()
            except Exception as exc:  # noqa: BLE001 - a kill outranks diagnostics
                print(
                    f"[qa] timeout diagnostics failed: {type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )
        if os.name != "nt":
            os.killpg(child.pid, signal.SIGTERM)
        else:
            child.terminate()
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            if os.name != "nt":
                os.killpg(child.pid, signal.SIGKILL)
            else:
                child.kill()

    if live_output:
        assert child.stdout is not None
        assert child.stderr is not None
        stdout_chunks: list[str] = []
        stderr_chunks: list[str] = []

        def _drain(
            pipe: TextIO,
            chunks: list[str],
            sink: TextIO,
        ) -> None:
            for line in iter(pipe.readline, ""):
                chunks.append(line)
                safe = _safe_log_tail(line, max_lines=1)
                if safe:
                    print(safe, file=sink, flush=True)
            pipe.close()

        stdout_thread = threading.Thread(
            target=_drain,
            args=(child.stdout, stdout_chunks, sys.stdout),
            name=f"qa-{step.name}-stdout",
        )
        stderr_thread = threading.Thread(
            target=_drain,
            args=(child.stderr, stderr_chunks, sys.stderr),
            name=f"qa-{step.name}-stderr",
        )
        stdout_thread.start()
        stderr_thread.start()
        while child.poll() is None:
            elapsed = time.monotonic() - started
            if progress is not None:
                progress(elapsed)
            if timeout_seconds is not None and elapsed >= timeout_seconds:
                timed_out = True
                _stop_child()
                break
            time.sleep(min(5.0, max(0.05, timeout_seconds or 5.0)))
        returncode = child.wait()
        stdout_thread.join()
        stderr_thread.join()
        proc = subprocess.CompletedProcess(
            step.command,
            returncode,
            stdout="".join(stdout_chunks),
            stderr="".join(stderr_chunks),
        )
    else:
        while True:
            elapsed = time.monotonic() - started
            remaining = None if timeout_seconds is None else timeout_seconds - elapsed
            if remaining is not None and remaining <= 0:
                timed_out = True
                _stop_child()
                stdout, stderr = child.communicate()
                break
            try:
                stdout, stderr = child.communicate(
                    timeout=min(5.0, remaining) if remaining is not None else 5.0
                )
                break
            except subprocess.TimeoutExpired:
                if progress is not None:
                    progress(time.monotonic() - started)
        proc = subprocess.CompletedProcess(
            step.command,
            child.returncode,
            stdout=stdout,
            stderr=stderr,
        )
    duration = time.monotonic() - started
    combined = f"{proc.stdout}\n{proc.stderr}"
    exit_code = 124 if timed_out else proc.returncode
    allow_empty_applied = False
    if exit_code == 5 and step.allow_empty:
        exit_code = 0
        allow_empty_applied = True
    durations = _summarize_durations(combined)
    return {
        "name": step.name,
        "kind": step.kind,
        "command": step.command,
        "exit_code": exit_code,
        "raw_exit_code": proc.returncode,
        "timed_out": timed_out,
        "classification": "process_timeout" if timed_out else "completed",
        "timeout_seconds": timeout_seconds,
        "allow_empty": step.allow_empty,
        "allow_empty_applied": allow_empty_applied,
        "duration_seconds": round(duration, 3),
        "counts": _summarize_counts(combined),
        "duration_phase_summary": _duration_phase_summary(durations),
        "durations": durations,
        "first_failure": _first_failure_summary(combined),
        "stdout_tail": _tail(proc.stdout),
        "stderr_tail": _tail(proc.stderr),
        "pytest_runtime": (
            json.loads(runtime_report_path.read_text(encoding="utf-8"))
            if runtime_report_path is not None and runtime_report_path.exists()
            else {"status": "unavailable", "reason": "report was not written"}
        ),
    }


def _coverage_json_path(command: list[str]) -> Path | None:
    for arg in command:
        if not arg.startswith("--cov-report=json"):
            continue
        if arg == "--cov-report=json":
            raw = "coverage.json"
        else:
            _, _, raw = arg.partition(":")
            raw = raw or "coverage.json"
        path = Path(raw)
        return path if path.is_absolute() else REPO_ROOT / path
    return None


def _coverage_fail_under() -> float | None:
    pyproject = REPO_ROOT / "pyproject.toml"
    if not pyproject.exists():
        return None
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    value = data.get("tool", {}).get("coverage", {}).get("report", {}).get("fail_under")
    return None if value is None else float(value)


def _coverage_summary(step_reports: list[dict[str, Any]]) -> dict[str, Any]:
    coverage_commands = [
        step["command"]
        for step in step_reports
        if any(str(arg).startswith("--cov") for arg in step.get("command", []))
        and "--no-cov" not in step.get("command", [])
    ]
    if not coverage_commands:
        return {"enabled": False}

    json_path = next(
        (
            path
            for command in coverage_commands
            if (path := _coverage_json_path([str(arg) for arg in command])) is not None
        ),
        None,
    )
    summary: dict[str, Any] = {
        "enabled": True,
        "fail_under": _coverage_fail_under(),
    }
    if json_path is None:
        return summary

    try:
        summary["json"] = str(json_path.relative_to(REPO_ROOT))
    except ValueError:
        summary["json"] = str(json_path)
    if not json_path.exists():
        summary["json_exists"] = False
        return summary

    coverage_json = json.loads(json_path.read_text(encoding="utf-8"))
    totals = coverage_json.get("totals", {})
    percent = totals.get("percent_covered")
    display_percent = totals.get("percent_covered_display")
    summary.update(
        {
            "json_exists": True,
            "percent_covered": percent,
            "percent_covered_display": display_percent,
            "percent_statements_covered": totals.get("percent_statements_covered"),
        }
    )
    fail_under = summary["fail_under"]
    if percent is not None and fail_under is not None:
        # coverage.py applies report precision before enforcing fail_under.
        # The JSON exposes the raw value and the displayed value; use the
        # displayed value so the QA report agrees with pytest-cov's exit code.
        comparable_percent = (
            float(str(display_percent).rstrip("%"))
            if display_percent is not None
            else float(percent)
        )
        summary["floor_passed"] = comparable_percent >= float(fail_under)
    return summary


def _cost_decomposition(
    step_report: dict[str, Any],
    cost_report_dir: Path,
) -> dict[str, Any]:
    """Summarize worker timing events without summing parallel work as wall time."""
    events: list[dict[str, Any]] = []
    if cost_report_dir.exists():
        for path in sorted(cost_report_dir.glob("worker-*.jsonl")):
            for raw in path.read_text(encoding="utf-8").splitlines():
                if raw.strip():
                    events.append(json.loads(raw))

    def numeric(event: dict[str, Any], key: str) -> float:
        if key not in event:
            raise QALaneError(f"cost decomposition event is missing {key!r}: {event!r}")
        try:
            value = float(event[key])
        except (TypeError, ValueError) as exc:
            raise QALaneError(
                f"invalid cost decomposition value for {key!r}: {event!r}"
            ) from exc
        if not math.isfinite(value) or value < 0:
            raise QALaneError(
                f"cost decomposition value for {key!r} must be finite and non-negative: {event!r}"
            )
        return value

    def phase_events(phase: str) -> list[dict[str, Any]]:
        selected = [event for event in events if event.get("phase") == phase]
        if not selected:
            raise QALaneError(
                f"substrate-db cost decomposition is missing {phase!r} measurements"
            )
        return selected

    def seconds(phase: str) -> float:
        return round(
            sum(numeric(event, "seconds") for event in phase_events(phase)),
            3,
        )

    def nonnegative_integer(raw: Any, label: str, context: object) -> int:
        try:
            value = int(raw)
        except (TypeError, ValueError) as exc:
            raise QALaneError(
                f"invalid cost decomposition count for {label!r}: {context!r}"
            ) from exc
        if value < 0 or str(raw) != str(value):
            raise QALaneError(
                f"cost decomposition count for {label!r} must be a non-negative integer: {context!r}"
            )
        return value

    def count(event: dict[str, Any], key: str) -> int:
        if key not in event:
            raise QALaneError(f"cost decomposition event is missing {key!r}: {event!r}")
        return nonnegative_integer(event[key], key, event)

    def worker_name(event: dict[str, Any]) -> str:
        worker = event.get("worker")
        if not isinstance(worker, str) or not worker:
            raise QALaneError(
                f"cost decomposition event is missing a worker name: {event!r}"
            )
        return worker

    worker_wall_events = [
        event for event in events if event.get("phase") == "worker_wall"
    ]
    if not worker_wall_events:
        raise QALaneError(
            "substrate-db cost decomposition is missing worker wall measurements"
        )
    execution_worker_names = {
        worker_name(event)
        for event in worker_wall_events
        if worker_name(event) != "master"
    }
    if execution_worker_names:
        worker_walls = [
            numeric(event, "seconds")
            for event in worker_wall_events
            if worker_name(event) != "master"
        ]
    else:
        worker_walls = [numeric(event, "seconds") for event in worker_wall_events]
    worker_databases = {
        worker_name(event): str(event["database"])
        for event in events
        if event.get("database")
    }
    missing_databases = execution_worker_names - worker_databases.keys()
    if missing_databases:
        raise QALaneError(
            "cost decomposition is missing databases for workers: "
            + ", ".join(sorted(missing_databases))
        )
    databases = sorted(set(worker_databases.values()))
    counts = step_report.get("counts")
    if not isinstance(counts, dict) or not counts:
        raise QALaneError("cost decomposition is missing test counts")
    count_values = {
        kind: nonnegative_integer(value, f"counts[{kind!r}]", counts)
        for kind, value in counts.items()
    }
    executed_test_count = sum(
        value for kind, value in count_values.items() if kind != "deselected"
    )
    if executed_test_count <= 0:
        raise QALaneError("cost decomposition has no executed tests")
    worker_critical_path = max(worker_walls, default=0.0)
    observed_wall = numeric(step_report, "duration_seconds")
    residual = observed_wall - worker_critical_path
    if residual < 0:
        raise QALaneError(
            "cost decomposition worker critical path exceeds observed wall time"
        )
    measured_call_events = [
        event for event in events if event.get("phase") == "pytest_call"
    ]
    if not measured_call_events:
        raise QALaneError(
            "substrate-db cost decomposition is missing pytest_call measurements"
        )
    pytest_call_work = sum(numeric(event, "seconds") for event in measured_call_events)
    migration_events = phase_events("migration_setup")
    migration_workers = {worker_name(event) for event in migration_events}
    expected_migration_workers = execution_worker_names or {
        worker_name(event) for event in worker_wall_events
    }
    if migration_workers != expected_migration_workers:
        missing_workers = sorted(expected_migration_workers - migration_workers)
        unexpected_workers = sorted(migration_workers - expected_migration_workers)
        details = []
        if missing_workers:
            details.append("missing " + ", ".join(missing_workers))
        if unexpected_workers:
            details.append("unexpected " + ", ".join(unexpected_workers))
        raise QALaneError(
            "cost decomposition migration setup worker attribution mismatch: "
            + "; ".join(details)
        )
    migration_counts_by_worker: dict[str, int] = {}
    for event in migration_events:
        worker = worker_name(event)
        migration_counts_by_worker[worker] = migration_counts_by_worker.get(
            worker, 0
        ) + count(event, "migration_count")
    reset_events = phase_events("reset")
    migration_seconds = seconds("migration_setup")
    reset_seconds = seconds("reset")
    # Setup, teardown and collection come from the same conftest hooks as the
    # call timing, so a run with call events and none of these is a broken
    # recorder, not a lane with no fixture work: phase_events refuses it.
    setup_events = phase_events("pytest_setup")
    teardown_events = phase_events("pytest_teardown")
    collection_events = [
        event
        for event in phase_events("collection")
        if not execution_worker_names or worker_name(event) in execution_worker_names
    ]
    setup_work = sum(numeric(event, "seconds") for event in setup_events)
    teardown_work = sum(numeric(event, "seconds") for event in teardown_events)
    collection_work = sum(numeric(event, "seconds") for event in collection_events)
    worker_wall_sum = sum(worker_walls)
    attributed = collection_work + setup_work + pytest_call_work + teardown_work
    # Collection and the three test phases are disjoint intervals inside each
    # worker's wall clock, so they cannot add up to more than the walls.
    # The tolerance covers each event's rounding to a microsecond.
    if attributed > worker_wall_sum + 1e-5 * max(1, len(events)):
        raise QALaneError(
            "cost decomposition phases exceed the worker wall time: "
            f"{attributed:.3f}s attributed, {worker_wall_sum:.3f}s of worker wall"
        )
    reset_during: dict[str, float] = {}
    for event in reset_events:
        key = str(event.get("during") or "outside_test")
        reset_during[key] = reset_during.get(key, 0.0) + numeric(event, "seconds")

    def slowest(selected: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ranked = sorted(selected, key=lambda event: -numeric(event, "seconds"))
        return [
            {"nodeid": str(event.get("nodeid", "")), "seconds": round(numeric(event, "seconds"), 3)}
            for event in ranked[:10]
        ]

    return {
        "schema": "okg.qa.cost_decomposition.v1",
        "dominant_repeated_phase": (
            "reset" if reset_seconds >= migration_seconds else "migration_setup"
        ),
        "executed_test_count": executed_test_count,
        "worker_count": len(execution_worker_names) or len(worker_walls),
        "worker_databases": databases,
        "migration_setup": {
            "count": sum(migration_counts_by_worker.values()),
            "seconds": migration_seconds,
            "per_worker": dict(sorted(migration_counts_by_worker.items())),
        },
        "reset": {
            "count": sum(count(event, "reset_count") for event in reset_events),
            "seconds": reset_seconds,
            "relational_seconds": round(
                sum(numeric(event, "relational_seconds") for event in reset_events),
                3,
            ),
            "chronos_seconds": round(
                sum(numeric(event, "chronos_seconds") for event in reset_events),
                3,
            ),
            "seconds_by_phase": {
                key: round(value, 3) for key, value in sorted(reset_during.items())
            },
        },
        "pytest_call_work_seconds": round(pytest_call_work, 3),
        "pytest_setup_work_seconds": round(setup_work, 3),
        "pytest_teardown_work_seconds": round(teardown_work, 3),
        "collection_worker_seconds": round(collection_work, 3),
        "worker_wall_sum_seconds": round(worker_wall_sum, 3),
        "unattributed_worker_seconds": round(max(0.0, worker_wall_sum - attributed), 3),
        "timed_test_count": len({str(event.get("nodeid")) for event in setup_events}),
        "slowest_setups": slowest(setup_events),
        "slowest_teardowns": slowest(teardown_events),
        "worker_critical_path_wall_seconds": round(worker_critical_path, 3),
        "observed_wall_seconds": round(observed_wall, 3),
        "infrastructure_residual_seconds": round(max(0.0, residual), 3),
        "clock_skew_seconds": 0.0,
        "event_count": len(events),
    }


def run_lane(
    manifest: dict[str, Any],
    lane_name: str,
    *,
    dry_run: bool = False,
    serial: bool = False,
    report_override: Path | None = None,
    enforce_budget_override: bool | None = None,
    partition_index: int | None = None,
    partition_count: int | None = None,
    selected_step: str | None = None,
) -> int:
    lanes = manifest["lanes"]
    if lane_name not in lanes:
        raise QALaneError(f"unknown lane {lane_name!r}")
    lane = lanes[lane_name]
    if (partition_index is None) != (partition_count is None):
        raise QALaneError(
            "--partition-index and --partition-count must be used together"
        )
    partition_kind: str | None = None
    if partition_index is not None:
        partition_kind = _partition_kind(lane_name, selected_step)
        expected_count = (
            FAST_PARTITION_COUNT
            if partition_kind == "fast"
            else DB_PARTITION_COUNT_BY_LANE[lane_name]
        )
        if partition_count != expected_count:
            raise QALaneError(
                f"{partition_kind} partition count must be {expected_count}, "
                f"got {partition_count}"
            )
        if not 0 <= partition_index < partition_count:
            raise QALaneError(
                f"{partition_kind} partition index must be in [0, {partition_count})"
            )
    if selected_step is not None and "composed_of" in lane:
        raise QALaneError("--step is not valid for a composed lane")
    steps = resolve_steps(
        manifest,
        lane_name,
        serial=serial,
        partition_index=partition_index,
        partition_count=partition_count,
        partition_kind=partition_kind or "fast",
    )
    if selected_step is not None:
        matches = [step for step in steps if step.name == selected_step]
        if len(matches) != 1:
            raise QALaneError(
                f"--step must name exactly one step in lane {lane_name!r}; "
                f"{selected_step!r} matched {len(matches)}"
            )
        steps = matches
    requires_postgres = _lane_requires_postgres(manifest, lane_name)
    optional_deployment_names = _lane_optional_deployment_names(manifest, lane_name)
    _validate_lane_environment(
        lane,
        steps,
        serial=serial,
        requires_postgres=requires_postgres,
    )
    budget, warn_seconds = _budget_thresholds(lane_name, lane)
    partition_process_timeout: float | None = None
    if partition_kind == "fast":
        budget, warn_seconds, partition_process_timeout = _partition_budget_thresholds(
            lane_name, lane
        )
    enforce_budget = bool(lane.get("enforce_budget", False))
    if enforce_budget_override is not None:
        # Advisory introduction on new hardware: measure + report the duration,
        # but do not fail on a budget breach until the budget is re-measured.
        enforce_budget = enforce_budget_override
    out_dir = report_dir(manifest, report_override)
    out_dir.mkdir(parents=True, exist_ok=True)
    # A substrate-db shard keeps the lane's report name: each shard is its
    # own CI job, and the executor's classifier reads substrate-db.json. Its
    # runtime_identity names the shard, which the classifier checks.
    report_stem = (
        f"{lane_name}-partition-{partition_index}-of-{partition_count}"
        if partition_kind == "fast"
        else lane_name
    )
    report_path = out_dir / f"{report_stem}.json"
    env = _lane_env(lane, requires_postgres=requires_postgres)
    # One PYTEST_ADDOPTS, many pytest children: without per-step JUnit targets
    # the LAST pytest step truncates every earlier step's rows in the single
    # declared file (issue #1474 / #1473). Shard per step, merge at the end, so
    # the declared path still exists and now holds every row.
    junit_target = _addopts_junit_path(env.get("PYTEST_ADDOPTS", ""))
    pytest_step_names = [step.name for step in steps if step.kind == "pytest"]
    junit_shards: dict[str, Path] = {}
    if junit_target is not None and len(pytest_step_names) > 1:
        shard_dir = junit_target.parent / JUNIT_SHARD_DIRNAME
        shard_dir.mkdir(parents=True, exist_ok=True)
        # Sweep the WHOLE directory, not only the shards this run will write.
        # A renamed step leaves its old shard behind, and merging it would
        # report a previous run's rows as this one's evidence.
        for stale in shard_dir.glob("*.xml"):
            stale.unlink()
        for name in pytest_step_names:
            junit_shards[name] = (
                shard_dir / f"{junit_target.stem}.{name}{junit_target.suffix}"
            )
    cost_report_dir = out_dir / f"{lane_name}-cost"
    if lane_name == "substrate-db":
        cost_report_dir.mkdir(parents=True, exist_ok=True)
        for event_path in cost_report_dir.glob("worker-*.jsonl"):
            event_path.unlink()
        env[QA_COST_REPORT_DIR_ENV] = str(cost_report_dir)

    if dry_run:
        # Sampled ONCE and reused below: a report describes one observation of
        # the machine, and the load average moves between reads.
        dry_run_platform = _platform_context()
        print(f"lane: {lane_name}")
        if "composed_of" in lane:
            print(f"composed_of: {' -> '.join(lane['composed_of'])}")
        if budget is not None:
            print(f"budget_seconds: {budget}")
            print(f"budget_warn_seconds: {warn_seconds:g}")
        for step in steps:
            print(f"{step.name}: {' '.join(step.command)}")
        report = {
            "schema": "okg.ci.qa_runtime.v1",
            "lane": lane_name,
            "status": "complete",
            "description": lane.get("description"),
            "started_at": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": 0.0,
            "exit_code": 0,
            "budget_seconds": budget,
            "budget_passed": None,
            "budget_warn_seconds": warn_seconds,
            "budget_warn_passed": None,
            "budget_status": "no_budget" if budget is None else "not_measured",
            "enforce_budget": enforce_budget,
            "serial": serial,
            "dry_run": True,
            "platform": dry_run_platform,
            "runtime_identity": _runtime_identity(
                lane_name,
                lane,
                partition_index=partition_index,
                partition_count=partition_count,
                selected_step=selected_step,
            ),
            "optional_deployments": _optional_deployments(optional_deployment_names),
            "coverage": {"enabled": False},
            "performance_context": _performance_context(
                lane_name,
                budget_seconds=budget,
                budget_passed=None,
                warn_seconds=warn_seconds,
                warn_passed=None,
                budget_status="no_budget" if budget is None else "not_measured",
                duration_seconds=None,
                steps=steps,
                dry_run=True,
                platform=dry_run_platform,
            ),
            "steps": [
                {
                    "name": step.name,
                    "kind": step.kind,
                    "command": step.command,
                    "exit_code": None,
                    "duration_seconds": 0.0,
                    "counts": {},
                }
                for step in steps
            ],
        }
        _write_json_atomic(report_path, report)
        return 0

    started_at = datetime.now(timezone.utc).isoformat()
    lane_started = time.monotonic()
    lane_started_wall = time.time()
    step_reports: list[dict[str, Any]] = []
    exit_code = 0
    # keep_going lanes (e.g. coverage) run ALL steps even after a failure, so a
    # later step's side effect is never skipped -- the coverage lane relies on this
    # so cov-serial always appends its coverage even if cov-parallel fails. The
    # lane still reports failure (first nonzero exit code wins).
    keep_going = bool(lane.get("keep_going", False))
    process_timeout = lane.get("process_timeout_seconds")
    if partition_kind == "fast":
        # Already resolved and range-checked against the partition ceiling by
        # _partition_budget_thresholds above.
        process_timeout = partition_process_timeout
    if process_timeout is not None:
        process_timeout = float(process_timeout)
        if process_timeout <= 0:
            raise QALaneError("process_timeout_seconds must be positive")
    # A lane with no in-step kill ends, when it hangs, by the CI job being
    # cancelled. Buffered output is printed only after the child exits, so a
    # cancel would discard every stack the dump wrote. Such a lane therefore
    # always streams (PACT ci-measured-timeouts).
    live_output = bool(lane.get("live_output", False)) or process_timeout is None
    if live_output:
        env.setdefault("PYTHONUNBUFFERED", "1")
    stack_dump_at, stack_dump_basis = _job_stack_dump_at(env)
    if stack_dump_at is None:
        print(
            f"[qa:{lane_name}] no timed stack dump: {stack_dump_basis}. "
            "A cancel still dumps stacks through SIGUSR1.",
            file=sys.stderr,
            flush=True,
        )
    else:
        print(
            f"[qa:{lane_name}] stacks of every test process print at "
            f"{datetime.fromtimestamp(stack_dump_at, timezone.utc).isoformat()} "
            f"({stack_dump_basis})",
            file=sys.stderr,
            flush=True,
        )
    partial_report: dict[str, Any] = {
        "schema": "okg.ci.qa_runtime.v1",
        "lane": lane_name,
        "status": "running",
        "started_at": started_at,
        "duration_seconds": 0.0,
        "exit_code": None,
        "classification": "running",
        "process_timeout_seconds": process_timeout,
        "platform": _platform_context(),
        "runtime_identity": _runtime_identity(
            lane_name,
            lane,
            partition_index=partition_index,
            partition_count=partition_count,
            selected_step=selected_step,
        ),
        "pytest_runtime": {
            "status": "pending",
            "elapsed_seconds": 0.0,
            "collection": {"status": "pending", "seconds": None},
            "unit_test_count": {"status": "pending"},
            "worker_utilization": {"status": "pending"},
            "slowest_reports": [],
        },
        "steps": [],
    }
    _write_json_atomic(report_path, partial_report)

    def _write_progress(step: ResolvedStep, elapsed: float) -> None:
        partial_report["duration_seconds"] = round(time.monotonic() - lane_started, 3)
        partial_report["current_step"] = step.name
        partial_report["current_step_elapsed_seconds"] = round(elapsed, 3)
        runtime_path = out_dir / f"{report_stem}-{step.name}-pytest-runtime.json"
        if runtime_path.exists():
            try:
                partial_report["pytest_runtime"] = json.loads(
                    runtime_path.read_text(encoding="utf-8")
                )
            except (OSError, json.JSONDecodeError) as exc:
                partial_report["pytest_runtime"] = {
                    "status": "unavailable",
                    "reason": str(exc),
                }
        _write_json_atomic(report_path, partial_report)

    for step in steps:
        print(
            f"[qa:{lane_name}] {step.name}: {' '.join(step.command)}",
            flush=True,
        )
        runtime_path = (
            out_dir / f"{report_stem}-{step.name}-pytest-runtime.json"
            if step.runtime_report
            else None
        )
        # The cost decomposition is an analysis of the PARALLEL step's worker
        # model: it compares the worker critical path against that step's own
        # wall time. Two things follow, and PR #1460 broke on both when it
        # added a second, serial step to this lane.
        #
        # First, it only means anything for a step that HAS workers -- a serial
        # step has no worker decomposition to make. Second, each step needs its
        # own event directory: with one shared directory the second step
        # measured the FIRST step's workers, comparing ~250s of parallel worker
        # wall against a 12s serial step and refusing the lane on a negative
        # residual. Per-step directories also remove the dependency on step
        # ORDER that made the parallel step's own number correct only by luck.
        decompose_cost = (
            lane_name == "substrate-db"
            and step.kind == "pytest"
            and "-n" in step.command
        )
        step_env = env
        if step.kind == "pytest":
            plugins = [
                name
                for name in step_env.get("PYTEST_PLUGINS", "").split(",")
                if name and name != STACK_DUMP_PLUGIN
            ]
            step_env = {
                **step_env,
                "PYTEST_PLUGINS": ",".join([*plugins, STACK_DUMP_PLUGIN]),
            }
            if stack_dump_at is not None:
                step_env[STACK_DUMP_AT_ENV] = f"{stack_dump_at:.3f}"
            else:
                step_env.pop(STACK_DUMP_AT_ENV, None)
        if step.name in junit_shards:
            step_env = {
                **step_env,
                "PYTEST_ADDOPTS": _replace_addopts_junit(
                    step_env.get("PYTEST_ADDOPTS", ""), junit_shards[step.name]
                ),
            }
        step_cost_dir = cost_report_dir
        if decompose_cost:
            step_cost_dir = cost_report_dir / step.name
            step_cost_dir.mkdir(parents=True, exist_ok=True)
            for event_path in step_cost_dir.glob("worker-*.jsonl"):
                event_path.unlink()
            step_env = {**step_env, QA_COST_REPORT_DIR_ENV: str(step_cost_dir)}
        timeout_pg_path = out_dir / f"{report_stem}-{step.name}-timeout-postgres.json"

        def _capture_timeout_evidence(
            capture_env: dict[str, str] = step_env,
            target: Path = timeout_pg_path,
            current: ResolvedStep = step,
        ) -> None:
            if not requires_postgres:
                return
            snapshot = _postgres_activity_snapshot(capture_env)
            snapshot["lane"] = lane_name
            snapshot["step"] = current.name
            _write_json_atomic(target, snapshot)
            print(
                f"[qa:{lane_name}] {current.name} killed by the process timeout; "
                f"{_postgres_activity_summary(snapshot)}\n"
                f"[qa:{lane_name}] postgres snapshot written to {target}",
                file=sys.stderr,
                flush=True,
            )

        step_report = _run_step(
            step,
            env=step_env,
            live_output=live_output,
            timeout_seconds=process_timeout,
            progress=lambda elapsed, current=step: _write_progress(current, elapsed),
            runtime_report_path=runtime_path,
            on_timeout=_capture_timeout_evidence,
        )
        if decompose_cost:
            if step_report.get("timed_out"):
                # A KILLED worker never reaches pytest_sessionfinish, so it
                # writes no cost event, and `_cost_decomposition` would refuse
                # the lane with "missing worker wall measurements". That
                # refusal is correct for a run that COMPLETED without events --
                # it keeps a silently eventless lane from passing -- but on a
                # kill it raised out of run_lane before the captured pytest
                # tails were printed and before the report was written,
                # destroying the only evidence of why the lane hung and
                # leaving the classifier a schema-invalid partial report. The
                # decomposition is an analysis of a finished parallel step; a
                # step that never finished has nothing to decompose. Record
                # WHY it is absent, in the report, rather than inferring it
                # from a missing key.
                step_report["cost_decomposition_skipped"] = {
                    "reason": "process_timeout",
                    "detail": (
                        "the step was killed by its process timeout, so no "
                        "xdist worker reached pytest_sessionfinish and no cost "
                        "event was written"
                    ),
                    "cost_report_dir": str(step_cost_dir),
                    "postgres_snapshot": (
                        str(timeout_pg_path) if timeout_pg_path.exists() else None
                    ),
                }
            else:
                step_report["cost_decomposition"] = _cost_decomposition(
                    step_report,
                    step_cost_dir,
                )
        step_reports.append(step_report)
        if step_report["exit_code"] != 0:
            # Captured output must remain available in the live job log when
            # artifact publication is degraded; otherwise a correctly red
            # required lane can lose the only evidence naming its failure.
            for stream in ("stdout", "stderr"):
                tail = _safe_log_tail(
                    str(step_report.get(f"{stream}_tail") or "")
                ).strip()
                if tail:
                    print(
                        f"[qa:{lane_name}] {step.name} {stream} tail:\n{tail}",
                        file=sys.stderr,
                    )
            if exit_code == 0:
                exit_code = int(step_report["exit_code"])
            if not keep_going:
                break

    if junit_shards and junit_target is not None:
        # Merge whatever exists. A lane that stopped on a failing step has
        # fewer shards, and reporting the rows it did produce is right; the
        # steps that never ran genuinely have no rows.
        _merge_junit_shards(junit_target, list(junit_shards.values()))

    duration = time.monotonic() - lane_started
    # After the duration is taken, so reading and printing the list is never
    # charged to the lane's measured time or its budget.
    if junit_target is not None:
        _report_junit_failures(
            lane_name, junit_target, lane_started_wall=lane_started_wall
        )
    lane_timed_out = any(step.get("timed_out") for step in step_reports)
    budget_passed = None if budget is None else duration <= float(budget)
    budget_warn_passed = None if warn_seconds is None else duration <= warn_seconds
    budget_status = _budget_status(duration, budget, warn_seconds)
    # Captured BEFORE the budget verdict can overwrite it, because "did any
    # step fail on its own" is exactly the question the job summary has to
    # answer for a budget-only red (#899).
    steps_exit_code = exit_code
    failed_tests = _failed_test_count(step_reports)
    passed_tests = _passed_test_count(step_reports)
    # RUNNER VARIANCE IS NOT A TEST FAILURE (#899).
    #
    # A lane that RAN TO COMPLETION with zero failing tests cannot be told
    # apart from a slow runner pool by its duration alone, so failing it on
    # that number reds pull requests that broke nothing -- which is what the
    # 1064s ceiling did to a run that passed 3754 tests. Crossing the
    # ceiling in that state is now classified `runner_variance`: annotated
    # and written to the job summary, never blocking.
    #
    # WHAT STILL BLOCKS, so this is not a weakening dressed as a fix:
    #   * a failing test, exactly as before;
    #   * NON-COMPLETION. A lane that reaches its process timeout is killed
    #     and fails. That is the runaway guard, and it is the only duration
    #     signal that can be trusted, because a hung lane never finishes.
    #
    # THE COST, stated: a genuine pathological slowdown that still completes
    # under the kill now warns instead of failing. It is reported by the
    # warn line and the recorded p95 rather than stopped by a hard line.
    # The CLASSIFICATION describes the run and does not depend on whether
    # the lane enforces its budget; `enforce_budget` decides only whether a
    # classification blocks. Folding the two together mislabelled an
    # advisory lane's clean overshoot as a failure-bearing one.
    completed_clean = exit_code == 0 and not lane_timed_out and failed_tests == 0
    if lane_timed_out:
        overshoot_kind = "process_timeout"
    elif budget_passed is False and completed_clean:
        overshoot_kind = "runner_variance"
    elif budget_passed is False:
        overshoot_kind = "with_failures"
    else:
        overshoot_kind = "none"
    if (
        enforce_budget
        and exit_code == 0
        and budget_passed is False
        and overshoot_kind != "runner_variance"
    ):
        exit_code = 1

    optional_deployments = _optional_deployments(optional_deployment_names)
    coverage = _coverage_summary(step_reports)
    # Sampled ONCE and reused below, for the same reason as the dry-run arm.
    lane_platform = _platform_context()
    report = {
        "schema": "okg.ci.qa_runtime.v1",
        "lane": lane_name,
        "status": "timeout" if lane_timed_out else "complete",
        "description": lane.get("description"),
        "started_at": started_at,
        "duration_seconds": round(duration, 3),
        "exit_code": exit_code,
        "budget_seconds": budget,
        "budget_passed": budget_passed,
        "budget_warn_seconds": warn_seconds,
        "budget_warn_passed": budget_warn_passed,
        "budget_status": budget_status,
        "enforce_budget": enforce_budget,
        # #899: a consumer of this report must be able to tell a budget-only
        # red from a test failure WITHOUT re-parsing the step logs.
        "steps_exit_code": steps_exit_code,
        "failed_test_count": failed_tests,
        "passed_test_count": passed_tests,
        "budget_overshoot_kind": overshoot_kind,
        "runner_variance_tolerated": bool(overshoot_kind == "runner_variance"),
        "serial": serial,
        "dry_run": False,
        "platform": lane_platform,
        "runtime_identity": _runtime_identity(
            lane_name,
            lane,
            partition_index=partition_index,
            partition_count=partition_count,
            selected_step=selected_step,
        ),
        "optional_deployments": optional_deployments,
        "coverage": coverage,
        "performance_context": _performance_context(
            lane_name,
            budget_seconds=budget,
            budget_passed=budget_passed,
            warn_seconds=warn_seconds,
            warn_passed=budget_warn_passed,
            budget_status=budget_status,
            duration_seconds=round(duration, 3),
            steps=steps,
            dry_run=False,
            platform=lane_platform,
        ),
        "steps": step_reports,
        "classification": "process_timeout" if lane_timed_out else "completed",
        "process_timeout_seconds": process_timeout,
    }
    _write_json_atomic(report_path, report)
    print(
        f"[qa:{lane_name}] exit={exit_code} "
        f"duration={duration:.2f}s report={report_path}"
    )
    _emit_budget_annotation(
        lane_name,
        status=budget_status,
        duration=duration,
        budget=budget,
        warn_seconds=warn_seconds,
        enforce_budget=enforce_budget,
        timed_out=lane_timed_out,
        overshoot_kind=overshoot_kind,
    )
    # The annotation above is collapsed in the Actions UI and never appears in
    # the job summary. #899: a budget-only red must state its reason where the
    # reader of a red required check actually looks.
    _write_job_summary(
        _budget_summary_markdown(
            lane_name,
            status=budget_status,
            duration=duration,
            budget=budget,
            warn_seconds=warn_seconds,
            enforce_budget=enforce_budget,
            timed_out=lane_timed_out,
            failed_tests=failed_tests,
            steps_exit_code=steps_exit_code,
            passed_tests=passed_tests,
            overshoot_kind=overshoot_kind,
            partition_index=partition_index,
            partition_count=partition_count,
        )
    )
    if budget_passed is False:
        print(
            f"[qa:{lane_name}] exceeded budget {budget}s with duration {duration:.2f}s",
            file=sys.stderr,
        )
    elif budget_status == "warn":
        outcome = (
            "KILLED by the process timeout before the ceiling"
            if lane_timed_out
            else "not failing"
        )
        print(
            f"[qa:{lane_name}] warn threshold {warn_seconds:g}s crossed "
            f"with duration {duration:.2f}s (ceiling {budget}s, {outcome})",
            file=sys.stderr,
        )
    return exit_code


def list_lanes(manifest: dict[str, Any]) -> int:
    for name, lane in manifest["lanes"].items():
        budget = lane.get("budget_seconds")
        suffix = f" ({budget}s)" if budget is not None else ""
        print(f"{name}{suffix}: {lane.get('description', '')}")
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "lane",
        nargs="?",
        default="list",
        help="Lane to run, or 'list'.",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MANIFEST,
        help="Path to qa_lanes.yaml.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print resolved commands without executing them.",
    )
    parser.add_argument(
        "--serial",
        action="store_true",
        help="Disable per-step parallelism such as pytest-xdist.",
    )
    parser.add_argument(
        "--no-enforce-budget",
        action="store_true",
        help="Run the lane WITHOUT failing on a budget_seconds breach (still "
        "measures + reports the duration). For introducing a lane on new "
        "hardware before its budget is measured -- never guess the budget.",
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        help="Override report output directory.",
    )
    parser.add_argument(
        "--partition-index",
        type=int,
        help=(
            "Zero-based required-unit index for the fast lane, or the shard "
            f"index for {DB_PARTITION_LANE} --step {DB_PARTITION_STEP} and for "
            f"the whole {DB_FULL_PARTITION_LANE} and "
            f"{COVERAGE_FULL_PARTITION_LANE} lanes."
        ),
    )
    parser.add_argument(
        "--partition-count",
        type=int,
        help=(
            f"Required-unit count: {FAST_PARTITION_COUNT} for the fast lane, "
            f"{DB_PARTITION_COUNT} for {DB_PARTITION_LANE} --step {DB_PARTITION_STEP}, "
            f"{DB_FULL_PARTITION_COUNT} for {DB_FULL_PARTITION_LANE}, "
            f"{COVERAGE_FULL_PARTITION_COUNT} for {COVERAGE_FULL_PARTITION_LANE}."
        ),
    )
    parser.add_argument(
        "--step",
        action="append",
        help="Run exactly one named step from a non-composed lane.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        if args.step is not None and len(args.step) != 1:
            raise QALaneError("--step must be supplied exactly once when used")
        manifest = load_manifest(args.manifest)
        if args.lane == "list":
            return list_lanes(manifest)
        return run_lane(
            manifest,
            args.lane,
            dry_run=args.dry_run,
            serial=args.serial,
            report_override=args.report_dir,
            enforce_budget_override=(False if args.no_enforce_budget else None),
            partition_index=args.partition_index,
            partition_count=args.partition_count,
            selected_step=(args.step[0] if args.step else None),
        )
    except QALaneError as exc:
        print(f"qa: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
