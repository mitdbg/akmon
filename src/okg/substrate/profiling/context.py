from __future__ import annotations

import contextlib
import contextvars
import hashlib
import logging
import os
import re
import secrets
import sys
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

from .artifacts import (
    deployment_root,
    ensure_profile_root,
    live_dir,
    utc_now,
    write_json_atomic,
    write_profile_bundle,
)
from .model import (
    ArtifactRef,
    Evidence,
    ProfileRun,
    ProfileSummary,
    QueryTiming,
    StageTiming,
)
from .policy import ProfilingPolicy

logger = logging.getLogger(__name__)


_current: contextvars.ContextVar["ProfileContext | None"] = contextvars.ContextVar(
    "okg_profile_context", default=None,
)
_stage_stack: contextvars.ContextVar[tuple[str, ...]] = contextvars.ContextVar(
    "okg_profile_stage_stack", default=(),
)
_patch_lock = threading.Lock()
_patch_state = {"patched": False}
_LIVE_SUMMARY_LAST_WRITTEN: dict[tuple[str, str, int], float] = {}
_LIVE_SUMMARY_LAST_WRITTEN_LOCK = threading.Lock()


def current_profile() -> "ProfileContext | None":
    return _current.get()


@dataclass
class ProfileContext:
    deployment: str
    target_kind: str
    command: str
    policy: ProfilingPolicy
    collect_detail: bool
    write_reason: str | None = None
    profile_run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    started_at_ns: int = field(default_factory=time.perf_counter_ns)
    started_at: str = field(default_factory=utc_now)
    stages: list[StageTiming] = field(default_factory=list)
    queries: list[QueryTiming] = field(default_factory=list)
    # Always-on count of DB round-trips for this context. Incremented for
    # every recorded query regardless of `collect_detail`, so a deterministic
    # per-run query count exists even on the lightweight profiling path (the
    # detailed `queries` list is only populated under collect_detail).
    # count-based-perf-gate / req.counts.emit.
    query_count: int = 0
    ring: deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=10_000))
    limitations: list[str] = field(default_factory=list)
    backend_pids: set[int] = field(default_factory=set)
    rootless_sampler: "_RootlessSpeedscopeSampler | None" = None
    generation_id: int | None = None
    request_path: str | None = None
    owner_id: str | None = None
    owner_meta: dict[str, Any] = field(default_factory=dict)
    source_sync: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.ring = deque(maxlen=max(1, self.policy.lightweight.ring_buffer_events))
        if self.collect_detail:
            self._start_rootless_speedscope()

    def record_stage(
        self, name: str, start_ns: int, duration_ns: int,
        parent: str | None, attrs: dict[str, Any] | None,
    ) -> None:
        if self.policy.lightweight.enabled:
            self.ring.append({
                "kind": "stage",
                "name": name,
                "duration_ms": duration_ns / 1_000_000,
                "at": utc_now(),
            })
        if self.collect_detail:
            self.stages.append(StageTiming(
                name=name,
                started_at_ns=start_ns,
                duration_ns=duration_ns,
                parent=parent,
                attrs=dict(attrs or {}),
            ))

    def record_query(
        self,
        sql: Any,
        duration_ns: int,
        rowcount: int | None,
        started_at_ns: int | None = None,
        backend_pid: int | None = None,
    ) -> None:
        # Count every round-trip unconditionally — this is the deterministic
        # per-run query count the bench gate reads, and it must not depend on
        # collect_detail (which only governs the heavyweight `queries` list).
        self.query_count += 1
        shape = normalize_sql(str(sql))
        fp = hashlib.sha256(shape.encode("utf-8")).hexdigest()[:16]
        stage = _stage_stack.get()[-1] if _stage_stack.get() else None
        duration_ms = duration_ns / 1_000_000
        if backend_pid is not None:
            self.backend_pids.add(backend_pid)
        if (
            self.policy.lightweight.enabled
            and duration_ms >= self.policy.lightweight.slow_query_ms
        ):
            self.ring.append({
                "kind": "slow_query",
                "fingerprint": fp,
                "stage": stage,
                "duration_ms": duration_ms,
                "at": utc_now(),
            })
        if self.collect_detail:
            self.queries.append(QueryTiming(
                fingerprint=fp,
                stage=stage,
                duration_ns=duration_ns,
                rowcount=rowcount if rowcount is not None and rowcount >= 0 else None,
                sql_shape=shape,
                backend_pid=backend_pid,
                started_at_ns=started_at_ns,
            ))

    def finish(self) -> ProfileRun | None:
        ended_ns = time.perf_counter_ns()
        duration_ms = (ended_ns - self.started_at_ns) / 1_000_000
        self._write_live_summary(duration_ms)
        if self.write_reason is None:
            self._stop_samplers(write=False)
            return None
        sampler_artifacts = self._stop_samplers(write=True)
        run = ProfileRun(
            profile_run_id=self.profile_run_id,
            deployment=self.deployment,
            target_kind=self.target_kind,
            command=self.command,
            started_at=self.started_at,
            ended_at=utc_now(),
            duration_ms=duration_ms,
            query_count=self.query_count,
            generation_id=self.generation_id,
            request_path=self.request_path,
            owner_id=self.owner_id,
            owner_meta=dict(self.owner_meta),
            source_sync=dict(self.source_sync),
            policy=self.policy.to_dict(),
            evidence=_evidence_for_policy(
                self.policy,
                self.limitations,
                rootless_speedscope_available=any(
                    artifact.kind == "rootless-speedscope"
                    for artifact in sampler_artifacts
                ),
            ),
            artifacts=sampler_artifacts,
            stages=list(self.stages),
            queries=list(self.queries),
            backend_pids=sorted(self.backend_pids),
            limitations=list(self.limitations),
        )
        run.summary = summarize(run)
        try:
            write_profile_bundle(run)
        except Exception as exc:
            # Fail open: profiling can disappear, substrate work cannot.
            self.limitations.append(f"artifact write failed: {type(exc).__name__}")
            return None
        return run

    def _write_live_summary(self, duration_ms: float) -> None:
        if not self.policy.lightweight.enabled:
            return
        if not _claim_live_summary_write(
            self.deployment,
            self.target_kind,
            int(self.policy.lightweight.live_snapshot_stale_seconds),
            force=self.write_reason is not None,
        ):
            return
        stages: dict[str, list[float]] = {}
        slow_queries: list[dict[str, Any]] = []
        for event in self.ring:
            if event.get("kind") == "stage":
                stages.setdefault(event["name"], []).append(float(event["duration_ms"]))
            elif event.get("kind") == "slow_query":
                slow_queries.append(event)
        payload = {
            "deployment": self.deployment,
            "target_kind": self.target_kind,
            "profile_run_id": self.profile_run_id,
            "pid": os.getpid(),
            "updated_at": utc_now(),
            "duration_ms": duration_ms,
            "stage_p95_ms": {
                k: percentile(v, 0.95) for k, v in stages.items()
            },
            "slow_queries": slow_queries[-10:],
            "policy": self.policy.to_dict(),
            "limitations": self.limitations,
        }
        try:
            ensure_profile_root()
            write_json_atomic(
                live_dir(self.deployment) / f"{self.target_kind}.{os.getpid()}.json",
                payload,
            )
        except Exception:  # noqa: BLE001 — heartbeat is best-effort; full traceback in debug log.
            logger.debug("failed to write live profile heartbeat", exc_info=True)

    def _start_rootless_speedscope(self) -> None:
        if "rootless-speedscope" not in self.policy.deep_capture.collectors:
            return
        ensure_profile_root()
        root = deployment_root(self.deployment) / self.profile_run_id
        root.mkdir(parents=True, exist_ok=True)
        try:
            sampler = _RootlessSpeedscopeSampler(
                path=root / "speedscope.json",
                name=f"{self.target_kind}:{self.command}",
                target_thread_id=threading.get_ident(),
                interval_seconds=0.01,
            )
            sampler.start()
        except Exception as exc:  # noqa: BLE001
            self.limitations.append(
                "rootless_speedscope: failed to start sampler "
                f"({type(exc).__name__})",
            )
            return
        self.rootless_sampler = sampler

    def _stop_samplers(self, *, write: bool) -> list[ArtifactRef]:
        artifacts: list[ArtifactRef] = []
        sampler = self.rootless_sampler
        self.rootless_sampler = None
        if sampler is None:
            return artifacts
        path = sampler.stop(write=write)
        if not write:
            return artifacts
        if path is not None and path.is_file() and path.stat().st_size > 0:
            artifacts.append(ArtifactRef(
                "speedscope.json",
                "application/json",
                "rootless-speedscope",
            ))
        else:
            self.limitations.append(
                "rootless_speedscope: sampler produced no speedscope.json",
            )
        return artifacts


class _RootlessSpeedscopeSampler:
    """Small in-process wall-clock sampler that writes Speedscope JSON.

    This intentionally avoids native stack sampling and external processes. It samples
    the current Python thread from a daemon thread via ``sys._current_frames()``, so
    it is useful for Python-level bottleneck diagnosis. Postgres/Chronos time is
    represented separately by query timings in the surrounding profile bundle.
    """

    def __init__(
        self,
        *,
        path: Path,
        name: str,
        target_thread_id: int,
        interval_seconds: float,
    ) -> None:
        self.path = path
        self.name = name
        self.target_thread_id = target_thread_id
        self.interval_seconds = max(0.001, float(interval_seconds))
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._started_at = time.perf_counter()
        self._last_sample_at = self._started_at
        self._frames: list[dict[str, Any]] = []
        self._frame_ids: dict[tuple[str, str, int], int] = {}
        self._samples: list[list[int]] = []
        self._weights: list[float] = []

    def start(self) -> None:
        self._sample()
        self._thread = threading.Thread(
            target=self._run,
            name="okg-rootless-speedscope-sampler",
            daemon=True,
        )
        self._thread.start()

    def stop(self, *, write: bool) -> Path | None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=max(0.05, self.interval_seconds * 5))
        self._sample()
        if not write:
            return None
        self._write()
        return self.path

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            self._sample()

    def _sample(self) -> None:
        frame = sys._current_frames().get(self.target_thread_id)
        now = time.perf_counter()
        if frame is None:
            return
        stack: list[int] = []
        while frame is not None:
            code = frame.f_code
            stack.append(self._frame_id(
                code.co_name,
                code.co_filename,
                frame.f_lineno,
            ))
            frame = frame.f_back
        stack.reverse()
        if not stack:
            return
        with self._lock:
            weight = max(0.000001, now - self._last_sample_at)
            self._last_sample_at = now
            self._samples.append(stack)
            self._weights.append(weight)

    def _frame_id(self, name: str, filename: str, line: int) -> int:
        key = (name, filename, int(line))
        with self._lock:
            existing = self._frame_ids.get(key)
            if existing is not None:
                return existing
            frame_id = len(self._frames)
            self._frame_ids[key] = frame_id
            self._frames.append({
                "name": name,
                "file": filename,
                "line": int(line),
            })
            return frame_id

    def _write(self) -> None:
        with self._lock:
            samples = list(self._samples)
            weights = list(self._weights)
            frames = list(self._frames)
        duration = max(0.000001, sum(weights))
        payload = {
            "$schema": "https://www.speedscope.app/file-format-schema.json",
            "exporter": "okg rootless speedscope sampler",
            "name": self.name,
            "activeProfileIndex": 0,
            "shared": {"frames": frames},
            "profiles": [{
                "type": "sampled",
                "name": self.name,
                "unit": "seconds",
                "startValue": 0,
                "endValue": duration,
                "samples": samples,
                "weights": weights,
            }],
        }
        write_json_atomic(self.path, payload)


def summarize(run: ProfileRun) -> ProfileSummary:
    stage_totals: dict[str, float] = {}
    for s in run.stages:
        stage_totals[s.name] = stage_totals.get(s.name, 0.0) + s.duration_ms
    top_stage = max(stage_totals, key=stage_totals.get) if stage_totals else None
    top_stage_ms = stage_totals.get(top_stage, 0.0) if top_stage else 0.0
    query_totals: dict[str, float] = {}
    for q in run.queries:
        query_totals[q.fingerprint] = query_totals.get(q.fingerprint, 0.0) + q.duration_ms
    top_query = max(query_totals, key=query_totals.get) if query_totals else None
    db_ms = sum(q.duration_ms for q in run.queries)
    non_db = max(0.0, (run.duration_ms or 0.0) - db_ms)
    bottleneck = "postgres_execution" if db_ms > non_db else "python_cpu"
    # Prefer the always-on per-run counter (populated even when the detailed
    # query list was not collected); fall back to len(queries) for runs built
    # directly from a captured query list (e.g. reconstructed drilldowns).
    query_count = run.query_count or len(run.queries)
    return ProfileSummary(
        bottleneck_class=bottleneck,
        top_stage=top_stage,
        top_stage_ms=top_stage_ms,
        top_query=top_query,
        db_round_trip_ms=db_ms,
        non_db_ms=non_db,
        query_count=query_count,
    )


def _evidence_for_policy(
    policy: ProfilingPolicy,
    limitations: list[str],
    *,
    rootless_speedscope_available: bool = False,
) -> dict[str, Evidence]:
    collectors = set(policy.deep_capture.collectors)
    evidence = {
        "timing": Evidence("available"),
        "postgres_stats": Evidence("degraded", "round_trip_only"),
        "waits": Evidence("unavailable", "wait sampler not implemented"),
        "rootless_speedscope": _collector_evidence(
            "rootless-speedscope",
            collectors,
            available=rootless_speedscope_available,
            unavailable_reason="rootless sampler produced no artifact",
        ),
        "postgres_flamegraph": _collector_evidence(
            "postgres-flamegraph",
            collectors,
            available=False,
            unavailable_reason="postgres stack sampler not configured",
        ),
        "explain_samples": _collector_evidence(
            "explain-samples",
            collectors,
            available=False,
            unavailable_reason=(
                "EXPLAIN replay is not safe without captured parameter bindings"
            ),
        ),
    }
    for key, item in evidence.items():
        if key in {"timing", "postgres_stats", "waits"}:
            continue
        if item.reason and item.reason != "collector not requested":
            limitations.append(f"{key}: {item.reason}")
    return evidence


def _collector_evidence(
    collector: str,
    requested: set[str],
    *,
    available: bool,
    unavailable_reason: str,
    degraded_reason: str | None = None,
) -> Evidence:
    if collector not in requested:
        return Evidence("unavailable", "collector not requested")
    if available and degraded_reason:
        return Evidence("degraded", degraded_reason)
    if available:
        return Evidence("available")
    return Evidence("unavailable", unavailable_reason)


@contextlib.contextmanager
def profiling_context(
    *,
    deployment: str,
    target_kind: str,
    command: str,
    policy: ProfilingPolicy,
    force_deep: bool = False,
) -> Iterator[ProfileContext]:
    install_psycopg_patch()
    parent = _current.get()
    if parent is not None:
        yield parent
        return
    write_reason: str | None = None
    collect_detail = False
    mode = policy.deep_capture.mode
    if force_deep:
        write_reason = "manual"
        collect_detail = True
    elif mode == "sampled":
        sample_rate = max(0.0, min(1.0, policy.deep_capture.sample_rate))
        if secrets.SystemRandom().random() < sample_rate:
            write_reason = "sampled"
            collect_detail = True
    elif mode == "on_slow":
        collect_detail = True
    ctx = ProfileContext(
        deployment=deployment,
        target_kind=target_kind,
        command=command,
        policy=policy,
        collect_detail=collect_detail,
        write_reason=write_reason,
    )
    token = _current.set(ctx)
    try:
        yield ctx
    finally:
        try:
            if ctx.write_reason is None and mode == "on_slow":
                elapsed_ms = (time.perf_counter_ns() - ctx.started_at_ns) / 1_000_000
                threshold = _slow_threshold_ms(policy, target_kind)
                if elapsed_ms >= threshold:
                    ctx.write_reason = "on_slow"
                else:
                    ctx.stages.clear()
                    ctx.queries.clear()
            ctx.finish()
        finally:
            _current.reset(token)


@contextlib.contextmanager
def profile_stage(name: str, attrs: dict[str, Any] | None = None) -> Iterator[None]:
    ctx = _current.get()
    if ctx is None or (not ctx.policy.lightweight.enabled and not ctx.collect_detail):
        yield
        return
    stack = _stage_stack.get()
    parent = stack[-1] if stack else None
    token = _stage_stack.set((*stack, name))
    start = time.perf_counter_ns()
    try:
        yield
    finally:
        duration = time.perf_counter_ns() - start
        ctx.record_stage(name, start, duration, parent, attrs)
        _stage_stack.reset(token)


def normalize_sql(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    sql = re.sub(r"--.*?$", " ", sql, flags=re.M)
    sql = re.sub(r"'(?:''|[^'])*'", "?", sql)
    sql = re.sub(r"\b\d+(?:\.\d+)?\b", "?", sql)
    sql = re.sub(r"\s+", " ", sql).strip()
    return sql[:500]


def _claim_live_summary_write(
    deployment: str,
    target_kind: str,
    stale_seconds: int,
    *,
    force: bool = False,
) -> bool:
    if force or stale_seconds <= 0:
        return True
    key = (deployment, target_kind, os.getpid())
    now = time.monotonic()
    with _LIVE_SUMMARY_LAST_WRITTEN_LOCK:
        last = _LIVE_SUMMARY_LAST_WRITTEN.get(key)
        if last is not None and now - last < stale_seconds:
            return False
        _LIVE_SUMMARY_LAST_WRITTEN[key] = now
    return True


def _slow_threshold_ms(policy: ProfilingPolicy, target_kind: str) -> int:
    if target_kind == "mcp":
        return policy.deep_capture.slow_mcp_ms
    if target_kind == "viz-request":
        return policy.deep_capture.slow_viz_request_ms
    return policy.deep_capture.slow_publish_ms


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    idx = min(len(values) - 1, max(0, int(round((len(values) - 1) * p))))
    return values[idx]


def install_psycopg_patch() -> None:
    if _patch_state["patched"]:
        return
    with _patch_lock:
        if _patch_state["patched"]:
            return
        try:
            import psycopg
            cursor_cls = psycopg.Cursor
            original = cursor_cls.execute

            def execute(self, query, params=None, *args, **kwargs):
                ctx = _current.get()
                if ctx is None:
                    return original(self, query, params, *args, **kwargs)
                start = time.perf_counter_ns()
                try:
                    return original(self, query, params, *args, **kwargs)
                finally:
                    ctx.record_query(
                        query,
                        time.perf_counter_ns() - start,
                        getattr(self, "rowcount", None),
                        started_at_ns=start,
                        backend_pid=_backend_pid_for_cursor(self),
                    )

            cursor_cls.execute = execute

            # executemany + copy are real DB round-trips the count gate must
            # see, but they bypass Cursor.execute -- without these wraps a
            # regression that shifts work onto executemany/COPY would be
            # invisible to query_count. A batched executemany is one pipelined
            # round-trip; a COPY is one operation -- count each as one.
            original_executemany = getattr(cursor_cls, "executemany", None)
            if original_executemany is not None:
                def executemany(self, query, params_seq=None, *args, **kwargs):
                    ctx = _current.get()
                    if ctx is None:
                        return original_executemany(
                            self, query, params_seq, *args, **kwargs
                        )
                    start = time.perf_counter_ns()
                    try:
                        return original_executemany(
                            self, query, params_seq, *args, **kwargs
                        )
                    finally:
                        ctx.record_query(
                            query,
                            time.perf_counter_ns() - start,
                            getattr(self, "rowcount", None),
                            started_at_ns=start,
                            backend_pid=_backend_pid_for_cursor(self),
                        )

                cursor_cls.executemany = executemany

            original_copy = getattr(cursor_cls, "copy", None)
            if original_copy is not None:
                def copy(self, statement, *args, **kwargs):
                    ctx = _current.get()
                    if ctx is not None:
                        ctx.record_query(
                            statement,
                            0,
                            getattr(self, "rowcount", None),
                            started_at_ns=time.perf_counter_ns(),
                            backend_pid=_backend_pid_for_cursor(self),
                        )
                    return original_copy(self, statement, *args, **kwargs)

                cursor_cls.copy = copy

            _patch_state["patched"] = True
        except Exception:  # noqa: BLE001 — psycopg monkey-patch is best-effort instrumentation; set _patched to prevent retry loop.
            _patch_state["patched"] = True


def _backend_pid_for_cursor(cursor: Any) -> int | None:
    info = getattr(getattr(cursor, "connection", None), "info", None)
    pid = getattr(info, "backend_pid", None)
    return pid if isinstance(pid, int) else None
