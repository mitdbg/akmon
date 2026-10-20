"""One process per ingest source (lane H stage 1, PR #2917 section 3.7).

The process mechanics run with real ``spawn`` children: pipe drain, memory
kill, crash, lifeline and start order. The database tests run the real
``run_ingest`` with its production runtime, so each source runs in a child
that loads its adapter through the registry.
"""
from __future__ import annotations

import dataclasses
import multiprocessing
import os
import pickle
import signal
import threading
import time
import types
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml

from okg.substrate.errors import SourceRunAlreadyActive, SubstrateError
from okg.substrate.ingest import service as ingest_service
from okg.substrate.ingest import source_job_runtime, source_jobs
from okg.substrate.ingest.compensation import SourceStateSnapshot
from okg.substrate.ingest.runner import SourceRunContext
from okg.substrate.ingest.service import IngestOptions, run_ingest
from tests.substrate import _source_job_children as children

FIXTURE_MODULE = "tests.substrate._source_job_fixture_source"
_MB = 1024 * 1024


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _close_pools_after_each_test():
    """Leave no pool open: later tests size their own pools for the DSN."""
    yield
    from okg.substrate.db.pool import close_all

    close_all()



class _FakeRuntime:
    """A coordinator runtime whose children are stand-in processes."""

    def __init__(
        self,
        launchers: dict[str, Any],
        *,
        connection_state: Any = (0, 1000),
        heavy: frozenset[str] = frozenset(),
        limits: dict[str, int] | None = None,
        budget: int = 1 << 60,
        sampler: Any = None,
    ) -> None:
        self.launchers = launchers
        self.launched: list[str] = []
        self._connection_state = connection_state
        self._heavy = heavy
        self._limits = limits or {}
        self._budget = budget
        self._sampler = sampler

    def launch(self, name: str) -> Any:
        self.launched.append(name)
        return self.launchers[name]()

    def connection_state(self) -> tuple[int, int]:
        if callable(self._connection_state):
            return self._connection_state()
        return self._connection_state

    def recorded_peak(self, name: str) -> int | None:
        return None

    def memory_limit(self, name: str) -> int:
        return self._limits.get(name, 1 << 40)

    def is_heavy(self, name: str) -> bool:
        return name in self._heavy

    def sample_memory(self, pid: int) -> int | None:
        return self._sampler(pid) if self._sampler else None

    def memory_budget(self) -> int:
        return self._budget

    def connection_budget_exhausted(self, name: str, **kwargs: Any) -> dict[str, Any]:
        return source_job_runtime.connection_budget_exhausted_result(name, **kwargs)

    def start_failed(self, name: str, **kwargs: Any) -> dict[str, Any]:
        return source_job_runtime.source_start_failed_result(name, **kwargs)


def _spawned(name: str, target: Any, *, memory_limit: int = 1 << 40, **payload: Any):
    def launch() -> source_jobs.SpawnedSourceJob:
        return source_jobs.SpawnedSourceJob(
            name=name,
            payload={"name": name, **payload},
            memory_limit=memory_limit,
            on_crash=lambda job: {
                "name": job.name,
                "status": "FAIL",
                "stage": "child_exit",
                "memory_kill": job.memory_kill,
                "exit": job.describe_exit(),
            },
            target=target,
        )

    return launch


def _run_with_watchdog(seconds: float, **kwargs: Any) -> source_jobs.SourceJobOutcome:
    """Run the coordinator; fail the test instead of hanging on a deadlock."""
    box: dict[str, Any] = {}

    def body() -> None:
        try:
            box["outcome"] = source_jobs.run_source_jobs(**kwargs)
        except BaseException as exc:  # noqa: BLE001 - re-raised below
            box["error"] = exc

    thread = threading.Thread(target=body, daemon=True)
    thread.start()
    thread.join(seconds)
    assert not thread.is_alive(), f"coordinator did not finish in {seconds}s"
    if "error" in box:
        raise box["error"]
    return box["outcome"]


# --------------------------------------------------------------------------
# spill files: the write-set travels through a file, never the pipe
# --------------------------------------------------------------------------


def _big_payload() -> dict[str, Any]:
    return {
        "graph_write_set": {
            "schema": "okg.graph_write_set.v1",
            "nodes_by_subtype": {
                "document": [f"doc:{index}" for index in range(25_001)],
                "file": [],
            },
            "node_attr_changes_by_subtype": {
                "document": {
                    f"doc:{index}": ["text", "title"] for index in range(12_345)
                },
            },
            "edges_by_type": {"mentions": list(range(10_001))},
            "edge_table_available": True,
        },
        "source_delta_batch": {
            "contract": "valid",
            "counts": {"records": 3},
            "records": [{"id": "a"}, {"id": "b"}],
        },
    }


def test_spill_round_trip_chunks_large_write_sets(tmp_path: Path) -> None:
    payload = _big_payload()
    path = tmp_path / "spill.jsonl"

    lines = source_jobs.write_spill(path, payload)

    assert source_jobs.read_spill(path) == payload
    # 25,001 ids -> 3 chunks; 12,345 entries -> 2; 10,001 edges -> 2.
    assert lines > 7
    assert path.stat().st_mode & 0o777 == 0o600
    assert not (tmp_path / "spill.jsonl.partial").exists()


def test_truncated_or_missing_spill_is_unreadable(tmp_path: Path) -> None:
    path = tmp_path / "spill.jsonl"
    source_jobs.write_spill(path, _big_payload())
    text = path.read_text().splitlines(keepends=True)
    path.write_text("".join(text[:-1]))

    with pytest.raises(source_jobs.SpillUnreadable, match="truncated"):
        source_jobs.read_spill(path)
    with pytest.raises(source_jobs.SpillUnreadable, match="cannot be opened"):
        source_jobs.read_spill(tmp_path / "absent.jsonl")


def test_lost_spill_file_means_no_hint_never_no_work(tmp_path: Path) -> None:
    """A source with work whose spill is gone sends the publish full-scope."""
    lost = {
        "name": "a",
        "status": "OK",
        "nodes_appended": 5,
        "spill": {
            "status": "written",
            "path": str(tmp_path / "gone.jsonl"),
            "keys": ["graph_write_set"],
        },
    }

    source_job_runtime.merge_source_job_spill(lost)

    assert "graph_write_set" not in lost
    assert lost["spill"]["status"] == "unreadable"
    assert ingest_service._layer_results_have_source_work([lost]) is True
    assert ingest_service._merge_layer_chronos_dirty_scope_hint([lost]) is None

    kept_path = tmp_path / "kept.jsonl"
    source_jobs.write_spill(kept_path, _big_payload())
    kept = {
        "name": "b",
        "status": "OK",
        "nodes_appended": 5,
        "spill": {"status": "written", "path": str(kept_path), "keys": []},
    }
    source_job_runtime.merge_source_job_spill(kept)
    hint = ingest_service._merge_layer_chronos_dirty_scope_hint([kept])
    assert kept["spill"]["status"] == "merged"
    assert hint is not None
    assert len(hint[0]["document"]) == 25_001


def test_step_dir_is_private_and_leftovers_are_swept(tmp_path: Path) -> None:
    base = tmp_path / "jobs"
    base.mkdir()
    dead = multiprocessing.get_context("spawn").Process(target=time.sleep, args=(0,))
    dead.start()
    dead.join()
    leftover = base / f"step-{dead.pid}-leftover"
    leftover.mkdir()
    (leftover / "x.jsonl").write_text("secret\n")
    alive = base / f"step-{os.getppid()}-live"
    alive.mkdir()

    with source_jobs.source_job_step_dir(base) as step:
        assert not leftover.exists()
        assert alive.exists(), "a live coordinator's directory must survive"
        assert step.stat().st_mode & 0o777 == 0o700
        assert base.stat().st_mode & 0o777 == 0o700
    assert not step.exists()


# --------------------------------------------------------------------------
# the pipe, memory, crashes and start order (real spawn children)
# --------------------------------------------------------------------------


def test_parent_drains_a_large_summary_before_waiting() -> None:
    size = 64 * _MB
    runtime = _FakeRuntime(
        {"a": _spawned("a", children.send_large_result, size=size)}
    )

    outcome = _run_with_watchdog(
        120, ordered=["a"], deps={}, runtime=runtime, max_concurrency=1,
    )

    result = outcome.results["a"]
    assert result["status"] == "OK"
    assert len(result["blob"]) == size
    assert result["source_process"]["exit_code"] == 0


def test_connection_admission_refuses_after_bounded_wait(monkeypatch) -> None:
    monkeypatch.setenv("OKG_SOURCE_CONNECTION_WAIT_SECONDS", "0")
    runtime = _FakeRuntime({}, connection_state=(90, 100))

    outcome = _run_with_watchdog(
        30, ordered=["a", "b"], deps={"b": {"a"}}, runtime=runtime,
        max_concurrency=6,
    )

    assert runtime.launched == []
    refused = outcome.results["a"]
    assert refused["status"] == "FAIL"
    assert refused["code"] == "source_connection_budget_exhausted"
    assert refused["stage"] == "connection_admission"
    per_child = source_jobs.connections_per_child()
    assert refused["error_data"] == {
        "source": "a",
        "live": 90,
        "needed": per_child,
        "allowed": 80,
        "waited_seconds": refused["error_data"]["waited_seconds"],
    }
    assert outcome.skipped == {"b": ["a"]}


def test_connection_admission_counts_children_not_yet_connected(monkeypatch) -> None:
    """A child that has not reported ready counts at its full five."""
    monkeypatch.setenv("OKG_SOURCE_CONNECTION_WAIT_SECONDS", "30")
    per_child = source_jobs.connections_per_child()
    # Room for exactly one child: live + per_child == allowed.
    runtime = _FakeRuntime(
        {
            "a": _spawned("a", children.sleep_then_result, seconds=1.0),
            "b": _spawned("b", children.sleep_then_result, seconds=0.1),
        },
        connection_state=(100 - 20 - per_child, 100),
    )

    outcome = _run_with_watchdog(
        60, ordered=["a", "b"], deps={}, runtime=runtime, max_concurrency=6,
    )

    assert outcome.results["a"]["status"] == "OK"
    assert outcome.results["b"]["status"] == "OK"
    assert "b" in outcome.admission_waits


def test_memory_over_limit_kills_the_child(monkeypatch) -> None:
    monkeypatch.setattr(source_jobs, "MEMORY_SAMPLE_INTERVAL_SECONDS", 0.2)
    limit = 64 * _MB
    runtime = _FakeRuntime(
        {
            "hog": _spawned(
                "hog", children.hog_memory, memory_limit=limit,
                megabytes=400, hold_seconds=60,
            ),
        },
        sampler=source_jobs.process_footprint_bytes,
    )

    outcome = _run_with_watchdog(
        60, ordered=["hog"], deps={}, runtime=runtime, max_concurrency=1,
    )

    result = outcome.results["hog"]
    assert result["status"] == "FAIL"
    assert result["memory_kill"]["limit_bytes"] == limit
    assert result["memory_kill"]["peak_bytes"] > limit
    assert result["exit"]["exit_code"] == -signal.SIGKILL
    assert result["source_process"]["peak_memory_bytes"] > limit


def _prepared() -> Any:
    return source_job_runtime.PreparedSourceRun(
        run_mode="scope_complete",
        sync_scope={},
        run_context=SourceRunContext(
            run_id=str(uuid.uuid4()),
            observed_at=datetime.now(timezone.utc),
            admission_id="admission-crash",
        ),
        workflow_ref={"workflow_id": "wf-crash"},
        envelope_status="pending",
        strict_fingerprint=None,
        timings_ms={"admission_prepare": 1.0},
    )


def _capture_compensation(monkeypatch) -> dict[str, list[Any]]:
    calls: dict[str, list[Any]] = {"retract": [], "restore": [], "fail": []}
    monkeypatch.setattr(
        ingest_service,
        "_retract_chronos_run",
        lambda dsn, **kwargs: calls["retract"].append(kwargs),
    )
    monkeypatch.setattr(
        ingest_service,
        "_restore_source_state",
        lambda dsn, snapshot, **kwargs: calls["restore"].append((snapshot, kwargs)),
    )
    monkeypatch.setattr(
        ingest_service,
        "_fail_source_admission",
        lambda **kwargs: calls["fail"].append(kwargs),
    )
    return calls


def test_child_crash_runs_the_failure_path(monkeypatch, tmp_path: Path) -> None:
    calls = _capture_compensation(monkeypatch)
    prepared = _prepared()
    snapshot = SourceStateSnapshot(
        deployment="svc", source="svc.a", watermark={"cursor": 1}, progress_rows=(),
    )
    entry = types.SimpleNamespace(ownership_id="svc.a", raw={}, admission_policy=None)
    options = IngestOptions(dsn="postgres://unused", deployment="svc", deployment_dir=tmp_path)

    def launch() -> source_jobs.SpawnedSourceJob:
        return source_jobs.SpawnedSourceJob(
            name="a",
            payload={
                "name": "a",
                "exit_status": 3,
                "started": {
                    "source_state_snapshot": snapshot,
                    "graph_branch": "fixture-branch",
                    "managed_change_identity": True,
                },
            },
            memory_limit=1 << 40,
            on_crash=lambda job: source_job_runtime.fail_source_job(
                options, entry, "a", prepared, job=job, started=time.time(),
            ),
            target=children.crash_after_started,
        )

    outcome = _run_with_watchdog(
        60,
        ordered=["a", "b"],
        deps={"b": {"a"}},
        runtime=_FakeRuntime({"a": launch}),
        max_concurrency=2,
    )

    result = outcome.results["a"]
    assert result["status"] == "FAIL"
    assert result["stage"] == "child_exit"
    assert result["error"] == "source child exited with status 3"
    assert result["error_data"]["exit_code"] == 3
    assert result["admission_id"] == "admission-crash"
    assert result["source_state_restored"] is True
    assert calls["retract"] == [{
        "admission_id": "admission-crash",
        "source": "svc.a",
        "run_id": prepared.run_context.run_id,
        "branch": "fixture-branch",
    }]
    assert calls["restore"] == [
        (snapshot, {"expected_replay_run_id": prepared.run_context.run_id})
    ]
    assert calls["fail"][0]["admission_id"] == "admission-crash"
    assert calls["fail"][0]["error"] == "source child exited with status 3"
    assert outcome.skipped == {"b": ["a"]}


def test_memory_kill_result_names_the_registered_code(monkeypatch, tmp_path) -> None:
    calls = _capture_compensation(monkeypatch)
    job = types.SimpleNamespace(
        started=None,
        memory_kill={"peak_bytes": 9 * _MB, "limit_bytes": 4 * _MB},
        result=None,
        describe_exit=lambda: {"exit_code": -9, "signal": "SIGKILL"},
    )
    entry = types.SimpleNamespace(ownership_id="svc.hog", raw={}, admission_policy=None)

    result = source_job_runtime.fail_source_job(
        IngestOptions(dsn="postgres://unused", deployment="svc", deployment_dir=tmp_path),
        entry,
        "hog",
        _prepared(),
        job=job,
        started=time.time(),
    )

    assert result["code"] == "source_memory_limit_exceeded"
    assert result["stage"] == "child_exit"
    assert result["error_data"]["peak_bytes"] == 9 * _MB
    assert result["error_data"]["limit_bytes"] == 4 * _MB
    assert result["error_data"]["signal"] == "SIGKILL"
    assert "source_state_restored" in result
    assert calls["restore"] == []  # the child never reported its snapshot
    assert len(calls["retract"]) == 1 and len(calls["fail"]) == 1
    # The kill message names the limit in GiB and bytes, and the one variable
    # that raises it.
    assert f"0.0 GiB ({4 * _MB} bytes)" in result["error"]
    assert "OKG_SOURCE_MEMORY_LIMIT_BYTES" in result["error"]


def test_memory_kill_message_names_a_16_gib_limit(monkeypatch, tmp_path) -> None:
    _capture_compensation(monkeypatch)
    limit = 16 * 1024 ** 3
    job = types.SimpleNamespace(
        started=None,
        memory_kill={"peak_bytes": limit + 1, "limit_bytes": limit},
        result=None,
        describe_exit=lambda: {"exit_code": -9, "signal": "SIGKILL"},
    )
    entry = types.SimpleNamespace(ownership_id="svc.hog", raw={}, admission_policy=None)

    result = source_job_runtime.fail_source_job(
        IngestOptions(dsn="postgres://unused", deployment="svc", deployment_dir=tmp_path),
        entry,
        "hog",
        _prepared(),
        job=job,
        started=time.time(),
    )

    assert "over its limit of 16.0 GiB (17179869184 bytes)" in result["error"]
    assert "raise OKG_SOURCE_MEMORY_LIMIT_BYTES" in result["error"]


@pytest.mark.parametrize(
    ("platform", "signal_name", "hinted"),
    [
        ("darwin", "SIGKILL", True),
        ("linux", "SIGKILL", False),
        ("darwin", "SIGTERM", False),
    ],
)
def test_unexplained_sigkill_on_macos_points_to_crash_reports(
    monkeypatch, tmp_path, platform, signal_name, hinted,
) -> None:
    """PACT macos-first-install-kill: a kernel kill on macOS (no memory kill
    recorded) leaves a crash report; the error says where to look."""
    _capture_compensation(monkeypatch)
    monkeypatch.setattr(source_job_runtime.sys, "platform", platform)
    job = types.SimpleNamespace(
        started=None,
        memory_kill=None,
        abort_reason=None,
        result=None,
        describe_exit=lambda: {"exit_code": -9, "signal": signal_name},
    )
    entry = types.SimpleNamespace(ownership_id="svc.code", raw={}, admission_policy=None)

    result = source_job_runtime.fail_source_job(
        IngestOptions(dsn="postgres://unused", deployment="svc", deployment_dir=tmp_path),
        entry,
        "code",
        _prepared(),
        job=job,
        started=time.time(),
    )

    assert result["error"].startswith(
        f"source child killed by signal {signal_name}"
    )
    assert ("~/Library/Logs/DiagnosticReports" in result["error"]) is hinted


def _strict_scaffold_entry(tmp_path: Path) -> Any:
    """The scaffold's source, admitted under strict_v1 like guided add does."""
    from okg.substrate.deployment_scaffold import create_deployment_scaffold
    from okg.substrate.sources.registry import admit_source_registry_document

    dep = create_deployment_scaffold(
        "strictchild", deployments_root=tmp_path,
    ).deployment_dir
    registry = dep / "source_registry.yaml"
    document = yaml.safe_load(registry.read_text(encoding="utf-8"))
    document["admission_contract"] = "strict_v1"
    admitted = admit_source_registry_document(document, registry_path=registry)
    assert admitted.strict
    return admitted.entries["bootstrap-fixture"]


def test_a_strict_entry_reaches_the_child_as_plain_data(tmp_path: Path) -> None:
    """A strict entry is frozen into read-only mapping views that cannot be
    pickled; the child payload carries a plain description that rebuilds the
    same entry on the other side of the spawn."""
    from okg.substrate.sources.registry import source_entry_from_description

    entry = _strict_scaffold_entry(tmp_path)
    with pytest.raises(TypeError, match="mappingproxy"):
        pickle.dumps(entry)
    payload = source_job_runtime.source_child_payload(
        IngestOptions(dsn="postgres://unused", deployment="svc", deployment_dir=tmp_path),
        entry,
        "bootstrap-fixture",
        _prepared(),
        spill_path=tmp_path / "spill.jsonl",
        lock_wait_seconds=1.0,
    )

    received = pickle.loads(pickle.dumps(payload))

    rebuilt = source_entry_from_description(received["entry"])
    assert rebuilt == entry
    assert rebuilt.admission_contract == "strict_v1"
    assert received["prepared"] == payload["prepared"]


def test_the_child_payload_hands_over_the_run_guard(tmp_path: Path) -> None:
    """#3003 round 3: a spawn child does not inherit the guard's context, so
    the payload carries the holder's pid for the child's per-chunk check;
    a run outside any guard (the runtime worker) hands over nothing."""
    import contextvars

    from okg.substrate.runtime.source_sync_priority import adopt_run_guard

    entry = _strict_scaffold_entry(tmp_path)

    def build() -> dict[str, Any]:
        return source_job_runtime.source_child_payload(
            IngestOptions(
                dsn="postgres://unused", deployment="svc", deployment_dir=tmp_path,
            ),
            entry,
            "bootstrap-fixture",
            _prepared(),
            spill_path=tmp_path / "spill.jsonl",
            lock_wait_seconds=1.0,
        )

    def build_inside_guard() -> dict[str, Any]:
        adopt_run_guard(
            {"deployment": "svc", "pid": 4242, "backend_start": "2026-09-30 00:00:00+00"},
            dsn="postgres://unused",
        )
        return build()

    inside = contextvars.copy_context().run(build_inside_guard)
    assert pickle.loads(pickle.dumps(inside))["run_guard"] == {
        "deployment": "svc", "pid": 4242, "backend_start": "2026-09-30 00:00:00+00",
    }
    assert build()["run_guard"] is None

    # A spawned child adopts the handoff during its process setup.
    assert _spawn_child(children.report_run_guard, inside)["held"] is True
    assert _spawn_child(children.report_run_guard, build())["held"] is False


def _spawn_child(target: Any, payload: dict[str, Any]) -> dict[str, Any]:
    ctx = multiprocessing.get_context("spawn")
    receiver, sender = ctx.Pipe(duplex=False)
    lifeline_receiver, lifeline_sender = ctx.Pipe(duplex=False)
    process = ctx.Process(target=target, args=(sender, lifeline_receiver, payload))
    process.start()
    sender.close()
    try:
        assert receiver.poll(60), "child sent no result"
        kind, body = receiver.recv()
    finally:
        process.join(30)
        lifeline_sender.close()
    assert kind == "result"
    return body


def _child_deployment_lookup(payload: dict[str, Any]) -> str | None:
    ctx = multiprocessing.get_context("spawn")
    receiver, sender = ctx.Pipe(duplex=False)
    lifeline_receiver, lifeline_sender = ctx.Pipe(duplex=False)
    process = ctx.Process(
        target=children.report_deployment_lookup,
        args=(sender, lifeline_receiver, payload),
    )
    process.start()
    sender.close()
    try:
        assert receiver.poll(60), "child sent no result"
        kind, body = receiver.recv()
    finally:
        process.join(30)
        lifeline_sender.close()
    assert kind == "result"
    return body["found"]


def test_a_source_child_finds_the_folder_its_command_resolved(tmp_path: Path) -> None:
    """A deployment given by folder (#2951) is found by name only inside the
    command's binding scope. That scope is a context variable, which a
    ``spawn`` child does not inherit, so the payload carries the bindings and
    the child binds them again. Without that the child would not find a
    deployment outside the deployments roots."""
    from okg.substrate.deployments import (
        bind_resolved_deployment,
        deployment_binding_scope,
        find_deployment_dir,
    )

    entry = _strict_scaffold_entry(tmp_path)
    dep = tmp_path / "strictchild"
    assert find_deployment_dir("strictchild") is None
    options = IngestOptions(
        dsn="postgres://unused", deployment="strictchild", deployment_dir=dep,
    )

    def payload() -> dict[str, Any]:
        built = source_job_runtime.source_child_payload(
            options,
            entry,
            "bootstrap-fixture",
            _prepared(),
            spill_path=tmp_path / "spill.jsonl",
            lock_wait_seconds=1.0,
        )
        return {**pickle.loads(pickle.dumps(built)), "name": "strictchild"}

    with deployment_binding_scope():
        assert bind_resolved_deployment("strictchild", dep)
        bound = payload()
    assert bound["deployment_bindings"] == {"strictchild": str(dep.resolve())}
    assert _child_deployment_lookup(bound) == str(dep.resolve())

    unbound = payload()
    assert unbound["deployment_bindings"] is None
    assert _child_deployment_lookup(unbound) is None


def test_an_entry_that_would_not_rebuild_equal_is_refused(tmp_path: Path) -> None:
    """The parent refuses to start a child on an entry other than the one it
    admitted, by name, instead of letting the child run a different one."""
    entry = dataclasses.replace(
        _strict_scaffold_entry(tmp_path), ownership_id="strictchild.other",
    )

    with pytest.raises(SubstrateError, match="does not rebuild equal"):
        source_job_runtime.source_child_payload(
            IngestOptions(dsn="postgres://unused", deployment="svc", deployment_dir=tmp_path),
            entry,
            "bootstrap-fixture",
            _prepared(),
            spill_path=tmp_path / "spill.jsonl",
            lock_wait_seconds=1.0,
        )


def test_dependent_starts_when_its_predecessor_finishes_not_its_layer() -> None:
    """Layers are [[a, b], [c]] with c -> a. c must not wait for slow b."""
    runtime = _FakeRuntime({
        "a": _spawned("a", children.sleep_then_result, seconds=0.2),
        "b": _spawned("b", children.sleep_then_result, seconds=6.0),
        "c": _spawned("c", children.sleep_then_result, seconds=0.2),
    })

    outcome = _run_with_watchdog(
        120, ordered=["a", "b", "c"], deps={"c": {"a"}}, runtime=runtime,
        max_concurrency=6,
    )

    assert outcome.start_order == ["a", "b", "c"]
    assert all(outcome.results[name]["status"] == "OK" for name in "abc")
    assert outcome.started_at["c"] >= outcome.finished_at["a"]
    assert outcome.finished_at["c"] < outcome.finished_at["b"]
    assert outcome.results["c"]["child_finished"] < outcome.results["b"]["child_finished"]


def test_heavy_sources_run_one_at_a_time() -> None:
    runtime = _FakeRuntime(
        {
            "agent-sessions": _spawned("agent-sessions", children.sleep_then_result, seconds=1.0),
            "code-structure": _spawned("code-structure", children.sleep_then_result, seconds=0.2),
        },
        heavy=frozenset({"agent-sessions", "code-structure"}),
    )

    outcome = _run_with_watchdog(
        60, ordered=["agent-sessions", "code-structure"], deps={},
        runtime=runtime, max_concurrency=6,
    )

    first, second = outcome.results["agent-sessions"], outcome.results["code-structure"]
    assert second["child_started"] >= first["child_finished"]


def test_memory_admission_holds_a_start_that_does_not_fit() -> None:
    runtime = _FakeRuntime(
        {
            "a": _spawned("a", children.sleep_then_result, seconds=1.0),
            "b": _spawned("b", children.sleep_then_result, seconds=0.2),
        },
        limits={"a": 6, "b": 6},
        budget=10,
    )

    outcome = _run_with_watchdog(
        60, ordered=["a", "b"], deps={}, runtime=runtime, max_concurrency=6,
    )

    assert outcome.results["b"]["child_started"] >= outcome.results["a"]["child_finished"]


def test_every_source_gets_the_same_16_gib_limit(monkeypatch) -> None:
    monkeypatch.delenv("OKG_SOURCE_MEMORY_LIMIT_BYTES", raising=False)
    gib = 1024 ** 3
    assert source_jobs.memory_limit_bytes() == 16 * gib
    runtime = source_job_runtime.ProcessSourceJobRuntime.__new__(
        source_job_runtime.ProcessSourceJobRuntime
    )
    for name in ("code-structure", "git-history", "pact", "openspec", "anything"):
        assert runtime.memory_limit(name) == 16 * gib
    # ingest.source_process (4: run, nested borrow, spare, run lock)
    # + interval GC + compensation.
    assert source_jobs.connections_per_child() == 6


def test_one_variable_sets_the_limit_for_every_source(monkeypatch) -> None:
    monkeypatch.setenv("OKG_SOURCE_MEMORY_LIMIT_BYTES", str(40 * 1024 ** 3))
    assert source_jobs.memory_limit_bytes() == 40 * 1024 ** 3
    monkeypatch.setenv("OKG_SOURCE_MEMORY_LIMIT_BYTES", " 1024 ")
    assert source_jobs.memory_limit_bytes() == 1024


@pytest.mark.parametrize("raw", ["abc", "1.5", "0", "-1", "inf"])
def test_a_bad_limit_value_is_refused_naming_the_variable(monkeypatch, raw) -> None:
    monkeypatch.setenv("OKG_SOURCE_MEMORY_LIMIT_BYTES", raw)
    with pytest.raises(ValueError, match="OKG_SOURCE_MEMORY_LIMIT_BYTES"):
        source_jobs.memory_limit_bytes()


def test_footprint_of_this_process_is_measured() -> None:
    measured = source_jobs.process_footprint_bytes(os.getpid())
    assert measured is not None and measured > 10 * _MB


# --------------------------------------------------------------------------
# database: the run lock and the real ingest path
# --------------------------------------------------------------------------


def _wait_for(predicate: Any, seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.1)
    return predicate()


def _pid_gone(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    # A zombie still answers kill(0); ask ps for its state.
    import subprocess

    state = subprocess.run(
        ["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True,
    ).stdout.strip()
    return state == "" or state.startswith("Z")


@pytest.mark.postgres
def test_killed_parent_takes_its_child_and_run_lock_with_it(
    clean_runtime: str, tmp_path: Path,
) -> None:
    pid_file = tmp_path / "child.pid"
    payload = {
        "dsn": clean_runtime,
        "deployment": "svc",
        "source": "svc.orphan",
        "pid_file": str(pid_file),
    }
    parent = multiprocessing.get_context("spawn").Process(
        target=children.orphaning_parent, args=(payload,),
    )
    parent.start()
    try:
        assert _wait_for(pid_file.exists, 60), "child never took the run lock"
        child_pid = int(pid_file.read_text())

        second = source_jobs.ChildJobHooks(
            dsn=clean_runtime, deployment="svc", result_conn=None,
            lock_wait_seconds=1.0,
        )
        with pytest.raises(SourceRunAlreadyActive) as refused:
            second.acquire_run_lock("svc.orphan")
        second.close()
        assert refused.value.code == "source_run_already_active"
        assert refused.value.data["lock_key"] == "okg.source.run:svc:svc.orphan"
        assert refused.value.data["source"] == "svc.orphan"

        os.kill(parent.pid, signal.SIGKILL)
        parent.join(10)
        assert _wait_for(lambda: _pid_gone(child_pid), 30), (
            "the child outlived its killed parent"
        )

        third = source_jobs.ChildJobHooks(
            dsn=clean_runtime, deployment="svc", result_conn=None,
            lock_wait_seconds=10.0,
        )
        third.acquire_run_lock("svc.orphan")
        assert third.lock_key == "okg.source.run:svc:svc.orphan"
        third.close()
    finally:
        if parent.is_alive():
            parent.kill()
            parent.join()


def _write_fixture_deployment(tmp_path: Path, sources: dict[str, dict]) -> Path:
    from tests.substrate.test_ingest_service import (
        _commit_layered_fixture_deployment,
        _fixture_source_privacy_policy,
        _source,
    )

    dep = tmp_path / "deployments" / "svc"
    dep.mkdir(parents=True)
    (dep / "schemas").mkdir()
    (dep / "schemas" / "fixture.yaml").write_text(
        "id: https://example.invalid/okg/fixture\n"
        "name: ingest_fixture\n"
        "classes:\n"
        "  Document:\n"
        "    attributes:\n"
        "      text: {}\n"
        "    annotations:\n"
        "      archetype: artifact\n"
        "      transient: 'false'\n"
    )
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "svc",
        "description": "source job fixture",
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "modules": [],
        "nomos": {
            "deployment_posture": {
                "deployment_class": "dogfood_workspace",
                "read_principals": ["human_operator", "agent"],
                "write_principals": ["human_operator", "source_adapter"],
                "mcp_write_default": "review",
                "export_policy": "internal_only",
                "secret_handling": "secret_ref_required",
                "live_call_default": "review",
                "allowed_source_sensitivities": ["local_private"],
                "allowed_data_classifications": ["private"],
                "allowed_exportability": ["no_export"],
                "privacy_obligations": ["audit"],
                "readiness_profiles": ["dogfood"],
            },
        },
    }))
    registry = {}
    for name, spec in sources.items():
        registry[name] = {
            **_source(f"svc.{name}", predecessor_source=spec.get("after")),
            "module": FIXTURE_MODULE,
            "class": spec["class"],
            "params": spec["params"],
            "source_policy": _fixture_source_privacy_policy(),
            "sync": {
                "triggers": ["manual", "reconcile"],
                "default_event_mode": "scope_complete",
                "reconcile_mode": "scope_complete",
            },
        }
    (dep / "source_registry.yaml").write_text(yaml.safe_dump({"sources": registry}))
    _commit_layered_fixture_deployment(dep, message="source job fixture")
    return dep


def _ingest(dsn: str, dep: Path):
    from tests.pytest.catalog_fixture_builders import seed_catalog
    from tests.substrate.test_ingest_service import _claim_layered_fixture_catalog

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    _claim_layered_fixture_catalog(dsn, dep)
    return run_ingest(
        IngestOptions(dsn=dsn, deployment="svc", deployment_dir=dep, publish=False),
    )


def _envelope_status(dsn: str, admission_id: str) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn:
        row = conn.execute(
            "SELECT status FROM okg.admission_envelopes WHERE admission_id = %s",
            (admission_id,),
        ).fetchone()
    return str(row[0])


@pytest.mark.postgres
def test_real_ingest_runs_each_source_in_its_own_process(
    clean_runtime: str, tmp_path: Path, monkeypatch,
) -> None:
    spill_base = tmp_path / "spill"
    monkeypatch.setenv("OKG_SOURCE_SPILL_DIR", str(spill_base))
    dep = _write_fixture_deployment(tmp_path, {
        "docs": {"class": "DocFixtureSource", "params": {"prefix": "docs", "count": 3}},
        "more": {
            "class": "DocFixtureSource",
            "params": {"prefix": "more", "count": 2},
            "after": "svc.docs",
        },
    })

    outcome = _ingest(clean_runtime, dep)

    assert outcome.any_failed is False, outcome.report["results"]
    by_name = {result["name"]: result for result in outcome.report["results"]}
    assert set(by_name) == {"docs", "more"}
    for name, count in (("docs", 3), ("more", 2)):
        result = by_name[name]
        assert result["status"] == "OK", result
        assert result["nodes_appended"] == count
        process = result["source_process"]
        assert process["pid"] != os.getpid()
        assert process["exit_code"] == 0
        # The child counted the database sockets it really held.
        assert 1 <= process["peak_connections"] <= source_jobs.connections_per_child()
        assert result["spill"]["status"] == "merged"
        ids = result["graph_write_set"]["nodes_by_subtype"]["document"]
        assert sorted(ids) == sorted(f"{name}:{index}" for index in range(count))
    assert outcome.report["source_jobs"]["start_order"] == ["docs", "more"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        recorded = dict(conn.execute(
            "SELECT source_name, value FROM okg.source_progress"
            " WHERE key = %s",
            (source_jobs.PEAK_CONNECTIONS_PROGRESS_KEY,),
        ).fetchall())
    assert set(recorded) == {"svc.docs", "svc.more"}, recorded
    assert list(spill_base.iterdir()) == [], "the step directory must be removed"


@pytest.mark.postgres
def test_real_ingest_killed_child_fails_alone(
    clean_runtime: str, tmp_path: Path, monkeypatch,
) -> None:
    monkeypatch.setenv("OKG_SOURCE_SPILL_DIR", str(tmp_path / "spill"))
    dep = _write_fixture_deployment(tmp_path, {
        "crash": {"class": "CrashFixtureSource", "params": {"prefix": "crash", "exit_status": 9}},
        "docs": {"class": "DocFixtureSource", "params": {"prefix": "docs", "count": 2}},
        "after-crash": {
            "class": "DocFixtureSource",
            "params": {"prefix": "later"},
            "after": "svc.crash",
        },
    })

    outcome = _ingest(clean_runtime, dep)

    by_name = {result["name"]: result for result in outcome.report["results"]}
    crashed = by_name["crash"]
    assert crashed["status"] == "FAIL"
    assert crashed["stage"] == "child_exit"
    assert crashed["error"] == "source child exited with status 9"
    assert crashed["source_process"]["exit_code"] == 9
    assert _envelope_status(clean_runtime, crashed["admission_id"]) == "failed"
    assert by_name["docs"]["status"] == "OK"
    assert by_name["docs"]["nodes_appended"] == 2
    assert "after-crash" not in by_name
    assert outcome.report["completeness"]["dependency_blocked_sources"] == [
        {"source": "after-crash", "blocked_by": ["crash"]},
    ]


@pytest.mark.postgres
def test_real_ingest_memory_limit_kills_only_that_source(
    clean_runtime: str, tmp_path: Path, monkeypatch,
) -> None:
    monkeypatch.setenv("OKG_SOURCE_SPILL_DIR", str(tmp_path / "spill"))
    monkeypatch.setattr(source_jobs, "MEMORY_SAMPLE_INTERVAL_SECONDS", 0.2)
    monkeypatch.setenv("OKG_SOURCE_MEMORY_LIMIT_BYTES", str(256 * _MB))
    dep = _write_fixture_deployment(tmp_path, {
        "hog": {
            "class": "MemoryHogFixtureSource",
            "params": {"prefix": "hog", "megabytes": 768, "hold_seconds": 60},
        },
        "docs": {"class": "DocFixtureSource", "params": {"prefix": "docs"}},
    })

    outcome = _ingest(clean_runtime, dep)

    by_name = {result["name"]: result for result in outcome.report["results"]}
    hog = by_name["hog"]
    assert hog["status"] == "FAIL"
    assert hog["code"] == "source_memory_limit_exceeded"
    assert hog["error_data"]["limit_bytes"] == 256 * _MB
    assert hog["error_data"]["peak_bytes"] > 256 * _MB
    assert _envelope_status(clean_runtime, hog["admission_id"]) == "failed"
    assert by_name["docs"]["status"] == "OK"
    failed = outcome.report["completeness"]["failed_sources"]
    assert failed == [{
        "source": "hog",
        "stage": "child_exit",
        "code": "source_memory_limit_exceeded",
    }]


# --------------------------------------------------------------------------
# review fixes: pool wait, run-lock refusal, coordinator failure, admission
# --------------------------------------------------------------------------


def test_a_ready_child_counts_the_connections_it_has_not_opened(monkeypatch) -> None:
    """A ready child holding 2 of its 6 still counts 4 more; b fits beside it."""
    monkeypatch.setenv("OKG_SOURCE_CONNECTION_WAIT_SECONDS", "30")
    per_child = source_jobs.connections_per_child()
    held = 2
    # Room for b (per_child) plus what a may still open (per_child - held).
    live = 100 - 20 - per_child - (per_child - held)
    runtime = _FakeRuntime(
        {
            "a": _spawned("a", children.report_connections_then_sleep, held=held, seconds=3.0),
            "b": _spawned("b", children.sleep_then_result, seconds=0.2),
        },
        connection_state=(live, 100),
    )

    outcome = _run_with_watchdog(
        60, ordered=["a", "b"], deps={}, runtime=runtime, max_concurrency=6,
    )

    a, b = outcome.results["a"], outcome.results["b"]
    assert a["status"] == b["status"] == "OK"
    assert b["child_started"] < a["child_finished"], "b waited for a to finish"
    assert a["source_process"]["peak_connections"] == held
    # b waited only while a had not reported: then a counted at its full 6.
    assert outcome.admission_waits["b"]["needed"] == 2 * per_child


def test_a_child_that_delivered_its_result_is_never_memory_killed(monkeypatch) -> None:
    monkeypatch.setattr(source_jobs, "MEMORY_SAMPLE_INTERVAL_SECONDS", 0.2)
    runtime = _FakeRuntime(
        {
            "a": _spawned(
                "a", children.result_then_hog, memory_limit=64 * _MB,
                megabytes=300, hold_seconds=2,
            ),
        },
        sampler=source_jobs.process_footprint_bytes,
    )

    outcome = _run_with_watchdog(
        60, ordered=["a"], deps={}, runtime=runtime, max_concurrency=1,
    )

    result = outcome.results["a"]
    assert result["status"] == "OK", result
    assert "memory_kill" not in result
    assert result["source_process"]["exit_code"] == 0
    assert result["source_process"]["peak_memory_bytes"] > 64 * _MB


def test_a_transient_admission_error_is_retried(monkeypatch) -> None:
    monkeypatch.setenv("OKG_SOURCE_CONNECTION_WAIT_SECONDS", "30")
    calls = {"n": 0}

    def flaky() -> tuple[int, int]:
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("connection reset")
        return 0, 1000

    runtime = _FakeRuntime(
        {"a": _spawned("a", children.sleep_then_result, seconds=0.1)},
        connection_state=flaky,
    )

    outcome = _run_with_watchdog(
        60, ordered=["a"], deps={}, runtime=runtime, max_concurrency=1,
    )

    assert calls["n"] >= 2
    assert outcome.results["a"]["status"] == "OK"


def test_a_start_that_keeps_failing_fails_only_that_source(monkeypatch) -> None:
    monkeypatch.setenv("OKG_SOURCE_CONNECTION_WAIT_SECONDS", "0")

    def broken() -> Any:
        raise RuntimeError("spawn refused")

    runtime = _FakeRuntime({
        "a": broken,
        "b": _spawned("b", children.sleep_then_result, seconds=0.2),
        "c": _spawned("c", children.sleep_then_result, seconds=0.1),
    })

    outcome = _run_with_watchdog(
        60, ordered=["a", "b", "c"], deps={"c": {"a"}}, runtime=runtime,
        max_concurrency=6,
    )

    failed = outcome.results["a"]
    assert failed["status"] == "FAIL"
    assert failed["stage"] == "child_start"
    assert "RuntimeError: spawn refused" in failed["error"]
    assert outcome.results["b"]["status"] == "OK"
    assert outcome.skipped == {"c": ["a"]}


def test_a_coordinator_failure_stops_every_running_child_with_its_failure_record() -> None:
    crashes: list[dict[str, Any]] = []

    def launch() -> source_jobs.SpawnedSourceJob:
        def on_crash(job: source_jobs.SpawnedSourceJob) -> dict[str, Any]:
            crashes.append({"abort": job.abort_reason, **job.describe_exit()})
            return {"name": job.name, "status": "FAIL", "stage": "child_exit"}

        return source_jobs.SpawnedSourceJob(
            name="a",
            payload={"name": "a", "seconds": 120},
            memory_limit=1 << 40,
            on_crash=on_crash,
            target=children.sleep_then_result,
        )

    def sampler(_pid: int) -> int:
        raise RuntimeError("sampler broke")

    runtime = _FakeRuntime({"a": launch}, sampler=sampler)

    with pytest.raises(RuntimeError, match="sampler broke"):
        _run_with_watchdog(
            60, ordered=["a"], deps={}, runtime=runtime, max_concurrency=1,
        )

    assert len(crashes) == 1, crashes
    assert crashes[0]["abort"] == "RuntimeError: sampler broke"
    assert crashes[0]["exit_code"] == -signal.SIGKILL


def test_the_default_spill_base_is_per_user_and_refuses_a_symlink(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("OKG_SOURCE_SPILL_DIR", raising=False)
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    assert source_jobs.spill_base_dir() == tmp_path / "cache" / "okg" / "source-jobs"

    real = tmp_path / "elsewhere"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real)
    with pytest.raises(PermissionError, match="not a real directory"):
        with source_jobs.source_job_step_dir(link):
            pass


def _run_source_failing_at_the_run_lock(monkeypatch, tmp_path, error: Exception) -> Any:
    class _Adapter:
        pass

    monkeypatch.setattr(ingest_service, "import_source_adapter_class", lambda _e: _Adapter)
    monkeypatch.setattr(ingest_service, "source_adapter_init_params", lambda *_a, **_k: {})
    monkeypatch.setattr(ingest_service, "bind_source_adapter_identity", lambda *_a: "fp")
    monkeypatch.setattr(
        ingest_service,
        "_snapshot_source_state",
        lambda *_a, **_k: pytest.fail("no state may be touched without the run lock"),
    )

    class _Hooks:
        def acquire_run_lock(self, source: str) -> None:
            raise error

        def started(self, **_payload: Any) -> None:
            pytest.fail("a run without its lock never starts")

    entry = types.SimpleNamespace(
        name="a", ownership_id="svc.a", admission_policy=None,
        raw={"module": "unused", "class": "Unused"},
    )
    return ingest_service._run_source(
        IngestOptions(dsn="postgres://unused", deployment="svc", deployment_dir=tmp_path),
        entry,
        "a",
        prepared=_prepared(),
        job_hooks=_Hooks(),
    )


def test_a_refused_run_lock_leaves_the_other_runs_work_alone(monkeypatch, tmp_path) -> None:
    """M2: the orphan holding the lock may share this run id and admission."""
    calls = _capture_compensation(monkeypatch)

    result = _run_source_failing_at_the_run_lock(
        monkeypatch,
        tmp_path,
        SourceRunAlreadyActive(
            "another run of this source is still active",
            data={
                "source": "svc.a", "deployment": "svc",
                "lock_key": "okg.source.run:svc:svc.a", "waited_seconds": 0.0,
            },
        ),
    )

    assert result["status"] == "FAIL"
    assert result["stage"] == "source_run_lock"
    assert result["code"] == "source_run_already_active"
    assert calls == {"retract": [], "restore": [], "fail": []}


def test_a_lock_connection_error_fails_the_admission_but_retracts_nothing(
    monkeypatch, tmp_path,
) -> None:
    """Only a REFUSED lock leaves the admission alone. A lock that could not be
    asked for at all (pool refusal, database error) still fails this run's
    admission, so it is not left pending; nothing was written, so nothing is
    retracted."""
    from okg.substrate.db.pool import PoolReservationExhausted

    calls = _capture_compensation(monkeypatch)

    result = _run_source_failing_at_the_run_lock(
        monkeypatch,
        tmp_path,
        PoolReservationExhausted("no connection on the source child's pool within 10s"),
    )

    assert result["status"] == "FAIL"
    assert result["stage"] == "source_run_lock"
    assert "code" not in result
    assert calls["retract"] == [] and calls["restore"] == []
    assert len(calls["fail"]) == 1
    assert calls["fail"][0]["admission_id"] == "admission-crash"
    assert "PoolReservationExhausted" in calls["fail"][0]["error"]


def test_a_coordinator_failure_collects_a_child_whose_result_is_unread(
    tmp_path: Path,
) -> None:
    """A child that sent its result before the coordinator failed finished its
    run and admission: it is collected as OK, never killed and compensated,
    even when its result was still unread in the pipe."""
    crashes: list[str] = []
    result_sent = tmp_path / "result-sent"

    def launch() -> source_jobs.SpawnedSourceJob:
        return source_jobs.SpawnedSourceJob(
            name="a",
            # Sends "started" then "result", marks the result as sent, then
            # stays alive.
            payload={
                "name": "a",
                "megabytes": 1,
                "hold_seconds": 3,
                "result_sent_marker": str(result_sent),
            },
            memory_limit=1 << 40,
            on_crash=lambda job: crashes.append(job.name) or {
                "name": job.name, "status": "FAIL", "stage": "child_exit",
            },
            target=children.result_then_hog,
        )

    def sampler(_pid: int) -> int:
        # Called when "started" is read. The coordinator reads one message
        # per wake, so "result" is still unread here, but under CPU load the
        # child may not have sent it yet; a child that has not sent its
        # result is rightly killed. Fail only once the result is in the pipe.
        deadline = time.monotonic() + 30
        while not result_sent.exists():
            if time.monotonic() >= deadline:
                raise RuntimeError("the child never sent its result")
            time.sleep(0.01)
        raise RuntimeError("sampler broke")

    box: dict[str, Any] = {}

    def body() -> None:
        runtime = _FakeRuntime({"a": launch}, sampler=sampler)
        box["runtime"] = runtime
        try:
            source_jobs.run_source_jobs(
                ordered=["a"], deps={}, runtime=runtime, max_concurrency=1,
            )
        except RuntimeError as exc:
            box["error"] = exc

    thread = threading.Thread(target=body, daemon=True)
    thread.start()
    thread.join(60)
    assert not thread.is_alive()
    assert str(box.get("error")) == "sampler broke"
    assert crashes == [], "a finished child was killed and compensated"


@pytest.mark.postgres
def test_the_child_pool_refuses_by_name_after_ten_seconds(clean_runtime: str, monkeypatch) -> None:
    """M1: a borrow beyond the child's pool fails with a named refusal, in the
    wait the design sets, not psycopg_pool's anonymous 30 s PoolTimeout."""
    from okg.substrate.db import concurrency_budget as budget
    from okg.substrate.db import pool as pool_mod

    pool_mod.close_all()
    monkeypatch.setattr(pool_mod, "_POOL_TERM", "ingest.source_process")
    budget.set_override("ingest.source_process", 2)
    try:
        started = time.monotonic()
        with pool_mod.pooled_connection(clean_runtime, purpose=pool_mod.RESERVED):
            with pool_mod.pooled_connection(clean_runtime, purpose=pool_mod.GENERAL):
                with pytest.raises(pool_mod.PoolReservationExhausted) as refused:
                    with pool_mod.pooled_connection(
                        clean_runtime, purpose=pool_mod.GENERAL,
                    ):
                        pass
        waited = time.monotonic() - started
    finally:
        budget.clear_overrides()
        pool_mod.close_all()
    assert "ingest.source_process" in str(refused.value)
    assert "within 10s" in str(refused.value)
    assert 9.0 <= waited < 20.0, waited
