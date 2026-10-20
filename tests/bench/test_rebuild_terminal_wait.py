"""Terminal-rebuild wait for the rebuild-equivalence proof (issue #1539).

The forced rebuild publishes one generation per ingest layer. The wait must
return only when the substrate's completion signals are all terminal and
stable, and must fail loudly instead of handing back a partial generation.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

import okg.bench.orchestrator as orchestrator

SOURCES = ("rb.git-files", "rb.code-structure", "rb.code-lsp-resolution")
T0 = "2026-09-12T12:28:30+00:00"
T1 = "2026-09-12T12:29:10+00:00"
T2 = "2026-09-12T12:29:40+00:00"
T3 = "2026-09-12T12:31:05+00:00"


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def _experiment(monkeypatch, tmp_path: Path) -> orchestrator.Experiment:
    monkeypatch.setattr(orchestrator, "BENCH_REPOS_DIR", tmp_path / "repos")
    monkeypatch.setattr(orchestrator, "GENERATED_DIR", tmp_path / "generated")
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", tmp_path / "results")
    return orchestrator.Experiment(
        id="rb",
        repo=orchestrator.Repo(name="tinydb", url="https://example.invalid/t.git"),
        source_set="files+code+lsp",
        replay=orchestrator.Replay(strategy="head_only", last_n=10),
    )


def _observation(
    *,
    generation_id,
    published_at,
    counts,
    reconciled: dict[str, str | None],
):
    return {
        "generation_id": generation_id,
        "generation_published_at": published_at,
        "workflow_counts": dict(counts),
        "sources": {
            name: {
                "reconciled_at": reconciled.get(name),
                "last_run_id": f"run-{name}" if reconciled.get(name) else None,
                "last_run_status": "OK" if reconciled.get(name) else None,
                "last_run_mode": "scope_complete",
            }
            for name in SOURCES
        },
    }


def _all_reconciled(at: str = T2) -> dict[str, str]:
    return {name: at for name in SOURCES}


def _script(observations):
    """An observe() that replays a scripted sequence, then repeats its last."""
    calls: list[int] = []

    def observe():
        index = min(len(calls), len(observations) - 1)
        calls.append(index)
        return observations[index]

    observe.calls = calls
    return observe


def _wait(exp, observe, clock, **kwargs):
    return orchestrator._wait_for_rebuild_publish(
        exp,
        previous_generation_id="okg:gen-0",
        timeout_s=kwargs.pop("timeout_s", 120.0),
        settle_s=kwargs.pop("settle_s", 3.0),
        poll_s=kwargs.pop("poll_s", 0.5),
        source_names=SOURCES,
        observe=observe,
        clock=clock,
        sleep=clock.sleep,
        **kwargs,
    )


def test_wait_skips_the_partial_layer_generation_and_returns_the_terminal_one(
    monkeypatch, tmp_path: Path,
) -> None:
    exp = _experiment(monkeypatch, tmp_path)
    clock = FakeClock()
    # Layer 1 published (generation 1) while the workflow is still PENDING and
    # only git-files has reconciled -- the exact capture the nightly made.
    partial = _observation(
        generation_id="okg:gen-1",
        published_at=T1,
        counts={"PENDING": 1},
        reconciled={"rb.git-files": T0},
    )
    # Queue reads empty between layers, but two sources have no scope-walk
    # watermark yet: still not terminal.
    empty_queue_gap = _observation(
        generation_id="okg:gen-1",
        published_at=T1,
        counts={"SUCCESS": 1},
        reconciled={"rb.git-files": T0},
    )
    terminal = _observation(
        generation_id="okg:gen-3",
        published_at=T3,
        counts={"SUCCESS": 2},
        reconciled=_all_reconciled(T2),
    )
    observe = _script([partial, partial, empty_queue_gap, terminal])

    receipt = _wait(exp, observe, clock)

    assert receipt["generation_id"] == "okg:gen-3"
    assert receipt["previous_generation_id"] == "okg:gen-0"
    assert receipt["observation"] == terminal
    assert receipt["source_names"] == list(SOURCES)
    assert receipt["settle_s"] == 3.0
    assert receipt["timeout_s"] == 120.0
    # The terminal observation was seen more than once: it had to hold
    # unchanged across the settle window before the wait returned.
    assert observe.calls.count(3) >= 2
    assert receipt["polls"] == len(observe.calls)
    assert receipt["elapsed_s"] >= 3.0


def test_wait_restarts_the_settle_window_when_a_new_generation_appears(
    monkeypatch, tmp_path: Path,
) -> None:
    exp = _experiment(monkeypatch, tmp_path)
    clock = FakeClock()
    first_terminal = _observation(
        generation_id="okg:gen-2",
        published_at=T2,
        counts={"SUCCESS": 1},
        reconciled=_all_reconciled(T1),
    )
    later = _observation(
        generation_id="okg:gen-3",
        published_at=T3,
        counts={"SUCCESS": 2},
        reconciled=_all_reconciled(T2),
    )
    # Terminal-looking for two polls (1 s), then a later generation lands.
    observe = _script([first_terminal, first_terminal, first_terminal, later])

    receipt = _wait(exp, observe, clock, settle_s=3.0, poll_s=0.5)

    assert receipt["generation_id"] == "okg:gen-3"


def test_wait_times_out_naming_the_unreconciled_sources(
    monkeypatch, tmp_path: Path,
) -> None:
    exp = _experiment(monkeypatch, tmp_path)
    clock = FakeClock()
    stuck = _observation(
        generation_id="okg:gen-1",
        published_at=T1,
        counts={"SUCCESS": 1},
        reconciled={"rb.git-files": T0},
    )
    observe = _script([stuck])

    with pytest.raises(TimeoutError) as excinfo:
        _wait(exp, observe, clock, timeout_s=10.0)

    message = str(excinfo.value)
    assert "rb: rebuild not terminal within 10s" in message
    assert excinfo.value.outstanding == [
        "source_not_reconciled:rb.code-lsp-resolution",
        "source_not_reconciled:rb.code-structure",
    ]
    assert excinfo.value.observation == stuck
    assert "generation_id=okg:gen-1" in message
    assert "previous_generation_id=okg:gen-0" in message


def test_wait_times_out_while_a_workflow_is_still_active(
    monkeypatch, tmp_path: Path,
) -> None:
    exp = _experiment(monkeypatch, tmp_path)
    clock = FakeClock()
    in_flight = _observation(
        generation_id="okg:gen-2",
        published_at=T2,
        counts={"SUCCESS": 1, "PENDING": 1},
        reconciled=_all_reconciled(T1),
    )
    observe = _script([in_flight])

    with pytest.raises(TimeoutError, match="active_source_sync_workflows:1"):
        _wait(exp, observe, clock, timeout_s=5.0)


def test_wait_requires_the_generation_to_postdate_source_completion(
    monkeypatch, tmp_path: Path,
) -> None:
    exp = _experiment(monkeypatch, tmp_path)
    clock = FakeClock()
    stale_generation = _observation(
        generation_id="okg:gen-1",
        published_at=T1,
        counts={"SUCCESS": 2},
        reconciled=_all_reconciled(T2),
    )
    observe = _script([stale_generation])

    with pytest.raises(TimeoutError, match="generation_predates_source_completion"):
        _wait(exp, observe, clock, timeout_s=5.0)


def test_wait_refuses_the_pre_worker_generation(monkeypatch, tmp_path: Path) -> None:
    exp = _experiment(monkeypatch, tmp_path)
    clock = FakeClock()
    unchanged = _observation(
        generation_id="okg:gen-0",
        published_at=T0,
        counts={},
        reconciled=_all_reconciled(T0),
    )
    observe = _script([unchanged])

    with pytest.raises(TimeoutError, match="no_new_published_generation"):
        _wait(exp, observe, clock, timeout_s=5.0)


def test_wait_raises_when_a_source_sync_workflow_failed(
    monkeypatch, tmp_path: Path,
) -> None:
    exp = _experiment(monkeypatch, tmp_path)
    clock = FakeClock()
    failed = _observation(
        generation_id="okg:gen-1",
        published_at=T1,
        counts={"SUCCESS": 1, "ERROR": 1},
        reconciled={"rb.git-files": T0},
    )
    observe = _script([failed])

    with pytest.raises(RuntimeError, match=r"ended failed \{'ERROR': 1\}"):
        _wait(exp, observe, clock)
    assert len(observe.calls) == 1


@pytest.mark.parametrize(
    "receipt, match",
    [
        (
            {"returncode": 1, "workflows": [], "error": "boom: no worker"},
            r"reconcile pass failed \(exit 1: boom: no worker\)",
        ),
        (
            {
                "returncode": 0,
                "workflows": [
                    {"workflow_id": "wf-1", "status": "SUCCESS", "any_failed": True},
                ],
                "error": None,
            },
            r"workflows not clean: \['wf-1'\]",
        ),
    ],
)
def test_wait_refuses_a_failed_reconcile_pass_before_polling(
    monkeypatch, tmp_path: Path, receipt, match,
) -> None:
    exp = _experiment(monkeypatch, tmp_path)
    clock = FakeClock()
    observe = _script([_observation(
        generation_id="okg:gen-1",
        published_at=T1,
        counts={"SUCCESS": 1},
        reconciled={"rb.git-files": T0},
    )])

    with pytest.raises(RuntimeError, match=match):
        _wait(exp, observe, clock, reconcile_receipt=receipt)
    assert observe.calls == []


def test_wait_accepts_a_clean_reconcile_receipt(monkeypatch, tmp_path: Path) -> None:
    exp = _experiment(monkeypatch, tmp_path)
    clock = FakeClock()
    terminal = _observation(
        generation_id="okg:gen-3",
        published_at=T3,
        counts={"SUCCESS": 2},
        reconciled=_all_reconciled(T2),
    )
    receipt = {
        "returncode": 0,
        "workflows": [{"workflow_id": "wf-1", "status": "SUCCESS", "any_failed": False}],
        "error": None,
    }

    result = _wait(exp, _script([terminal]), clock, reconcile_receipt=receipt)

    assert result["generation_id"] == "okg:gen-3"


class _FakeCursor:
    """Answers the SQL `_rebuild_terminal_observation` issues, in order, with
    the column types the live tables actually return: chronos checkpoint
    `created_at` is TEXT (an ISO string), `last_reconcile_at` is TIMESTAMPTZ."""

    def __init__(self, rows_by_marker: dict[str, list[tuple]]) -> None:
        self._rows_by_marker = rows_by_marker
        self._rows: list[tuple] = []
        self.statements: list[str] = []

    def execute(self, statement: str, params=None) -> None:
        self.statements.append(statement)
        for marker, rows in self._rows_by_marker.items():
            if marker in statement:
                self._rows = list(rows)
                return
        self._rows = []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> bool:
        return False


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> bool:
        return False


def test_observation_reads_text_chronos_created_at_and_timestamptz_watermarks(
    monkeypatch, tmp_path: Path,
) -> None:
    from datetime import datetime, timezone

    exp = _experiment(monkeypatch, tmp_path)
    reconciled_at = datetime(2026, 9, 12, 12, 29, 40, tzinfo=timezone.utc)
    cursor = _FakeCursor({
        "metadata::jsonb->>'kind' = 'okg_generation'": [("okg:gen-3",)],
        "FROM public._chronos_branch_interval_checkpoints\n                 WHERE checkpoint_id": [
            ("2026-09-12T12:31:05.123456+00:00",),
        ],
        "to_regclass('dbos.workflow_status')": [("dbos.workflow_status",)],
        "GROUP BY upper(status)": [("SUCCESS", 2)],
        "FROM okg.source_watermarks": [
            ("rb.git-files", reconciled_at),
            ("rb.code-structure", reconciled_at),
            ("rb.code-lsp-resolution", reconciled_at),
        ],
        "FROM okg.source_progress": [
            (
                "rb.git-files",
                json.dumps({"run_id": "r1", "status": "OK", "run_mode": "scope_complete"}),
            ),
        ],
    })
    monkeypatch.setattr(
        orchestrator.psycopg, "connect", lambda *a, **k: _FakeConnection(cursor),
    )

    observation = orchestrator._rebuild_terminal_observation(
        exp, source_names=SOURCES,
    )

    assert observation["generation_id"] == "okg:gen-3"
    assert observation["generation_published_at"] == "2026-09-12T12:31:05.123456+00:00"
    assert observation["workflow_counts"] == {"SUCCESS": 2}
    assert observation["sources"]["rb.code-lsp-resolution"]["reconciled_at"] == (
        "2026-09-12T12:29:40+00:00"
    )
    assert observation["sources"]["rb.git-files"]["last_run_id"] == "r1"
    assert observation["sources"]["rb.code-structure"]["last_run_id"] is None
    # The string created_at must survive into a comparable timestamp: this
    # observation is terminal, not blocked on
    # `generation_publish_time_unavailable`.
    assert orchestrator._rebuild_terminal_blockers(
        observation, previous_generation_id="okg:gen-0",
    ) == []
    json.dumps(observation)


def test_terminal_timeout_env_override(monkeypatch) -> None:
    monkeypatch.delenv(orchestrator._REBUILD_TERMINAL_TIMEOUT_ENV, raising=False)
    assert orchestrator._rebuild_terminal_timeout_s() == 900.0
    monkeypatch.setenv(orchestrator._REBUILD_TERMINAL_TIMEOUT_ENV, "1200")
    assert orchestrator._rebuild_terminal_timeout_s() == 1200.0
    monkeypatch.setenv(orchestrator._REBUILD_TERMINAL_TIMEOUT_ENV, "0")
    with pytest.raises(ValueError, match="must be positive"):
        orchestrator._rebuild_terminal_timeout_s()
    monkeypatch.setenv(orchestrator._REBUILD_TERMINAL_TIMEOUT_ENV, "soon")
    with pytest.raises(ValueError, match="must be a number"):
        orchestrator._rebuild_terminal_timeout_s()


def test_deployment_source_names_come_from_the_generated_registry(
    monkeypatch, tmp_path: Path,
) -> None:
    exp = _experiment(monkeypatch, tmp_path)
    orchestrator._write_deployment_files(exp)

    names = orchestrator._deployment_source_names(exp)

    assert names == list(SOURCES)


def test_deployment_source_names_refuse_an_empty_registry(
    monkeypatch, tmp_path: Path,
) -> None:
    exp = _experiment(monkeypatch, tmp_path)
    exp.deployment_dir.mkdir(parents=True)
    (exp.deployment_dir / "source_registry.yaml").write_text("sources: {}\n")

    with pytest.raises(ValueError, match="declares no sources"):
        orchestrator._deployment_source_names(exp)


def test_reconcile_receipt_keeps_each_workflow_completeness() -> None:
    completeness = {
        "schema": "okg.ingest_completeness.v1",
        "status": "complete",
        "selected_sources": list(SOURCES),
        "completed_sources": list(SOURCES),
        "failed_sources": [],
        "dependency_blocked_sources": [],
    }
    payload = {
        "workflow_id": "wf-1",
        "status": "SUCCESS",
        "any_failed": False,
        "elapsed_seconds": 41.2,
        "report": {"completeness": completeness, "publishes": [{"x": 1}]},
        "error": None,
    }
    res = subprocess.CompletedProcess(
        args=["okg"], returncode=0, stdout=json.dumps(payload), stderr="",
    )

    receipt = orchestrator._reconcile_receipt(res)

    assert receipt == {
        "returncode": 0,
        "error": None,
        "workflows": [{
            "workflow_id": "wf-1",
            "status": "SUCCESS",
            "any_failed": False,
            "elapsed_seconds": 41.2,
            "error": None,
            "completeness": completeness,
        }],
    }


def test_reconcile_receipt_records_a_failed_pass_verbatim() -> None:
    res = subprocess.CompletedProcess(
        args=["okg"], returncode=1, stdout="", stderr="boom: no worker\n",
    )

    receipt = orchestrator._reconcile_receipt(res)

    assert receipt == {
        "returncode": 1,
        "workflows": [],
        "error": "boom: no worker\n",
    }


def test_rebuild_report_carries_both_terminal_receipts(
    monkeypatch, tmp_path: Path,
) -> None:
    graph = {
        "generation_id": "gen",
        "nodes": {"count": 12, "digest": "sha256:nodes"},
        "edges": {"count": 20, "digest": "sha256:edges"},
        "view_cites": {"count": 0, "digest": "sha256:view_cites"},
    }
    outputs = {"count": 0, "digest": "sha256:empty", "rows": []}
    attrs = {
        "count": 4,
        "digest": "sha256:attrs",
        "generation_id": "gen",
        "present": True,
    }
    delta_proof = {
        "summary": {"final_generation_id": "gen-final"},
        "rows": [
            {
                "boundaries": {
                    "repo_scope": {"scope_count": 1},
                    "graph_delta": {"nodes_added": 3, "edges_added": 4},
                }
            },
        ],
    }
    terminal = _observation(
        generation_id="okg:gen-3",
        published_at=T3,
        counts={"SUCCESS": 2},
        reconciled=_all_reconciled(T2),
    )
    rebuild_terminal = {
        "generation_id": "okg:gen-3",
        "previous_generation_id": None,
        "observation": terminal,
        "source_names": list(SOURCES),
        "elapsed_s": 95.5,
        "polls": 191,
        "settle_s": 3.0,
        "timeout_s": 900.0,
        "reconcile": {"returncode": 0, "workflows": [], "error": None},
    }
    incremental_terminal = {
        "final_generation_id": "gen-final",
        "latest_published_generation_id": "gen-final",
        "final_generation_is_latest": True,
        "non_terminal_source_sync_workflows": 0,
        "observation": terminal,
    }

    report = orchestrator._build_rebuild_equivalence_report(
        exp=_experiment(monkeypatch, tmp_path),
        incremental_graph=graph,
        rebuild_graph=graph,
        incremental_outputs=outputs,
        rebuild_outputs=outputs,
        incremental_attrs=attrs,
        rebuild_attrs=attrs,
        delta_proof=delta_proof,
        rebuild_experiment_id="rebuild-test",
        rebuild_generation_id="okg:gen-3",
        rebuild_terminal=rebuild_terminal,
        incremental_terminal=incremental_terminal,
    )

    assert report["ok"] is True
    assert report["rebuild_generation_id"] == "okg:gen-3"
    assert report["forced_rebuild"]["terminal"] == rebuild_terminal
    assert report["forced_rebuild"]["terminal"]["observation"]["sources"][
        "rb.code-lsp-resolution"
    ]["reconciled_at"] == T2
    assert report["incremental"]["terminal"] == incremental_terminal
    assert len(report["checks"]) == 11
