"""okg#3112 step 2, slice 1: the declared-attribute index lifecycle (PACT
lane-h-3112-step2, req.lane-h-3112-step2.reconcile; design.md section 4 and
section 9 item 8). Native PostgreSQL.

* The CLI reconcile is a dry run unless ``--apply``; ``--apply`` builds
  every declared index with CREATE INDEX CONCURRENTLY and records it; a
  second reconciler is refused.
* Every E8 case: a build that fails at its last wait (a ready, invalid index
  every write maintains) is dropped at once; one that fails at its first
  wait while a writer blocks the drop too is dropped first on the next run.
* A build waiting behind an open transaction does not hold a publish, and
  completes after it.
* Migrate preempts a waiting build, which is dropped on the next run and not
  counted; catalog apply's in-transaction form does the same.
* A worker restart mid-build; backoff after 1, 2, 4 and 8 runs, the
  five-failure cap and ``--retry-failed``; retype and undeclare.
* The preconditions (publish lock, source run, old transaction), the
  collation check, foreign ``okg_dattr_`` indexes.
* Cut-over gate 5: step 1's judge never lists a declared index, however
  bloated; no build on a table with another actor's invalid ``*_ccnew``;
  step 1 defers a graph table while the reconciler holds the DDL lock.
* The publisher cycle runs no index DDL; the maintenance workflow body does
  nothing before the operator opts in, reports a refused reconcile, and
  ends as a failure (with
  the error in ``last_run_report``) when the reconcile raised.
* ``reconcile --apply`` needs ``--deployment``.
* Preemption cancels only the reconciler's index statement, once, and never
  a backend whose start time is not the recorded one; a cancel that lands on
  a bookkeeping statement under a preemption request is a preemption, not
  an error and not a counted failure.
* Doctor escalates an invalid index of ours left by a failed build or a
  failed drop like one left by a running build.
"""

from __future__ import annotations

import contextlib
import io
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")

from psycopg.rows import dict_row  # noqa: E402

import declared_attr_index_support as s  # noqa: E402
from okg.substrate import declared_attr_indexes as dai  # noqa: E402
from okg.substrate.cli.main import main  # noqa: E402
from okg.substrate.db import index_reclaim  # noqa: E402
from okg.substrate.db.migrate import migrate  # noqa: E402

pytestmark = pytest.mark.postgres

ONE = {"t_text": "text"}


def _cli(*argv: str) -> tuple[int, dict[str, Any]]:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        code = main(["--json", *argv])
    text = buf.getvalue().strip()
    return code, (json.loads(text) if text else {})


def _seeded(dsn: str, decl: dict[str, str] | None = None, *, nodes: int = 60) -> Any:
    identity = s.seed(dsn, decl or ONE)
    s.apply(dsn, [s.node(i, 1) for i in range(nodes)])
    s.apply(dsn, s.edges(range(nodes - 1), 1))
    return identity


def _old_snapshot(dsn: str) -> psycopg.Connection:
    """A transaction holding a snapshot and no table lock: every concurrent
    index build waits for it at its last wait (E8 case 1)."""
    holder = psycopg.connect(dsn)
    holder.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    holder.execute("SELECT 1")
    return holder


def _writer(dsn: str, table: str) -> psycopg.Connection:
    """An open writer on a graph table: a build fails at its first wait and a
    concurrent drop waits for it too (E8 case 4)."""
    writer = psycopg.connect(dsn)
    writer.execute(f'UPDATE public."{table}" SET deleted = deleted WHERE false')
    return writer


def _in_thread(fn: Any, *args: Any, **kw: Any) -> tuple[threading.Thread, dict[str, Any]]:
    box: dict[str, Any] = {}

    def run() -> None:
        try:
            box["result"] = fn(*args, **kw)
        except BaseException as exc:  # noqa: BLE001 -- inspected by the test
            box["error"] = exc

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, box


def _wait_for_waiting_build(dsn: str, timeout: float = 60.0) -> dict[str, Any]:
    """The manifest row being built, once its CREATE INDEX CONCURRENTLY waits."""
    deadline = time.monotonic() + timeout
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        while time.monotonic() < deadline:
            cur.execute(
                """
                SELECT m.index_name, m.backend_pid, a.wait_event_type, a.query
                  FROM okg.declared_attr_indexes m
                  JOIN pg_stat_activity a ON a.pid = m.backend_pid
                 WHERE m.state = 'building'
                """
            )
            row = cur.fetchone()
            if row and row["wait_event_type"] == "Lock" and "CONCURRENTLY" in (row["query"] or ""):
                return dict(row)
            time.sleep(0.1)
    raise AssertionError("no waiting declared-attribute index build appeared")


def _dattr_indexes(dsn: str) -> dict[str, dict[str, Any]]:
    return s.catalog(dsn)


# --------------------------------------------------------------------------- #
# Build, dry run, refusal                                                      #
# --------------------------------------------------------------------------- #

#: pg_get_indexdef of each value type's index, written out by hand: the
#: renderer must keep producing exactly these.
_EXPECTED_INDEXDEF = {
    "text": "CREATE INDEX {name} ON public.{table} USING btree (\"left\"((attrs ->> "
            "'t_text'::text), 512), live_lo, live_hi) WHERE ((deleted = false) AND "
            "(jsonb_typeof((attrs -> 't_text'::text)) = 'string'::text))",
    "num": "CREATE INDEX {name} ON public.{table} USING btree ((\nCASE\n    WHEN "
           "(jsonb_typeof((attrs -> 't_num'::text)) = 'number'::text) THEN "
           "okg.attr_num_key(((attrs -> 't_num'::text))::numeric)\n    ELSE NULL::numeric\n"
           "END), live_lo, live_hi) WHERE ((deleted = false) AND (jsonb_typeof((attrs -> "
           "'t_num'::text)) = 'number'::text))",
    "bool": "CREATE INDEX {name} ON public.{table} USING btree ((\nCASE\n    WHEN "
            "(jsonb_typeof((attrs -> 't_bool'::text)) = 'boolean'::text) THEN "
            "((attrs -> 't_bool'::text))::boolean\n    ELSE NULL::boolean\nEND), live_lo, "
            "live_hi) WHERE ((deleted = false) AND (jsonb_typeof((attrs -> 't_bool'::text)) "
            "= 'boolean'::text))",
    "ts": "CREATE INDEX {name} ON public.{table} USING btree (okg.attr_ts_v4((attrs ->> "
          "'t_ts'::text)), live_lo, live_hi) WHERE ((deleted = false) AND "
          "(jsonb_typeof((attrs -> 't_ts'::text)) = 'string'::text))",
}


def test_dry_run_plans_without_ddl_and_apply_builds_every_declared_index(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    _seeded(dsn, s.DECL)
    code, plan = _cli("db", "attr-index", "reconcile", "--dsn", dsn)
    assert code == 0 and plan["status"] == "dry_run", plan
    assert len(plan["would_build"]) == len(s.DECL), "one index per declaration, nodes only (decision 287d)"
    assert _dattr_indexes(dsn) == {} and s.manifest(dsn) == {}

    code, applied = _cli("db", "attr-index", "reconcile", "--apply", "--deployment", "default",
                       "--dsn", dsn)
    assert code == 0 and applied["status"] == "converged", applied
    rows = s.manifest(dsn)
    catalog = _dattr_indexes(dsn)
    assert len(rows) == len(s.DECL) and set(rows) == set(catalog)
    tables = s.physical(dsn)
    for name, row in rows.items():
        assert row["state"] == "ready" and row["attempts"] == 0
        assert name.startswith("okg_dattr_" + row["target_kind"][0] + "_")
        assert catalog[name]["valid"] and catalog[name]["ready"]
        # The recorded definition is the live one, and equals the renderer's
        # expression as PostgreSQL deparses it.
        assert row["indexdef"] == catalog[name]["indexdef"]
        expected = _EXPECTED_INDEXDEF[row["value_type"]].format(
            name=name, table=tables[row["target_kind"]]
        )
        assert catalog[name]["indexdef"] == expected
    # Idempotent: a second run builds and drops nothing.
    again = s.reconcile(dsn)
    assert again["status"] == "converged" and again["built"] == [] and again["dropped"] == []
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT enabled_at IS NOT NULL AS on, run_count FROM okg.declared_attr_index_state")
        state = cur.fetchone()
    assert state["on"] and state["run_count"] == 2


def test_a_second_reconciler_is_refused(clean_runtime: str) -> None:
    dsn = clean_runtime
    _seeded(dsn)
    with psycopg.connect(dsn, autocommit=True) as other:
        other.execute("SELECT pg_advisory_lock(hashtext(%s))", (dai.INDEX_DDL_LOCK,))
        code, refused = _cli("db", "attr-index", "reconcile", "--apply", "--deployment", "default",
                       "--dsn", dsn)
        pid = other.info.backend_pid
    assert code == 1
    assert refused["status"] == "refused" and refused["reason"] == "index_ddl_lock_held"
    assert refused["holders"] == [pid]
    assert _dattr_indexes(dsn) == {}
    assert s.reconcile(dsn)["status"] == "converged"


# --------------------------------------------------------------------------- #
# E8 cases                                                                     #
# --------------------------------------------------------------------------- #


def test_e8_a_build_failing_at_its_last_wait_is_dropped_at_once_and_retried(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    _seeded(dsn)
    holder = _old_snapshot(dsn)
    try:
        failed = s.reconcile(dsn, lock_timeout_seconds=1.0)
    finally:
        holder.rollback()
        holder.close()
    assert failed["status"] == "partial" and failed["reason"] == "lock_timeout", failed
    first = failed["failed"][0]
    assert first["reason"] == "lock_timeout" and first["attempts"] == 1
    # The ready-but-invalid index the build left was dropped in the same run.
    assert first["index_name"] in {d["index_name"] for d in failed["dropped"]}
    assert first["index_name"] not in _dattr_indexes(dsn)
    row = s.manifest(dsn)[first["index_name"]]
    assert row["state"] == "failed" and row["next_attempt_run"] == failed["run"] + 1
    retried = s.reconcile(dsn)
    assert retried["status"] == "converged", retried
    assert all(r["state"] == "ready" and r["attempts"] == 0 for r in s.manifest(dsn).values())


def test_e8_a_build_failing_at_its_first_wait_is_dropped_first_on_the_next_run(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(dai, "DROP_LOCK_TIMEOUT_SECONDS", 1.0)
    dsn = clean_runtime
    _seeded(dsn)
    tables = s.physical(dsn)
    writer = _writer(dsn, tables["node"])
    try:
        failed = s.reconcile(dsn, lock_timeout_seconds=1.0)
        node_name = dai.index_name("node", "t_text", "text")
        leftover = _dattr_indexes(dsn).get(node_name)
        # Not ready, not valid, and the writer blocks its drop too.
        assert leftover is not None and not leftover["ready"] and not leftover["valid"]
        assert s.manifest(dsn)[node_name]["state"] == "failed"
        # While it is there, no other build touches the node table (rule 3).
        blocked = s.reconcile(dsn, lock_timeout_seconds=1.0)
        assert node_name in _dattr_indexes(dsn)
        assert any(sk["index_name"] == node_name for sk in blocked["skipped"]) or any(
            f["index_name"] == node_name and f["action"] == "drop" for f in blocked["failed"]
        ), blocked
    finally:
        writer.rollback()
        writer.close()
    assert failed["failed"][0]["reason"] == "lock_timeout"
    recovered = s.reconcile(dsn)
    assert node_name in {d["index_name"] for d in recovered["dropped"]}
    assert recovered["status"] == "converged", recovered
    assert _dattr_indexes(dsn)[node_name]["valid"]


def test_a_build_waiting_behind_an_open_transaction_does_not_hold_a_publish(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    identity = _seeded(dsn)
    holder = _old_snapshot(dsn)
    thread, box = _in_thread(s.reconcile, dsn, lock_timeout_seconds=120.0)
    try:
        waiting = _wait_for_waiting_build(dsn)
        # The publisher cycle runs and returns while the build waits.
        s.apply(dsn, [s.node(i, 2) for i in range(10)])
        begun = time.monotonic()
        published = s.publish(dsn, identity)
        assert published["status"] == "published"
        assert time.monotonic() - begun < 60
        assert thread.is_alive(), "the build must still be waiting for the old transaction"
        assert s.manifest(dsn)[waiting["index_name"]]["state"] == "building"
    finally:
        holder.rollback()
        holder.close()
    thread.join(120)
    assert "error" not in box, box
    assert box["result"]["status"] == "converged", box["result"]
    # The rows the publish wrote while the index was being built are in it.
    result = s.g_equals_x(dsn)
    assert result["mismatches"] == [] and result["plans_without_index"] == []


# --------------------------------------------------------------------------- #
# Preemption                                                                   #
# --------------------------------------------------------------------------- #


def test_migrate_preempts_a_waiting_build_which_is_dropped_next_run_and_not_counted(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    _seeded(dsn)
    holder = _old_snapshot(dsn)
    thread, box = _in_thread(s.reconcile, dsn, lock_timeout_seconds=600.0)
    try:
        waiting = _wait_for_waiting_build(dsn)
        begun = time.monotonic()
        result = migrate(dsn)
        took = time.monotonic() - begun
        thread.join(30)
        preempted_name = waiting["index_name"]
        row = s.manifest(dsn)[preempted_name]
        leftover = _dattr_indexes(dsn).get(preempted_name)
    finally:
        holder.rollback()
        holder.close()
    assert not thread.is_alive()
    assert "error" not in box, box
    assert box["result"]["status"] == "preempted", box["result"]
    report = result.declared_attr_index_preemption
    assert report["status"] == "acquired", report
    assert waiting["backend_pid"] in report["cancelled_pids"]
    assert took < 120
    assert row["state"] == "failed" and row["last_reason"] == "preempted"
    assert row["attempts"] == 0, "a preempted build is not counted"
    assert leftover is not None and not leftover["valid"], "left for the next run to drop"
    nxt = s.reconcile(dsn)
    assert preempted_name in {d["index_name"] for d in nxt["dropped"]}
    assert nxt["status"] == "converged", nxt
    assert s.manifest(dsn)[preempted_name]["attempts"] == 0


def test_catalog_apply_preemption_cancels_the_build_and_holds_the_lock_to_commit(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    _seeded(dsn)
    holder = _old_snapshot(dsn)
    thread, box = _in_thread(s.reconcile, dsn, lock_timeout_seconds=600.0)
    try:
        waiting = _wait_for_waiting_build(dsn)
        with psycopg.connect(dsn) as apply_conn:
            with apply_conn.transaction():
                report = dai.preempt_in_transaction(apply_conn, actor="catalog_apply")
                thread.join(30)
                # While the apply's transaction is open, a new run is refused.
                assert s.reconcile(dsn)["status"] == "refused"
                requested = dai.record_maintenance_request(apply_conn, reason="catalog_apply")
    finally:
        holder.rollback()
        holder.close()
    assert report["status"] == "acquired" and waiting["backend_pid"] in report["cancelled_pids"]
    assert box["result"]["status"] == "preempted"
    assert requested == {"status": "recorded", "reason": "catalog_apply"}
    # After commit the locks are gone and the next run converges.
    assert s.reconcile(dsn)["status"] == "converged"


def test_a_worker_restart_mid_build_is_failed_and_dropped_on_the_next_run(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    _seeded(dsn)
    holder = _old_snapshot(dsn)
    thread, box = _in_thread(s.reconcile, dsn, lock_timeout_seconds=600.0)
    try:
        waiting = _wait_for_waiting_build(dsn)
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute("SELECT pg_terminate_backend(%s)", (waiting["backend_pid"],))
        thread.join(30)
    finally:
        holder.rollback()
        holder.close()
    assert "error" in box, "the worker's session died mid-build"
    name = waiting["index_name"]
    assert s.manifest(dsn)[name]["state"] == "building"
    nxt = s.reconcile(dsn)
    row = s.manifest(dsn)[name]
    assert row["state"] == "failed" and row["last_reason"] == "interrupted" and row["attempts"] == 1
    assert name not in _dattr_indexes(dsn) or name in {d["index_name"] for d in nxt["dropped"]}
    assert any(sk["index_name"] == name and sk["reason"] == "backoff" for sk in nxt["skipped"])
    final = s.reconcile(dsn)
    assert final["status"] == "converged", final


# --------------------------------------------------------------------------- #
# Retries, retype, undeclare                                                   #
# --------------------------------------------------------------------------- #


def test_backoff_after_1_2_4_8_runs_then_the_failure_cap_until_retry_failed(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    dsn = clean_runtime
    _seeded(dsn)
    real_ddl = dai.index_ddl
    node_name = dai.index_name("node", "t_text", "text")

    def failing_ddl(**kw: Any) -> str:
        ddl = real_ddl(**kw)
        if kw["target_kind"] == "node":
            # Fails while building (division by zero on the first row).
            return ddl.replace("live_lo, live_hi)", "live_lo, (1 / (live_hi - live_hi)))")
        return ddl

    monkeypatch.setattr(dai, "index_ddl", failing_ddl)
    attempted_runs: list[int] = []
    previous = 0
    for _ in range(20):
        report = s.reconcile(dsn)
        attempts = s.manifest(dsn)[node_name]["attempts"]
        if attempts > previous:
            attempted_runs.append(report["run"])
            previous = attempts
        assert node_name not in _dattr_indexes(dsn), "a failed build leaves no index behind"
    assert attempted_runs == [1, 2, 4, 8, 16]
    row = s.manifest(dsn)[node_name]
    assert row["state"] == "failed" and row["attempts"] == dai.MAX_CONSECUTIVE_FAILURES
    assert node_name in report["capped"]
    doctor = dai.doctor_report(dsn)
    assert doctor["status"] == "ERROR"
    assert any(f["code"] == "failure_cap" and f["index"] == node_name for f in doctor["findings"])
    code, capped = _cli("db", "attr-index", "reconcile", "--apply", "--deployment", "default",
                       "--dsn", dsn)
    assert code == 1 and node_name in capped["capped"]

    monkeypatch.setattr(dai, "index_ddl", real_ddl)
    code, retried = _cli("db", "attr-index", "reconcile", "--apply", "--retry-failed",
                        "--deployment", "default", "--dsn", dsn)
    assert code == 0 and retried["status"] == "converged", retried
    assert s.manifest(dsn)[node_name]["attempts"] == 0


def test_a_failed_build_is_retried_after_24_hours_whatever_the_run_count() -> None:
    now = datetime.now(timezone.utc)
    row = {"state": "failed", "attempts": 4, "next_attempt_run": 99, "last_reason": "build_error",
           "state_since": now - timedelta(hours=25)}
    assert dai._eligible(row, run_no=10, retry_failed=False, now=now) is None
    row["state_since"] = now - timedelta(hours=1)
    assert dai._eligible(row, run_no=10, retry_failed=False, now=now) == "backoff"
    row["attempts"] = 5
    row["state_since"] = now - timedelta(days=30)
    assert dai._eligible(row, run_no=10**6, retry_failed=False, now=now) == "failure_cap"
    assert dai._eligible(row, run_no=10, retry_failed=True, now=now) is None


def test_retype_and_undeclare_drop_the_old_index(clean_runtime: str) -> None:
    dsn = clean_runtime
    _seeded(dsn, {"t_text": "text", "t_num": "num"})
    assert s.reconcile(dsn)["status"] == "converged"
    old_num = {dai.index_name(k, "t_num", "num") for k in dai.INDEXED_KINDS}
    old_text = {dai.index_name(k, "t_text", "text") for k in dai.INDEXED_KINDS}
    assert old_num | old_text == set(_dattr_indexes(dsn))

    # Retype t_num to text and undeclare t_text.
    s.redeclare(dsn, {"t_num": "text"})
    report = s.reconcile(dsn)
    assert report["status"] == "converged", report
    new = {dai.index_name(k, "t_num", "text") for k in dai.INDEXED_KINDS}
    assert set(_dattr_indexes(dsn)) == new
    assert set(s.manifest(dsn)) == new
    assert {d["index_name"] for d in report["dropped"]} == old_num | old_text


def test_no_edge_index_is_built_and_an_old_one_is_retired_not_drift(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """okg#3112 step 3, decision 287d: the reconciler builds declared indexes
    on the node table only. An edge index an earlier (step 2) run built is
    retired by the next run with ``last_reason`` ``kind_not_indexed`` and
    dropped; until then doctor reports it at INFO, never as drift or a
    missing index, and no reader uses it. With no edge index, doctor is OK,
    and ``maintenance_wanted`` is up to date.

    Failure case: with edges declared again (the step-2 rule), the same
    reconciler builds the edge index, so the node-only result is the rule's
    doing."""
    dsn = clean_runtime
    _seeded(dsn)
    node_name = dai.index_name("node", "t_text", "text")
    edge_name = dai.index_name("edge", "t_text", "text")

    # Failure case first: the step-2 rule (both kinds) builds the edge index.
    with monkeypatch.context() as patch:
        patch.setattr(dai, "INDEXED_KINDS", ("node", "edge"))
        assert s.reconcile(dsn)["status"] == "converged"
    catalog = _dattr_indexes(dsn)
    assert node_name in catalog and edge_name in catalog
    assert s.manifest(dsn)[edge_name]["state"] == "ready"

    # Under the step-3 rule the left-over edge index is no reader's and not
    # drift: readiness says kind_not_indexed while it still exists, and
    # doctor names it at INFO only.
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        ready, table = dai.reader_readiness(cur, target_kind="edge", declared={"t_text": "text"})
        assert ready == {"t_text": "kind_not_indexed"} and table is None
        ready, _table = dai.reader_readiness(cur, target_kind="node", declared={"t_text": "text"})
        assert ready == {"t_text": None}
    with psycopg.connect(dsn, autocommit=True) as conn:
        assert dai.maintenance_wanted(conn) == (True, "declaration_changed")
    doctor = dai.doctor_report(dsn)
    assert doctor["status"] == "OK", doctor["findings"]
    assert doctor["indexed_kinds"] == ["node"]
    assert [(f["code"], f["severity"], f["index"]) for f in doctor["findings"]
            if f.get("index") == edge_name] == [("kind_not_indexed", "INFO", edge_name)]

    # The next run retires and drops it, and builds nothing on the edge table.
    report = s.reconcile(dsn)
    assert report["status"] == "converged", report
    assert {d["index_name"] for d in report["dropped"]} == {edge_name}
    assert report["built"] == []
    catalog = _dattr_indexes(dsn)
    assert node_name in catalog and edge_name not in catalog
    assert not any(name.startswith("okg_dattr_e_") for name in catalog)
    assert set(s.manifest(dsn)) == {node_name}
    with psycopg.connect(dsn, autocommit=True) as conn:
        assert dai.maintenance_wanted(conn) == (False, "up_to_date")
    doctor = dai.doctor_report(dsn)
    assert doctor["status"] == "OK" and doctor["findings"] == [], doctor["findings"]
    plan = _cli("db", "attr-index", "reconcile", "--dsn", dsn)[1]
    assert plan["would_build"] == [] and plan["would_drop"] == [], plan


def test_a_leftover_edge_row_missing_or_failed_is_info_not_error(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Decision 287d on an upgraded deployment: a step-2 edge row whose index
    is gone, or whose build hit the failure cap, is not an error; the next
    reconcile retires it. Doctor names it at INFO only."""
    dsn = clean_runtime
    _seeded(dsn)
    edge_name = dai.index_name("edge", "t_text", "text")
    with monkeypatch.context() as patch:
        patch.setattr(dai, "INDEXED_KINDS", ("node", "edge"))
        assert s.reconcile(dsn)["status"] == "converged"
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"DROP INDEX public.{edge_name}")

    def edge_findings() -> tuple[str, list[tuple[str, str]]]:
        doctor = dai.doctor_report(dsn)
        return doctor["status"], [
            (f["code"], f["severity"]) for f in doctor["findings"]
            if f.get("index") == edge_name
        ]

    assert edge_findings() == ("OK", [("kind_not_indexed", "INFO")])
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            f"UPDATE {dai.MANIFEST_TABLE} SET state = 'failed', attempts = %s"
            " WHERE index_name = %s",
            (dai.MAX_CONSECUTIVE_FAILURES, edge_name),
        )
    assert edge_findings() == ("OK", [("kind_not_indexed", "INFO")])


def test_the_retiring_reason_names_the_kind_not_indexed(clean_runtime: str, monkeypatch) -> None:
    """The retirement is recorded with its reason before the drop: an edge
    index whose drop is blocked stays in the manifest as ``drop_failed`` with
    ``last_reason`` ``kind_not_indexed`` (not ``undeclared``: t_text is still
    declared)."""
    dsn = clean_runtime
    _seeded(dsn)
    edge_name = dai.index_name("edge", "t_text", "text")
    with monkeypatch.context() as patch:
        patch.setattr(dai, "INDEXED_KINDS", ("node", "edge"))
        assert s.reconcile(dsn)["status"] == "converged"

    def failing_drop(cur: Any, schema: str, name: str) -> None:
        raise psycopg.errors.LockNotAvailable("blocked for the test")

    monkeypatch.setattr(dai, "_drop_index", failing_drop)
    report = s.reconcile(dsn)
    assert [f["index_name"] for f in report["failed"]] == [edge_name], report
    row = s.manifest(dsn)[edge_name]
    assert row["state"] == "drop_failed" and row["last_reason"] == "kind_not_indexed", row


# --------------------------------------------------------------------------- #
# Preconditions, collation, foreign indexes                                    #
# --------------------------------------------------------------------------- #


def test_no_build_while_the_publish_lock_is_held(clean_runtime: str) -> None:
    from okg.substrate.runtime.source_sync_priority import publisher_publish_advisory_keys

    dsn = clean_runtime
    _seeded(dsn)
    with psycopg.connect(dsn, autocommit=True) as publisher:
        publisher.execute("SELECT pg_advisory_lock(%s, %s)", publisher_publish_advisory_keys("default"))
        held = s.reconcile(dsn)
    assert held["status"] == "partial" and held["reason"] == "publish_running", held
    assert _dattr_indexes(dsn) == {}
    assert all(r["state"] == "pending" and r["last_reason"] == "publish_running"
               for r in s.manifest(dsn).values())
    assert s.reconcile(dsn)["status"] == "converged"


def test_no_build_while_a_source_run_is_active(clean_runtime: str) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest.run_base import (
        hold_run_liveness,
        run_base_checkpoint_id,
        take_run_base,
    )

    dsn = clean_runtime
    identity = _seeded(dsn)
    s.publish(dsn, identity)
    checkpoint_id = run_base_checkpoint_id("default", "dattr-run")
    liveness = hold_run_liveness(dsn, checkpoint_id)
    try:
        store = ChronosGraphStore.connect(dsn)
        try:
            take_run_base(store, deployment="default", branch="default", source="default",
                          run_id="dattr-run")
        finally:
            store.close()
        held = s.reconcile(dsn)
    finally:
        liveness.close()
    assert held["status"] == "partial" and held["reason"] == "source_run_in_progress", held
    assert _dattr_indexes(dsn) == {}
    assert s.reconcile(dsn)["status"] == "converged"


def test_no_build_while_an_old_transaction_is_open(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(dai, "OLD_TRANSACTION_SECONDS", 0.5)
    dsn = clean_runtime
    _seeded(dsn)
    holder = _old_snapshot(dsn)
    try:
        time.sleep(1.0)
        held = s.reconcile(dsn)
    finally:
        holder.rollback()
        holder.close()
    assert held["status"] == "partial" and held["reason"] == "old_transaction_open", held
    assert held["old_transactions"][0]["xact_seconds"] >= 0.5
    assert _dattr_indexes(dsn) == {}


@pytest.mark.parametrize(
    ("collation", "ok"),
    [
        ({"provider": "c", "collate": "C", "locale": None}, True),
        ({"provider": "c", "collate": "POSIX", "locale": None}, True),
        ({"provider": "c", "collate": "C.UTF-8", "locale": None}, True),
        ({"provider": "c", "collate": "C.utf8", "locale": None}, True),
        ({"provider": "b", "collate": "C", "locale": "C"}, True),
        ({"provider": "b", "collate": "C", "locale": "C.UTF-8"}, True),
        ({"provider": "c", "collate": "en_US.UTF-8", "locale": None}, False),
        ({"provider": "i", "collate": "C", "locale": "en-US"}, False),
        ({"provider": "i", "collate": "C.UTF-8", "locale": "und"}, False),
        ({"provider": "b", "collate": "C", "locale": "PG_UNICODE_FAST"}, False),
    ],
)
def test_collation_check(collation: dict[str, Any], ok: bool) -> None:
    assert dai.collation_orders_by_code_point(collation) is ok


def test_text_indexes_are_refused_under_an_icu_collation_and_the_digest_names_it(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    dsn = clean_runtime
    _seeded(dsn, {"t_text": "text", "t_num": "num"})
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        real = dai.database_collation(cur)
    assert dai.collation_orders_by_code_point(real), real
    assert s.reconcile(dsn)["status"] == "converged"
    text_names = {dai.index_name(k, "t_text", "text") for k in dai.INDEXED_KINDS}
    assert text_names <= set(_dattr_indexes(dsn))

    icu = {"provider": "i", "collate": "C", "locale": "en-US"}
    monkeypatch.setattr(dai, "database_collation", lambda cur: dict(icu))
    report = s.reconcile(dsn)
    assert {r["attr_id"] for r in report["refused"]} == {"t_text"}
    assert not text_names & set(_dattr_indexes(dsn)), "text indexes leave under ICU"
    assert {dai.index_name(k, "t_num", "num") for k in dai.INDEXED_KINDS} <= set(_dattr_indexes(dsn))
    doctor = dai.doctor_report(dsn)
    assert any(f["code"] == "collation_refuses_text" for f in doctor["findings"])
    # The collation is part of a text index's definition digest, not a number's.
    assert dai.definition_digest("node", "a", "text", collation=real) != dai.definition_digest(
        "node", "a", "text", collation=icu)
    assert dai.definition_digest("node", "a", "num", collation=real) == dai.definition_digest(
        "node", "a", "num", collation=icu)


def test_a_foreign_okg_dattr_index_is_reported_and_never_dropped(clean_runtime: str) -> None:
    dsn = clean_runtime
    _seeded(dsn)
    tables = s.physical(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f'CREATE INDEX okg_dattr_n_manual ON public."{tables["node"]}" (node_id)')
    report = s.reconcile(dsn)
    assert report["foreign_dattr_indexes"] == ["okg_dattr_n_manual"]
    assert "okg_dattr_n_manual" in _dattr_indexes(dsn)
    doctor = dai.doctor_report(dsn)
    assert doctor["status"] == "WARN"
    assert [f["index"] for f in doctor["findings"] if f["code"] == "foreign_dattr_index"] == [
        "okg_dattr_n_manual"
    ]


# --------------------------------------------------------------------------- #
# Cut-over gate 5 and step 1                                                   #
# --------------------------------------------------------------------------- #


def _bloat_node_table(dsn: str, table: str, rounds: int) -> None:
    """Non-HOT updates (``attrs`` is in every declared index's key) leave
    dead entries behind in each index of the node table."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        for _ in range(rounds):
            conn.execute(f'UPDATE public."{table}" SET attrs = attrs || \'{{}}\'::jsonb')
        conn.execute(f'VACUUM ANALYZE public."{table}"')


def test_gate5_step1_never_judges_a_declared_index_however_bloated(clean_runtime: str) -> None:
    from okg.substrate.library.sources.base import NodeFact

    dsn = clean_runtime
    s.seed(dsn, ONE)
    # The key on every node, like krama's file_id on 99%: step 1 would judge
    # a partial index this full (it skips one under 90% of the table).
    s.apply(dsn, [NodeFact(s.nid(i), "thing", {"t_text": f"path/{i:06d}"}, {"row": i}, {"rev": 1})
                  for i in range(3000)])
    assert s.reconcile(dsn)["status"] == "converged"
    tables = s.physical(dsn)
    node_name = dai.index_name("node", "t_text", "text")
    # Failure arm: the same index under a name without the reserved prefix.
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            f'CREATE INDEX gate5_unreserved_copy ON public."{tables["node"]}" '
            f"({dai.key_expression('t_text', 'text')}, live_lo, live_hi) "
            f"WHERE {dai.admission_predicate('t_text', 'text')}"
        )
    _bloat_node_table(dsn, tables["node"], rounds=12)
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        judged = index_reclaim.judge_indexes(
            cur, [f"public.{tables['node']}"], ratio=1.5, min_reclaim_bytes=0, analyzed=True,
        )
        cur.execute("SELECT pg_relation_size(%s::regclass) AS b", (f"public.{node_name}",))
        declared_bytes = int(cur.fetchone()["b"])
    by_name = {item["index"].split(".", 1)[1]: item for item in judged}
    assert by_name["gate5_unreserved_copy"]["verdict"] == "rebuild", by_name["gate5_unreserved_copy"]
    assert declared_bytes >= by_name["gate5_unreserved_copy"]["bytes"] * 0.5
    assert not any(name.startswith("okg_dattr_") for name in by_name)


def test_gate5_no_build_on_a_table_with_another_actors_invalid_ccnew(clean_runtime: str) -> None:
    dsn = clean_runtime
    _seeded(dsn)
    tables = s.physical(dsn)
    foreign = "_chronos_b_interval_okg_graph_nodes_x_ccnew"
    with psycopg.connect(dsn, autocommit=True) as conn:
        with pytest.raises(psycopg.errors.UniqueViolation):
            conn.execute(
                f'CREATE UNIQUE INDEX CONCURRENTLY "{foreign}" ON public."{tables["node"]}" (subtype)'
            )
    report = s.reconcile(dsn)
    node_name = dai.index_name("node", "t_text", "text")
    edge_name = dai.index_name("edge", "t_text", "text")
    skipped = {sk["index_name"]: sk for sk in report["skipped"]}
    assert skipped[node_name]["reason"] == "foreign_invalid_index"
    assert skipped[node_name]["invalid_indexes"] == [foreign]
    catalog = _dattr_indexes(dsn)
    # Decision 287d: the edge table gets no declared index at all, so the
    # only table with one is the node table the foreign leftover blocks.
    assert node_name not in catalog and edge_name not in catalog
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_class WHERE relname = %s", (foreign,))
        assert cur.fetchone() is not None, "never drops another actor's index"
    doctor = dai.doctor_report(dsn)
    assert any(f["code"] == "foreign_invalid_index" and f["index"] == foreign
               for f in doctor["findings"])
    # Once the other actor's leftover is gone, the next run builds.
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f'DROP INDEX CONCURRENTLY public."{foreign}"')
    assert s.reconcile(dsn)["status"] == "converged"


def test_step1_defers_a_graph_table_while_the_reconciler_holds_the_ddl_lock(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    identity = _seeded(dsn, nodes=300)
    summary = s.publish(dsn, identity)
    tables = {f"public.{t}" for t in s.physical(dsn).values()}
    with psycopg.connect(dsn, autocommit=True) as reconciler:
        reconciler.execute("SELECT pg_advisory_lock(hashtext(%s))", (dai.INDEX_DDL_LOCK,))
        deferred = index_reclaim.rebuild_indexes_after_publish(
            dsn, deployment="default", summary=summary, ratio=1.0, min_reclaim_bytes=0,
        )
    assert set(deferred["deferred_graph_tables"]) & tables, deferred
    assert not {item["table"] for item in deferred["rebuilt"]} & tables
    assert not set(deferred["analyzed"]) & tables
    # Failure arm: with the lock free, the same call does rebuild graph-table
    # indexes, so the lock is what deferred them.
    rebuilt = index_reclaim.rebuild_indexes_after_publish(
        dsn, deployment="default", summary=summary, ratio=1.0, min_reclaim_bytes=0,
    )
    assert {item["table"] for item in rebuilt["rebuilt"]} & tables, rebuilt
    assert rebuilt["deferred_graph_tables"] == []


# --------------------------------------------------------------------------- #
# The publisher and the workflow                                               #
# --------------------------------------------------------------------------- #


def test_the_publisher_cycle_runs_no_index_ddl_and_only_asks_for_a_run(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from okg.substrate.runtime import publisher_workflow

    dsn = clean_runtime
    identity = _seeded(dsn)
    s.reconcile(dsn, apply=False)
    with psycopg.connect(dsn, autocommit=True) as conn:
        assert dai.maintenance_wanted(conn) == (False, "not_enabled")
    # Opt in, then a declaration changes: a publish finds work but builds nothing.
    assert s.reconcile(dsn)["status"] == "converged"
    identity = s.redeclare(dsn, {"t_text": "text", "t_num": "num"})
    s.publish(dsn, identity)
    assert {dai.index_name(k, "t_num", "num") for k in ("node", "edge")}.isdisjoint(_dattr_indexes(dsn))
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        "okg.substrate.runtime.declared_attr_index_workflow.enqueue_declared_attr_index_maintenance",
        lambda **kw: calls.append(kw) or {"status": "enqueued", **kw},
    )
    asked = publisher_workflow._enqueue_declared_attr_index_maintenance_after_cycle(
        dsn=dsn, deployment="default"
    )
    assert asked["status"] == "enqueued" and calls[0]["reason"] == "declaration_changed"
    assert {dai.index_name(k, "t_num", "num") for k in ("node", "edge")}.isdisjoint(_dattr_indexes(dsn))
    # A catalog apply's request also asks for a run.
    assert s.reconcile(dsn)["status"] == "converged"
    with psycopg.connect(dsn) as conn:
        dai.record_maintenance_request(conn, reason="catalog_apply")
        conn.commit()
    with psycopg.connect(dsn, autocommit=True) as conn:
        assert dai.maintenance_wanted(conn) == (True, "requested:catalog_apply")


def test_the_maintenance_workflow_does_nothing_before_the_operator_opts_in(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from okg.substrate.runtime.declared_attr_index_workflow import run_index_maintenance

    dsn = clean_runtime
    identity = _seeded(dsn)
    s.publish(dsn, identity)
    before = run_index_maintenance(deployment="default", dsn=dsn)
    assert before["reconcile"]["status"] == "not_enabled"
    assert _dattr_indexes(dsn) == {}
    s.reconcile(dsn)
    after = run_index_maintenance(deployment="default", dsn=dsn)
    assert after["reconcile"]["status"] == "converged"
    monkeypatch.setenv(dai.MAINTENANCE_ENV, "off")
    assert run_index_maintenance(deployment="default", dsn=dsn)["status"] == "disabled"


# --------------------------------------------------------------------------- #
# Review fixes: deployment, drift skip, run errors, preemption, doctor          #
# --------------------------------------------------------------------------- #


def test_reconcile_apply_needs_the_deployment(clean_runtime: str) -> None:
    dsn = clean_runtime
    _seeded(dsn)
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        code, _ = _cli("db", "attr-index", "reconcile", "--apply", "--dsn", dsn)
    assert code == 1
    refused = json.loads(err.getvalue())
    assert refused["error"]["data"]["reason"] == "deployment_required", refused
    assert "--deployment" in refused["error"]["message"]
    assert _dattr_indexes(dsn) == {} and s.manifest(dsn) == {}
    with pytest.raises(dai.SubstrateError):
        dai.reconcile(dsn, deployment=None, apply=True)
    # A dry run needs none, and --deployment with --dsn applies.
    code, plan = _cli("db", "attr-index", "reconcile", "--dsn", dsn)
    assert code == 0 and plan["status"] == "dry_run", plan
    code, applied = _cli("db", "attr-index", "reconcile", "--apply", "--deployment", "default",
                         "--dsn", dsn)
    assert code == 0 and applied["status"] == "converged" and applied["deployment"] == "default"


def test_a_refused_reconcile_is_reported_and_runs_no_drift_check(clean_runtime: str) -> None:
    """The workflow reports a refused reconcile and is not an error. PACT
    lane-s-storage-drop-copies retired the drift check with the view it read,
    so the report has no ``drift`` part, and migration 0046 dropped the
    drift-check table."""
    from okg.substrate.runtime.declared_attr_index_workflow import run_index_maintenance

    dsn = clean_runtime
    identity = _seeded(dsn)
    s.reconcile(dsn)
    s.publish(dsn, identity)
    with psycopg.connect(dsn, autocommit=True) as migrate_like:
        migrate_like.execute("SELECT pg_advisory_lock(hashtext(%s))", (dai.INDEX_DDL_LOCK,))
        out = run_index_maintenance(deployment="default", dsn=dsn)
    assert out["reconcile"]["status"] == "refused", out
    assert "drift" not in out, out
    assert out["status"] == "ok"
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('okg.declared_attr_index_drift_checks')")
        assert cur.fetchone()[0] is None, "0046 drops the drift-check table"
    assert not hasattr(dai, "run_drift_checks")
    assert not hasattr(dai, "verify_against_graph")


def test_the_drift_storage_is_gone_and_readiness_has_no_drift_reason(
    clean_runtime: str,
) -> None:
    """Migration 0046 dropped the drift flag, its detail and the drift-check
    cursor, and a ready index stays usable with no doctor drift finding:
    nothing computes drift any more, and readiness has no ``view_graph_drift``
    reason. Failure case: the same row in ``failed`` state is still not
    ready."""
    dsn = clean_runtime
    _seeded(dsn, dict(_UNINDEXED_DECL))
    assert s.reconcile(dsn)["status"] == "converged"
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema = 'okg' AND table_name IN "
            "('declared_attr_indexes', 'declared_attr_index_state') "
            "AND column_name LIKE 'drift%%'"
        )
        assert cur.fetchall() == []
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        ready, _table = dai.reader_readiness(
            cur, target_kind="node", declared=dict(_UNINDEXED_DECL),
        )
    assert ready == {attr: None for attr in _UNINDEXED_DECL}, ready
    assert "view_graph_drift" not in dai.READINESS_REASONS
    doctor = dai.doctor_report(dsn)
    assert not [f for f in doctor["findings"] if "drift" in f["code"]], doctor["findings"]
    assert _unindexed(dsn) == []
    _set_row(dsn, "state = 'failed', attempts = 1")
    assert _unindexed(dsn) == [("t_text", "failed", "WARN")]


def test_a_reconcile_that_raises_is_recorded_and_fails_the_workflow(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from okg.substrate.runtime.declared_attr_index_workflow import (
        maintenance_workflow_body,
        run_index_maintenance,
    )

    dsn = clean_runtime
    identity = _seeded(dsn)
    s.reconcile(dsn)
    s.publish(dsn, identity)

    def boom(*args: Any, **kw: Any) -> Any:
        raise RuntimeError("boom in the run")

    monkeypatch.setattr(dai, "_reconcile_run", boom)
    out = run_index_maintenance(deployment="default", dsn=dsn)
    assert out["status"] == "error", out
    assert out["reconcile"]["status"] == "error" and "boom" in out["reconcile"]["error"]
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM okg.declared_attr_index_state")
        state = cur.fetchone()
    assert state["last_run_report"]["status"] == "error", state["last_run_report"]
    assert "boom in the run" in state["last_run_report"]["error"]
    assert state["reconciler_pid"] is None
    with pytest.raises(dai.SubstrateError) as failed:
        maintenance_workflow_body(deployment="default", dsn=dsn)
    assert failed.value.data["report"]["reconcile"]["status"] == "error"
    doctor = dai.doctor_report(dsn)
    assert doctor["last_run"]["status"] == "error"
    monkeypatch.undo()
    assert maintenance_workflow_body(deployment="default", dsn=dsn)["status"] == "ok"


def _wait_for_reconciler_query(dsn: str, needle: str, timeout: float = 30.0) -> int:
    """The reconciler's pid once its running statement contains ``needle``."""
    deadline = time.monotonic() + timeout
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        while time.monotonic() < deadline:
            cur.execute(
                """
                SELECT a.pid, a.state, a.query
                  FROM okg.declared_attr_index_state st
                  JOIN pg_stat_activity a ON a.pid = st.reconciler_pid
                """
            )
            row = cur.fetchone()
            if row and row["state"] == "active" and needle in (row["query"] or ""):
                return int(row["pid"])
            time.sleep(0.05)
    raise AssertionError(f"the reconciler never ran {needle!r}")


def test_preemption_never_cancels_a_bookkeeping_statement(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The preemptor cancels only an index statement. While the reconciler
    holds the lock in a non-index statement, it is waited for; it then sees
    the preemption request before its next index statement."""
    dsn = clean_runtime
    _seeded(dsn)
    real_check = dai._check_build_preconditions

    def slow_check(conn: Any, cur: Any, deployment: str) -> None:
        cur.execute("SELECT pg_sleep(3) /* bookkeeping */")
        real_check(conn, cur, deployment)

    monkeypatch.setattr(dai, "_check_build_preconditions", slow_check)
    thread, box = _in_thread(s.reconcile, dsn)
    pid = _wait_for_reconciler_query(dsn, "bookkeeping")
    with dai.index_ddl_preemption(dsn, actor="migrate", wait_seconds=30) as holder:
        report = holder.hold_ddl_key()
        thread.join(30)
    assert "error" not in box, box
    assert box["result"]["status"] == "preempted", box["result"]
    assert report["status"] == "acquired", report
    assert pid not in report["cancelled_pids"] and report["cancelled_pids"] == []
    assert report["waited_seconds"] >= 1.0, "it waited for the statement to finish"
    rows = s.manifest(dsn)
    assert all(r["state"] == "pending" and r["attempts"] == 0 for r in rows.values()), rows
    monkeypatch.setattr(dai, "_check_build_preconditions", real_check)
    assert s.reconcile(dsn)["status"] == "converged"


def test_a_cancel_landing_on_bookkeeping_under_preemption_is_a_preemption(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A cancel can race a statement boundary and land on a bookkeeping
    statement after a row is marked building. Under a preemption request
    that is a preemption: no error, the row failed with reason preempted,
    not counted."""
    dsn = clean_runtime
    _seeded(dsn)
    real_set = dai._set

    def slow_set(cur: Any, name: str, **fields: Any) -> None:
        real_set(cur, name, **fields)
        if fields.get("state") == "building":
            cur.execute("SELECT pg_sleep(10) /* after building */")

    monkeypatch.setattr(dai, "_set", slow_set)
    thread, box = _in_thread(s.reconcile, dsn)
    pid = _wait_for_reconciler_query(dsn, "after building")
    with psycopg.connect(dsn, autocommit=True) as preemptor:
        preemptor.execute("SELECT pg_advisory_lock(hashtext(%s))", (dai.PREEMPT_LOCK,))
        preemptor.execute("SELECT pg_cancel_backend(%s)", (pid,))
        thread.join(30)
    assert not thread.is_alive()
    assert "error" not in box, box
    result = box["result"]
    assert result["status"] == "preempted", result
    assert result["cancelled_at"] == "between_index_statements"
    (name,) = [f["index_name"] for f in result["failed"]]
    row = s.manifest(dsn)[name]
    assert row["state"] == "failed" and row["last_reason"] == "preempted", row
    assert row["attempts"] == 0
    monkeypatch.setattr(dai, "_set", real_set)
    final = s.reconcile(dsn)
    assert final["status"] == "converged", final
    assert s.manifest(dsn)[name]["attempts"] == 0


def test_preemption_does_not_wait_out_a_holder_that_is_not_the_reconciler(
    clean_runtime: str,
) -> None:
    """Another index change (a rotation, a concurrent migrate) holds the DDL
    lock and no reconciler runs: the preemption cannot free that holder, so
    on entry and in hold_ddl_key it reports not_acquired with that reason
    after the short grace instead of the full wait, and cancels nothing."""
    dsn = clean_runtime
    _seeded(dsn)
    with psycopg.connect(dsn, autocommit=True) as other:
        other.execute("SELECT pg_advisory_lock(hashtext(%s))", (dai.INDEX_DDL_LOCK,))
        begun = time.monotonic()
        with dai.index_ddl_preemption(dsn, actor="migrate", wait_seconds=30) as preempt:
            entry = dict(preempt.report)
            report = preempt.hold_ddl_key()
            holding = preempt.holding_ddl_key
        took = time.monotonic() - begun
        other_pid = other.info.backend_pid
        for seen in (entry, report):
            assert seen["status"] == "not_acquired", seen
            assert seen["reason"] == "held_by_another_index_change", seen
            assert seen["holders"] == [other_pid], seen
            assert seen["cancelled_pids"] == [], seen
        assert holding is False
        assert entry["waited_seconds"] >= dai.PREEMPT_OTHER_HOLDER_GRACE_SECONDS, entry
        assert took < 10, took
    # With the holder gone the same preemption takes the lock.
    with dai.index_ddl_preemption(dsn, actor="migrate", wait_seconds=30) as preempt:
        assert preempt.report["status"] == "free", preempt.report
        assert preempt.hold_ddl_key()["status"] == "acquired"


def test_preemption_never_cancels_a_backend_that_reused_the_pid(clean_runtime: str) -> None:
    """The recorded backend start must match: a lock holder whose start time
    differs is waited for, not cancelled."""
    dsn = clean_runtime
    _seeded(dsn)
    holder = _old_snapshot(dsn)
    thread, box = _in_thread(s.reconcile, dsn, lock_timeout_seconds=120.0)
    try:
        waiting = _wait_for_waiting_build(dsn)
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(
                "UPDATE okg.declared_attr_index_state "
                "SET reconciler_backend_start = reconciler_backend_start - interval '1 day'"
            )
        with dai.index_ddl_preemption(dsn, actor="migrate", wait_seconds=2) as preempt:
            report = preempt.report
        assert report["status"] == "not_acquired", report
        assert report["reason"] == "wait_expired", report
        assert report["cancelled_pids"] == []
        assert report["unverified_pids"] == [waiting["backend_pid"]]
        assert thread.is_alive(), "the build was not cancelled"
    finally:
        holder.rollback()
        holder.close()
    thread.join(120)
    assert "error" not in box, box
    assert box["result"]["status"] == "converged", box["result"]


def _invalid_dattr_index(dsn: str, name: str, table: str) -> None:
    """An invalid index under a declared index's name: a unique build over
    duplicate values fails and leaves it behind."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        with pytest.raises(psycopg.errors.UniqueViolation):
            conn.execute(f'CREATE UNIQUE INDEX CONCURRENTLY "{name}" ON public."{table}" (subtype)')


@pytest.mark.parametrize("state", ["failed", "drop_failed", "building"])
def test_doctor_escalates_our_invalid_index_whatever_state_left_it(
    clean_runtime: str, state: str
) -> None:
    dsn = clean_runtime
    _seeded(dsn)
    s.reconcile(dsn, apply=False)
    tables = s.physical(dsn)
    name = dai.index_name("node", "t_text", "text")
    _invalid_dattr_index(dsn, name, tables["node"])
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO okg.declared_attr_indexes
              (index_name, target_kind, attr_id, value_type, key_version, physical_table,
               definition_digest, state, attempts, state_since)
            VALUES (%s, 'node', 't_text', 'text', %s, %s, 'sha256:x', %s, 1,
                    now() - interval '2 hours')
            """,
            (name, dai.KEY_VERSION, tables["node"], state),
        )

    def finding() -> dict[str, Any]:
        found = [f for f in dai.doctor_report(dsn)["findings"]
                 if f["code"] == "maintained_invalid_index"]
        assert len(found) == 1, found
        return found[0]

    warn = finding()
    assert warn["severity"] == "WARN" and warn["state"] == state and warn["index"] == name
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("UPDATE okg.declared_attr_indexes SET state_since = now() - interval '25 hours'")
    error = finding()
    assert error["severity"] == "ERROR"
    assert dai.doctor_report(dsn)["status"] == "ERROR"


# --------------------------------------------------------------------------- #
# okg#3112 step 4a: doctor names every declared attribute without an index    #
# --------------------------------------------------------------------------- #

#: Three declared node attributes; ``t_text`` is the one each case breaks.
_UNINDEXED_DECL = {"t_text": "text", "t_num": "num", "t_bool": "bool"}


def _unindexed(dsn: str) -> list[tuple[str, str, str]]:
    """``(attr_id, reason, severity)`` of each ``attr_not_indexed`` finding."""
    return [(f["attr_id"], f["reason"], f["severity"])
            for f in dai.doctor_report(dsn)["findings"] if f["code"] == "attr_not_indexed"]


def _set_row(dsn: str, assignments: str, params: tuple[Any, ...] = ()) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"UPDATE {dai.MANIFEST_TABLE} SET {assignments} WHERE index_name = %s",
                     (*params, dai.index_name("node", "t_text", "text")))


_ICU = {"provider": "i", "collate": "C", "locale": "en-US"}

#: (case id, manifest change for t_text's row or None, environment/patch
#: hook, expected reason, expected severity).
_UNINDEXED_CASES: list[tuple[str, str | None, str | None, str, str]] = [
    ("pending-fresh", "state = 'pending', state_since = now()", None, "pending", "INFO"),
    ("pending-old", "state = 'pending', state_since = now() - interval '2 hours'", None,
     "pending", "WARN"),
    ("building-fresh", "state = 'building', state_since = now()", None, "building", "INFO"),
    ("building-old", "state = 'building', state_since = now() - interval '2 hours'", None,
     "building", "WARN"),
    ("failed", "state = 'failed', attempts = 1", None, "failed", "WARN"),
    ("failure_cap", f"state = 'failed', attempts = {dai.MAX_CONSECUTIVE_FAILURES}", None,
     "failure_cap", "ERROR"),
    ("declaration_changed", "definition_digest = 'sha256:other'", None,
     "declaration_changed", "WARN"),
    ("maintenance_disabled", "state = 'pending', state_since = now()", "maintenance_off",
     "maintenance_disabled", "ERROR"),
    ("collation_refuses_text", None, "icu", "collation_refuses_text", "ERROR"),
    ("index_missing", None, "drop", "index_missing", "ERROR"),
]


@pytest.mark.parametrize(("case", "change", "hook", "reason", "severity"), _UNINDEXED_CASES,
                         ids=[case[0] for case in _UNINDEXED_CASES])
def test_doctor_names_a_declared_attribute_without_a_ready_index(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch, case: str, change: str | None,
    hook: str | None, reason: str, severity: str,
) -> None:
    """One ``attr_not_indexed`` finding for ``t_text``, with the reason and
    severity its state gives; the ready ``t_num`` and ``t_bool`` get none.
    Failure case: the same deployment with every index ready has no such
    finding."""
    dsn = clean_runtime
    _seeded(dsn, dict(_UNINDEXED_DECL))
    assert s.reconcile(dsn)["status"] == "converged"
    assert _unindexed(dsn) == []
    if change is not None:
        _set_row(dsn, change)
    if hook == "maintenance_off":
        monkeypatch.setenv(dai.MAINTENANCE_ENV, "off")
    elif hook == "icu":
        monkeypatch.setattr(dai, "database_collation", lambda cur: dict(_ICU))
    elif hook == "drop":
        # The manifest row stays ready; the index itself is gone.
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(f"DROP INDEX public.{dai.index_name('node', 't_text', 'text')}")
    assert _unindexed(dsn) == [("t_text", reason, severity)]
    found = next(f for f in dai.doctor_report(dsn)["findings"]
                 if f["code"] == "attr_not_indexed")
    assert found["value_type"] == "text" and found["fix"], found
    assert "okg.attr_index" in found["message"], found
    if severity == "ERROR":
        assert dai.doctor_report(dsn)["status"] == "ERROR"


def test_doctor_reads_the_live_manifest_row_beside_a_retired_one(clean_runtime: str) -> None:
    """A retired row of an earlier value type (here ``drop_failed``, named to
    sort after the live row) never answers for the attribute: the live row's
    capped failure is still ``failure_cap``. Failure case: reading the last
    row by name would report the retired row's state."""
    dsn = clean_runtime
    _seeded(dsn, dict(_UNINDEXED_DECL))
    assert s.reconcile(dsn)["status"] == "converged"
    _set_row(dsn, f"state = 'failed', attempts = {dai.MAX_CONSECUTIVE_FAILURES}")
    live = dai.index_name("node", "t_text", "text")
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"CREATE TEMP TABLE retired AS SELECT * FROM {dai.MANIFEST_TABLE} "
                     "WHERE index_name = %s", (live,))
        conn.execute("UPDATE retired SET index_name = 'okg_dattr_zzzz_retired', "
                     "value_type = 'num', state = 'drop_failed', attempts = 0")
        conn.execute(f"INSERT INTO {dai.MANIFEST_TABLE} SELECT * FROM retired")
    assert _unindexed(dsn) == [("t_text", "failure_cap", "ERROR")]


def test_doctor_names_every_declared_attribute_before_indexes_are_enabled(
    clean_runtime: str,
) -> None:
    """Declared, never enabled: one ``never_enabled`` ERROR per declared
    attribute, and no ``not_enabled`` INFO (that one is for a deployment
    that declares nothing)."""
    dsn = clean_runtime
    _seeded(dsn, dict(_UNINDEXED_DECL))
    doctor = dai.doctor_report(dsn)
    assert doctor["enabled"] is False
    assert _unindexed(dsn) == [(attr, "never_enabled", "ERROR")
                               for attr in sorted(_UNINDEXED_DECL)]
    assert doctor["status"] == "ERROR"
    assert not [f for f in doctor["findings"] if f["code"] == "not_enabled"]
    assert s.reconcile(dsn)["status"] == "converged"
    assert _unindexed(dsn) == []


def test_doctor_has_no_unindexed_finding_when_nothing_is_declared(clean_runtime: str) -> None:
    """A deployment that declares no filterable attribute: no
    ``attr_not_indexed`` finding, before and after it is enabled; before, the
    ``not_enabled`` INFO says no index is maintained."""
    from tests.pytest.catalog_fixture_builders import SubtypeSpec, seed_catalog

    dsn = clean_runtime
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=[SubtypeSpec(name="thing"), SubtypeSpec(name="other")],
                     narrowings=[("thing", "rel", "thing")])
    doctor = dai.doctor_report(dsn)
    assert [(f["code"], f["severity"]) for f in doctor["findings"]] == [("not_enabled", "INFO")]
    assert doctor["status"] == "OK"
    s.reconcile(dsn)
    doctor = dai.doctor_report(dsn)
    assert not [f for f in doctor["findings"] if f["code"] == "attr_not_indexed"], doctor
    assert doctor["status"] == "OK", doctor["findings"]
