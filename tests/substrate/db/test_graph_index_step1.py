"""okg#3112 step 1: retired graph indexes and the post-publish index rebuild.

PACT lane-h-3112-step1. Native PostgreSQL.

* req.migration -- ``okg migrate`` builds the new run-key index with CREATE
  INDEX CONCURRENTLY and drops every retired graph read index (and a
  cold-history hot replacement of one) with DROP INDEX CONCURRENTLY; a second
  run changes nothing; an invalid index left by an interrupted build is
  rebuilt; reconciliation never recreates a retired one. A lock timeout or a
  concurrent index change defers the step without failing the migrate, and
  the next migrate or the post-publish rebuild finishes it
  (``test_migrate_*``, ``test_a_concurrent_migrate_*``,
  ``test_the_post_publish_rebuild_finishes_a_deferred_migrate``).
* req.plans -- the reads the retired indexes served still run on an index
  (``test_reads_*``).
* req.rebuild -- the rebuild runs after every successful publish cycle (end
  to end through ``Publisher.cycle()``), returns the space, yields to a
  waiting publish, a held publisher lock or a source run, skips a table with a leftover invalid index, cleans up after a
  failed rebuild under the lock, handles an empty table without re-analyzing
  it, and has a kill switch (``test_post_publish_*``). The bloated table is
  ``okg.attribute_versions`` (before okg#3112 step 4b removed it, it was
  ``okg.attr_index``).

Scale is small so the file fits a CI lane; the size floor is lowered per call.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")

from psycopg.rows import dict_row  # noqa: E402

from okg.substrate.db import cold_history, index_reclaim  # noqa: E402
from okg.substrate.db.migrate import migrate  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from okg.substrate.runtime.source_sync_priority import (  # noqa: E402
    publisher_publish_advisory_keys,
)
from tests.pytest.catalog_fixture_builders import (  # noqa: E402
    SubtypeSpec,
    bind_test_catalog_writer,
    seed_catalog,
)

pytestmark = pytest.mark.postgres

_DEPLOYMENT = "default"
#: Columns of the retired indexes as dev built them before this change.
_RETIRED_COLUMNS = {
    "node_live_lo_visible": ("live_lo",),
    "node_live_hi_visible": ("live_hi",),
    "edge_live_lo_visible": ("live_lo",),
    "edge_live_hi_visible": ("live_hi",),
    "edge_src_visible": ("src", "live_lo", "live_hi", "edge_id"),
    "edge_source_order_visible": ("source", "src", "edge_type", "dst", "live_lo", "live_hi"),
    "edge_source_run_visible": (
        "source", "run_id", "live_lo", "live_hi", "src", "edge_type", "dst",
    ),
    # Built only by an early lane-s-redesign-s1 build (text-keyed edge id).
    "edge_type_text_id": ("edge_type", "(edge_id::text)", "live_lo", "live_hi"),
}
_SMALL = 256 * 1024


# --------------------------------------------------------------------------- #
# Seeding                                                                      #
# --------------------------------------------------------------------------- #


def _s1_nid(i: int, suffix: str) -> str:
    return f"doc:{i:07d}:{suffix}"


def _s1_apply(dsn: str, facts: list[Any], *, source: str = "default", run_id: str | None = None) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        for start in range(0, len(facts), 5000):
            kwargs: dict[str, Any] = {"source": source, "strict_endpoints": True}
            if run_id is not None:
                kwargs["run_id"] = run_id
            store.apply_facts(facts[start:start + 5000], **kwargs)


def _s1_seed(dsn: str, *, n_nodes: int, edges_per_node: int) -> tuple[Any, str]:
    suffix = uuid.uuid4().hex[:6]
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=[
                SubtypeSpec(
                    name="doc",
                    metadata={"filterable_attrs": {"path": "text", "owner": "text", "rank": "num"}},
                )
            ],
            narrowings=[("doc", "links", "doc")],
        )
    identity = bind_test_catalog_writer(dsn, _DEPLOYMENT)
    _s1_apply(dsn, [
        NodeFact(
            _s1_nid(i, suffix), "doc",
            {"path": f"dir{i % 97:02d}/file{i:07d}.csv", "owner": f"o{i % 4}", "rank": i % 1000},
            {"id": _s1_nid(i, suffix)}, {"v": 1},
        )
        for i in range(n_nodes)
    ])
    _s1_apply(dsn, [
        EdgeFact(
            src=_s1_nid(i, suffix), dst=_s1_nid((i + k * 101) % n_nodes, suffix), edge_type="links",
            attrs={"rank": (i + k) % 50},
            source_record_id={"id": f"link:{i}:{k}:{suffix}"}, source_revision={"v": 1},
        )
        for i in range(n_nodes)
        for k in range(1, edges_per_node + 1)
    ])
    return identity, suffix


def _s1_publish(dsn: str, identity: Any) -> dict[str, Any]:
    return Publisher(dsn, _DEPLOYMENT, catalog_identity=identity).cycle()


def _physical(dsn: str) -> dict[str, str]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT table_name, physical_table FROM _chronos_branch_tables "
            "WHERE backend = 'interval' AND table_name IN ('okg.graph_nodes', 'okg.graph_edges')"
        )
        return {str(r[0]): str(r[1]) for r in cur.fetchall()}


def _graph_indexes(dsn: str) -> dict[str, dict[str, Any]]:
    """Every index on the two graph tables: name -> {valid, definition}."""
    physical = _physical(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT ic.relname AS name, (i.indisvalid AND i.indisready) AS valid,
                   pg_get_indexdef(i.indexrelid) AS definition
              FROM pg_index i
              JOIN pg_class ic ON ic.oid = i.indexrelid
              JOIN pg_class tc ON tc.oid = i.indrelid
             WHERE tc.relname = ANY(%s)
            """,
            (list(physical.values()),),
        )
        return {str(r["name"]): {"valid": bool(r["valid"]), "definition": r["definition"]} for r in cur.fetchall()}


def _retired_names(dsn: str) -> dict[str, str]:
    physical = _physical(dsn)
    out = {}
    for logical, suffixes in cold_history.RETIRED_INTERVAL_SOURCE_READ_INDEX_SUFFIXES.items():
        for suffix in suffixes:
            out[suffix] = cold_history._full_index_name(physical[logical], suffix)
    return out


def _run_key_name(dsn: str) -> str:
    return cold_history._full_index_name(_physical(dsn)["okg.graph_edges"], "edge_source_run_key_visible")


def _recreate_pre_upgrade_indexes(dsn: str) -> None:
    """Put the database in dev's pre-change shape: every retired index, no
    run-key index."""
    physical = _physical(dsn)
    names = _retired_names(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        for suffix, name in names.items():
            table = physical["okg.graph_nodes" if suffix.startswith("node_") else "okg.graph_edges"]
            cols = ", ".join(_RETIRED_COLUMNS[suffix])
            cur.execute(
                f'CREATE INDEX IF NOT EXISTS "{name}" ON public."{table}" ({cols}) WHERE deleted = FALSE'
            )
        cur.execute(f'DROP INDEX IF EXISTS public."{_run_key_name(dsn)}"')


# --------------------------------------------------------------------------- #
# req.migration                                                                #
# --------------------------------------------------------------------------- #


def test_migrate_drops_retired_indexes_builds_the_run_key_index_and_is_idempotent(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    _s1_seed(dsn, n_nodes=300, edges_per_node=2)
    fresh = _graph_indexes(dsn)
    retired = _retired_names(dsn)
    run_key = _run_key_name(dsn)
    # A fresh database never builds a retired index; it has the run-key one.
    assert not set(retired.values()) & set(fresh), sorted(set(retired.values()) & set(fresh))
    assert fresh[run_key]["valid"]
    assert "(source, run_id, live_lo, live_hi, edge_id)" in fresh[run_key]["definition"]

    # The upgrade path: dev's indexes, plus an active hot replacement of a
    # retired one (built by `okg db cold-history`).
    _recreate_pre_upgrade_indexes(dsn)
    physical = _physical(dsn)["okg.graph_edges"]
    hot = cold_history.hot_index_name(physical, "edge_source_run_visible", 1)
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f'CREATE INDEX "{hot}" ON public."{physical}" (source, run_id, live_lo, live_hi, src, edge_type, dst) '
            "WHERE deleted = FALSE AND live_hi > 1"
        )
        cur.execute(
            """
            INSERT INTO okg.chronos_hot_index_manifest
              (logical_table, physical_table, suffix, full_index_name, hot_index_name,
               floor, headroom_generations, floor_checkpoint_segment_id,
               floor_checkpoint_branch_point, open_interval_value, state)
            VALUES ('okg.graph_edges', %s, 'edge_source_run_visible', %s, %s, 1, 100, 1, 1, 1, 'active')
            """,
            (physical, retired["edge_source_run_visible"], hot),
        )
    before = _graph_indexes(dsn)
    assert set(retired.values()) <= set(before) and run_key not in before and hot in before

    report = cold_history.converge_interval_read_indexes(dsn, lock_timeout_ms=5000)
    assert report["status"] == "converged"
    assert [item["index_name"] for item in report["created"]] == [run_key]
    assert sorted(item["index_name"] for item in report["dropped"]) == sorted(retired.values())
    assert [item["hot_index_name"] for item in report["retired_hot_indexes"]] == [hot]
    assert report["dropped_bytes"] > 0
    after = _graph_indexes(dsn)
    assert not set(retired.values()) & set(after) and hot not in after
    assert after[run_key]["valid"]
    assert all(item["valid"] for item in after.values()), after
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT state FROM okg.chronos_hot_index_manifest WHERE hot_index_name = %s", (hot,)
        )
        assert cur.fetchone()[0] == "retired"

    # Idempotent: a second run and a full migrate change nothing.
    again = cold_history.converge_interval_read_indexes(dsn, lock_timeout_ms=5000)
    assert again["created"] == [] and again["dropped"] == [] and again["retired_hot_indexes"] == []
    migrate(dsn)
    migrate(dsn)
    assert _graph_indexes(dsn) == after


def test_migrate_itself_runs_the_online_step(clean_runtime: str) -> None:
    """`okg migrate` (migrate()) converges a pre-change database: the retired
    indexes go and reconciliation does not bring any back."""
    dsn = clean_runtime
    _s1_seed(dsn, n_nodes=200, edges_per_node=1)
    _recreate_pre_upgrade_indexes(dsn)
    migrate(dsn)
    after = _graph_indexes(dsn)
    assert not set(_retired_names(dsn).values()) & set(after)
    assert after[_run_key_name(dsn)]["valid"]


def test_migrate_rebuilds_an_invalid_index_left_by_an_interrupted_build(
    clean_runtime: str,
) -> None:
    """A failed CREATE INDEX CONCURRENTLY leaves an invalid index under the
    spec name; reconciliation's IF NOT EXISTS would accept it. The online
    step drops it and builds the real one."""
    dsn = clean_runtime
    _s1_seed(dsn, n_nodes=200, edges_per_node=1)
    run_key = _run_key_name(dsn)
    physical = _physical(dsn)["okg.graph_edges"]
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'DROP INDEX public."{run_key}"')
        # Every edge has the same source: a unique build fails after the
        # catalog entry exists, leaving it invalid.
        with pytest.raises(psycopg.errors.UniqueViolation):
            cur.execute(f'CREATE UNIQUE INDEX CONCURRENTLY "{run_key}" ON public."{physical}" (source)')
    assert _graph_indexes(dsn)[run_key]["valid"] is False
    report = cold_history.converge_interval_read_indexes(dsn, lock_timeout_ms=5000)
    (created,) = report["created"]
    assert created["index_name"] == run_key and created["rebuilt_invalid"] is True
    rebuilt = _graph_indexes(dsn)[run_key]
    assert rebuilt["valid"] and "UNIQUE" not in rebuilt["definition"]
    assert "(source, run_id, live_lo, live_hi, edge_id)" in rebuilt["definition"]


def _old_snapshot(dsn: str) -> Any:
    """A transaction holding an old snapshot (a long publish, in miniature).
    CREATE INDEX CONCURRENTLY builds its index, then waits for it: with a
    lock timeout that wait fails and leaves the invalid index behind."""
    holder = psycopg.connect(dsn)
    holder.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    holder.execute("SELECT count(*) FROM pg_class").fetchone()
    return holder


def _invalid_graph_indexes(dsn: str) -> list[str]:
    return sorted(name for name, item in _graph_indexes(dsn).items() if not item["valid"])


def test_migrate_online_step_defers_on_a_lock_timeout_and_cleans_up(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A long transaction makes the concurrent build's wait hit the lock
    timeout: the step drops the invalid index its build left, reports
    `deferred` with what is pending, and the rest of the migrate succeeds
    without building the index the blocking way. The next migrate, once the
    transaction ended, finishes the job."""
    dsn = clean_runtime
    _s1_seed(dsn, n_nodes=200, edges_per_node=1)
    _recreate_pre_upgrade_indexes(dsn)
    run_key = _run_key_name(dsn)
    retired = set(_retired_names(dsn).values())
    monkeypatch.setenv("OKG_MIGRATE_LOCK_TIMEOUT_MS", "1000")
    holder = _old_snapshot(dsn)
    try:
        started = time.monotonic()
        result = migrate(dsn)
        assert time.monotonic() - started < 120
    finally:
        holder.rollback()
        holder.close()
    step = result.graph_read_indexes
    assert step["status"] == "deferred" and step["reason"] == "lock_timeout", step
    assert "lock timeout" in step["error"].lower(), step
    assert step["invalid_index_left"] == [], step
    assert run_key in step["pending"]["build"], step
    after = _graph_indexes(dsn)
    # Reconciliation did not build it with a plain, write-blocking CREATE INDEX.
    assert run_key not in after, sorted(after)
    assert retired <= set(after)
    assert _invalid_graph_indexes(dsn) == []
    # Next migrate: converged.
    again = migrate(dsn)
    assert again.graph_read_indexes["status"] == "converged", again.graph_read_indexes
    final = _graph_indexes(dsn)
    assert final[run_key]["valid"] and not retired & set(final)


def test_a_concurrent_migrate_defers_the_online_step_and_succeeds(clean_runtime: str) -> None:
    """While another index change holds the shared DDL lock (a first migrate
    still building, a cold-history rotation), a second migrate does not wait
    or fail: its online step reports `deferred` and it builds nothing."""
    dsn = clean_runtime
    _s1_seed(dsn, n_nodes=150, edges_per_node=1)
    _recreate_pre_upgrade_indexes(dsn)
    run_key = _run_key_name(dsn)
    with psycopg.connect(dsn, autocommit=True) as other:
        other.execute("SELECT pg_advisory_lock(hashtext(%s))", (cold_history._INDEX_DDL_LOCK,))
        result = migrate(dsn)
    step = result.graph_read_indexes
    assert step["status"] == "deferred" and step["reason"] == "another_index_change_running", step
    assert run_key not in _graph_indexes(dsn)
    assert migrate(dsn).graph_read_indexes["status"] == "converged"
    assert _graph_indexes(dsn)[run_key]["valid"]


def test_the_post_publish_rebuild_finishes_a_deferred_migrate(clean_runtime: str) -> None:
    """After a deferred migrate, the next successful publish's rebuild builds
    the pending index and drops the retired ones, one step at a time under
    the publisher lock."""
    dsn = clean_runtime
    identity, _suffix = _s1_seed(dsn, n_nodes=200, edges_per_node=1)
    _recreate_pre_upgrade_indexes(dsn)
    run_key = _run_key_name(dsn)
    retired = set(_retired_names(dsn).values())
    with psycopg.connect(dsn, autocommit=True) as other:
        other.execute("SELECT pg_advisory_lock(hashtext(%s))", (cold_history._INDEX_DDL_LOCK,))
        assert migrate(dsn).graph_read_indexes["status"] == "deferred"
    worker = Publisher(dsn, _DEPLOYMENT, catalog_identity=identity)
    worker.post_publish_index_rebuild_in_cycle = True
    summary = worker.cycle()
    assert summary["status"] == "published", summary
    report = summary["post_publish_index_rebuild"]
    convergence = report["graph_index_convergence"]
    assert convergence["status"] == "converged", report
    assert [item["index_name"] for item in convergence["created"]] == [run_key]
    assert {item["index_name"] for item in convergence["dropped"]} == retired
    assert report["max_publisher_lock_hold_seconds"] > 0
    after = _graph_indexes(dsn)
    assert after[run_key]["valid"] and not retired & set(after)


# --------------------------------------------------------------------------- #
# req.plans                                                                    #
# --------------------------------------------------------------------------- #


def _plan(cur: Any, statement: str, params: tuple[Any, ...]) -> dict[str, Any]:
    cur.execute("EXPLAIN (FORMAT JSON) " + statement, params)
    return cur.fetchone()[0][0]["Plan"]


def _nodes(plan: dict[str, Any]) -> list[dict[str, Any]]:
    out = [plan]
    for child in plan.get("Plans", []):
        out.extend(_nodes(child))
    return out


def test_reads_served_by_the_retired_indexes_still_use_an_index(clean_runtime: str) -> None:
    """EXPLAIN every read the retired indexes served, on the visible-row shape
    Chronos runs, with default planner settings: none plans a sequential scan
    of the edge table, the record-set reads need no sort, and the run reads
    are index-only on the new run-key index."""
    dsn = clean_runtime
    _identity, suffix = _s1_seed(dsn, n_nodes=6000, edges_per_node=3)
    # A second, small source written by one run: the selective case every
    # record-set and run read has in production (one source of many).
    run_id = str(uuid.uuid4())
    _s1_apply(dsn, [
        EdgeFact(
            src=_s1_nid(i, suffix), dst=_s1_nid(i + 1, suffix), edge_type="links", attrs={},
            source_record_id={"id": f"small:{i}"}, source_revision={"v": 1},
        )
        for i in range(0, 200)
    ], source="small", run_id=run_id)
    _recreate_pre_upgrade_indexes(dsn)
    cold_history.converge_interval_read_indexes(dsn, lock_timeout_ms=5000)
    edges = _physical(dsn)["okg.graph_edges"]
    run_key = _run_key_name(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'VACUUM ANALYZE public."{edges}"')
        table = f'public."{edges}"'
        # A point every written row is visible at: the newest write's lo.
        cur.execute(f"SELECT max(live_lo) FROM {table} WHERE deleted = FALSE")
        bp = cur.fetchone()[0]
        vis = f"live_lo <= {bp} AND {bp} < live_hi AND deleted = FALSE"
        cur.execute(f"SELECT array_agg(src) FROM (SELECT src FROM {table} WHERE {vis} LIMIT 50) s")
        srcs = cur.fetchone()[0]
        shapes = {
            "record_set_full": (f"SELECT src, edge_type, dst FROM {table} WHERE {vis} AND source = %s ORDER BY edge_id", ("small",)),
            "record_set_strays": (
                f"SELECT src, dst, edge_type, attrs FROM {table} WHERE {vis} AND source = %s ORDER BY edge_id", ("small",),
            ),
            "record_set_sample": (f"SELECT src, edge_type, dst FROM {table} WHERE {vis} AND source = %s ORDER BY edge_id LIMIT 10", ("small",)),
            "run_count": (f"SELECT count(*) FROM {table} WHERE {vis} AND source = %s AND run_id = %s", ("small", run_id)),
            "run_keys": (f"SELECT edge_id FROM {table} WHERE {vis} AND source = %s AND run_id = %s", ("small", run_id)),
            "src_one": (f"SELECT edge_id, dst FROM {table} WHERE {vis} AND src = %s", (srcs[0],)),
            "src_any": (f"SELECT edge_id FROM {table} WHERE {vis} AND src = ANY(%s)", (srcs,)),
        }
        plans = {name: _plan(cur, sql_text, params) for name, (sql_text, params) in shapes.items()}
    for name, plan in plans.items():
        kinds = [node["Node Type"] for node in _nodes(plan)]
        assert "Seq Scan" not in kinds, (name, kinds)
    for name in ("record_set_full", "record_set_strays", "record_set_sample"):
        kinds = [node["Node Type"] for node in _nodes(plans[name])]
        assert "Sort" not in kinds and "Incremental Sort" not in kinds, (name, kinds)
    for name in ("run_count", "run_keys"):
        scans = [n for n in _nodes(plans[name]) if "Index Name" in n]
        assert [n["Index Name"] for n in scans] == [run_key], (name, scans)
        assert scans[0]["Node Type"] == "Index Only Scan", (name, scans[0]["Node Type"])
    for name in ("src_one", "src_any"):
        used = {n["Index Name"] for n in _nodes(plans[name]) if "Index Name" in n}
        assert any(index.endswith("_edge_identity_visible") for index in used), (name, used)


# --------------------------------------------------------------------------- #
# req.rebuild                                                                  #
# --------------------------------------------------------------------------- #


#: A table the post-publish rebuild judges, written in bulk by a publish.
_BLOAT = "okg.attribute_versions"


def _bloat(dsn: str, rows: int = 40_000) -> None:
    """Allocate attribute_versions index pages for rows that roll back (a
    failed write)."""
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.attribute_versions
              (target_kind, target_id, attrs_hash, attrs)
            SELECT 'node', 'bloat:' || g, sha256(convert_to('v' || g, 'UTF8')),
                   jsonb_build_object('path', 'v' || g)
              FROM generate_series(1, %s) g
            """,
            (rows,),
        )
        conn.rollback()


def _bloat_two_tables(dsn: str, rows: int = 40_000) -> None:
    """Like :func:`_bloat`, plus open intervals for the same rolled-back
    versions, so two tables carry an index to rebuild (attribute_versions has
    only its primary key; the rebuild needs a second step to yield before)."""
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.attribute_versions
              (target_kind, target_id, attrs_hash, attrs)
            SELECT 'node', 'bloat:' || g, sha256(convert_to('v' || g, 'UTF8')),
                   jsonb_build_object('path', 'v' || g)
              FROM generate_series(1, %s) g
            """,
            (rows,),
        )
        cur.execute(
            """
            INSERT INTO okg.attribute_effective_view_intervals
              (target_kind, target_id, namespace, branch_id, from_ord, attrs_hash)
            SELECT 'node', 'bloat:' || g, 'default', 'main', 1,
                   sha256(convert_to('v' || g, 'UTF8'))
              FROM generate_series(1, %s) g
            """,
            (rows,),
        )
        conn.rollback()


def _bloat_oids(dsn: str) -> dict[str, int]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT n.nspname || '.' || c.relname, i.indexrelid::bigint FROM pg_index i "
            "JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE i.indrelid = 'okg.attribute_versions'::regclass"
        )
        return {str(r[0]): int(r[1]) for r in cur.fetchall()}


def _invalid_bloat_indexes(dsn: str) -> list[str]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT indexrelid::regclass::text FROM pg_index "
            "WHERE indrelid = 'okg.attribute_versions'::regclass "
            "AND NOT (indisvalid AND indisready AND indislive)"
        )
        return [str(r[0]) for r in cur.fetchall()]


def _published(dsn: str) -> dict[str, Any]:
    identity, _suffix = _s1_seed(dsn, n_nodes=400, edges_per_node=1)
    summary = _s1_publish(dsn, identity)
    assert summary["status"] == "published", summary
    return summary


def _rebuild(dsn: str, summary: Any, **kwargs: Any) -> dict[str, Any]:
    return index_reclaim.rebuild_indexes_after_publish(
        dsn, deployment=_DEPLOYMENT, summary=summary,
        min_reclaim_bytes=kwargs.pop("min_reclaim_bytes", _SMALL), **kwargs,
    )


def test_post_publish_rebuild_returns_bloated_space_only_after_a_successful_publish(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    summary = _published(dsn)
    _bloat(dsn)
    oids = _bloat_oids(dsn)
    # Not after a failed, blocked or no-op publish, nor without a summary.
    for not_success in (
        None,
        {"status": "blocked", "generation_id": "g"},
        {**summary, "noop": True, "reused_generation": True},
    ):
        skipped = _rebuild(dsn, not_success)
        assert skipped["status"] == "skipped", skipped
    assert _bloat_oids(dsn) == oids
    rebuilt = _rebuild(dsn, summary)
    assert rebuilt["status"] == "rebuilt", rebuilt
    names = {item["index"] for item in rebuilt["rebuilt"]}
    assert "okg.attribute_versions_pkey" in names, names
    assert rebuilt["reclaimed_bytes"] > 1_000_000, rebuilt
    for item in rebuilt["rebuilt"]:
        assert item["bytes_after"] < item["bytes"], item
        assert item["ratio"] >= 1.5 and item["bytes"] - item["estimate_bytes"] >= _SMALL, item
    assert _BLOAT in rebuilt["analyzed"]
    assert rebuilt["max_publisher_lock_hold_seconds"] >= 0
    assert rebuilt["leftover_invalid_indexes"] == []
    assert _invalid_bloat_indexes(dsn) == []
    # Exactly the rebuilt attribute_versions indexes have new OIDs (REINDEX
    # CONCURRENTLY swaps in a new index under the old name).
    changed = {name for name, oid in _bloat_oids(dsn).items() if oids.get(name) != oid}
    assert changed == {name for name in names if name in oids}, (changed, names)
    # Nothing is left to do on the next publish; the publisher lock is free.
    assert _rebuild(dsn, summary)["status"] == "nothing_to_rebuild"
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s, %s)", publisher_publish_advisory_keys(_DEPLOYMENT))
        assert cur.fetchone()[0] is True


def test_post_publish_rebuild_kill_switch(clean_runtime: str, monkeypatch: pytest.MonkeyPatch) -> None:
    dsn = clean_runtime
    summary = _published(dsn)
    _bloat(dsn)
    oids = _bloat_oids(dsn)
    for off in ("0", "false", "OFF", "no"):
        monkeypatch.setenv(index_reclaim.POST_PUBLISH_REBUILD_ENV, off)
        assert _rebuild(dsn, summary)["status"] == "disabled"
    assert _bloat_oids(dsn) == oids
    monkeypatch.setenv(index_reclaim.POST_PUBLISH_REBUILD_ENV, "1")
    # The thresholds come from the environment too, and a bad value is an
    # error in the report, never a silent default.
    monkeypatch.setenv(index_reclaim.POST_PUBLISH_REBUILD_RATIO_ENV, "abc")
    bad = index_reclaim.rebuild_indexes_after_publish(dsn, deployment=_DEPLOYMENT, summary=summary)
    assert bad["status"] == "error" and index_reclaim.POST_PUBLISH_REBUILD_RATIO_ENV in bad["error"]
    monkeypatch.setenv(index_reclaim.POST_PUBLISH_REBUILD_RATIO_ENV, "1000")
    monkeypatch.setenv(index_reclaim.POST_PUBLISH_REBUILD_MIN_BYTES_ENV, str(_SMALL))
    assert index_reclaim.rebuild_indexes_after_publish(
        dsn, deployment=_DEPLOYMENT, summary=summary
    )["status"] == "nothing_to_rebuild"
    assert _bloat_oids(dsn) == oids
    monkeypatch.delenv(index_reclaim.POST_PUBLISH_REBUILD_RATIO_ENV)
    assert index_reclaim.rebuild_indexes_after_publish(
        dsn, deployment=_DEPLOYMENT, summary=summary
    )["status"] == "rebuilt"


def test_post_publish_rebuild_stops_while_a_publish_holds_the_lock(clean_runtime: str) -> None:
    dsn = clean_runtime
    summary = _published(dsn)
    _bloat(dsn)
    oids = _bloat_oids(dsn)
    with psycopg.connect(dsn, autocommit=True) as holder, holder.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(%s, %s)", publisher_publish_advisory_keys(_DEPLOYMENT))
        stopped = _rebuild(dsn, summary)
        assert stopped["status"] == "stopped" and stopped["reason"] == "publish_running", stopped
        assert stopped["rebuilt"] == [] and stopped["analyzed"] == []
    assert _bloat_oids(dsn) == oids
    assert _rebuild(dsn, summary)["status"] == "rebuilt"


def test_post_publish_rebuild_stops_while_a_source_run_is_writing(clean_runtime: str) -> None:
    from okg.substrate.ingest.run_base import (
        hold_run_liveness,
        run_base_checkpoint_id,
        take_run_base,
    )

    dsn = clean_runtime
    summary = _published(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT DISTINCT branch_id FROM okg.generation_ordinals")
        (branch,) = [str(r[0]) for r in cur.fetchall()]
    _bloat(dsn)
    oids = _bloat_oids(dsn)
    checkpoint_id = run_base_checkpoint_id(branch, "okg-3112-s1-run")
    liveness = hold_run_liveness(dsn, checkpoint_id)
    try:
        store = ChronosGraphStore.connect(dsn)
        try:
            take_run_base(
                store, deployment=_DEPLOYMENT, branch=branch, source="default",
                run_id="okg-3112-s1-run",
            )
        finally:
            store.close()
        stopped = _rebuild(dsn, summary)
        assert stopped["status"] == "stopped", stopped
        assert stopped["reason"] == "source_run_in_progress", stopped
        assert [run["run_id"] for run in stopped["in_progress"]] == ["okg-3112-s1-run"]
        assert _bloat_oids(dsn) == oids
    finally:
        liveness.close()
    assert _rebuild(dsn, summary)["status"] == "rebuilt"


def test_post_publish_rebuild_skips_a_table_with_a_leftover_invalid_index(clean_runtime: str) -> None:
    dsn = clean_runtime
    summary = _published(dsn)
    _bloat(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        # A leftover of an interrupted concurrent build on attribute_versions.
        with pytest.raises(psycopg.errors.UniqueViolation):
            cur.execute("CREATE UNIQUE INDEX CONCURRENTLY attribute_versions_3112_ccnew ON okg.attribute_versions (target_kind)")
    oids = _bloat_oids(dsn)
    report = _rebuild(dsn, summary)
    leftovers = report["leftover_invalid_indexes"]
    assert [bad["index"] for bad in leftovers] == ["okg.attribute_versions_3112_ccnew"], report
    assert leftovers[0]["drop_command"].startswith("DROP INDEX CONCURRENTLY ")
    assert "attribute_versions_3112_ccnew" in leftovers[0]["drop_command"]
    assert not any(item["table"] == _BLOAT for item in report.get("rebuilt") or []), report
    assert _bloat_oids(dsn) == oids
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(leftovers[0]["drop_command"])
    assert _rebuild(dsn, summary)["status"] == "rebuilt"


def test_post_publish_rebuild_failure_leaves_no_invalid_index(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A transaction with an old snapshot makes the REINDEX's wait hit the
    lock timeout after it built its copy: the rebuild stops, drops the
    invalid copy it made, and the next publish's run succeeds."""
    dsn = clean_runtime
    summary = _published(dsn)
    _bloat(dsn)
    real = index_reclaim._reindex_index
    seen: dict[str, Any] = {}

    def with_old_snapshot(cur: Any, schema: str, index: str) -> None:
        holder = psycopg.connect(dsn)
        try:
            holder.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            holder.execute("SELECT count(*) FROM okg.attribute_versions").fetchone()
            seen.setdefault("calls", 0)
            seen["calls"] += 1
            real(cur, schema, index)
        finally:
            holder.rollback()
            holder.close()

    monkeypatch.setattr(index_reclaim, "_reindex_index", with_old_snapshot)
    stopped = _rebuild(dsn, summary, lock_timeout_seconds=1.0)
    assert seen["calls"] == 1
    assert stopped["status"] == "stopped" and stopped["reason"] == "reindex_failed", stopped
    failure = stopped["failure"]
    assert "lock timeout" in failure["error"].lower(), failure
    assert all("_ccnew" in name for name in failure["invalid_indexes_dropped"]), failure
    assert failure["invalid_indexes_left"] == [], failure
    assert _invalid_bloat_indexes(dsn) == []
    monkeypatch.setattr(index_reclaim, "_reindex_index", real)
    assert _rebuild(dsn, summary)["status"] == "rebuilt"


def test_packed_estimate_tracks_a_fresh_build(clean_runtime: str) -> None:
    """Right after REINDEX no btree of the publish-written tables is more
    than 1.25 times its estimate, so the default 1.5 ratio does not fire on a
    freshly built index. (An index with many equal keys is far below it:
    deduplication is not modelled, which only makes a rebuild less likely.)"""
    dsn = clean_runtime
    _identity, _suffix = _s1_seed(dsn, n_nodes=3000, edges_per_node=3)
    _s1_publish(dsn, _identity)
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn, conn.cursor() as cur:
        tables = index_reclaim._post_publish_tables(cur)
        for table in tables:
            cur.execute(f"REINDEX TABLE {table}")
            cur.execute(f"ANALYZE {table}")
        judged = index_reclaim.judge_indexes(cur, tables, ratio=1.5, min_reclaim_bytes=0)
    measured = [item for item in judged if item["ratio"] is not None and item["bytes"] >= 64 * 1024]
    assert measured, judged
    over = [item for item in measured if item["ratio"] > 1.25]
    assert over == [], over
    # Most are close to it: the estimate is a size model, not a bound.
    close = [item for item in measured if 0.8 <= item["ratio"] <= 1.25]
    assert len(close) >= len(measured) * 3 // 4, measured
    assert all(item["verdict"] == "keep" for item in measured), measured


def test_ingest_hook_runs_once_for_the_last_successful_publish_and_never_after_a_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    from okg.substrate.ingest import service

    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        index_reclaim, "rebuild_indexes_after_publish",
        lambda dsn, *, deployment, summary: calls.append(summary) or {"status": "rebuilt"},
    )
    options = SimpleNamespace(dsn="postgresql://unused", deployment="d")
    failed = service.IngestRunResult(report={"publishes": [{"status": "published"}]}, any_failed=True)
    index_reclaim.rebuild_indexes_after_ingest(options, failed)
    assert calls == [] and "post_publish_index_rebuild" not in failed.report
    none = service.IngestRunResult(report={"publishes": []}, any_failed=False)
    index_reclaim.rebuild_indexes_after_ingest(options, none)
    assert calls == []
    first, last, noop = (
        {"status": "published", "generation_id": "a"},
        {"status": "published", "generation_id": "b"},
        {"status": "published", "generation_id": "b", "noop": True},
    )
    ok = service.IngestRunResult(report={"publishes": [first, last, noop]}, any_failed=False)
    index_reclaim.rebuild_indexes_after_ingest(options, ok)
    assert calls == [last] and ok.report["post_publish_index_rebuild"] == {"status": "rebuilt"}


def test_post_publish_rebuild_runs_inside_a_real_publish_cycle(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End to end through the in-cycle hook: a `Publisher.cycle()` with no
    caller connection that its caller opted in (the publisher worker does)
    takes and releases the publisher lock itself, then rebuilds. A publisher
    left at the default (the MCP connector and exchange request paths) does
    not."""
    dsn = clean_runtime
    identity, suffix = _s1_seed(dsn, n_nodes=400, edges_per_node=1)
    _bloat(dsn)
    monkeypatch.setenv(index_reclaim.POST_PUBLISH_REBUILD_MIN_BYTES_ENV, str(_SMALL))
    first = Publisher(dsn, _DEPLOYMENT, catalog_identity=identity).cycle()
    assert first["status"] == "published" and "post_publish_index_rebuild" not in first
    oids = _bloat_oids(dsn)
    _s1_apply(dsn, [
        NodeFact(_s1_nid(i, suffix), "doc", {"path": f"changed/{i}", "owner": "o9", "rank": 7},
                 {"id": _s1_nid(i, suffix)}, {"v": 2})
        for i in range(0, 400, 10)
    ])
    worker = Publisher(dsn, _DEPLOYMENT, catalog_identity=identity)
    worker.post_publish_index_rebuild_in_cycle = True
    summary = worker.cycle()
    assert summary["status"] == "published", summary
    report = summary["post_publish_index_rebuild"]
    assert report["status"] == "rebuilt", {
        key: value for key, value in report.items() if key != "thresholds"
    }
    rebuilt = {item["index"] for item in report["rebuilt"]}
    assert "okg.attribute_versions_pkey" in rebuilt, rebuilt
    changed = {name for name, oid in _bloat_oids(dsn).items() if oids.get(name) != oid}
    assert changed == {name for name in rebuilt if name in oids}
    assert _invalid_bloat_indexes(dsn) == []


def test_post_publish_rebuild_yields_to_a_waiting_publish(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A publish that starts waiting for the publisher lock during a rebuild
    step gets it as soon as that step ends: the rebuild sees the waiter
    before its next step and stops (`publish_waiting`)."""
    import threading

    from okg.substrate.runtime.source_sync_priority import (
        publisher_publish_lock_on_connection,
    )

    dsn = clean_runtime
    summary = _published(dsn)
    _bloat_two_tables(dsn)
    real = index_reclaim._reindex_index
    waited: dict[str, Any] = {}
    step_ended: dict[str, float] = {}

    def poller() -> None:
        with psycopg.connect(dsn, autocommit=True) as conn:
            started = time.monotonic()
            with publisher_publish_lock_on_connection(
                conn=conn, deployment=_DEPLOYMENT, wait_seconds=60, operation="test publish",
            ):
                waited["got_at"] = time.monotonic()
                waited["seconds"] = waited["got_at"] - started
                time.sleep(2.0)  # the publish runs

    thread = threading.Thread(target=poller)

    def first_step_with_a_waiter(cur: Any, schema: str, index: str) -> None:
        if "started" not in waited:
            waited["started"] = True
            thread.start()
            time.sleep(1.5)  # the poller is now polling and has announced itself
        real(cur, schema, index)
        step_ended.setdefault("at", time.monotonic())

    monkeypatch.setattr(index_reclaim, "_reindex_index", first_step_with_a_waiter)
    report = _rebuild(dsn, summary)
    thread.join(timeout=60)
    # It stops before its next step: on the waiter's announced intent, or,
    # if the waiter already took the lock, on the held lock.
    assert report["status"] == "stopped", report
    assert report["reason"] in {"publish_waiting", "publish_running"}, report
    assert sum(item["verdict"] == "rebuild" for item in report["judged"]) >= 2, report
    assert len(report["rebuilt"]) == 1, report
    assert waited["got_at"] - step_ended["at"] < 2.0, (waited, step_ended)
    assert _invalid_bloat_indexes(dsn) == []


def test_post_publish_rebuild_handles_an_empty_table_without_reanalyzing(
    clean_runtime: str,
) -> None:
    """An empty table (a failed publish's allocation over an empty heap) is
    analyzed once, rebuilt once, and then left alone: later publishes neither
    analyze nor rebuild it again."""
    dsn = clean_runtime
    _bloat(dsn, rows=60_000)
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.attribute_versions")
        assert cur.fetchone()[0] == 0
    summary = {"status": "published", "generation_id": "g"}
    first = _rebuild(dsn, summary)
    assert first["status"] == "rebuilt", first
    assert _BLOAT in first["analyzed"]
    assert {item["table"] for item in first["rebuilt"]} == {_BLOAT}
    for _ in range(2):
        later = _rebuild(dsn, summary)
        assert later["status"] == "nothing_to_rebuild", later
        assert later["analyzed"] == [], later
    # A small empty table is never analyzed in a loop either.
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn, conn.cursor() as cur:
        judged = index_reclaim.judge_indexes(
            cur, [_BLOAT], ratio=1.5, min_reclaim_bytes=_SMALL,
        )
    assert {item["verdict"] for item in judged} <= {"keep"}, judged


# --------------------------------------------------------------------------- #
# Review round 2: long transactions, time budget, leftovers, intent release    #
# --------------------------------------------------------------------------- #


def _reader(dsn: str) -> Any:
    """A REPEATABLE READ reader with an open snapshot that has read attribute_versions
    (so it also holds a lock a DROP INDEX CONCURRENTLY waits for)."""
    holder = psycopg.connect(dsn)
    holder.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    holder.execute("SELECT count(*) FROM okg.attribute_versions").fetchone()
    return holder


def test_post_publish_rebuild_skips_while_an_old_snapshot_is_open(clean_runtime: str) -> None:
    """A transaction older than the lock timeout, whose snapshot predates
    later writes, would make every concurrent index statement wait under the
    publisher lock: the rebuild does not start a step (`old_snapshot_open`),
    holds the lock for no time, warns after three such runs in a row, and
    runs once the transaction ended."""
    dsn = clean_runtime
    index_reclaim._OLD_SNAPSHOT_STREAK.clear()
    summary = _published(dsn)
    _bloat(dsn)
    oids = _bloat_oids(dsn)
    holder = _reader(dsn)
    runs: list[dict[str, Any]] = []
    try:
        time.sleep(1.5)
        _advance_xid(dsn)  # a write commits after the reader's snapshot
        _wait_until_old_snapshot(dsn, holder.info.backend_pid)
        for _ in range(3):
            runs.append(_rebuild(dsn, summary, lock_timeout_seconds=1.0))
    finally:
        holder.rollback()
        holder.close()
    stopped = runs[0]
    assert stopped["status"] == "stopped" and stopped["reason"] == "old_snapshot_open", stopped
    assert stopped["old_snapshots"] and stopped["old_snapshots"][0]["xact_seconds"] >= 1.0
    assert stopped["stats_visible"] is True
    assert stopped["max_publisher_lock_hold_seconds"] == 0.0
    assert [run["old_snapshot_streak"] for run in runs] == [1, 2, 3]
    assert _bloat_oids(dsn) == oids
    done = _rebuild(dsn, summary, lock_timeout_seconds=1.0)
    assert done["status"] == "rebuilt" and "old_snapshot_streak" not in done, done


def _advance_xid(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("SELECT txid_current()").fetchone()


def _wait_until_old_snapshot(dsn: str, holder_pid: int) -> None:
    """Wait, at most 60 s, until the guard counts the holder as old.

    backend_xmin is cluster-wide. A write another xdist worker has open at,
    or starts just after, the holder's snapshot has an xid equal to the
    holder's xmin, and every new session's xmin equals it too until that
    write ends, so the holder is not strictly older yet. Once the guard sees
    the holder, nothing can tie again: new xids only grow.
    """
    deadline = time.monotonic() + 60
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn, conn.cursor() as cur:
        while True:
            old = index_reclaim._old_snapshots(cur, 1.0)
            if holder_pid in {item["pid"] for item in old}:
                return
            if time.monotonic() >= deadline:
                cur.execute("SELECT backend_xmin::text AS xmin FROM pg_stat_activity WHERE pid = pg_backend_pid()")
                raise AssertionError(
                    f"holder {holder_pid} never showed as an old snapshot: a write in another database "
                    f"stayed open, or the guard regressed; old={old}, checker={cur.fetchone()}"
                )
            time.sleep(0.2)


def test_old_snapshot_streak_warns(caplog: pytest.LogCaptureFixture) -> None:
    """The third consecutive `old_snapshot_open` run logs a WARNING naming the
    blocking session; any other outcome resets the count."""
    payload = {"reason": "old_snapshot_open", "old_snapshots": [{"pid": 4242, "xact_seconds": None}]}
    key_dsn = "postgresql://x@127.0.0.1:1/okg_streak_test"
    with caplog.at_level("WARNING", logger="okg.substrate.db.index_reclaim"):
        for expected in (1, 2, 3):
            item = dict(payload)
            index_reclaim._note_old_snapshot_streak(key_dsn, "d", item)
            assert item["old_snapshot_streak"] == expected
    warnings = [r for r in caplog.records if "starved" in r.getMessage()]
    assert len(warnings) == 1 and "4242" in warnings[0].getMessage()
    index_reclaim._note_old_snapshot_streak(key_dsn, "d", {"reason": "nothing"})
    item = dict(payload)
    index_reclaim._note_old_snapshot_streak(key_dsn, "d", item)
    assert item["old_snapshot_streak"] == 1


def test_guards_without_pg_read_all_stats(clean_runtime: str) -> None:
    """A role without pg_read_all_stats sees other roles' xact_start and
    build relid as NULL: the old-snapshot guard still finds the old snapshot
    by backend_xmin, and a build it cannot attribute is counted, so the
    rebuild treats invalid indexes as `unverifiable` instead of leftovers."""
    import threading

    dsn = clean_runtime
    _published(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "DO $$ BEGIN CREATE ROLE okg_3112_lowpriv NOLOGIN; "
            "EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$"
        )
    holder = _reader(dsn)
    build_errors: list[BaseException] = []

    def build() -> None:
        try:
            with psycopg.connect(dsn, autocommit=True) as conn:
                conn.execute("CREATE INDEX CONCURRENTLY attribute_versions_3112_lowpriv ON okg.attribute_versions (target_id)")
        except BaseException as exc:  # noqa: BLE001 -- surfaced below
            build_errors.append(exc)

    thread = threading.Thread(target=build)
    try:
        time.sleep(1.5)
        _advance_xid(dsn)
        thread.start()
        deadline = time.monotonic() + 30
        while "okg.attribute_versions_3112_lowpriv" not in _invalid_bloat_indexes(dsn):
            assert time.monotonic() < deadline, "the concurrent build never started"
            time.sleep(0.2)
        holder_pid = holder.info.backend_pid
        with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn, conn.cursor() as cur:
            assert index_reclaim.stats_visible(cur) is True
            _wait_until_old_snapshot(dsn, holder_pid)
            cur.execute("SET ROLE okg_3112_lowpriv")
            assert index_reclaim.stats_visible(cur) is False
            old = index_reclaim._old_snapshots(cur, 1.0)
            assert holder_pid in {item["pid"] for item in old}, old
            assert all(item["xact_seconds"] is None for item in old), old
            building, unattributed = index_reclaim._tables_with_index_build_in_progress(cur)
            assert building == set() and unattributed >= 1, (building, unattributed)
            cur.execute("RESET ROLE")
            building, unattributed = index_reclaim._tables_with_index_build_in_progress(cur)
            assert _BLOAT in building and unattributed == 0
    finally:
        holder.rollback()
        holder.close()
        thread.join(timeout=60)
    assert not build_errors, build_errors
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DROP INDEX okg.attribute_versions_3112_lowpriv")


def test_post_publish_rebuild_bounds_its_lock_hold_and_leaves_no_permanent_copy(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A reader that starts during a REINDEX and stays open: the REINDEX gives
    up after the short lock timeout, the cleanup DROP gives up after it too
    (the reader holds the table), so the copy stays for now, and the lock was
    held about two lock timeouts. Once the reader is gone, the next run drops
    that copy first, then rebuilds; no invalid index remains."""
    dsn = clean_runtime
    summary = _published(dsn)
    _bloat(dsn)
    real = index_reclaim._reindex_index
    readers: list[Any] = []

    def reader_arrives(cur: Any, schema: str, index: str) -> None:
        if not readers:
            readers.append(_reader(dsn))
        real(cur, schema, index)

    monkeypatch.setattr(index_reclaim, "_reindex_index", reader_arrives)
    try:
        stopped = _rebuild(dsn, summary, lock_timeout_seconds=1.0)
    finally:
        for holder in readers:
            holder.rollback()
            holder.close()
    assert stopped["status"] == "stopped" and stopped["reason"] == "reindex_failed", stopped
    assert stopped["max_publisher_lock_hold_seconds"] < 10.0, stopped
    left = [bad["index"] for bad in stopped["failure"]["invalid_indexes_left"]]
    monkeypatch.setattr(index_reclaim, "_reindex_index", real)
    after = _rebuild(dsn, summary, lock_timeout_seconds=1.0)
    assert after["status"] == "rebuilt", after
    assert sorted(after["cleaned_up_leftovers"]) == sorted(left), (after, left)
    assert after["leftover_invalid_indexes"] == []
    assert _invalid_bloat_indexes(dsn) == []


def test_post_publish_rebuild_drops_a_crashed_runs_copy(clean_runtime: str) -> None:
    """A `<index>_ccnew` left by a worker that died mid-rebuild (no build in
    progress, named after a valid index of the same table) is dropped on the
    next run instead of blocking the table forever."""
    dsn = clean_runtime
    summary = _published(dsn)
    _bloat(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        with pytest.raises(psycopg.errors.UniqueViolation):
            cur.execute(
                "CREATE UNIQUE INDEX CONCURRENTLY attribute_versions_pkey_ccnew ON okg.attribute_versions (target_kind)"
            )
    assert _invalid_bloat_indexes(dsn) == ["okg.attribute_versions_pkey_ccnew"]
    report = _rebuild(dsn, summary)
    assert report["cleaned_up_leftovers"] == ["okg.attribute_versions_pkey_ccnew"], report
    assert report["leftover_invalid_indexes"] == [] and report["status"] == "rebuilt", report
    assert _invalid_bloat_indexes(dsn) == []


def test_post_publish_rebuild_ignores_a_build_in_progress(clean_runtime: str) -> None:
    """An index another session is building concurrently (a migrate) is
    invalid until it finishes: it is neither reported as a leftover nor
    dropped, and its table waits for the next run."""
    import threading

    dsn = clean_runtime
    summary = _published(dsn)
    holder = psycopg.connect(dsn)
    holder.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    holder.execute("SELECT 1").fetchone()
    errors: list[BaseException] = []

    def build() -> None:
        try:
            with psycopg.connect(dsn, autocommit=True) as conn:
                conn.execute("CREATE INDEX CONCURRENTLY attribute_versions_3112_building ON okg.attribute_versions (target_id)")
        except BaseException as exc:  # noqa: BLE001 -- surfaced below
            errors.append(exc)

    thread = threading.Thread(target=build)
    thread.start()
    try:
        deadline = time.monotonic() + 30
        while "okg.attribute_versions_3112_building" not in _invalid_bloat_indexes(dsn):
            assert time.monotonic() < deadline, "the concurrent build never started"
            time.sleep(0.2)
        report = _rebuild(dsn, summary)
        assert _BLOAT in report["builds_in_progress"], report
        assert all(
            "attribute_versions_3112_building" not in bad["index"]
            for bad in report["leftover_invalid_indexes"]
        ), report
        assert "okg.attribute_versions_3112_building" in _invalid_bloat_indexes(dsn)
    finally:
        holder.rollback()
        holder.close()
        thread.join(timeout=60)
    assert not errors, errors
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DROP INDEX okg.attribute_versions_3112_building")


def test_post_publish_rebuild_stops_at_its_time_budget(clean_runtime: str) -> None:
    """No step starts after the budget; the next run resumes."""
    dsn = clean_runtime
    summary = _published(dsn)
    _bloat(dsn)
    oids = _bloat_oids(dsn)
    stopped = _rebuild(dsn, summary, budget_seconds=0.0)
    assert stopped["status"] == "stopped" and stopped["reason"] == "time_budget", stopped
    assert _bloat_oids(dsn) == oids
    assert _rebuild(dsn, summary)["status"] == "rebuilt"


def test_publisher_wait_intent_release_after_a_failed_transaction(clean_runtime: str) -> None:
    """A release in a failed transaction rolls back and retries the unlock,
    so the intent lock cannot outlive the wait on a long-lived connection
    (a leak would make every later rebuild yield)."""
    from okg.substrate.runtime.source_sync_priority import (
        PublisherWaitIntent,
        publisher_publish_intent_advisory_keys,
    )

    dsn = clean_runtime
    key = publisher_publish_intent_advisory_keys(_DEPLOYMENT)

    def intent_free() -> bool:
        with psycopg.connect(dsn, autocommit=True) as probe, probe.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s, %s)", key)
            got = bool(cur.fetchone()[0])
            if got:
                cur.execute("SELECT pg_advisory_unlock(%s, %s)", key)
            return got

    with psycopg.connect(dsn) as conn:  # not autocommit, like a publish cycle's
        intent = PublisherWaitIntent(conn, _DEPLOYMENT)
        intent.announce()
        assert not intent_free()
        with pytest.raises(psycopg.errors.DivisionByZero):
            conn.execute("SELECT 1/0")
        assert conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR
        intent.release()
        assert not conn.closed, "the retried unlock should have worked"
        assert intent_free()


def test_post_publish_rebuild_reanalyzes_a_table_last_analyzed_while_empty(
    clean_runtime: str,
) -> None:
    """A table analyzed while empty has no column statistics; VACUUM later
    sets its row count without adding them. The rebuild analyzes it once and
    then judges it, instead of skipping it on every publish.

    Autovacuum is off for the table, in this test's own database: its
    auto-ANALYZE could otherwise run between the publish and the check
    and add the column statistics this test needs absent (a nightly run
    failed that way, `assert 6 == 0`)."""
    dsn = clean_runtime
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("ALTER TABLE okg.attribute_versions SET (autovacuum_enabled = off)")
        conn.execute("ANALYZE okg.attribute_versions")
    summary = _published(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("VACUUM okg.attribute_versions")
        cur.execute(
            "SELECT count(*) FROM pg_stats WHERE schemaname = 'okg' AND tablename = 'attribute_versions'"
        )
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT reltuples FROM pg_class WHERE oid = 'okg.attribute_versions'::regclass")
        assert cur.fetchone()[0] > 0
    _bloat(dsn)
    report = _rebuild(dsn, summary)
    assert report["status"] == "rebuilt", report
    assert _BLOAT in report["analyzed"], report
    again = _rebuild(dsn, summary)
    assert again["status"] == "nothing_to_rebuild" and again["analyzed"] == [], again
