#!/usr/bin/env python3
"""Benchmark the real Chronos node relation and ordinary generation path."""
from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.history_compaction import (
    apply_history_compaction_plan,
    create_history_compaction_plan,
)
from okg.substrate.library.sources.base import NodeFact


ROOT = Path(__file__).resolve().parents[1]
_ACTIVE_STATEMENTS: list[str] | None = None


class _RecordingCursor(psycopg.Cursor[Any]):
    """Record SQL that actually crosses the ordinary publish connection."""

    def execute(
        self,
        query: Any,
        params: Any = None,
        *,
        prepare: bool | None = None,
        binary: bool | None = None,
    ) -> "_RecordingCursor":
        if _ACTIVE_STATEMENTS is not None:
            try:
                rendered = query.as_string(self.connection)
            except AttributeError:
                rendered = str(query)
            _ACTIVE_STATEMENTS.append(rendered)
        return super().execute(query, params, prepare=prepare, binary=binary)

    def executemany(
        self,
        query: Any,
        params_seq: Any,
        *,
        returning: bool = False,
    ) -> None:
        materialized = list(params_seq)
        if _ACTIVE_STATEMENTS is not None:
            try:
                rendered = query.as_string(self.connection)
            except AttributeError:
                rendered = str(query)
            _ACTIVE_STATEMENTS.extend(rendered for _ in materialized)
        return super().executemany(query, materialized, returning=returning)


def _uses_index(node: Any) -> bool:
    if isinstance(node, dict):
        if "Index" in str(node.get("Node Type", "")):
            return True
        return any(_uses_index(value) for value in node.values())
    if isinstance(node, list):
        return any(_uses_index(value) for value in node)
    return False


def _identifier(table: str) -> sql.Identifier:
    return sql.Identifier(*table.split("."))


def _looks_like_compaction_sql(statement: str, physical_table: str) -> bool:
    normalized = " ".join(statement.lower().split())
    destructive_patterns = (
        "history_compaction_plans",
        "compacted_generations",
        "vacuum full",
        "delete from okg.attribute_effective_view_intervals",
        f"delete from {physical_table.lower()}",
    )
    return any(pattern in normalized for pattern in destructive_patterns)


def run_benchmark(dsn: str) -> dict[str, Any]:
    """Use actual GraphStore publishes, metadata, relation, and installed indexes."""
    global _ACTIVE_STATEMENTS
    deployment = "chronos-history-benchmark"
    latencies_ms: list[float] = []
    wal_samples: list[int] = []
    publish_statements: list[list[str]] = []
    publish_conn = psycopg.connect(
        dsn, cursor_factory=_RecordingCursor, row_factory=dict_row
    )
    store = ChronosGraphStore.from_connection(
        publish_conn, ensure_schema=False
    )
    active_catalog = int(
        publish_conn.execute(
            "SELECT max(s.catalog_version_id) FROM okg.subtypes AS s "
            "JOIN okg.catalog_versions AS cv USING (catalog_version_id) "
            "WHERE cv.status = 'applied' AND s.name = 'claim'"
        ).fetchone()["max"]
    )
    try:
        for revision in range(40):
            store.apply_facts(
                [
                    NodeFact(
                        f"n:{key}",
                        "claim",
                        {"revision": revision},
                        {},
                        {},
                    )
                    for key in range(10)
                ]
            )
            store.checkpoint_generation(
                f"history-benchmark-{revision}",
                deployment_name=deployment,
                catalog_version_id=active_catalog,
            )
        publish_conn.commit()
    finally:
        _ACTIVE_STATEMENTS = None
        store.close()
        publish_conn.close()

    with psycopg.connect(dsn, autocommit=True) as conn:
        physical = str(
            conn.execute(
                "SELECT physical_table FROM _chronos_branch_tables "
                "WHERE backend = 'interval' AND table_name = 'okg.graph_nodes'"
            ).fetchone()[0]
        )
        relation = _identifier(physical)
        checkpoints = conn.execute(
            "SELECT cp.checkpoint_id, s.branch_point, s.segment_id "
            "FROM _chronos_branch_interval_checkpoints AS cp "
            "JOIN _chronos_branch_interval_segments AS s ON s.segment_id = cp.segment_id "
            "WHERE cp.metadata::jsonb ->> 'deployment_name' = %s "
            "ORDER BY cp.created_at, cp.checkpoint_id",
            (deployment,),
        ).fetchall()
        if len(checkpoints) != 40:
            raise RuntimeError(f"expected 40 actual checkpoints, found {len(checkpoints)}")
        points = [int(row[1]) for row in checkpoints[:31]]
        current_segment = conn.execute(
            "SELECT s.segment_id, s.live_hi FROM _chronos_branch_interval_branches AS b "
            "JOIN _chronos_branch_interval_segments AS s "
            "ON s.segment_id = b.current_segment_id WHERE b.branch_id = 'default'"
        ).fetchone()
        writer_segment = int(current_segment[0])
        open_hi = int(current_segment[1])

        # Add representative key cardinality directly to the real Chronos
        # physical relation. Timed publishes run only after this 310k-row
        # history and its installed indexes exist.
        values = [
            (version, points[version], points[version + 1] if version < 30 else open_hi)
            for version in range(31)
        ]
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TEMP TABLE _okg_history_bench_points "
                "(version integer, live_lo numeric, live_hi numeric)"
            )
            cur.executemany(
                "INSERT INTO _okg_history_bench_points VALUES (%s, %s, %s)",
                values,
            )
            cur.execute(
                sql.SQL(
                    "INSERT INTO {} "
                    "(node_id, subtype, attrs, source_record_id, source_revision, "
                    "live_lo, live_hi, writer_segment_id, deleted) "
                    "SELECT 'n:' || key_id, 'claim', jsonb_build_object('revision', p.version), "
                    "'{{}}'::jsonb, to_jsonb(p.version), p.live_lo, p.live_hi, %s, false "
                    "FROM generate_series(10, 9999) AS key_id "
                    "CROSS JOIN _okg_history_bench_points AS p"
                ).format(relation),
                (writer_segment,),
            )
        conn.execute(sql.SQL("ANALYZE {}").format(relation))

        measured_conn = psycopg.connect(
            dsn, cursor_factory=_RecordingCursor, row_factory=dict_row
        )
        measured_store = ChronosGraphStore.from_connection(
            measured_conn, ensure_schema=False
        )
        try:
            for revision in range(40, 80):
                wal_before = measured_store.context.conn.execute(
                    "SELECT pg_current_wal_lsn() AS wal_lsn"
                ).fetchone()["wal_lsn"]
                _ACTIVE_STATEMENTS = []
                started = time.perf_counter()
                measured_store.apply_facts(
                    [
                        NodeFact(
                            f"n:{key}",
                            "claim",
                            {"revision": revision},
                            {},
                            {},
                        )
                        for key in range(10)
                    ]
                )
                measured_store.checkpoint_generation(
                    f"history-benchmark-{revision}",
                    deployment_name=deployment,
                    catalog_version_id=active_catalog,
                )
                latencies_ms.append((time.perf_counter() - started) * 1000)
                publish_statements.append(list(_ACTIVE_STATEMENTS))
                _ACTIVE_STATEMENTS = None
                wal_after = measured_store.context.conn.execute(
                    "SELECT pg_current_wal_lsn() AS wal_lsn"
                ).fetchone()["wal_lsn"]
                wal_samples.append(
                    int(
                        measured_store.context.conn.execute(
                            "SELECT pg_wal_lsn_diff(%s, %s)::bigint",
                            (wal_after, wal_before),
                        ).fetchone()["pg_wal_lsn_diff"]
                    )
                )
            measured_conn.commit()
        finally:
            _ACTIVE_STATEMENTS = None
            measured_store.close()
            measured_conn.close()

        checkpoints = conn.execute(
            "SELECT cp.checkpoint_id, s.branch_point, s.segment_id "
            "FROM _chronos_branch_interval_checkpoints AS cp "
            "JOIN _chronos_branch_interval_segments AS s ON s.segment_id = cp.segment_id "
            "WHERE cp.metadata::jsonb ->> 'deployment_name' = %s "
            "ORDER BY cp.created_at, cp.checkpoint_id",
            (deployment,),
        ).fetchall()
        if len(checkpoints) != 80:
            raise RuntimeError(
                f"expected 80 seeded + measured checkpoints, found {len(checkpoints)}"
            )
        current_point = int(checkpoints[-1][1])
        recent_point = int(checkpoints[-2][1])
        current_plan = conn.execute(
            sql.SQL(
                "EXPLAIN (FORMAT JSON) SELECT attrs FROM {} WHERE node_id = 'n:42' "
                "AND live_lo <= %s AND %s < live_hi AND deleted = false"
            ).format(relation),
            (current_point, current_point),
        ).fetchone()[0][0]
        recent_plan = conn.execute(
            sql.SQL(
                "EXPLAIN (FORMAT JSON) SELECT node_id FROM {} WHERE subtype = 'claim' "
                "AND node_id = 'n:42' AND live_lo <= %s AND %s < live_hi "
                "AND deleted = false"
            ).format(relation),
            (recent_point, recent_point),
        ).fetchone()[0][0]
        relation_bytes = int(
            conn.execute(
                "SELECT pg_total_relation_size(%s::regclass)", (physical,)
            ).fetchone()[0]
        )
        index_bytes = int(
            conn.execute(
                "SELECT pg_indexes_size(%s::regclass)", (physical,)
            ).fetchone()[0]
        )
        actual_rows = int(
            conn.execute(sql.SQL("SELECT count(*) FROM {}").format(relation)).fetchone()[0]
        )
        installed_indexes = [
            {"name": str(row[0]), "definition": str(row[1])}
            for row in conn.execute(
                "SELECT indexname, indexdef FROM pg_indexes "
                "WHERE schemaname = current_schema() AND tablename = %s ORDER BY indexname",
                (physical.split(".")[-1],),
            ).fetchall()
        ]

        policy = {
            "default": "latest-only",
            "audit_tail_generations": 2,
            "nodes": {"claim": "latest-only"},
            "edges": {},
        }
        compaction_store = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        )
        compaction_plan = create_history_compaction_plan(
            conn,
            store=compaction_store,
            deployment_name=deployment,
            policy_raw=policy,
        )
        compaction_floor = int(
            compaction_plan["floors"]["nodes"]["claim"]["point"]
        )
        compaction_candidate_plan = conn.execute(
            sql.SQL(
                "EXPLAIN (FORMAT JSON) SELECT ctid FROM {} WHERE subtype = 'claim' "
                "AND live_hi <= %s LIMIT 5000"
            ).format(relation),
            (compaction_floor,),
        ).fetchone()[0][0]
        compaction_wal_before = conn.execute(
            "SELECT pg_current_wal_lsn()"
        ).fetchone()[0]
        compaction_started = time.perf_counter()
        compaction_result = apply_history_compaction_plan(
            conn,
            store=compaction_store,
            plan_id=compaction_plan["plan_id"],
            confirm=compaction_plan["plan_id"],
            apply=True,
            policy_raw=policy,
            expected_deployment_name=deployment,
            expected_branch="default",
            batch_size=5000,
        )
        compaction_ms = (time.perf_counter() - compaction_started) * 1000
        compaction_wal_after = conn.execute(
            "SELECT pg_current_wal_lsn()"
        ).fetchone()[0]
        compaction_wal_bytes = int(
            conn.execute(
                "SELECT pg_wal_lsn_diff(%s, %s)::bigint",
                (compaction_wal_after, compaction_wal_before),
            ).fetchone()[0]
        )
        rows_after_compaction = int(
            conn.execute(
                sql.SQL("SELECT count(*) FROM {}").format(relation)
            ).fetchone()[0]
        )

    sorted_latency = sorted(latencies_ms)
    p95_index = max(0, int(len(sorted_latency) * 0.95) - 1)
    statement_counts = [len(sample) for sample in publish_statements]
    compaction_statement_counts = [
        sum(
            _looks_like_compaction_sql(statement, physical)
            for statement in sample
        )
        for sample in publish_statements
    ]
    return {
        "fixture": {
            "live_keys": 10000,
            "generations": 40,
            "history_seed_generations": 40,
            "measured_generations": 40,
            "closed_versions_per_selected_key": 30,
            "interval_rows": actual_rows,
            "physical_relation": physical,
        },
        "plans": {
            "current_uses_index": _uses_index(current_plan),
            "recent_pin_uses_index": _uses_index(recent_plan),
            "current": current_plan,
            "recent_pin": recent_plan,
            "installed_indexes": installed_indexes,
            "compaction_candidate_uses_index": _uses_index(
                compaction_candidate_plan
            ),
            "compaction_candidate": compaction_candidate_plan,
        },
        "storage": {
            "relation_bytes": relation_bytes,
            "index_bytes": index_bytes,
            "offline_reclaim_headroom_bytes": relation_bytes,
            "peak_temporary_disk_headroom_bytes": relation_bytes,
            "headroom_basis": (
                "measured pg_total_relation_size of the actual relation before "
                "its guarded offline rewrite"
            ),
        },
        "publish": {
            "path": "ChronosGraphStore.apply_facts + checkpoint_generation",
            "samples": len(latencies_ms),
            "p50_ms": statistics.median(latencies_ms),
            "p95_ms": sorted_latency[p95_index],
            "wal_bytes": sum(wal_samples),
            "wal_bytes_per_generation_p95": sorted(wal_samples)[p95_index],
            "sql_statements_per_generation": statement_counts,
            "sql_statements_per_generation_p95": sorted(statement_counts)[p95_index],
            "executed_compaction_statements_per_generation": (
                compaction_statement_counts
            ),
            "compaction_statement_delta_per_publish": max(
                compaction_statement_counts
            ),
            "statement_delta_basis": (
                "client-side recording cursor counted every SQL execute/executemany "
                "on each ordinary apply_facts + checkpoint_generation sample"
            ),
        },
        "compaction": {
            "plan_id": compaction_plan["plan_id"],
            "candidate_uses_index": _uses_index(compaction_candidate_plan),
            "elapsed_ms": compaction_ms,
            "wal_bytes": compaction_wal_bytes,
            "rows_before": actual_rows,
            "rows_after": rows_after_compaction,
            "progress": compaction_result["progress"],
            "applied": compaction_result["applied"],
        },
        "index_change": "none; measured the installed Chronos index set",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    report = run_benchmark(args.dsn)
    rendered = json.dumps(report, indent=2, default=str)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
