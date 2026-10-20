#!/usr/bin/env python3
"""Publish and compact an isolated mixed-retention Chronos fixture."""
from __future__ import annotations

import argparse
import json
import random
import string
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from okg.substrate.errors import GenerationCompacted
from okg.substrate.generation_resolver import resolve_okg_generation
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.history_compaction import (
    apply_history_compaction_plan,
    create_history_compaction_plan,
    offline_reclaim_history_storage,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.db.management import database_health_report
from okg.substrate.runtime.bootstrap import run_dbos_migrate


def _payload(revision: int, key: int) -> str:
    rng = random.Random(revision * 1000 + key)
    alphabet = string.ascii_letters + string.digits
    return "".join(rng.choice(alphabet) for _ in range(4096))


def _physical_tables(conn: psycopg.Connection[Any]) -> dict[str, str]:
    return {
        str(row[0]): str(row[1])
        for row in conn.execute(
            "SELECT table_name, physical_table FROM _chronos_branch_tables "
            "WHERE backend = 'interval' AND table_name IN ('okg.graph_nodes', 'okg.graph_edges')"
        ).fetchall()
    }


def _count(conn: psycopg.Connection[Any], table: str) -> int:
    return int(
        conn.execute(
            sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(*table.split(".")))
        ).fetchone()[0]
    )


def _queue_state(conn: psycopg.Connection[Any]) -> dict[str, int]:
    present = conn.execute("SELECT to_regclass('dbos.workflow_status')").fetchone()[0]
    if present is None:
        raise RuntimeError(
            "DBOS workflow_status is absent; queue/DLQ evidence is unavailable"
        )
    rows = conn.execute(
        "SELECT status, count(*) FROM dbos.workflow_status GROUP BY status"
    ).fetchall()
    counts = {str(status): int(count) for status, count in rows}
    return {
        "queue_depth": sum(count for status, count in counts.items() if status not in {"SUCCESS", "CANCELLED"}),
        "dlq": counts.get("ERROR", 0),
    }


def run_proof(dsn: str, deployment: str) -> dict[str, Any]:
    # This proof deliberately mutates interval history. It is valid only on an
    # empty, isolated fixture database; refuse rather than touching a deployment.
    with psycopg.connect(dsn) as conn:
        dbos_present = conn.execute(
            "SELECT to_regclass('dbos.workflow_status')"
        ).fetchone()[0]
    if dbos_present is None:
        run_dbos_migrate(dsn=dsn)

    with psycopg.connect(dsn) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        existing = store.context.list_checkpoints(
            branch=None, metadata_filter={"kind": "okg_generation"}
        )
        if existing:
            raise RuntimeError(
                "proof requires an isolated database with no OKG generation checkpoints"
            )
        active_catalog = int(
            conn.execute(
                "SELECT max(s.catalog_version_id) FROM okg.subtypes AS s "
                "JOIN okg.catalog_versions AS cv USING (catalog_version_id) "
                "WHERE cv.status = 'applied' "
                "GROUP BY s.catalog_version_id "
                "HAVING bool_or(s.name = 'analysis_chunk') AND bool_or(s.name = 'pact_task') "
                "ORDER BY s.catalog_version_id DESC LIMIT 1"
            ).fetchone()[0]
        )

    checkpoint_ids: list[str] = []
    proof_now = datetime.now(timezone.utc)
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        for revision in range(40):
            nodes = [
                NodeFact(
                    f"durable:{key}",
                    "pact_task",
                    {"revision": revision, "payload": _payload(revision, key)},
                    {},
                    {},
                )
                for key in range(5)
            ] + [
                NodeFact(
                    f"transient:{key}",
                    "analysis_chunk",
                    {"revision": revision, "payload": _payload(revision, key + 10)},
                    {},
                    {},
                )
                for key in range(5)
            ]
            edges = [
                EdgeFact(
                    f"durable:{key}",
                    f"transient:{key}",
                    "references",
                    {"revision": revision},
                )
                for key in range(5)
            ]
            store.apply_facts([*nodes, *edges])
            checkpoint_ids.append(
                store.checkpoint_generation(
                    f"retention-proof-{revision}",
                    deployment_name=deployment,
                    catalog_version_id=active_catalog,
                )
            )
        with psycopg.connect(dsn) as timestamp_conn:
            with timestamp_conn.cursor() as cur:
                cur.executemany(
                    "UPDATE _chronos_branch_interval_checkpoints SET created_at = %s "
                    "WHERE checkpoint_id = %s",
                    [
                        (
                            (
                                proof_now
                                - timedelta(hours=39 - revision)
                            ).isoformat(),
                            checkpoint,
                        )
                        for revision, checkpoint in enumerate(checkpoint_ids)
                    ],
                )
        protected_ids = checkpoint_ids[-3:]
        protected_before = {
            checkpoint: store.checkout_generation(checkpoint).query(
                "SELECT node_id, attrs FROM okg.graph_nodes ORDER BY node_id"
            )
            for checkpoint in protected_ids
        }
        current_before = {
            "nodes": store.checkout_generation(checkpoint_ids[-1]).query(
                "SELECT node_id, subtype, attrs FROM okg.graph_nodes ORDER BY node_id"
            ),
            "edges": store.checkout_generation(checkpoint_ids[-1]).query(
                "SELECT src, dst, edge_type, attrs FROM okg.graph_edges ORDER BY src, dst"
            ),
        }
    finally:
        store.close()

    policy = {
        "default": "full",
        "audit_tail_generations": 3,
        "nodes": {
            "pact_task": "full",
            "analysis_chunk": {"horizon": "12h"},
        },
        "edges": {"references": "latest-only"},
    }
    with psycopg.connect(dsn) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        physical = _physical_tables(conn)
        ordinals = {
            str(checkpoint): int(ordinal)
            for checkpoint, ordinal in conn.execute(
                "SELECT checkpoint_id, ordinal FROM okg.generation_ordinals "
                "WHERE checkpoint_id = ANY(%s)",
                (checkpoint_ids,),
            ).fetchall()
        }
        with conn.cursor() as cur:
            for revision, checkpoint in enumerate(checkpoint_ids):
                ordinal = ordinals[checkpoint]
                to_ord = ordinals[checkpoint_ids[revision + 1]] if revision < 39 else 9223372036854775807
                for key in range(5):
                    cur.execute(
                        """
                        INSERT INTO okg.attribute_effective_view_intervals
                          (target_kind, target_id, namespace, branch_id, from_ord,
                           to_ord, subtype, attrs, overlay_count)
                        VALUES ('node', %s, 'proof', 'default', %s, %s,
                                'analysis_chunk', %s, 0)
                        """,
                        (f"transient:{key}", ordinal, to_ord, Jsonb({"revision": revision})),
                    )
                    cur.execute(
                        """
                        INSERT INTO okg.attribute_effective_view_intervals
                          (target_kind, target_id, namespace, branch_id, from_ord,
                           to_ord, edge_type, attrs, overlay_count)
                        VALUES ('edge', %s, 'proof', 'default', %s, %s,
                                'references', %s, 0)
                        """,
                        (f"reference:{key}", ordinal, to_ord, Jsonb({"revision": revision})),
                    )
        conn.commit()
        effective_table = "okg.attribute_effective_view_intervals"
        head_ordinal = ordinals[checkpoint_ids[-1]]
        current_before["effective"] = conn.execute(
            """
            SELECT target_kind, target_id, attrs FROM okg.attribute_effective_view_intervals
             WHERE namespace = 'proof' AND branch_id = 'default'
               AND from_ord <= %s AND %s < to_ord ORDER BY target_id
            """,
            (head_ordinal, head_ordinal),
        ).fetchall()
        protected_effective_before = {
            checkpoint: conn.execute(
                """
                SELECT target_kind, target_id, attrs FROM okg.attribute_effective_view_intervals
                 WHERE namespace = 'proof' AND branch_id = 'default'
                   AND from_ord <= %s AND %s < to_ord ORDER BY target_id
                """,
                (ordinals[checkpoint], ordinals[checkpoint]),
            ).fetchall()
            for checkpoint in protected_ids
        }
        before_rows = {logical: _count(conn, table) for logical, table in physical.items()}
        before_rows[effective_table] = _count(conn, effective_table)
        before_classifier_rows = {
            "full_nodes": int(
                conn.execute(
                    sql.SQL("SELECT count(*) FROM {} WHERE subtype = 'pact_task'").format(
                        sql.Identifier(*physical["okg.graph_nodes"].split("."))
                    )
                ).fetchone()[0]
            ),
            "horizon_nodes": int(
                conn.execute(
                    sql.SQL(
                        "SELECT count(*) FROM {} WHERE subtype = 'analysis_chunk'"
                    ).format(sql.Identifier(*physical["okg.graph_nodes"].split(".")))
                ).fetchone()[0]
            ),
            "latest_effective_edges": int(
                conn.execute(
                    "SELECT count(*) FROM okg.attribute_effective_view_intervals "
                    "WHERE namespace = 'proof' AND target_kind = 'edge'"
                ).fetchone()[0]
            ),
        }
        before_bytes = {
            logical: int(
                conn.execute("SELECT pg_total_relation_size(%s::regclass)", (table,)).fetchone()[0]
            )
            for logical, table in physical.items()
        }
        plan = create_history_compaction_plan(
            conn,
            store=store,
            deployment_name=deployment,
            policy_raw=policy,
            now=proof_now,
        )
        applied = apply_history_compaction_plan(
            conn,
            store=store,
            plan_id=plan["plan_id"],
            confirm=plan["plan_id"],
            apply=True,
            policy_raw=policy,
            batch_size=17,
        )
        replay = apply_history_compaction_plan(
            conn,
            store=store,
            plan_id=plan["plan_id"],
            confirm=plan["plan_id"],
            apply=True,
            policy_raw=policy,
            batch_size=17,
        )
        after_rows = {logical: _count(conn, table) for logical, table in physical.items()}
        after_rows[effective_table] = _count(conn, effective_table)
        after_classifier_rows = {
            "full_nodes": int(
                conn.execute(
                    sql.SQL("SELECT count(*) FROM {} WHERE subtype = 'pact_task'").format(
                        sql.Identifier(*physical["okg.graph_nodes"].split("."))
                    )
                ).fetchone()[0]
            ),
            "horizon_nodes": int(
                conn.execute(
                    sql.SQL(
                        "SELECT count(*) FROM {} WHERE subtype = 'analysis_chunk'"
                    ).format(sql.Identifier(*physical["okg.graph_nodes"].split(".")))
                ).fetchone()[0]
            ),
            "latest_effective_edges": int(
                conn.execute(
                    "SELECT count(*) FROM okg.attribute_effective_view_intervals "
                    "WHERE namespace = 'proof' AND target_kind = 'edge'"
                ).fetchone()[0]
            ),
        }
        current_after = {
            "nodes": store.checkout_generation(checkpoint_ids[-1]).query(
                "SELECT node_id, subtype, attrs FROM okg.graph_nodes ORDER BY node_id"
            ),
            "edges": store.checkout_generation(checkpoint_ids[-1]).query(
                "SELECT src, dst, edge_type, attrs FROM okg.graph_edges ORDER BY src, dst"
            ),
            "effective": conn.execute(
                """
                SELECT target_kind, target_id, attrs FROM okg.attribute_effective_view_intervals
                 WHERE namespace = 'proof' AND branch_id = 'default'
                   AND from_ord <= %s AND %s < to_ord ORDER BY target_id
                """,
                (head_ordinal, head_ordinal),
            ).fetchall(),
        }
        protected_after = {
            checkpoint: store.checkout_generation(checkpoint).query(
                "SELECT node_id, attrs FROM okg.graph_nodes ORDER BY node_id"
            )
            for checkpoint in protected_ids
        }
        protected_effective_after = {
            checkpoint: conn.execute(
                """
                SELECT target_kind, target_id, attrs FROM okg.attribute_effective_view_intervals
                 WHERE namespace = 'proof' AND branch_id = 'default'
                   AND from_ord <= %s AND %s < to_ord ORDER BY target_id
                """,
                (ordinals[checkpoint], ordinals[checkpoint]),
            ).fetchall()
            for checkpoint in protected_ids
        }
        try:
            resolve_okg_generation(
                conn,
                deployment_name=deployment,
                requested=checkpoint_ids[0],
            )
        except GenerationCompacted as exc:
            retired_refusal = exc.code
        else:
            retired_refusal = "missing"
        reclaim = offline_reclaim_history_storage(
            conn,
            plan_id=plan["plan_id"],
            workers_stopped=True,
        )
        current = resolve_okg_generation(
            conn,
            deployment_name=deployment,
            requested=checkpoint_ids[-1],
            include_counts=True,
        )
        queue = _queue_state(conn)

    doctor = database_health_report(dsn, deployment_name=deployment)

    checks = {
        "current_nodes_edges_unchanged": current_after == current_before,
        "protected_checkouts_unchanged": (
            protected_after == protected_before
            and protected_effective_after == protected_effective_before
        ),
        "interval_rows_decreased": sum(after_rows.values()) < sum(before_rows.values()),
        "retired_generation_refusal": retired_refusal == "generation_compacted",
        "replay_noop": bool(replay.get("replayed")),
        "physical_bytes_decreased": reclaim["physically_reclaimed_bytes"] > 0,
        "published_generation": current is not None and current.status == "published",
        "queue_clean": queue["queue_depth"] == 0 and queue["dlq"] == 0,
        "doctor_clean": bool(doctor["ok"]),
        "full_classifier_history_retained": (
            after_classifier_rows["full_nodes"]
            == before_classifier_rows["full_nodes"]
        ),
        "horizon_classifier_is_bounded": (
            5 * policy["audit_tail_generations"]
            < after_classifier_rows["horizon_nodes"]
            < before_classifier_rows["horizon_nodes"]
            and plan["floors"]["nodes"]["analysis_chunk"]["mode"] == "horizon"
        ),
        "latest_effective_is_audit_tail_only": (
            after_classifier_rows["latest_effective_edges"]
            == 5 * policy["audit_tail_generations"]
        ),
    }
    return {
        "ok": all(checks.values()),
        "deployment": deployment,
        "generation_id": checkpoint_ids[-1],
        "plan_id": plan["plan_id"],
        "checks": checks,
        "before_rows": before_rows,
        "after_rows": after_rows,
        "before_classifier_rows": before_classifier_rows,
        "after_classifier_rows": after_classifier_rows,
        "before_bytes": before_bytes,
        "logical_estimates": plan["logical_estimates"],
        "progress": applied["progress"],
        "offline_reclaim": reclaim,
        "queue": queue,
        "doctor": doctor,
        "nodes_live": current.node_count if current else None,
        "edges_live": current.edge_count if current else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--deployment", required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    report = run_proof(args.dsn, args.deployment)
    print(json.dumps(report, indent=2, default=str))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
