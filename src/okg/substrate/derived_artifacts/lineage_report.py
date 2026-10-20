"""CLI report for materialized lineage reverse-index coverage."""
from __future__ import annotations

import argparse
import json
import os
from typing import Any

import psycopg

from okg.substrate.derived_artifacts.dependency_index import (
    summarize_lineage_index,
)
from okg.substrate.db.connection import connect as db_connect


def build_lineage_report(
    conn: psycopg.Connection[Any],
    *,
    deployment: str,
    target_generation_id: int | None = None,
) -> dict[str, Any]:
    summary = summarize_lineage_index(
        conn,
        deployment_name=deployment,
        target_generation_id=target_generation_id,
    )
    status = (
        "available"
        if summary.envelope_count and summary.dependency_key_count
        else "unavailable"
    )
    return {
        "kind": "materialized_dependency_index_report_v1",
        "deployment": deployment,
        "target_generation_id": target_generation_id,
        "status": status,
        "summary": summary.to_dict(),
        "planning_summary": _planning_summary(
            conn,
            deployment=deployment,
            target_generation_id=target_generation_id,
        ),
    }


def _planning_summary(
    conn: psycopg.Connection[Any],
    *,
    deployment: str,
    target_generation_id: int | None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.metrics->'lineage_planning'
              FROM okg.derived_artifact_runs r
              JOIN okg.derived_artifact_definitions d
                ON d.definition_id = r.definition_id
             WHERE d.deployment_name = %(deployment)s
               AND r.metrics ? 'lineage_planning'
               AND (%(target_generation_id)s::bigint IS NULL
                    OR r.target_generation_id =
                       %(target_generation_id)s::bigint)
            """,
            {
                "deployment": deployment,
                "target_generation_id": target_generation_id,
            },
        )
        plans = [dict(row[0] or {}) for row in cur.fetchall()]

    unavailable_kinds: set[str] = set()
    for plan in plans:
        unavailable_kinds.update(plan.get("unavailable_dependency_kinds") or [])
    return {
        "planning_run_count": len(plans),
        "max_dirty_output_count": max(
            (int(plan.get("dirty_output_count") or 0) for plan in plans),
            default=0,
        ),
        "max_reusable_output_count": max(
            (int(plan.get("reusable_output_count") or 0) for plan in plans),
            default=0,
        ),
        "unavailable_run_count": sum(
            1 for plan in plans if plan.get("status") == "unavailable"
        ),
        "force_full_run_count": sum(
            1 for plan in plans if plan.get("force_full") is True
        ),
        "unavailable_dependency_kinds": sorted(unavailable_kinds),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m okg.substrate.derived_artifacts.lineage_report"
    )
    parser.add_argument("--deployment", required=True)
    parser.add_argument("--target-generation-id", type=int)
    parser.add_argument("--dsn")
    args = parser.parse_args(argv)

    dsn = args.dsn or os.environ.get("OKG_DSN") or os.environ.get(
        "OKG_TEST_DSN"
    )
    if not dsn:
        parser.error("set OKG_DSN or pass --dsn")
    with db_connect(dsn) as conn:
        report = build_lineage_report(
            conn,
            deployment=args.deployment,
            target_generation_id=args.target_generation_id,
        )
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
