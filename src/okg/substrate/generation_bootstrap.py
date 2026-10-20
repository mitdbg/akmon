"""Validation for the formal generation-zero bootstrap ledger anchor."""

from __future__ import annotations

from typing import Any


def formal_bootstrap_generation_inventory(conn: Any) -> dict[str, Any]:
    """Return canonical-anchor status and the real generation count.

    Migration reserves generation zero as a formal FK anchor. It is safe to
    exclude only when both the generation row and its materialized default
    branch exactly match the schema seed contract.
    """
    row = conn.execute(
        """
        WITH generation_zero AS (
          SELECT gg.*, b.deployment_name AS branch_deployment_name,
                 b.name AS branch_name, b.role AS branch_role,
                 b.status AS branch_status,
                 b.base_branch_id, b.base_generation_id,
                 b.policy_clauses, b.closed_at,
                 b.metadata AS branch_metadata
            FROM okg.graph_generations gg
            LEFT JOIN okg.branches b USING (branch_id)
           WHERE gg.generation_id = 0
        )
        SELECT
          (SELECT count(*)
             FROM okg.graph_generations
            WHERE generation_id <> 0) AS non_bootstrap_count,
          count(*) AS generation_zero_count,
          coalesce(bool_and(
                 deployment_name = 'bootstrap'
             AND status = 'published'
             AND fact_watermark IS NULL
             AND edge_fact_watermark IS NULL
             AND catalog_version_id = 1
             AND published_at IS NOT NULL
             AND invariant_summary =
                 '{"reason":"bootstrap generation zero"}'::jsonb
             AND excluded_sources = '[]'::jsonb
             AND blocked_reason IS NULL
             AND correlation_id = 'bootstrap:generation:0'
             AND trace_id IS NULL
             AND nodes_added = 0
             AND nodes_retracted = 0
             AND edges_added = 0
             AND edges_retracted = 0
             AND nomos_policy_bundle_ids = '{}'::text[]
             AND nomos_policy_fingerprints = '{}'::text[]
             AND branch_deployment_name = 'bootstrap'
             AND branch_name = 'default'
             AND branch_role = 'default'
             AND branch_status = 'active'
             AND base_branch_id IS NULL
             AND base_generation_id IS NULL
             AND policy_clauses = '{}'::jsonb
             AND closed_at IS NULL
             AND branch_metadata =
                 '{"auto_materialized":"_fill_default_branch_id trigger"}'::jsonb
          ), false) AS canonical
          FROM generation_zero
        """
    ).fetchone()
    non_bootstrap_count = int(row[0])
    generation_zero_count = int(row[1])
    canonical = bool(row[2]) and generation_zero_count == 1
    return {
        "ok": canonical,
        "canonical": canonical,
        "generation_zero_count": generation_zero_count,
        "non_bootstrap_count": non_bootstrap_count,
    }
