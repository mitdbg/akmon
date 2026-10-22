"""Publish gate: no stored node of a live type (#3666).

A live type (``okg_live_type``) is never stored: its nodes are read from an
upstream system when they are read. A stored node whose subtype is a live
type, or whose id starts with a live type's ``<prefix>:``, would make
``site:X`` name two different things, so a generation that holds one is
blocked.

This is its own check, with its own check id, beside the transform gate (it
does not change the transform gate's check id, so no deployment loses that
gate's incremental basis):

* No live type in the active catalog: the check records "not applicable:
  no live types", at the cost of one catalog query, and never sweeps.
* A full sweep over every live node when there is no previous generation,
  the catalog changed (a newly declared live type is checked against nodes
  stored before it existed), the publish brings no dirty-node scope, or the
  check has no clean basis of its own on the previous published checkpoint.
* Otherwise it runs scoped over the dirty nodes: a node that did not change,
  under a catalog that did not change, cannot become a violation.
"""

from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any, Mapping

import psycopg

from okg.substrate.consistency.gates import (
    ConsistencyPublishGateEvaluation,
    persist_precomputed_gate_evaluation,
)
from okg.substrate.consistency.observations import (
    CandidateCoverageReport,
    ConsistencyCheckDefinition,
    complete_consistency_run,
    record_candidate_coverage,
    start_consistency_run,
)
from okg.substrate.consistency.sql_gate import _fail_run
from okg.substrate.consistency.transform_gate import (
    _any_predicate,
    _catalog_equivalence,
    _side_table_generation_token,
)
from okg.substrate.derived_artifacts import stable_hash
from okg.substrate.transform import (
    FunctionRegistry,
    Registries,
    Surface,
    SurfaceKind,
    chronos_surface_registry,
    lower_query,
)
from okg.substrate.transform.consistency import (
    catalog_live_types_binding,
    no_stored_live_nodes_program,
)
from okg.substrate.transform.operators import to_chronos_sql

CHECK_ID = "transform.consistency.no_stored_live_nodes.v1"
GATE_NAME = "stored_live_nodes_gate"
#: Node ids a result names; the count is always complete.
NODE_ID_SAMPLE_LIMIT = 200
NOT_APPLICABLE = "not applicable: no live types"


def stored_live_nodes_definition(deployment_name: str) -> ConsistencyCheckDefinition:
    return ConsistencyCheckDefinition(
        check_id=CHECK_ID,
        deployment_name=deployment_name,
        predicate_family="transform_structural_integrity",
        candidate_rule_hash=stable_hash({
            "kind": "stored_live_nodes_candidate_rule_v1",
            "programs": ["no_stored_live_nodes"],
        }),
        evaluator_ref="okg.evaluator.transform.no_stored_live_nodes.v1",
        evaluator_hash=stable_hash({"kind": "stored_live_nodes_evaluator_v1"}),
        confidence_type="deterministic_boolean",
        compute_hash=stable_hash({"kind": "stored_live_nodes_compute_v1"}),
        output_schema_hash=stable_hash({"kind": "stored_live_nodes_output_schema_v1"}),
        renderer_hash=stable_hash({"kind": "stored_live_nodes_renderer_v1"}),
        dirty_rule_hash=stable_hash({"kind": "stored_live_nodes_dirty_rule_v1"}),
        check_kind="rule_based",
    )


@dataclass(frozen=True)
class _Verdict:
    blocks_publish: bool
    reason: str
    offending: list[str]
    coverage_complete: bool
    coverage_basis: str
    details: Mapping[str, Any]
    coverage: CandidateCoverageReport | None = None
    trigger_kind: str = "full"


class StoredLiveNodesGate:
    """The publisher's Phase B gate for stored nodes of a live type."""

    name = GATE_NAME

    def cache_identity(self) -> dict[str, str]:
        """Stable semantics included in the publisher's no-op cache key."""
        return {"check_id": CHECK_ID}

    # -- entry points --------------------------------------------------------

    def evaluate(
        self,
        conn: psycopg.Connection[Any],
        *,
        target_generation_id: int | str,
        catalog_version_id: int,
        deployment_name: str,
        branch_name: str = "default",
    ) -> ConsistencyPublishGateEvaluation:
        token = _side_table_generation_token(conn, target_generation_id)
        live_types = self._live_type_count(conn, catalog_version_id)
        if not live_types:
            verdict = self._not_applicable(catalog_version_id)
        else:
            verdict = self._full(
                conn, target_generation_id, catalog_version_id, branch_name,
                live_types=live_types, fallback=None,
            )
        return self._persist(conn, deployment_name, token, verdict)

    def evaluate_incremental(
        self,
        conn: psycopg.Connection[Any],
        *,
        target_generation_id: int | str,
        catalog_version_id: int,
        deployment_name: str,
        previous_catalog_version_id: int | None,
        delta_counts: Mapping[str, Any] | None,
        dirty_scope: Any,
        branch_name: str = "default",
    ) -> ConsistencyPublishGateEvaluation:
        token = _side_table_generation_token(conn, target_generation_id)
        live_types = self._live_type_count(conn, catalog_version_id)
        if not live_types:
            return self._persist(
                conn, deployment_name, token, self._not_applicable(catalog_version_id),
            )
        fallback = self._fallback_reason(
            conn,
            deployment_name=deployment_name,
            branch_name=branch_name,
            catalog_version_id=catalog_version_id,
            previous_catalog_version_id=previous_catalog_version_id,
            delta_counts=delta_counts,
            dirty_scope=dirty_scope,
        )
        if fallback is not None:
            verdict = self._full(
                conn, target_generation_id, catalog_version_id, branch_name,
                live_types=live_types, fallback=fallback,
            )
        else:
            dirty = sorted({
                str(node_id)
                for node_ids in dirty_scope[0].values()
                for node_id in node_ids
            })
            verdict = self._scoped(
                conn, target_generation_id, catalog_version_id, branch_name,
                live_types=live_types, dirty_node_ids=dirty,
            )
        return self._persist(conn, deployment_name, token, verdict)

    # -- the checks ---------------------------------------------------------

    @staticmethod
    def _live_type_count(conn: psycopg.Connection[Any], catalog_version_id: int) -> int:
        """The one catalog query every publish pays."""

        row = conn.execute(
            f"SELECT count(*) FROM {catalog_live_types_binding(catalog_version_id)} lt"
        ).fetchone()
        return int(row[0]) if row is not None else 0

    @staticmethod
    def _not_applicable(catalog_version_id: int) -> _Verdict:
        return _Verdict(
            blocks_publish=False,
            reason="not_applicable_no_live_types",
            offending=[],
            coverage_complete=True,
            coverage_basis="catalog_live_types",
            details={
                "applicable": False,
                "note": NOT_APPLICABLE,
                "catalog_version_id": catalog_version_id,
                "scoped": False,
                "stored_live_node_ids": [],
                "stored_live_node_count": 0,
            },
        )

    def _full(
        self, conn: psycopg.Connection[Any], gen: int | str, catalog_version_id: int,
        branch_name: str, *, live_types: int, fallback: str | None,
    ) -> _Verdict:
        t0 = time.monotonic()
        offending = self._stored_live_nodes(conn, gen, catalog_version_id, branch_name, None)
        return _Verdict(
            blocks_publish=bool(offending),
            reason="stored_live_nodes" if offending else "no_violations",
            offending=offending,
            coverage_complete=True,
            coverage_basis="chronos_transform_ir",
            details={
                "applicable": True,
                "live_type_count": live_types,
                "scoped": False,
                "scope_mode": "full",
                **({"scope_fallback_reason": fallback} if fallback else {}),
                "stored_live_node_ids": offending[:NODE_ID_SAMPLE_LIMIT],
                "stored_live_node_count": len(offending),
                "elapsed_ms": int((time.monotonic() - t0) * 1000),
            },
        )

    def _scoped(
        self, conn: psycopg.Connection[Any], gen: int | str, catalog_version_id: int,
        branch_name: str, *, live_types: int, dirty_node_ids: list[str],
    ) -> _Verdict:
        t0 = time.monotonic()
        offending = self._stored_live_nodes(
            conn, gen, catalog_version_id, branch_name, dirty_node_ids,
        )
        coverage = CandidateCoverageReport(
            universe_key=f"{CHECK_ID}:{gen}:scoped",
            coverage_basis="exact_postgres",
            coverage_complete=False,
            scope_restricted=True,
            scope_subject_count=len(dirty_node_ids),
            eligible_input_count=len(dirty_node_ids),
            candidate_count=len(offending),
            evaluator_invocation_count=0,
            payload={"runtime": "chronos_scoped_sql", "scope_mode": "dirty_nodes"},
        )
        return _Verdict(
            blocks_publish=bool(offending),
            reason="stored_live_nodes" if offending else "no_violations",
            offending=offending,
            coverage_complete=False,
            coverage_basis="chronos_dirty_nodes",
            coverage=coverage,
            trigger_kind="dirty",
            details={
                "applicable": True,
                "live_type_count": live_types,
                "scoped": True,
                "scope_mode": "dirty_nodes",
                "dirty_node_count": len(dirty_node_ids),
                # The rest of the graph passed this check on the previous
                # published checkpoint, under the same catalog.
                "verdict_universe_valid_by_induction": True,
                "stored_live_node_ids": offending[:NODE_ID_SAMPLE_LIMIT],
                "stored_live_node_count": len(offending),
                "elapsed_ms": int((time.monotonic() - t0) * 1000),
            },
        )

    @staticmethod
    def _stored_live_nodes(
        conn: psycopg.Connection[Any], gen: int | str, catalog_version_id: int,
        branch_name: str, node_ids: list[str] | None,
    ) -> list[str]:
        """``node_ids=None``: `no_stored_live_nodes_program` over every live
        node, lowered over the Chronos surfaces. A list restricts the same
        join to those nodes; an empty list checks nothing."""

        from okg.substrate.graph_store import ChronosGraphStore

        if node_ids is not None and not node_ids:
            return []
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        try:
            session = store.checkout_generation(gen)
        except Exception:  # noqa: BLE001 - a pending publish reads the branch head
            session = store.checkout(branch=branch_name)
        if node_ids is None:
            registry = chronos_surface_registry()
            registry.register(Surface(
                "catalog_live_types",
                SurfaceKind.CATALOG_POLICY,
                catalog_live_types_binding(catalog_version_id),
                pinned=False,
            ))
            sql, params = lower_query(
                no_stored_live_nodes_program(catalog_version_id),
                Registries(surfaces=registry, functions=FunctionRegistry()),
            )
            rows = session.query(to_chronos_sql(sql), params)
        else:
            predicate, params = _any_predicate(
                "n.node_id", node_ids, prefix="eval_live_node_id",
            )
            rows = session.query(
                f"""
                SELECT n.node_id
                  FROM okg.graph_nodes n
                  JOIN {catalog_live_types_binding(catalog_version_id)} lt
                    ON lt.subtype_name = n.subtype
                    OR starts_with(n.node_id, lt.id_prefix)
                 WHERE {predicate}
                """,
                params,
            )
        return sorted({str(r["node_id"]) for r in rows})

    # -- when the scoped path is sound ------------------------------------------

    def _fallback_reason(
        self, conn: psycopg.Connection[Any], *, deployment_name: str, branch_name: str,
        catalog_version_id: int, previous_catalog_version_id: int | None,
        delta_counts: Mapping[str, Any] | None, dirty_scope: Any,
    ) -> str | None:
        if previous_catalog_version_id is None:
            return "no_previous_generation"
        equivalence = _catalog_equivalence(
            conn,
            deployment_name=deployment_name,
            previous_catalog_version_id=previous_catalog_version_id,
            catalog_version_id=catalog_version_id,
        )
        if not equivalence.get("equivalent"):
            return "catalog_version_changed"
        # Without the change counts the dirty scope cannot be checked against
        # what the publish wrote: a full sweep, never "no change".
        if delta_counts is None:
            return "delta_counts_unavailable"
        if dirty_scope is None or not isinstance(dirty_scope[0], Mapping):
            return "dirty_scope_unavailable"
        changed = sum(
            int(delta_counts.get(key, 0) or 0)
            for key in ("nodes_added", "nodes_updated")
        )
        dirty = sum(len(ids) for ids in dirty_scope[0].values())
        if changed and not dirty:
            return "dirty_node_scope_mismatch"
        basis = self._basis_reason(
            conn, deployment_name=deployment_name, branch_name=branch_name,
        )
        return basis

    def _basis_reason(
        self, conn: psycopg.Connection[Any], *, deployment_name: str, branch_name: str,
    ) -> str | None:
        """None when the previous published checkpoint carries a clean,
        applicable result of THIS check and no other checkpoint sits after
        it; otherwise why the scoped path is not sound."""

        from okg.substrate.graph_store import ChronosGraphStore

        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        previous = store.latest_generation(
            deployment_name=deployment_name, branch=branch_name, status="published",
        )
        inherited = False
        if previous is None and branch_name != "default":
            previous = store.resolve_fork_base_checkpoint(
                branch_name, expected_deployment=deployment_name,
            )
            inherited = previous is not None
        if previous is None:
            return "no_previous_published_checkpoint"
        latest = store.latest_generation(
            deployment_name=deployment_name, branch=branch_name, status=None,
        )
        latest_id = None if latest is None else str(getattr(latest, "checkpoint_id", ""))
        expected = None if inherited else str(getattr(previous, "checkpoint_id", ""))
        if latest_id != expected:
            # A blocked checkpoint after the basis sealed writes no later
            # dirty scope reaches.
            return "unpublished_checkpoint_after_published_basis"
        metadata = dict(getattr(previous, "metadata", None) or {})
        gates = dict(dict(metadata.get("invariant_summary") or {}).get("consistency_gates") or {})
        for raw in gates.get("results") or []:
            result = dict(raw or {})
            if result.get("check_id") != CHECK_ID or result.get("gate_name") != GATE_NAME:
                continue
            details = dict(result.get("details") or {})
            clean = (
                result.get("result") == "evaluated"
                and result.get("blocks_publish") is False
                and details.get("applicable") is True
            )
            return None if clean else "previous_result_not_clean_or_not_applicable"
        return "no_clean_basis_of_its_own"

    # -- persistence -----------------------------------------------------------

    def _persist(
        self, conn: psycopg.Connection[Any], deployment_name: str, gen: int,
        verdict: _Verdict,
    ) -> ConsistencyPublishGateEvaluation:
        definition = stored_live_nodes_definition(deployment_name)
        run_id = start_consistency_run(
            conn, definition, target_generation_id=gen,
            trigger_kind=verdict.trigger_kind,
            metrics={
                "gate_name": self.name,
                "gate_type": "transform_ir",
                "execution_phase": "phase_b",
                "blocks_publish": verdict.blocks_publish,
                "trigger_kind": verdict.trigger_kind,
            },
        )
        try:
            if verdict.coverage is not None:
                record_candidate_coverage(
                    conn, definition, target_generation_id=gen, run_id=run_id,
                    report=verdict.coverage,
                )
            evaluation = ConsistencyPublishGateEvaluation(
                run_id=run_id,
                check_id=CHECK_ID,
                target_generation_id=gen,
                gate_name=self.name,
                result="evaluated",
                blocks_publish=verdict.blocks_publish,
                reason=verdict.reason,
                open_observation_count=len(verdict.offending),
                coverage_complete=verdict.coverage_complete,
                coverage_basis=verdict.coverage_basis,
                budget_ms=None,
                elapsed_ms=int(verdict.details.get("elapsed_ms") or 0),
                preflight={},
                details=dict(verdict.details),
            )
            persist_precomputed_gate_evaluation(conn, evaluation)
            complete_consistency_run(
                conn, run_id=run_id,
                metrics={
                    "gate_name": self.name,
                    "gate_result": "evaluated",
                    "blocks_publish": verdict.blocks_publish,
                    "offending_row_count": len(verdict.offending),
                    "trigger_kind": verdict.trigger_kind,
                },
            )
            return evaluation
        except Exception as exc:
            _fail_run(conn, run_id, exc)
            raise


__all__ = ["CHECK_ID", "StoredLiveNodesGate", "stored_live_nodes_definition"]
