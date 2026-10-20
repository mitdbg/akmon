"""Deployment-loaded deterministic SQL consistency gates.

This is the config-to-runtime bridge for cheap corpus-wide checks.
A deployment supplies two read-only SQL row sources:

* ``candidate_sql`` returns deterministic candidate rows and optional
  observation events.
* ``coverage_sql`` returns one auditable coverage row for the run.

The gate maps those rows onto the durable consistency primitives and
then asks the publish-gate algebra for the admission decision.
"""
from __future__ import annotations

import os
import re
import logging
import time
import warnings
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterator, Literal, Mapping, Sequence, cast
from uuid import UUID

import psycopg
from okg.substrate import fastyaml
from psycopg import sql as pg_sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from okg.substrate.catalog.invariants import sql_contains_writes
from okg.substrate.consistency.gates import (
    BACKPRESSURE_PROBE_EVERY_ENV,
    DEFAULT_BACKPRESSURE_PROBE_EVERY,
    ConsistencyGateHealth,
    ConsistencyPublishGateEvaluation,
    consistency_gate_summary,
    evaluate_consistency_publish_gate,
    summarize_consistency_gate_health,
)
from okg.substrate.consistency.observations import (
    CHECK_KINDS,
    CandidateCoverageReport,
    ConfidenceType,
    ConsistencyCandidate,
    ConsistencyCandidateObservation,
    ConsistencyCheckDefinition,
    ObservationEvent,
    _dedupe_observation_current_for_staged_events,
    candidate_id_for,
    observation_id_for,
    record_candidate_observations_bulk,
    record_candidate_coverage,
    SCOPE_DIGEST_ALGORITHM,
    SCOPE_ID_LIMIT,
    scope_complement_recipe,
    scope_digest_sql,
    start_consistency_run,
    summarize_consistency_run_multiplicity,
)
from okg.substrate.derived_artifacts.read_views import read_view_slot
from okg.substrate.derived_artifacts.relations import stable_hash
from okg.substrate.errors import SubstrateError


DEFAULT_SQL_GATE_TIMEOUT_MS = 5000
DEFAULT_SQL_GATE_MATERIALIZATION_CHUNK_SIZE = 5000
SQL_GATE_EVALUATOR_REF = "okg.evaluator.sql.deterministic.v1"
BackpressureAction = Literal["block_without_evaluator", "run"]
MaterializationStrategy = Literal["chunked_python", "sql_native"]
_RELATION_ALIAS_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RELATION_PLACEHOLDER_RE = re.compile(
    r"\{\{relation:([A-Za-z_][A-Za-z0-9_]*)\}\}"
)
log = logging.getLogger(__name__)


class ConsistencyGateLoadError(SubstrateError):
    code = "consistency_gate_load_error"


@dataclass(frozen=True)
class SqlGateRelationInput:
    """A completed exact-generation view relation slot for SQL queries."""

    view_id: str
    slot_name: str
    definition_id: str | None = None

    def to_hash_payload(self) -> dict[str, Any]:
        return {
            "view_id": self.view_id,
            "slot_name": self.slot_name,
            "definition_id": self.definition_id,
        }


@dataclass(frozen=True)
class SqlGateScopeFrontier:
    """A latest source-watermark subject frontier for SQL queries."""

    source: str
    cursor_subject_ids_field: str = "scoped_subject_node_ids"
    require_scoped_cursor: bool = True

    def to_hash_payload(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "cursor_subject_ids_field": self.cursor_subject_ids_field,
            "require_scoped_cursor": self.require_scoped_cursor,
        }


@dataclass(frozen=True)
class _SqlGateRelationBinding:
    alias: str
    view_id: str
    slot_name: str
    view_definition_id: str
    view_manifest_id: UUID
    relation_definition_id: str
    relation_manifest_id: UUID | None
    relation_target_generation_id: int
    schema_name: str
    relation_name: str
    qualified_identifier: str

    def dependency_manifest_ids(self) -> list[UUID]:
        ids = [self.view_manifest_id]
        if self.relation_manifest_id is not None:
            ids.append(self.relation_manifest_id)
        return ids

    def params(self) -> dict[str, Any]:
        return {
            f"{self.alias}_definition_id": self.relation_definition_id,
            f"{self.alias}_target_generation_id": (
                self.relation_target_generation_id
            ),
        }

    def to_preflight(self) -> dict[str, Any]:
        return {
            "view_id": self.view_id,
            "slot_name": self.slot_name,
            "view_definition_id": self.view_definition_id,
            "view_manifest_id": str(self.view_manifest_id),
            "relation_definition_id": self.relation_definition_id,
            "relation_manifest_id": (
                str(self.relation_manifest_id)
                if self.relation_manifest_id is not None
                else None
            ),
            "relation_target_generation_id": (
                self.relation_target_generation_id
            ),
            "physical_relation": (
                f"{self.schema_name}.{self.relation_name}"
            ),
        }


@dataclass(frozen=True)
class SqlConsistencyGateSpec:
    """One deterministic SQL consistency gate from deployment config."""

    name: str
    check_id: str
    predicate_family: str
    confidence_type: ConfidenceType
    candidate_sql: str
    coverage_sql: str
    evaluator_ref: str = SQL_GATE_EVALUATOR_REF
    budget_ms: int = DEFAULT_SQL_GATE_TIMEOUT_MS
    timeout_ms: int = DEFAULT_SQL_GATE_TIMEOUT_MS
    materialization_chunk_size: int = (
        DEFAULT_SQL_GATE_MATERIALIZATION_CHUNK_SIZE
    )
    materialization_strategy: MaterializationStrategy = "chunked_python"
    relation_inputs: Mapping[str, SqlGateRelationInput] = field(
        default_factory=dict
    )
    scope_frontier: SqlGateScopeFrontier | None = None
    health_lookback: int = 20
    max_consecutive_blocked: int = 2
    backpressure_action: BackpressureAction = "block_without_evaluator"
    # Probing-breaker cadence (consistency-gate-durability task.backpressure.probe):
    # short-circuits between recovery probes. None => resolve at runtime from the
    # OKG_CONSISTENCY_BACKPRESSURE_PROBE_EVERY env, else the module default. A
    # config-declared ``backpressure.probe_every`` sets this and wins over the env.
    backpressure_probe_every: int | None = None
    candidate_rule_hash: str | None = None
    evaluator_hash: str | None = None
    compute_hash: str | None = None
    output_schema_hash: str | None = None
    renderer_hash: str | None = None
    dirty_rule_hash: str | None = None
    coverage_contract: Mapping[str, Any] = field(default_factory=dict)
    severity_policy: Mapping[str, Any] = field(default_factory=dict)
    # AKMON Phase 7 integrity taxonomy (consistency-gate-durability
    # task.checkkind.loader). Threaded into consistency_check_definitions so
    # doctor's taxonomy audit sees a real kind, not a NULL legacy row. None is
    # permitted (a config that declares no kind stays legacy, auditably).
    check_kind: str | None = None
    # Declarative blocking policy over findings (consistency-blocking-policy).
    # ``any_open`` (default) preserves the historical "every non-pass result
    # of a blocking gate blocks" contract; ``new_findings`` blocks only when
    # the run opens an observation not open at the previous published
    # generation (optionally severity-scoped); ``never`` blocks on operational
    # failure only. Not part of the check identity hash (see definition()).
    block_on: str = "any_open"
    block_on_min_severity: str | None = None

    def definition(self, deployment_name: str) -> ConsistencyCheckDefinition:
        candidate_rule_hash = self.candidate_rule_hash or stable_hash(
            {
                "kind": "sql_consistency_candidate_rule_v1",
                "check_id": self.check_id,
                "candidate_sql": self.candidate_sql,
                "relation_inputs": _relation_inputs_hash_payload(
                    self.relation_inputs
                ),
                "scope_frontier": _scope_frontier_hash_payload(
                    self.scope_frontier
                ),
            }
        )
        evaluator_hash = self.evaluator_hash or stable_hash(
            {
                "kind": "sql_consistency_evaluator_v1",
                "check_id": self.check_id,
                "evaluator_ref": self.evaluator_ref,
                "candidate_sql": self.candidate_sql,
                "relation_inputs": _relation_inputs_hash_payload(
                    self.relation_inputs
                ),
                "scope_frontier": _scope_frontier_hash_payload(
                    self.scope_frontier
                ),
            }
        )
        compute_hash = self.compute_hash or stable_hash(
            {
                "kind": "sql_consistency_compute_v1",
                "check_id": self.check_id,
                "candidate_sql": self.candidate_sql,
                "coverage_sql": self.coverage_sql,
                "relation_inputs": _relation_inputs_hash_payload(
                    self.relation_inputs
                ),
                "scope_frontier": _scope_frontier_hash_payload(
                    self.scope_frontier
                ),
                "candidate_rule_hash": candidate_rule_hash,
                "evaluator_hash": evaluator_hash,
            }
        )
        output_schema_hash = self.output_schema_hash or stable_hash(
            {
                "kind": "sql_consistency_output_schema_v1",
                "outputs": [
                    "consistency_candidates",
                    "observation_events",
                    "observation_current",
                    "consistency_candidate_coverage",
                ],
                "confidence_type": self.confidence_type,
            }
        )
        renderer_hash = self.renderer_hash or stable_hash(
            {
                "kind": "sql_consistency_renderer_v1",
                "renderer": "okg.renderer.observation.v1",
            }
        )
        dirty_rule_hash = self.dirty_rule_hash or stable_hash(
            {
                "kind": "sql_consistency_dirty_rule_v1",
                "coverage_contract": dict(self.coverage_contract),
                "relation_inputs": _relation_inputs_hash_payload(
                    self.relation_inputs
                ),
                "scope_frontier": _scope_frontier_hash_payload(
                    self.scope_frontier
                ),
            }
        )
        return ConsistencyCheckDefinition(
            check_id=self.check_id,
            deployment_name=deployment_name,
            predicate_family=self.predicate_family,
            candidate_rule_hash=candidate_rule_hash,
            evaluator_ref=self.evaluator_ref,
            evaluator_hash=evaluator_hash,
            confidence_type=self.confidence_type,
            compute_hash=compute_hash,
            output_schema_hash=output_schema_hash,
            renderer_hash=renderer_hash,
            dirty_rule_hash=dirty_rule_hash,
            coverage_contract=dict(self.coverage_contract),
            severity_policy=dict(self.severity_policy),
            check_kind=self.check_kind,
        )


class DeterministicSqlConsistencyGate:
    """Run a deterministic SQL check and persist gate-algebra results."""

    def __init__(
        self,
        spec: SqlConsistencyGateSpec,
        *,
        blocks_publish: bool = True,
        execution_phase: str = "publish_gate",
        trigger_kind: str = "dirty",
    ) -> None:
        self.spec = spec
        self.name = spec.name
        self.blocks_publish = bool(blocks_publish)
        self.execution_phase = str(execution_phase)
        self.trigger_kind = str(trigger_kind)

    def record_refusal(
        self,
        conn: psycopg.Connection[Any],
        exc: Exception,
        *,
        target_generation_id: int,
        catalog_version_id: int,
        deployment_name: str,
    ) -> ConsistencyPublishGateEvaluation:
        """Record that this check was refused before it could evaluate.

        Writes a failed ``derived_artifact_runs`` row and a ``blocked_error``
        gate row -- the durable record the check's own errors leave -- with
        the refusal's code and data (for a mirror refusal, its ``reason``).
        """
        definition = self.spec.definition(deployment_name)
        run_id = start_consistency_run(
            conn,
            definition,
            target_generation_id=target_generation_id,
            trigger_kind=self.trigger_kind,
            metrics={
                "gate_name": self.name,
                "gate_type": "deterministic_sql",
                "execution_phase": self.execution_phase,
                "blocks_publish": self.blocks_publish,
                "refused_before_evaluation": True,
            },
        )
        _fail_run(conn, run_id, exc)
        error: dict[str, Any] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
        code = getattr(exc, "code", None)
        if isinstance(code, str):
            error["code"] = code
        data = getattr(exc, "data", None)
        if isinstance(data, Mapping) and data:
            error["data"] = dict(data)
        return evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=target_generation_id,
            run_id=run_id,
            gate_name=self.name,
            budget_ms=self.spec.budget_ms,
            elapsed_ms=0,
            preflight={
                "catalog_version_id": catalog_version_id,
                "execution_phase": self.execution_phase,
                "blocks_publish": self.blocks_publish,
                "refused_before_evaluation": True,
            },
            error=error,
            blocks_publish=self._blocks_publish_override(),
            block_on=self.spec.block_on,
            block_on_min_severity=self.spec.block_on_min_severity,
        )

    def evaluate(
        self,
        conn: psycopg.Connection[Any],
        *,
        target_generation_id: int,
        catalog_version_id: int,
        deployment_name: str,
        scope_subject_ids: Sequence[str] | None = None,
        branch_name: str = "default",
    ) -> ConsistencyPublishGateEvaluation:
        # branch_name accepted for the uniform ConsistencyPublishGate contract; this
        # gate evaluates via its SQL definition/evaluator, not a Chronos branch
        # checkout, so it is branch-agnostic today (no graph_nodes/graph_edges read
        # to scope). If a future SQL check reads the branch graph, thread it here.
        definition = self.spec.definition(deployment_name)
        t0 = time.monotonic()
        phase_timings_ms: dict[str, int] = {}

        def mark_phase(name: str, started_at: float) -> None:
            phase_timings_ms[name] = int((time.monotonic() - started_at) * 1000)

        phase_t0 = time.monotonic()
        health = self._health(conn, definition)
        mark_phase("backpressure_health_ms", phase_t0)
        # Probing breaker (task.backpressure.probe): a degraded gate normally
        # short-circuits, but every ``probe_every`` short-circuits it RE-RUNS its
        # evaluator once (a probe) instead. A passing probe writes an ``evaluated``
        # row that breaks the consecutive-operational-blocked streak, clearing
        # degradation with NO config change; a failing probe re-enters
        # short-circuit for the next interval. ``is_probe`` rides into the
        # persisted row so this recovery attempt is auditable and never latches.
        is_probe = False
        if (
            self.spec.backpressure_action == "block_without_evaluator"
            and health.degraded
        ):
            if health.should_probe:
                is_probe = True
            else:
                return self._block_for_backpressure(
                    conn,
                    definition,
                    target_generation_id=target_generation_id,
                    catalog_version_id=catalog_version_id,
                    health=health,
                    started_at=t0,
                )

        phase_t0 = time.monotonic()
        run_id = start_consistency_run(
            conn,
            definition,
            target_generation_id=target_generation_id,
            trigger_kind=self.trigger_kind,
            metrics={
                "gate_name": self.name,
                "gate_type": "deterministic_sql",
                "execution_phase": self.execution_phase,
                "blocks_publish": self.blocks_publish,
                "materialization_strategy": (
                    self.spec.materialization_strategy
                ),
                "relation_inputs": {
                    alias: item.to_hash_payload()
                    for alias, item in sorted(
                        self.spec.relation_inputs.items()
                    )
                },
                "scope_frontier": (
                    self.spec.scope_frontier.to_hash_payload()
                    if self.spec.scope_frontier is not None else None
                ),
            },
        )
        mark_phase("start_run_ms", phase_t0)
        candidate_count = 0
        observation_count = 0
        relation_input_preflight: dict[str, Any] = {}
        scope_frontier_preflight: dict[str, Any] = {
            "configured": self.spec.scope_frontier is not None,
            "scope_active": False,
            "reason": (
                "pending"
                if self.spec.scope_frontier is not None else "not_configured"
            ),
        }
        try:
            params = {
                "target_generation_id": target_generation_id,
                "catalog_version_id": catalog_version_id,
                "deployment_name": deployment_name,
            }
            phase_t0 = time.monotonic()
            bindings = self._resolve_relation_inputs(
                conn,
                deployment_name=deployment_name,
                target_generation_id=target_generation_id,
            )
            relation_input_preflight = _relation_bindings_to_preflight(
                bindings
            )
            if bindings:
                _set_run_dependency_manifest_ids(
                    conn,
                    run_id,
                    _relation_dependency_manifest_ids(bindings),
                )
                for binding in bindings.values():
                    params.update(binding.params())
            mark_phase("resolve_relation_inputs_ms", phase_t0)
            phase_t0 = time.monotonic()
            candidate_sql, coverage_sql = _render_bound_sql(
                candidate_sql=self.spec.candidate_sql,
                coverage_sql=self.spec.coverage_sql,
                bindings=bindings,
            )
            mark_phase("render_bound_sql_ms", phase_t0)
            phase_t0 = time.monotonic()
            scope_frontier_preflight = self._stage_scope_frontier(
                conn,
                scope_subject_ids=scope_subject_ids,
            )
            mark_phase("stage_scope_frontier_ms", phase_t0)
            phase_t0 = time.monotonic()
            candidate_count = self._stage_candidate_rows(
                conn,
                candidate_sql,
                params,
            )
            mark_phase("stage_candidate_rows_ms", phase_t0)
            coverage_candidate_count = 0
            candidate_count_by_block: dict[str, int] = {}
            phase_t0 = time.monotonic()
            if self.spec.materialization_strategy == "sql_native":
                materialized = self._materialize_candidate_rows_sql_native(
                    conn,
                    definition,
                    target_generation_id=target_generation_id,
                    run_id=run_id,
                )
                coverage_candidate_count = int(
                    materialized["coverage_candidate_count"]
                )
                candidate_count_by_block = {
                    str(key): int(value)
                    for key, value in cast(
                        Mapping[str, Any],
                        materialized["candidate_count_by_block"],
                    ).items()
                }
                observation_count = int(materialized["observation_count"])
                mark_phase("materialize_sql_native_ms", phase_t0)
            else:
                for candidate_rows in self._fetch_candidate_row_batches(conn):
                    prepared_records: list[ConsistencyCandidateObservation] = []
                    for row in candidate_rows:
                        if str(row.get("coverage_status") or "candidate") == (
                            "candidate"
                        ):
                            coverage_candidate_count += 1
                            block = str(
                                row.get("blocking_rule_hash") or "unknown"
                            )
                            candidate_count_by_block[block] = (
                                candidate_count_by_block.get(block, 0) + 1
                            )
                        candidate = _candidate_from_row(row)
                        candidate_id = candidate_id_for(definition, candidate)
                        event = None
                        if _row_emits_observation(row):
                            event = _event_from_row(
                                definition,
                                candidate,
                                candidate_id,
                                row,
                            )
                        prepared_records.append(
                            ConsistencyCandidateObservation(
                                candidate=candidate,
                                candidate_id=candidate_id,
                                event=event,
                            )
                        )
                    observation_count += record_candidate_observations_bulk(
                        conn,
                        definition,
                        target_generation_id=target_generation_id,
                        run_id=run_id,
                        records=prepared_records,
                    )
                mark_phase("materialize_chunked_python_ms", phase_t0)

            phase_t0 = time.monotonic()
            coverage_rows = self._fetch_rows(conn, coverage_sql, params)
            mark_phase("coverage_sql_ms", phase_t0)
            if len(coverage_rows) != 1:
                raise ValueError(
                    "coverage_sql must return exactly one row; got "
                    f"{len(coverage_rows)}"
                )
            phase_t0 = time.monotonic()
            record_candidate_coverage(
                conn,
                definition,
                target_generation_id=target_generation_id,
                run_id=run_id,
                report=_coverage_from_row(
                    coverage_rows[0],
                    candidate_count=coverage_candidate_count,
                    candidate_count_by_block=candidate_count_by_block,
                ),
            )
            mark_phase("record_coverage_ms", phase_t0)
            phase_t0 = time.monotonic()
            self._drop_candidate_stage(conn)
            mark_phase("drop_candidate_stage_ms", phase_t0)
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            phase_timings_ms["total_elapsed_ms"] = elapsed_ms
            phase_t0 = time.monotonic()
            evaluation = evaluate_consistency_publish_gate(
                conn,
                definition,
                target_generation_id=target_generation_id,
                run_id=run_id,
                gate_name=self.name,
                budget_ms=self.spec.budget_ms,
                elapsed_ms=elapsed_ms,
                preflight={
                    "catalog_version_id": catalog_version_id,
                    "candidate_row_count": candidate_count,
                    "observation_event_count": observation_count,
                    "materialization_strategy": (
                        self.spec.materialization_strategy
                    ),
                    "materialization_chunk_size": (
                        self.spec.materialization_chunk_size
                    ),
                    "timeout_ms": self.spec.timeout_ms,
                    "execution_phase": self.execution_phase,
                    "blocks_publish": self.blocks_publish,
                    "relation_inputs": relation_input_preflight,
                    "scope_frontier": scope_frontier_preflight,
                    "backpressure_policy": self._backpressure_policy_dict(),
                    "backpressure_health": _health_to_dict(health),
                    "is_probe": is_probe,
                    "phase_timings_ms": dict(phase_timings_ms),
                },
                blocks_publish=self._blocks_publish_override(),
                block_on=self.spec.block_on,
                block_on_min_severity=self.spec.block_on_min_severity,
                is_probe=is_probe,
            )
            mark_phase("evaluate_publish_gate_ms", phase_t0)
            phase_timings_ms["total_elapsed_ms"] = int(
                (time.monotonic() - t0) * 1000
            )
            phase_t0 = time.monotonic()
            _complete_run(
                conn,
                run_id,
                {
                    "gate_name": self.name,
                    "gate_type": "deterministic_sql",
                    "execution_phase": self.execution_phase,
                    "blocks_publish": self.blocks_publish,
                    "candidate_row_count": candidate_count,
                    "observation_event_count": observation_count,
                    "materialization_strategy": (
                        self.spec.materialization_strategy
                    ),
                    "materialization_chunk_size": (
                        self.spec.materialization_chunk_size
                    ),
                    "relation_inputs": relation_input_preflight,
                    "scope_frontier": scope_frontier_preflight,
                    "gate_result": evaluation.result,
                    "phase_timings_ms": dict(phase_timings_ms),
                },
            )
            mark_phase("complete_run_ms", phase_t0)
            phase_timings_ms["total_elapsed_ms"] = int(
                (time.monotonic() - t0) * 1000
            )
            _update_run_phase_timings(conn, run_id, phase_timings_ms)
            return evaluation
        except Exception as exc:
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            phase_timings_ms["total_elapsed_ms"] = elapsed_ms
            try:
                phase_t0 = time.monotonic()
                self._drop_candidate_stage(conn)
                mark_phase("drop_candidate_stage_after_error_ms", phase_t0)
            except Exception as drop_exc:
                # Best-effort cleanup — behavior unchanged. The run already failed
                # on ``exc`` above and is recorded by ``_fail_run`` below. Name the
                # DROP failure's class at WARNING (was DEBUG) so a leaked candidate
                # stage is diagnosable via caplog at a normal level, not buried.
                log.warning(
                    "failed to drop SQL gate candidate stage after gate error "
                    "(%s: %s); candidate stage may leak",
                    type(drop_exc).__name__,
                    drop_exc,
                    exc_info=True,
                )
            _fail_run(conn, run_id, exc)
            return evaluate_consistency_publish_gate(
                conn,
                definition,
                target_generation_id=target_generation_id,
                run_id=run_id,
                gate_name=self.name,
                budget_ms=self.spec.budget_ms,
                elapsed_ms=elapsed_ms,
                preflight={
                    "catalog_version_id": catalog_version_id,
                    "candidate_row_count": candidate_count,
                    "observation_event_count": observation_count,
                    "materialization_strategy": (
                        self.spec.materialization_strategy
                    ),
                    "materialization_chunk_size": (
                        self.spec.materialization_chunk_size
                    ),
                    "timeout_ms": self.spec.timeout_ms,
                    "execution_phase": self.execution_phase,
                    "blocks_publish": self.blocks_publish,
                    "relation_inputs": relation_input_preflight,
                    "scope_frontier": scope_frontier_preflight,
                    "backpressure_policy": self._backpressure_policy_dict(),
                    "backpressure_health": _health_to_dict(health),
                    "is_probe": is_probe,
                    "phase_timings_ms": dict(phase_timings_ms),
                },
                timed_out=_is_query_timeout(exc),
                error={
                    "type": type(exc).__name__,
                    "message": str(exc),
                },
                blocks_publish=self._blocks_publish_override(),
                block_on=self.spec.block_on,
                block_on_min_severity=self.spec.block_on_min_severity,
                is_probe=is_probe,
            )

    def _resolve_relation_inputs(
        self,
        conn: psycopg.Connection[Any],
        *,
        deployment_name: str,
        target_generation_id: int,
    ) -> dict[str, _SqlGateRelationBinding]:
        # Internal gate evaluation binds relation storage at the exact target
        # generation; it never returns a view payload to a human/agent caller.
        bindings: dict[str, _SqlGateRelationBinding] = {}
        for alias, relation_input in self.spec.relation_inputs.items():
            read = read_view_slot(
                conn,
                deployment_name=deployment_name,
                view_id=relation_input.view_id,
                slot_name=relation_input.slot_name,
                generation_id=target_generation_id,
                definition_id=relation_input.definition_id,
            )
            if read.slot.storage_kind != "relation":
                raise ValueError(
                    f"relation input {alias!r} resolved to "
                    f"storage_kind={read.slot.storage_kind!r}"
                )
            storage_ref = dict(read.slot.payload_value() or {})
            schema_name = _required_storage_str(
                storage_ref,
                "schema",
                alias=alias,
            )
            relation_name = _required_storage_str(
                storage_ref,
                "relation",
                alias=alias,
            )
            relation_definition_id = _required_storage_str(
                storage_ref,
                "upstream_definition_id",
                alias=alias,
            )
            relation_generation = _relation_target_generation_id(
                storage_ref,
                alias=alias,
            )
            relation_manifest_id = _optional_uuid(
                storage_ref.get("upstream_manifest_id"),
                alias=alias,
                field="upstream_manifest_id",
            )
            bindings[alias] = _SqlGateRelationBinding(
                alias=alias,
                view_id=relation_input.view_id,
                slot_name=relation_input.slot_name,
                view_definition_id=read.artifact.definition_id,
                view_manifest_id=read.artifact.manifest_id,
                relation_definition_id=relation_definition_id,
                relation_manifest_id=relation_manifest_id,
                relation_target_generation_id=relation_generation,
                schema_name=schema_name,
                relation_name=relation_name,
                qualified_identifier=_quote_relation_identifier(
                    conn,
                    schema_name=schema_name,
                    relation_name=relation_name,
                ),
            )
        return bindings

    def _health(
        self,
        conn: psycopg.Connection[Any],
        definition: ConsistencyCheckDefinition,
    ) -> ConsistencyGateHealth:
        return summarize_consistency_gate_health(
            conn,
            check_id=definition.check_id,
            gate_name=self.name,
            lookback=self.spec.health_lookback,
            max_consecutive_blocked=self.spec.max_consecutive_blocked,
            probe_every=self._probe_every(),
        )

    def _probe_every(self) -> int:
        """Resolve the backpressure probe cadence (task.backpressure.probe).

        Precedence: a config-declared ``backpressure.probe_every`` wins, else the
        ``OKG_CONSISTENCY_BACKPRESSURE_PROBE_EVERY`` env (ops override without
        re-authoring config), else :data:`DEFAULT_BACKPRESSURE_PROBE_EVERY`. A
        non-positive value from either source is rejected in favor of the default
        so probing can never be silently disabled by a typo'd override (a real
        opt-out is ``backpressure.action: run``, not a zero cadence).
        """
        # The >0 guard applies to the config branch too (adversarial review F2):
        # the config-FILE loader already rejects <=0, but a spec constructed
        # directly in code could carry a non-positive value; honor the docstring
        # ("rejected in favor of the default so probing can never be silently
        # disabled") rather than returning it.
        if (
            self.spec.backpressure_probe_every is not None
            and self.spec.backpressure_probe_every > 0
        ):
            return self.spec.backpressure_probe_every
        raw = os.environ.get(BACKPRESSURE_PROBE_EVERY_ENV)
        if raw is not None:
            try:
                parsed = int(raw)
            except (TypeError, ValueError):
                parsed = 0
            if parsed > 0:
                return parsed
        return DEFAULT_BACKPRESSURE_PROBE_EVERY

    def _block_for_backpressure(
        self,
        conn: psycopg.Connection[Any],
        definition: ConsistencyCheckDefinition,
        *,
        target_generation_id: int,
        catalog_version_id: int,
        health: ConsistencyGateHealth,
        started_at: float,
    ) -> ConsistencyPublishGateEvaluation:
        run_id = start_consistency_run(
            conn,
            definition,
            target_generation_id=target_generation_id,
            trigger_kind=self.trigger_kind,
            metrics={
                "gate_name": self.name,
                "gate_type": "deterministic_sql",
                "execution_phase": self.execution_phase,
                "blocks_publish": self.blocks_publish,
                "backpressure": "short_circuit",
            },
        )
        elapsed_ms = int((time.monotonic() - started_at) * 1000)
        backpressure = {
            "policy": self._backpressure_policy_dict(),
            "health": _health_to_dict(health),
            "action": self.spec.backpressure_action,
            "remediation_actions": [
                "inspect okg generation describe for the latest blocked generation",
                "repair evaluator coverage/timeouts or resolve upstream data issues",
                "publish an audited catalog/config change to adjust or disable the gate",
            ],
        }
        evaluation = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=target_generation_id,
            run_id=run_id,
            gate_name=self.name,
            budget_ms=self.spec.budget_ms,
            elapsed_ms=elapsed_ms,
            preflight={
                "catalog_version_id": catalog_version_id,
                "candidate_row_count": 0,
                "observation_event_count": 0,
                "timeout_ms": self.spec.timeout_ms,
                "execution_phase": self.execution_phase,
                "blocks_publish": self.blocks_publish,
                "backpressure_policy": self._backpressure_policy_dict(),
                "backpressure_health": _health_to_dict(health),
            },
            backpressure=backpressure,
            blocks_publish=self._blocks_publish_override(),
            block_on=self.spec.block_on,
            block_on_min_severity=self.spec.block_on_min_severity,
        )
        _block_run(
            conn,
            run_id,
            {
                "gate_name": self.name,
                "gate_type": "deterministic_sql",
                "gate_result": evaluation.result,
                "backpressure": backpressure,
            },
        )
        return evaluation

    def _backpressure_policy_dict(self) -> dict[str, Any]:
        return {
            "lookback": self.spec.health_lookback,
            "max_consecutive_blocked": self.spec.max_consecutive_blocked,
            "action": self.spec.backpressure_action,
            "probe_every": self._probe_every(),
        }

    def _blocks_publish_override(self) -> bool | None:
        return None if self.blocks_publish else False

    def _fetch_rows(
        self,
        conn: psycopg.Connection[Any],
        sql: str,
        params: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SET TRANSACTION READ ONLY")
                cur.execute(
                    f"SET LOCAL statement_timeout = {int(self.spec.timeout_ms)}"
                )
                cur.execute(sql, dict(params))
                return [dict(row) for row in cur.fetchall()]

    def _stage_scope_frontier(
        self,
        conn: psycopg.Connection[Any],
        *,
        scope_subject_ids: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        frontier = self.spec.scope_frontier
        configured = frontier is not None
        # The temp tables are staged UNCONDITIONALLY: check SQL that references
        # pg_temp.okg_sql_gate_scope_context must see a real scope_active=false
        # row when no frontier is configured (the cern-twiki checks reference
        # the scope tables without declaring a frontier; an absent table errors
        # out the run, and an EMPTY scope_context NULLs both CTE branches into
        # a silent zero-row check — the P0 silent-failure shape).
        scope_active = False
        subject_count = 0
        subject_ids: list[str] = []
        subject_digest: str | None = None
        reason = "watermark_missing" if configured else "not_configured"
        cursor_schema: str | None = None
        cursor_content_hash: str | None = None
        cursor_scoped = False
        override_subject_ids = tuple(
            dict.fromkeys(
                subject.strip()
                for subject in (scope_subject_ids or ())
                if subject.strip()
            )
        )
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    "DROP TABLE IF EXISTS pg_temp.okg_sql_gate_subject_scope"
                )
                cur.execute(
                    "DROP TABLE IF EXISTS pg_temp.okg_sql_gate_scope_context"
                )
                cur.execute(
                    "DROP TABLE IF EXISTS pg_temp.okg_sql_gate_scoped_nodes"
                )
                cur.execute(
                    "DROP TABLE IF EXISTS pg_temp.okg_sql_gate_scoped_out_edges"
                )
                cur.execute(
                    "DROP TABLE IF EXISTS pg_temp.okg_sql_gate_scoped_in_edges"
                )
                cur.execute(
                    f"SET LOCAL statement_timeout = {int(self.spec.timeout_ms)}"
                )
                cur.execute(
                    """
                    CREATE TEMP TABLE okg_sql_gate_subject_scope (
                      subject_id text PRIMARY KEY
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TEMP TABLE okg_sql_gate_scope_context (
                      scope_active boolean NOT NULL,
                      source text,
                      cursor_subject_ids_field text,
                      subject_count bigint NOT NULL,
                      reason text NOT NULL,
                      cursor_schema text,
                      cursor_content_hash text,
                      cursor_scoped boolean,
                      graph_frontier_available boolean NOT NULL,
                      scoped_node_count bigint NOT NULL,
                      scoped_out_edge_count bigint NOT NULL,
                      scoped_in_edge_count bigint NOT NULL
                    )
                    """
                )
                if not configured:
                    pass  # tables staged empty; scope_active stays False
                elif override_subject_ids:
                    scope_active = True
                    # subject_count comes from the staged temp-table count below —
                    # one authoritative count for both override and cursor paths.
                    reason = "scope_override"
                    cursor_schema = "okg.sql_gate.scope_override.v1"
                    cursor_content_hash = stable_hash({
                        "source": frontier.source,
                        "subject_ids": list(override_subject_ids),
                    })
                    cursor_scoped = True
                    with cur.copy(
                        """
                        COPY okg_sql_gate_subject_scope(subject_id)
                        FROM STDIN
                        """
                    ) as copy:
                        for subject_id in override_subject_ids:
                            copy.write_row((subject_id,))
                else:
                    cur.execute(
                        """
                        SELECT 1
                          FROM okg.source_watermarks
                         WHERE source = %(source)s
                        """,
                        {"source": frontier.source},
                    )
                    row = cur.fetchone()
                    if row is not None and row[0] is not None:
                        cur.execute(
                            """
                            SELECT
                              lower(COALESCE(
                                last_applied_rev->>'scoped',
                                'false'
                              )) IN ('true', 't', '1', 'yes', 'y')
                                AS cursor_scoped,
                              last_applied_rev->>'schema' AS cursor_schema,
                              last_applied_rev->>'content_hash'
                                AS cursor_content_hash
                              FROM okg.source_watermarks
                             WHERE source = %(source)s
                            """,
                            {"source": frontier.source},
                        )
                        (
                            cursor_scoped,
                            cursor_schema,
                            cursor_content_hash,
                        ) = cur.fetchone()
                        if frontier.require_scoped_cursor and not cursor_scoped:
                            reason = "cursor_not_scoped"
                        else:
                            scope_active = True
                            reason = (
                                "scoped_cursor"
                                if cursor_scoped else "unscoped_allowed"
                            )
                            cur.execute(
                                """
                                INSERT INTO
                                  pg_temp.okg_sql_gate_subject_scope(subject_id)
                                SELECT DISTINCT value
                                  FROM okg.source_watermarks wm,
                                       jsonb_array_elements_text(
                                         CASE
                                           WHEN jsonb_typeof(
                                             wm.last_applied_rev -> %(field)s
                                           ) = 'array'
                                           THEN wm.last_applied_rev -> %(field)s
                                           ELSE '[]'::jsonb
                                         END
                                       ) AS scope_items(value)
                                 WHERE wm.source = %(source)s
                                   AND btrim(value) <> ''
                                ON CONFLICT DO NOTHING
                                """,
                                {
                                    "source": frontier.source,
                                    "field": frontier.cursor_subject_ids_field,
                                },
                            )

                # Count AND digest over the FULL staged scope. The digest is
                # computed in SQL over every row so it stays correct even when
                # the id list below is truncated -- a truncated list must never
                # be attributable to the wrong scope.
                #
                # The expression comes from observations.scope_digest_sql, the
                # same specification the Python helper implements, so the two
                # paths cannot drift; it forces COLLATE "C" because the previous
                # bare `ORDER BY subject_id` was COLLATION-dependent (the same
                # subject set digested differently under `C` and `en_US.UTF-8`
                # for mixed-case or punctuated ids, and differently again from
                # Python's codepoint sort). A digest that depends on where the
                # check ran is not identity.
                cur.execute(
                    f"""
                    SELECT count(*)::bigint,
                           {scope_digest_sql("subject_id")}
                      FROM pg_temp.okg_sql_gate_subject_scope
                    """
                )
                raw_subject_count, subject_digest = cur.fetchone()
                subject_count = int(raw_subject_count)
                # Scope IDENTITY, not just the count. Persisting only
                # subject_count told a reader that N subjects were checked but
                # never WHICH, so the unevaluated complement was unrecoverable
                # from the published generation.
                # Same COLLATE "C" ordering as the digest, so the retained prefix
                # is the prefix of the set the digest covers under one order --
                # not a differently-sorted sample of it.
                cur.execute(
                    """
                    SELECT subject_id
                      FROM pg_temp.okg_sql_gate_subject_scope
                     ORDER BY subject_id COLLATE "C"
                     LIMIT %s
                    """,
                    (SCOPE_ID_LIMIT,),
                )
                subject_ids = [str(row[0]) for row in cur.fetchall()]
                cur.execute("ANALYZE okg_sql_gate_subject_scope")
                graph_scope = self._stage_scoped_graph_frontier(
                    cur,
                    scope_active=scope_active,
                )
                cur.execute(
                    """
                    INSERT INTO pg_temp.okg_sql_gate_scope_context (
                      scope_active, source, cursor_subject_ids_field,
                      subject_count, reason, cursor_schema,
                      cursor_content_hash, cursor_scoped,
                      graph_frontier_available, scoped_node_count,
                      scoped_out_edge_count, scoped_in_edge_count
                    ) VALUES (
                      %(scope_active)s, %(source)s, %(field)s,
                      %(subject_count)s, %(reason)s, %(cursor_schema)s,
                      %(cursor_content_hash)s, %(cursor_scoped)s,
                      %(graph_frontier_available)s, %(scoped_node_count)s,
                      %(scoped_out_edge_count)s, %(scoped_in_edge_count)s
                    )
                    """,
                    {
                        "scope_active": scope_active,
                        "source": frontier.source if configured else None,
                        "field": (
                            frontier.cursor_subject_ids_field
                            if configured else None
                        ),
                        "subject_count": subject_count,
                        "reason": reason,
                        "cursor_schema": cursor_schema,
                        "cursor_content_hash": cursor_content_hash,
                        "cursor_scoped": cursor_scoped,
                        "graph_frontier_available": graph_scope["available"],
                        "scoped_node_count": graph_scope["node_count"],
                        "scoped_out_edge_count": graph_scope["out_edge_count"],
                        "scoped_in_edge_count": graph_scope["in_edge_count"],
                    },
                )

        # The complement recipe is a SCOPED-run disclosure. Emitting it on an
        # unscoped run (scope_active=False, subject_ids=[]) inverted the meaning:
        # a reader following "universe MINUS subject_ids" concluded that the
        # ENTIRE universe went unevaluated on a run that swept all of it. The
        # transform gate and the IR executor both gate this inside their scoped
        # branch; this frontier now matches. When the run IS scoped, the recipe
        # still suppresses itself if the id list was capped (a subtraction
        # against a truncated list reports evaluated subjects as unevaluated).
        scope_complement: dict[str, Any] = {}
        if scope_active:
            scope_complement["scope_complement_recipe"] = (
                scope_complement_recipe(
                    "subject",
                    {
                        "subject_ids_truncated": (
                            subject_count > len(subject_ids)
                        ),
                        "subject_count": subject_count,
                    },
                    universe=(
                        "this check's subject universe at this generation"
                    ),
                )
            )
        return {
            "configured": configured,
            "source": frontier.source if configured else None,
            "cursor_subject_ids_field": (
                frontier.cursor_subject_ids_field if configured else None
            ),
            "require_scoped_cursor": (
                frontier.require_scoped_cursor if configured else None
            ),
            "scope_active": scope_active,
            "subject_count": subject_count,
            # IDENTITY of the scope, so the complement is computable by a
            # reader of the published generation (finding: this frontier was
            # count-only). `subject_ids` is capped at SCOPE_ID_LIMIT and says
            # so LOUDLY when the cap binds; the digest always covers the full
            # staged set.
            "subject_ids": subject_ids,
            "subject_ids_truncated": subject_count > len(subject_ids),
            "subject_digest_sha256": subject_digest,
            # The algorithm rides BESIDE the digest: a hash whose function is not
            # recorded cannot be recomputed, so it certifies nothing.
            "subject_digest_algorithm": SCOPE_DIGEST_ALGORITHM,
            **scope_complement,
            "reason": reason,
            "cursor_schema": cursor_schema,
            "cursor_content_hash": cursor_content_hash,
            "cursor_scoped": cursor_scoped,
            "graph_frontier_available": graph_scope["available"],
            "scoped_node_count": graph_scope["node_count"],
            "scoped_out_edge_count": graph_scope["out_edge_count"],
            "scoped_in_edge_count": graph_scope["in_edge_count"],
            "current_nodes_available": graph_scope["nodes_available"],
            "current_edges_available": graph_scope["edges_available"],
        }

    def _stage_scoped_graph_frontier(
        self,
        cur: psycopg.Cursor[Any],
        *,
        scope_active: bool,
    ) -> dict[str, Any]:
        cur.execute(
            """
            CREATE TEMP TABLE okg_sql_gate_scoped_nodes (
              node_id TEXT PRIMARY KEY,
              subtype TEXT NOT NULL,
              attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
              text_hash TEXT,
              embedding_model TEXT,
              source TEXT,
              source_record_id JSONB NOT NULL DEFAULT '{}'::jsonb,
              source_revision JSONB NOT NULL DEFAULT '{}'::jsonb,
              run_id TEXT,
              observed_at TIMESTAMPTZ
            )
            """
        )
        cur.execute(
            """
            CREATE TEMP TABLE okg_sql_gate_scoped_out_edges (
              edge_id BIGINT PRIMARY KEY,
              src TEXT NOT NULL,
              dst TEXT NOT NULL,
              edge_type TEXT NOT NULL,
              attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
              provenance TEXT NOT NULL DEFAULT 'explicit',
              confidence REAL NOT NULL DEFAULT 1.0,
              source TEXT,
              source_record_id JSONB NOT NULL DEFAULT '{}'::jsonb,
              source_revision JSONB NOT NULL DEFAULT '{}'::jsonb,
              run_id TEXT,
              observed_at TIMESTAMPTZ
            )
            """
        )
        cur.execute(
            """
            CREATE TEMP TABLE okg_sql_gate_scoped_in_edges (
              edge_id BIGINT PRIMARY KEY,
              src TEXT NOT NULL,
              dst TEXT NOT NULL,
              edge_type TEXT NOT NULL,
              attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
              provenance TEXT NOT NULL DEFAULT 'explicit',
              confidence REAL NOT NULL DEFAULT 1.0,
              source TEXT,
              source_record_id JSONB NOT NULL DEFAULT '{}'::jsonb,
              source_revision JSONB NOT NULL DEFAULT '{}'::jsonb,
              run_id TEXT,
              observed_at TIMESTAMPTZ
            )
            """
        )
        cur.execute(
            """
            SELECT
              to_regclass('okg.chronos_graph_nodes_current') IS NOT NULL,
              to_regclass('okg.chronos_graph_edges_current') IS NOT NULL
            """
        )
        nodes_available, edges_available = cur.fetchone()
        if scope_active and nodes_available:
            cur.execute(
                """
                INSERT INTO pg_temp.okg_sql_gate_scoped_nodes (
                  node_id, subtype, attrs, text_hash, embedding_model,
                  source, source_record_id, source_revision, run_id,
                  observed_at
                )
                SELECT
                  n.node_id, n.subtype, n.attrs, n.text_hash,
                  n.embedding_model, n.source, n.source_record_id,
                  n.source_revision, n.run_id, n.observed_at
                  FROM pg_temp.okg_sql_gate_subject_scope s
                  JOIN LATERAL (
                    SELECT *
                      FROM okg.chronos_graph_nodes_current n
                     WHERE n.node_id = s.subject_id
                  ) n ON true
                ON CONFLICT DO NOTHING
                """
            )
        if scope_active and edges_available:
            cur.execute(
                """
                INSERT INTO pg_temp.okg_sql_gate_scoped_out_edges (
                  edge_id, src, dst, edge_type, attrs, provenance,
                  confidence, source, source_record_id, source_revision,
                  run_id, observed_at
                )
                SELECT
                  e.edge_id, e.src, e.dst, e.edge_type, e.attrs,
                  e.provenance, e.confidence, e.source, e.source_record_id,
                  e.source_revision, e.run_id, e.observed_at
                  FROM pg_temp.okg_sql_gate_subject_scope s
                  JOIN LATERAL (
                    SELECT *
                      FROM okg.chronos_graph_edges_current e
                     WHERE e.src = s.subject_id
                  ) e ON true
                ON CONFLICT DO NOTHING
                """
            )
            cur.execute(
                """
                INSERT INTO pg_temp.okg_sql_gate_scoped_in_edges (
                  edge_id, src, dst, edge_type, attrs, provenance,
                  confidence, source, source_record_id, source_revision,
                  run_id, observed_at
                )
                SELECT
                  e.edge_id, e.src, e.dst, e.edge_type, e.attrs,
                  e.provenance, e.confidence, e.source, e.source_record_id,
                  e.source_revision, e.run_id, e.observed_at
                  FROM pg_temp.okg_sql_gate_subject_scope s
                  JOIN LATERAL (
                    SELECT *
                      FROM okg.chronos_graph_edges_current e
                     WHERE e.dst = s.subject_id
                  ) e ON true
                ON CONFLICT DO NOTHING
                """
            )
        cur.execute(
            """
            CREATE INDEX okg_sql_gate_scoped_out_edges_src_type_idx
              ON okg_sql_gate_scoped_out_edges (src, edge_type)
            """
        )
        cur.execute(
            """
            CREATE INDEX okg_sql_gate_scoped_out_edges_dst_type_idx
              ON okg_sql_gate_scoped_out_edges (dst, edge_type)
            """
        )
        cur.execute(
            """
            CREATE INDEX okg_sql_gate_scoped_in_edges_dst_type_idx
              ON okg_sql_gate_scoped_in_edges (dst, edge_type)
            """
        )
        cur.execute(
            """
            CREATE INDEX okg_sql_gate_scoped_in_edges_src_type_idx
              ON okg_sql_gate_scoped_in_edges (src, edge_type)
            """
        )
        cur.execute("ANALYZE okg_sql_gate_scoped_nodes")
        cur.execute("ANALYZE okg_sql_gate_scoped_out_edges")
        cur.execute("ANALYZE okg_sql_gate_scoped_in_edges")
        cur.execute(
            """
            SELECT
              (SELECT count(*)::bigint
                 FROM pg_temp.okg_sql_gate_scoped_nodes),
              (SELECT count(*)::bigint
                 FROM pg_temp.okg_sql_gate_scoped_out_edges),
              (SELECT count(*)::bigint
                 FROM pg_temp.okg_sql_gate_scoped_in_edges)
            """
        )
        node_count, out_edge_count, in_edge_count = cur.fetchone()
        return {
            "available": bool(nodes_available and edges_available),
            "nodes_available": bool(nodes_available),
            "edges_available": bool(edges_available),
            "node_count": int(node_count),
            "out_edge_count": int(out_edge_count),
            "in_edge_count": int(in_edge_count),
        }

    def _stage_candidate_rows(
        self,
        conn: psycopg.Connection[Any],
        candidate_sql: str,
        params: Mapping[str, Any],
    ) -> int:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    "DROP TABLE IF EXISTS pg_temp.okg_sql_gate_candidate_rows"
                )
                cur.execute(
                    f"SET LOCAL statement_timeout = {int(self.spec.timeout_ms)}"
                )
                cur.execute(
                    f"""
                    CREATE TEMP TABLE okg_sql_gate_candidate_rows AS
                    SELECT row_number() OVER ()::bigint
                             AS __okg_stage_order,
                           candidate_rows.*
                      FROM ({candidate_sql}) AS candidate_rows
                    """,
                    dict(params),
                )
                cur.execute(
                    """
                    SELECT count(*)::bigint
                      FROM pg_temp.okg_sql_gate_candidate_rows
                    """
                )
                (candidate_count,) = cur.fetchone()
                return int(candidate_count)

    def _fetch_candidate_row_batches(
        self,
        conn: psycopg.Connection[Any],
    ) -> Iterator[list[dict[str, Any]]]:
        last_stage_order = 0
        while True:
            with conn.transaction():
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute("SET TRANSACTION READ ONLY")
                    cur.execute(
                        f"SET LOCAL statement_timeout = "
                        f"{int(self.spec.timeout_ms)}"
                    )
                    cur.execute(
                        """
                        SELECT *
                          FROM pg_temp.okg_sql_gate_candidate_rows
                         WHERE __okg_stage_order > %(last_stage_order)s
                         ORDER BY __okg_stage_order
                         LIMIT %(chunk_size)s
                        """,
                        {
                            "last_stage_order": last_stage_order,
                            "chunk_size": (
                                self.spec.materialization_chunk_size
                            ),
                        },
                    )
                    rows = [dict(row) for row in cur.fetchall()]
            if not rows:
                break
            last_stage_order = int(rows[-1]["__okg_stage_order"])
            for row in rows:
                row.pop("__okg_stage_order", None)
            yield rows

    def _materialize_candidate_rows_sql_native(
        self,
        conn: psycopg.Connection[Any],
        definition: ConsistencyCheckDefinition,
        *,
        target_generation_id: int,
        run_id: UUID,
    ) -> dict[str, Any]:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"SET LOCAL statement_timeout = "
                    f"{int(self.spec.timeout_ms)}"
                )
                _install_sql_native_helpers(cur)
                _ensure_sql_native_optional_columns(cur)
                self._prepare_sql_native_candidate_rows(
                    cur,
                    definition,
                    target_generation_id=target_generation_id,
                    run_id=run_id,
                )
                cur.execute(
                    """
                    INSERT INTO okg.consistency_candidates (
                      check_id, target_generation_id, candidate_id, run_id,
                      input_scope, blocking_rule_hash, participant_roles,
                      participant_anchors, predicate_key, generation_policy,
                      coverage_status, exclusion_reason, candidate_payload
                    )
                    SELECT DISTINCT ON (candidate_id)
                      check_id, target_generation_id, candidate_id, run_id,
                      input_scope, blocking_rule_hash, participant_roles,
                      participant_anchors, predicate_key, generation_policy,
                      coverage_status, exclusion_reason, candidate_payload
                      FROM pg_temp.okg_sql_native_consistency_candidates
                     ORDER BY candidate_id, stage_order DESC
                    ON CONFLICT (
                      check_id, target_generation_id, candidate_id
                    ) DO UPDATE SET
                      run_id = EXCLUDED.run_id,
                      coverage_status = EXCLUDED.coverage_status,
                      exclusion_reason = EXCLUDED.exclusion_reason,
                      candidate_payload = EXCLUDED.candidate_payload
                    """,
                )
                self._prepare_sql_native_observation_events(
                    cur,
                    definition,
                    target_generation_id=target_generation_id,
                    run_id=run_id,
                )
                cur.execute(
                    """
                    WITH inserted AS (
                      INSERT INTO okg.observation_events (
                        observation_id, check_id, candidate_id,
                        target_generation_id, run_id, event_seq,
                        finding_event, evaluation_event, input_scope,
                        predicate_key, normalized_predicate, normalized_values,
                        participant_roles, participant_anchors, severity,
                        confidence, confidence_type, evidence, payload,
                        coverage_complete
                      )
                      SELECT
                        s.observation_id, s.check_id, s.candidate_id,
                        s.target_generation_id, s.run_id, s.event_seq,
                        s.finding_event, s.evaluation_event, s.input_scope,
                        s.predicate_key, s.normalized_predicate,
                        s.normalized_values, s.participant_roles,
                        s.participant_anchors, s.severity, s.confidence,
                        s.confidence_type, s.event_evidence, s.event_payload,
                        s.coverage_complete
                        FROM pg_temp.okg_sql_native_observation_events s
                       WHERE NOT (
                         s.finding_event = 'observed'
                         AND s.evaluation_event IS NULL
                         AND s.current_finding_status = 'open'
                         AND EXISTS (
                           SELECT 1 FROM okg.observation_current oc
                            WHERE oc.observation_id = s.observation_id
                              AND oc.finding_status = 'open'
                              AND oc.evaluation_status
                                  IS NOT DISTINCT FROM
                                    s.current_evaluation_status
                              AND oc.severity
                                  IS NOT DISTINCT FROM s.severity
                              AND oc.confidence
                                  IS NOT DISTINCT FROM s.confidence
                              AND oc.confidence_type
                                  IS NOT DISTINCT FROM s.confidence_type
                              AND oc.evidence = s.current_evidence
                              AND oc.payload  = s.current_payload
                         )
                       )
                       ORDER BY s.event_seq
                      RETURNING event_seq, event_id
                    ),
                    latest AS (
                      SELECT DISTINCT ON (event_rows.observation_id)
                        event_rows.*,
                        inserted.event_id
                        FROM pg_temp.okg_sql_native_observation_events
                          event_rows
                        JOIN inserted USING (event_seq)
                       ORDER BY event_rows.observation_id,
                                event_rows.event_seq DESC
                    )
                    INSERT INTO okg.observation_current (
                      observation_id, check_id, candidate_id,
                      latest_generation_id, latest_run_id, latest_event_id,
                      finding_status, evaluation_status, input_scope,
                      predicate_key, normalized_predicate, normalized_values,
                      participant_roles, participant_anchors, severity,
                      confidence, confidence_type, evidence, payload,
                      updated_at
                    )
                    SELECT
                      observation_id, check_id, candidate_id,
                      target_generation_id, run_id, event_id,
                      current_finding_status, current_evaluation_status,
                      input_scope, predicate_key, normalized_predicate,
                      normalized_values, participant_roles,
                      participant_anchors, severity, confidence,
                      confidence_type, current_evidence, current_payload,
                      now()
                      FROM latest
                    ON CONFLICT (observation_id) DO UPDATE SET
                      check_id = EXCLUDED.check_id,
                      candidate_id = EXCLUDED.candidate_id,
                      latest_generation_id = EXCLUDED.latest_generation_id,
                      latest_run_id = EXCLUDED.latest_run_id,
                      latest_event_id = EXCLUDED.latest_event_id,
                      finding_status = EXCLUDED.finding_status,
                      evaluation_status = EXCLUDED.evaluation_status,
                      input_scope = EXCLUDED.input_scope,
                      predicate_key = EXCLUDED.predicate_key,
                      normalized_predicate = EXCLUDED.normalized_predicate,
                      normalized_values = EXCLUDED.normalized_values,
                      participant_roles = EXCLUDED.participant_roles,
                      participant_anchors = EXCLUDED.participant_anchors,
                      severity = EXCLUDED.severity,
                      confidence = EXCLUDED.confidence,
                      confidence_type = EXCLUDED.confidence_type,
                      evidence = EXCLUDED.evidence,
                      payload = EXCLUDED.payload,
                      updated_at = now()
                    """,
                )
                _dedupe_observation_current_for_staged_events(
                    cur,
                    "okg_sql_native_observation_events",
                )
                cur.execute(
                    """
                    WITH candidate_counts AS (
                      SELECT
                        count(*) FILTER (
                          WHERE coverage_status = 'candidate'
                        )::bigint AS coverage_candidate_count
                        FROM pg_temp.okg_sql_native_consistency_candidates
                    ),
                    block_counts AS (
                      SELECT blocking_rule_hash, count(*)::bigint AS n
                        FROM pg_temp.okg_sql_native_consistency_candidates
                       WHERE coverage_status = 'candidate'
                       GROUP BY blocking_rule_hash
                    ),
                    block_json AS (
                      SELECT COALESCE(
                        jsonb_object_agg(
                          blocking_rule_hash, n
                          ORDER BY blocking_rule_hash COLLATE "C"
                        ),
                        '{}'::jsonb
                      ) AS candidate_count_by_block
                        FROM block_counts
                    ),
                    event_counts AS (
                      SELECT count(*)::bigint AS observation_count
                        FROM pg_temp.okg_sql_native_observation_events
                    )
                    SELECT
                      candidate_counts.coverage_candidate_count,
                      block_json.candidate_count_by_block,
                      event_counts.observation_count
                      FROM candidate_counts
                      CROSS JOIN block_json
                      CROSS JOIN event_counts
                    """,
                )
                row = dict(cur.fetchone())
                self._drop_sql_native_stages(cur)
                return row

    def _prepare_sql_native_candidate_rows(
        self,
        cur: psycopg.Cursor[Any],
        definition: ConsistencyCheckDefinition,
        *,
        target_generation_id: int,
        run_id: UUID,
    ) -> None:
        cur.execute(
            "DROP TABLE IF EXISTS pg_temp.okg_sql_native_consistency_candidates"
        )
        cur.execute(
            "DROP TABLE IF EXISTS pg_temp.okg_sql_native_observation_inputs"
        )
        cur.execute(
            """
            CREATE TEMP TABLE okg_sql_native_consistency_candidates
            ON COMMIT DROP AS
            WITH normalized AS (
              SELECT
                __okg_stage_order::bigint AS stage_order,
                input_scope::text AS input_scope,
                blocking_rule_hash::text AS blocking_rule_hash,
                (
                  SELECT COALESCE(
                    jsonb_object_agg(role_key, anchor_value),
                    '{}'::jsonb
                  )
                    FROM jsonb_each_text(participant_roles::jsonb)
                      AS roles(role_key, anchor_value)
                ) AS participant_roles,
                predicate_key::text AS predicate_key,
                generation_policy::text AS generation_policy,
                COALESCE(coverage_status::text, 'candidate')
                  AS coverage_status,
                exclusion_reason::text AS exclusion_reason,
                COALESCE(candidate_payload::jsonb, '{}'::jsonb)
                  AS candidate_payload,
                COALESCE(symmetric_predicate::boolean, false)
                  AS symmetric_predicate
                FROM pg_temp.okg_sql_gate_candidate_rows
            ),
            anchored AS (
              SELECT
                normalized.*,
                CASE
                  WHEN symmetric_predicate THEN (
                    SELECT COALESCE(
                      jsonb_agg(
                        jsonb_build_object(
                          'role',
                          'participant_' || (anchor_row.ord - 1)::text,
                          'anchor',
                          anchor_row.anchor_value
                        )
                        ORDER BY anchor_row.ord
                      ),
                      '[]'::jsonb
                    )
                      FROM (
                        SELECT
                          row_number() OVER (
                            ORDER BY anchor_value COLLATE "C"
                          ) AS ord,
                          anchor_value
                          FROM jsonb_each_text(
                            normalized.participant_roles
                          ) AS roles(role_key, anchor_value)
                      ) AS anchor_row
                  )
                  ELSE (
                    SELECT COALESCE(
                      jsonb_agg(
                        jsonb_build_object(
                          'role', role_key,
                          'anchor', anchor_value
                        )
                        ORDER BY role_key COLLATE "C"
                      ),
                      '[]'::jsonb
                    )
                      FROM jsonb_each_text(normalized.participant_roles)
                        AS roles(role_key, anchor_value)
                  )
                END AS participant_anchors
                FROM normalized
            )
            SELECT
              anchored.stage_order,
              %(check_id)s::text AS check_id,
              %(target_generation_id)s::bigint AS target_generation_id,
              (
                'candidate:' ||
                pg_temp.okg_sha256_text(
                  '{' ||
                  '"blocking_rule_hash":' ||
                    pg_temp.okg_json_escape(anchored.blocking_rule_hash) ||
                  ',"candidate_rule_hash":' ||
                    pg_temp.okg_json_escape(%(candidate_rule_hash)s::text) ||
                  ',"check_id":' ||
                    pg_temp.okg_json_escape(%(check_id)s::text) ||
                  ',"evaluator_hash":' ||
                    pg_temp.okg_json_escape(%(evaluator_hash)s::text) ||
                  ',"generation_policy":' ||
                    pg_temp.okg_json_escape(anchored.generation_policy) ||
                  ',"input_scope":' ||
                    pg_temp.okg_json_escape(anchored.input_scope) ||
                  ',"kind":"consistency_candidate_v1"' ||
                  ',"participant_anchors":' ||
                    pg_temp.okg_json_canonical(
                      anchored.participant_anchors
                    ) ||
                  ',"predicate_key":' ||
                    pg_temp.okg_json_escape(anchored.predicate_key) ||
                  '}'
                )
              ) AS candidate_id,
              %(run_id)s::uuid AS run_id,
              anchored.input_scope,
              anchored.blocking_rule_hash,
              anchored.participant_roles,
              anchored.participant_anchors,
              anchored.predicate_key,
              anchored.generation_policy,
              anchored.coverage_status,
              anchored.exclusion_reason,
              anchored.candidate_payload,
              anchored.symmetric_predicate,
              pg_temp.okg_json_canonical(anchored.participant_anchors)
                AS participant_anchors_json
              FROM anchored
            """,
            {
                "check_id": definition.check_id,
                "candidate_rule_hash": definition.candidate_rule_hash,
                "evaluator_hash": definition.evaluator_hash,
                "target_generation_id": target_generation_id,
                "run_id": run_id,
            },
        )

    def _prepare_sql_native_observation_events(
        self,
        cur: psycopg.Cursor[Any],
        definition: ConsistencyCheckDefinition,
        *,
        target_generation_id: int,
        run_id: UUID,
    ) -> None:
        cur.execute(
            "DROP TABLE IF EXISTS pg_temp.okg_sql_native_observation_events"
        )
        cur.execute(
            """
            SELECT COALESCE(max(event_seq), 0)::int AS base_event_seq
              FROM okg.observation_events
             WHERE run_id = %(run_id)s
            """,
            {"run_id": run_id},
        )
        base_event_seq = int(cur.fetchone()["base_event_seq"])
        self._prepare_sql_native_observation_inputs(
            cur,
            definition,
            base_event_seq=base_event_seq,
        )
        cur.execute(
            """
            SELECT count(*) > count(DISTINCT observation_id) AS has_duplicates
              FROM pg_temp.okg_sql_native_observation_inputs
            """,
        )
        has_duplicates = bool(cur.fetchone()["has_duplicates"])
        if has_duplicates:
            self._prepare_sql_native_observation_events_recursive(
                cur,
                definition,
                target_generation_id=target_generation_id,
                run_id=run_id,
            )
        else:
            self._prepare_sql_native_observation_events_fast(
                cur,
                definition,
                target_generation_id=target_generation_id,
                run_id=run_id,
            )

    def _prepare_sql_native_observation_inputs(
        self,
        cur: psycopg.Cursor[Any],
        definition: ConsistencyCheckDefinition,
        *,
        base_event_seq: int,
    ) -> None:
        cur.execute(
            "DROP TABLE IF EXISTS pg_temp.okg_sql_native_observation_inputs"
        )
        cur.execute(
            """
            CREATE TEMP TABLE okg_sql_native_observation_inputs
            ON COMMIT DROP AS
            WITH raw_events AS (
              SELECT
                candidates.stage_order,
                candidates.check_id,
                candidates.candidate_id,
                candidates.input_scope,
                candidates.predicate_key,
                candidates.participant_roles,
                candidates.participant_anchors,
                candidates.participant_anchors_json,
                candidates.symmetric_predicate,
                rows.normalized_predicate::text AS normalized_predicate,
                rows.normalized_values::jsonb AS normalized_values,
                rows.source_revision_policy::text AS source_revision_policy,
                CASE
                  WHEN rows.finding_event IS NULL
                   AND rows.evaluation_event IS NULL THEN 'observed'::text
                  ELSE rows.finding_event::text
                END AS finding_event,
                rows.evaluation_event::text AS evaluation_event,
                rows.severity::text AS event_severity,
                rows.confidence::numeric(8, 6) AS event_confidence,
                %(confidence_type)s::text AS event_confidence_type,
                COALESCE(rows.evidence::jsonb, '{}'::jsonb) AS event_evidence,
                COALESCE(rows.payload::jsonb, '{}'::jsonb) AS event_payload,
                COALESCE(
                  rows.observation_coverage_complete::boolean,
                  true
                ) AS coverage_complete
                FROM pg_temp.okg_sql_native_consistency_candidates candidates
                JOIN pg_temp.okg_sql_gate_candidate_rows rows
                  ON rows.__okg_stage_order = candidates.stage_order
               WHERE CASE
                       WHEN rows.emit_observation IS NOT NULL
                         THEN rows.emit_observation::boolean
                       ELSE rows.finding_event IS NOT NULL
                         OR rows.evaluation_event IS NOT NULL
                         OR rows.normalized_predicate IS NOT NULL
                         OR rows.normalized_values IS NOT NULL
                     END
            ),
            identified AS (
              SELECT
                raw_events.*,
                (
                  'observation:' ||
                  pg_temp.okg_sha256_text(
                    '{' ||
                    '"candidate_id":' ||
                      pg_temp.okg_json_escape(raw_events.candidate_id) ||
                    ',"candidate_rule_hash":' ||
                      pg_temp.okg_json_escape(
                        %(candidate_rule_hash)s::text
                      ) ||
                    ',"check_id":' ||
                      pg_temp.okg_json_escape(%(check_id)s::text) ||
                    ',"evaluator_hash":' ||
                      pg_temp.okg_json_escape(%(evaluator_hash)s::text) ||
                    ',"input_scope":' ||
                      pg_temp.okg_json_escape(raw_events.input_scope) ||
                    ',"kind":"consistency_observation_v1"' ||
                    ',"normalized_predicate":' ||
                      pg_temp.okg_json_escape(
                        raw_events.normalized_predicate
                      ) ||
                    ',"normalized_values":' ||
                      pg_temp.okg_json_canonical(
                        raw_events.normalized_values
                      ) ||
                    ',"participant_anchors":' ||
                      raw_events.participant_anchors_json ||
                    ',"source_revision_policy":' ||
                      pg_temp.okg_json_escape(
                        COALESCE(
                          raw_events.source_revision_policy,
                          rows.generation_policy::text
                        )
                      ) ||
                    '}'
                  )
                ) AS observation_id
                FROM raw_events
                JOIN pg_temp.okg_sql_gate_candidate_rows rows
                  ON rows.__okg_stage_order = raw_events.stage_order
            )
            SELECT
              identified.*,
              (%(base_event_seq)s::int + row_number() OVER (
                ORDER BY identified.stage_order
              )::int) AS event_seq,
              row_number() OVER (
                PARTITION BY identified.observation_id
                ORDER BY identified.stage_order
              )::int AS observation_ordinal
              FROM identified
            """,
            {
                "base_event_seq": int(base_event_seq),
                "check_id": definition.check_id,
                "candidate_rule_hash": definition.candidate_rule_hash,
                "evaluator_hash": definition.evaluator_hash,
                "confidence_type": definition.confidence_type,
            },
        )

    def _prepare_sql_native_observation_events_fast(
        self,
        cur: psycopg.Cursor[Any],
        definition: ConsistencyCheckDefinition,
        *,
        target_generation_id: int,
        run_id: UUID,
    ) -> None:
        cur.execute(
            """
            CREATE TEMP TABLE okg_sql_native_observation_events
            ON COMMIT DROP AS
            SELECT
              inputs.stage_order,
              inputs.observation_id,
              inputs.check_id,
              inputs.candidate_id,
              %(target_generation_id)s::bigint AS target_generation_id,
              %(run_id)s::uuid AS run_id,
              inputs.event_seq,
              inputs.finding_event,
              inputs.evaluation_event,
              inputs.input_scope,
              inputs.predicate_key,
              inputs.normalized_predicate,
              inputs.normalized_values,
              inputs.participant_roles,
              inputs.participant_anchors,
              COALESCE(inputs.event_severity, current.severity) AS severity,
              COALESCE(inputs.event_confidence, current.confidence)
                AS confidence,
              COALESCE(
                inputs.event_confidence_type,
                current.confidence_type,
                %(confidence_type)s::text
              ) AS confidence_type,
              inputs.event_evidence,
              inputs.event_payload,
              inputs.coverage_complete,
              pg_temp.okg_next_finding_status(
                COALESCE(current.finding_status, 'absent')::text,
                inputs.finding_event,
                inputs.coverage_complete
              ) AS current_finding_status,
              COALESCE(
                inputs.evaluation_event,
                current.evaluation_status,
                'not_evaluated'
              )::text AS current_evaluation_status,
              CASE
                WHEN inputs.event_evidence <> '{}'::jsonb
                  THEN inputs.event_evidence
                ELSE COALESCE(current.evidence, '{}'::jsonb)
              END AS current_evidence,
              CASE
                WHEN inputs.event_payload <> '{}'::jsonb
                  THEN inputs.event_payload
                ELSE COALESCE(current.payload, '{}'::jsonb)
              END AS current_payload
              FROM pg_temp.okg_sql_native_observation_inputs inputs
              LEFT JOIN okg.observation_current current
                ON current.observation_id = inputs.observation_id
            """,
            {
                "confidence_type": definition.confidence_type,
                "target_generation_id": target_generation_id,
                "run_id": run_id,
            },
        )

    def _prepare_sql_native_observation_events_recursive(
        self,
        cur: psycopg.Cursor[Any],
        definition: ConsistencyCheckDefinition,
        *,
        target_generation_id: int,
        run_id: UUID,
    ) -> None:
        cur.execute(
            """
            CREATE TEMP TABLE okg_sql_native_observation_events
            ON COMMIT DROP AS
            WITH RECURSIVE lifecycle AS (
              SELECT
                inputs.*,
                COALESCE(current.finding_status, 'absent')::text
                  AS old_finding_status,
                COALESCE(
                  current.evaluation_status,
                  'not_evaluated'
                )::text AS old_evaluation_status,
                current.severity AS old_severity,
                current.confidence AS old_confidence,
                COALESCE(
                  current.confidence_type,
                  %(confidence_type)s::text
                ) AS old_confidence_type,
                COALESCE(current.evidence, '{}'::jsonb) AS old_evidence,
                COALESCE(current.payload, '{}'::jsonb) AS old_payload,
                pg_temp.okg_next_finding_status(
                  COALESCE(current.finding_status, 'absent')::text,
                  inputs.finding_event,
                  inputs.coverage_complete
                ) AS current_finding_status,
                COALESCE(
                  inputs.evaluation_event,
                  current.evaluation_status,
                  'not_evaluated'
                )::text AS current_evaluation_status,
                COALESCE(inputs.event_severity, current.severity)
                  AS severity,
                COALESCE(inputs.event_confidence, current.confidence)
                  AS confidence,
                COALESCE(
                  inputs.event_confidence_type,
                  current.confidence_type,
                  %(confidence_type)s::text
                ) AS confidence_type,
                CASE
                  WHEN inputs.event_evidence <> '{}'::jsonb
                    THEN inputs.event_evidence
                  ELSE COALESCE(current.evidence, '{}'::jsonb)
                END AS current_evidence,
                CASE
                  WHEN inputs.event_payload <> '{}'::jsonb
                    THEN inputs.event_payload
                  ELSE COALESCE(current.payload, '{}'::jsonb)
                END AS current_payload
                FROM pg_temp.okg_sql_native_observation_inputs inputs
                LEFT JOIN okg.observation_current current
                  ON current.observation_id = inputs.observation_id
               WHERE inputs.observation_ordinal = 1

              UNION ALL

              SELECT
                inputs.*,
                lifecycle.current_finding_status
                  AS old_finding_status,
                lifecycle.current_evaluation_status
                  AS old_evaluation_status,
                lifecycle.severity AS old_severity,
                lifecycle.confidence AS old_confidence,
                lifecycle.confidence_type AS old_confidence_type,
                lifecycle.current_evidence AS old_evidence,
                lifecycle.current_payload AS old_payload,
                pg_temp.okg_next_finding_status(
                  lifecycle.current_finding_status,
                  inputs.finding_event,
                  inputs.coverage_complete
                ) AS current_finding_status,
                COALESCE(
                  inputs.evaluation_event,
                  lifecycle.current_evaluation_status
                ) AS current_evaluation_status,
                COALESCE(inputs.event_severity, lifecycle.severity)
                  AS severity,
                COALESCE(inputs.event_confidence, lifecycle.confidence)
                  AS confidence,
                COALESCE(
                  inputs.event_confidence_type,
                  lifecycle.confidence_type
                ) AS confidence_type,
                CASE
                  WHEN inputs.event_evidence <> '{}'::jsonb
                    THEN inputs.event_evidence
                  ELSE lifecycle.current_evidence
                END AS current_evidence,
                CASE
                  WHEN inputs.event_payload <> '{}'::jsonb
                    THEN inputs.event_payload
                  ELSE lifecycle.current_payload
                END AS current_payload
                FROM lifecycle
                JOIN pg_temp.okg_sql_native_observation_inputs inputs
                  ON inputs.observation_id = lifecycle.observation_id
                 AND inputs.observation_ordinal =
                       lifecycle.observation_ordinal + 1
            )
            SELECT
              lifecycle.stage_order,
              lifecycle.observation_id,
              lifecycle.check_id,
              lifecycle.candidate_id,
              %(target_generation_id)s::bigint AS target_generation_id,
              %(run_id)s::uuid AS run_id,
              lifecycle.event_seq,
              lifecycle.finding_event,
              lifecycle.evaluation_event,
              lifecycle.input_scope,
              lifecycle.predicate_key,
              lifecycle.normalized_predicate,
              lifecycle.normalized_values,
              lifecycle.participant_roles,
              lifecycle.participant_anchors,
              lifecycle.severity,
              lifecycle.confidence,
              lifecycle.confidence_type,
              lifecycle.event_evidence,
              lifecycle.event_payload,
              lifecycle.coverage_complete,
              lifecycle.current_finding_status,
              lifecycle.current_evaluation_status,
              lifecycle.current_evidence,
              lifecycle.current_payload
              FROM lifecycle
            """,
            {
                "confidence_type": definition.confidence_type,
                "target_generation_id": target_generation_id,
                "run_id": run_id,
            },
        )

    def _drop_sql_native_stages(self, cur: psycopg.Cursor[Any]) -> None:
        cur.execute(
            "DROP TABLE IF EXISTS pg_temp.okg_sql_native_observation_events"
        )
        cur.execute(
            "DROP TABLE IF EXISTS pg_temp.okg_sql_native_consistency_candidates"
        )

    def _drop_candidate_stage(self, conn: psycopg.Connection[Any]) -> None:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    "DROP TABLE IF EXISTS pg_temp.okg_sql_gate_candidate_rows"
                )
                cur.execute(
                    "DROP TABLE IF EXISTS pg_temp.okg_sql_gate_scoped_in_edges"
                )
                cur.execute(
                    "DROP TABLE IF EXISTS pg_temp.okg_sql_gate_scoped_out_edges"
                )
                cur.execute(
                    "DROP TABLE IF EXISTS pg_temp.okg_sql_gate_scoped_nodes"
                )
                cur.execute(
                    "DROP TABLE IF EXISTS pg_temp.okg_sql_gate_scope_context"
                )
                cur.execute(
                    "DROP TABLE IF EXISTS pg_temp.okg_sql_gate_subject_scope"
                )


# task.ir.freeze-sql-path (PACT consistency-checks-as-ir): the candidate_sql
# authoring path is FROZEN. The (deployment, check_id) pairs below are the
# landed fleet at freeze time, grandfathered until those deployments re-author
# on ir_consistency_checks (proven with the equivalence harness,
# okg.substrate.consistency.equivalence). A deterministic_sql check under any
# OTHER pair is a deployment-lint BLOCKER — the pairing stops a new deployment
# from evading the freeze by reusing a grandfathered id. Do NOT add pairs
# here; author the check as a transform-IR program.
#
# This set is ALSO the dead_relation_read grandfather tier (deployment_lint.py:
# _check_dead_relation_surfaces): a grandfathered pair reading a retired
# relation warns instead of blocking. Dropping a pair here therefore promotes
# BOTH lints for that (deployment, check_id) to BLOCKER.
#
# consistency-deployment-currency (task.dep.prototype-disposition) RETIRED the
# prototype gates of two private deployments, one deleted since (their
# consistency_gates blocks were vacuous dead-relation reads and are removed
# from the manifests),
# so their pairs are dropped here — any future recurrence for those deployments
# is now a BLOCKER, not a grandfathered warning.
#
# cern-twiki's two pairs are KEPT: cern-twiki lives in okg-deployments (main)
# and still declares these frozen deterministic_sql checks until its cross-repo
# IR re-authoring lands (consistency-deployment-currency task.dep.twiki-ir-
# reauthor). Remove the cern-twiki pairs — and the whole grandfather tier — once
# that re-authoring merges and no fleet manifest reads okg.nodes_live/edges_live.
FROZEN_SQL_CHECK_ALLOWLIST = frozenset({
    ("cern-twiki", "cern_twiki.snapshot_hygiene.v1"),
    ("cern-twiki", "cern_twiki.structure_consistency.v1"),
})

SQL_CHECK_FREEZE_POINTER = (
    "the deterministic_sql (candidate_sql) path is frozen by PACT "
    "consistency-checks-as-ir — author checks as ir_consistency_checks "
    "(transform-IR programs) and prove parity with the equivalence harness "
    "(okg.substrate.consistency.equivalence)"
)


def load_consistency_gates(
    deployment_dir: Path | str,
) -> list[DeterministicSqlConsistencyGate]:
    """Load ``deployment.yaml``'s ``consistency_gates`` block."""

    return _load_sql_checks(
        deployment_dir,
        block_name="consistency_gates",
        blocks_publish=True,
        execution_phase="publish_gate",
        trigger_kind="dirty",
    )


def load_post_publish_consistency_checks(
    deployment_dir: Path | str,
) -> list[DeterministicSqlConsistencyGate]:
    """Load non-blocking ``post_publish_consistency_checks``."""

    return _load_sql_checks(
        deployment_dir,
        block_name="post_publish_consistency_checks",
        blocks_publish=False,
        execution_phase="post_publish",
        trigger_kind="post_publish",
    )


def build_post_publish_crash_summary(
    *,
    gate_name: str,
    target_generation_id: int | str | None,
    exc: BaseException,
    elapsed_ms: int,
    logger: logging.Logger,
) -> dict[str, Any]:
    """Honest crash-time summary for a post-publish consistency check.

    SINGLE SOURCE OF TRUTH for the post-publish crash SUMMARY across both paths
    — the publisher's Chronos-native inline loop (``Publisher._cycle_chronos_native``)
    and the standalone rerun function (:func:`run_post_publish_consistency_checks`,
    used by ``okg consistency-rerun``). (This covers the crash summary only; the
    per-check loop still has other divergent pieces — e.g. the rerun-only #603
    ``runs`` multiplicity block — tracked by PACT consistency-post-publish-path-parity.)
    One builder is what stops the two crash paths from drifting: when this
    summary was hardened the inline copy silently lagged (no log line, an
    undocumented contract), so the paths disagreed on the fallback surface. They
    no longer can.

    This is an explicit error state, NOT a synthesized measurement. The real
    check types (IrPostPublishCheck / DeterministicSqlConsistencyGate) catch
    their own exceptions and persist a ``blocked_error`` gate row ATOMICALLY
    inside ``conn.transaction()``; they only propagate to a caller when that
    persist itself failed — i.e. this run has no committed row. Reading back
    the latest row for ``(gate_name, generation)`` would therefore surface a
    DIFFERENT run's measurement (e.g. the publish-time row a consistency-rerun
    targets) and misattribute it to this crash — worse than zeros. So report
    the honest error state: ``result='blocked_error'`` (distinguishable from an
    evaluated zero-findings run) with the exception class recorded. The
    crash-summary read-back was removed as net-deceptive (adversarial review
    2026-07-07, consistency-engine-hardening task.impl.loud-fallbacks); the
    guard is ``test_post_publish_crash_does_not_misattribute_a_prior_runs_row``.
    The zero counts here are an explicit error sentinel, never a measurement —
    every consumer must key off ``result``, never sum the counts blind.
    """
    logger.error(
        "post-publish consistency check %s raised for generation %s; "
        "generation remains published",
        gate_name,
        target_generation_id,
        exc_info=exc,
    )
    return {
        "check_id": gate_name,
        "gate_name": gate_name,
        "target_generation_id": target_generation_id,
        "result": "blocked_error",
        "blocks_publish": False,
        "reason": "post_publish_check_exception",
        "open_observation_count": 0,
        "newly_opened_count": 0,
        "resolved_count": 0,
        "coverage_complete": None,
        "coverage_basis": None,
        "budget_ms": None,
        "elapsed_ms": elapsed_ms,
        "execution_phase": "post_publish",
        "details": _crash_details(exc),
    }


def _crash_details(exc: BaseException) -> dict[str, Any]:
    """The exception's class and message, plus a SubstrateError's code and data."""
    details: dict[str, Any] = {
        "error_type": type(exc).__name__,
        "error": str(exc),
    }
    code = getattr(exc, "code", None)
    if isinstance(code, str):
        details["error_code"] = code
    data = getattr(exc, "data", None)
    if isinstance(data, Mapping) and data:
        details["error_data"] = dict(data)
    return details


def evaluate_post_publish_check_reading_mirror(
    conn: psycopg.Connection[Any],
    check: Any,
    mirror: Any,
    evaluate: Callable[[], Any],
    *,
    target_generation_id: int,
    catalog_version_id: int,
    deployment_name: str,
) -> Any:
    """Run ``evaluate`` inside ``mirror.reading(check)``; record a refusal.

    Both post-publish paths use this for a check that may read the shared
    Chronos SQL mirror. When the mirror cannot be used (lock wait ran out,
    refresh failed, wrong stamp), a check that can record a refusal
    (:meth:`DeterministicSqlConsistencyGate.record_refusal`) writes the same
    failed run and ``blocked_error`` gate row its own errors write, with the
    error's code and data; any other check lets the error reach the caller's
    per-check crash summary.
    """
    from okg.substrate.errors import ChronosSqlMirrorMismatch

    try:
        with mirror.reading(check):
            return evaluate()
    except ChronosSqlMirrorMismatch as exc:
        record = getattr(check, "record_refusal", None)
        if not callable(record):
            raise
        return record(
            conn,
            exc,
            target_generation_id=target_generation_id,
            catalog_version_id=catalog_version_id,
            deployment_name=deployment_name,
        )


def attach_post_publish_run_multiplicity(
    conn: psycopg.Connection[Any],
    *,
    check: Any,
    summary: dict[str, Any],
) -> None:
    """Attach the operator-visible run-multiplicity block to ``summary`` in place.

    SINGLE SOURCE OF TRUTH for both post-publish paths (the publisher's live
    inline loop and the rerun function). Repeated reruns each add one terminal
    ``derived_artifact_runs`` row, so this reports how many runs exist for THIS
    ``(check, generation)`` and how many are still non-terminal ('running'
    orphans).

    Keyed off the check's DURABLE definition id (``spec.check_id``), NOT the
    summary's ``check_id``: on the crash path the summary ``check_id`` is the
    display gate_name (``spec.name`` may differ from ``spec.check_id``), which
    would query a non-existent definition and wrong-zero the orphan gauge
    EXACTLY when a run is left orphaned — defeating the mitigation (adversarial
    review 2026-07-07). The count is NOT swallowed: a failing query surfaces to
    the caller (a default-value-as-fallback would hide a real DB fault).
    (:func:`run_one_post_publish_check`, the shared caller, records that surfaced
    failure as an explicit logged ``runs`` error marker rather than propagating,
    because post-publish is non-blocking — still loud, never a silent default.)
    """
    definition_id = str(
        getattr(getattr(check, "spec", None), "check_id", None)
        or summary["check_id"]
    )
    summary["runs"] = summarize_consistency_run_multiplicity(
        conn,
        definition_id=definition_id,
        target_generation_id=int(summary["target_generation_id"]),
    )


def run_one_post_publish_check(
    conn: psycopg.Connection[Any],
    check: Any,
    *,
    target_generation_id: int | str | None,
    evaluate: Callable[[], Any],
    logger: logging.Logger,
) -> dict[str, Any]:
    """Run ONE post-publish consistency check and return its summary.

    The single per-check body shared by BOTH post-publish entry points — the
    publisher's live inline loop (``Publisher._cycle_chronos_native``) and the
    rerun function (:func:`run_post_publish_consistency_checks`). The ONLY thing
    each path supplies is ``evaluate``, a zero-arg closure: the live publish loop
    closes over the Chronos delta-scope / branch context it passes to IR checks;
    the rerun path closes over ``scope_subject_ids``. Everything else — timing,
    the honest crash summary, and the run-multiplicity block — is identical and
    lives here, so that shared body cannot drift again (only the ``evaluate``
    step deliberately differs per path).

    ``target_generation_id`` labels the crash summary (each path resolves the
    generation differently). The run-multiplicity block is observability, not a
    gate: post-publish is non-blocking, so a failure of that count is recorded as
    a LOUD explicit error marker (logged; ``runs`` becomes ``{error_type,
    error}``) — never silent zeros and never fatal to the caller. This also makes
    the rerun path graceful on a count failure, where it previously propagated;
    the fault is still surfaced (logged + explicit marker), just not by crashing
    the run.
    """
    gate_name = str(getattr(check, "name", check.__class__.__name__))
    t0 = time.monotonic()
    try:
        evaluation = evaluate()
        summary = consistency_gate_summary(evaluation)
        summary["execution_phase"] = "post_publish"
    except Exception as exc:  # noqa: BLE001
        summary = build_post_publish_crash_summary(
            gate_name=gate_name,
            target_generation_id=target_generation_id,
            exc=exc,
            elapsed_ms=int((time.monotonic() - t0) * 1000),
            logger=logger,
        )
    try:
        attach_post_publish_run_multiplicity(conn, check=check, summary=summary)
    except Exception as exc:  # noqa: BLE001
        conn.rollback()  # defensive: clear any partial txn state
        logger.exception(
            "post-publish run-multiplicity count failed for %s", gate_name
        )
        summary["runs"] = {
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
    return summary


def run_post_publish_consistency_checks(
    conn: psycopg.Connection[Any],
    checks: list[DeterministicSqlConsistencyGate],
    *,
    deployment_name: str,
    generation_id: int | str | None = None,
    scope_subject_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Run non-blocking post-publish checks for a published generation.

    Rerun multiplicity contract (documented multiplicity, NOT uniqueness):
    each invocation runs every check once against the target generation,
    creating exactly one new ``derived_artifact_runs`` row per check that must
    reach a terminal status. N reruns leave N terminal runs, each with its own
    coverage + gate rows — there is no uniqueness constraint or upsert, so
    prior runs' rows are retained (history is preserved, e.g. so both runs'
    gate rows stay present and consistent). Every check's summary carries a
    ``runs`` block (:func:`summarize_consistency_run_multiplicity`) reporting,
    for that ``(check, target generation)``, ``runs_total`` and
    ``runs_non_terminal`` — the operator-visible count that bounds multiplicity
    and makes orphaned (still ``'running'``) runs detectable rather than
    silently accumulating.

    ``scope_subject_ids`` (from ``consistency-rerun --scope-subject``) is an
    explicit operator scope: it replaces the delta-derived scope, is recorded
    as ``scope_reason='explicit_scope_subject_ids'``, and — being scope
    restricted — can never certify universe completeness
    (``coverage_complete=False``).
    """

    from okg.substrate.consistency.chronos_sql_mirror import (
        ChronosSqlMirrorReads,
        chronos_sql_mirror_for_checks,
    )
    from okg.substrate.graph_store import ChronosGraphStore

    target = _resolve_post_publish_target(
        conn,
        deployment_name=deployment_name,
        generation_id=generation_id,
    )
    native_generation_id = target["native_generation_id"]

    def _evaluate(check: Any, mirror: ChronosSqlMirrorReads) -> Any:
        kwargs: dict[str, Any] = {
            "target_generation_id": target["generation_id"],
            "catalog_version_id": target["catalog_version_id"],
            "deployment_name": deployment_name,
            "scope_subject_ids": scope_subject_ids,
        }
        if getattr(check, "accepts_publish_delta", False):
            # IR checks read a Chronos checkout: give them the checkpoint id.
            # target_generation_id stays the compat id the records key on.
            kwargs["checkout_generation_id"] = native_generation_id
        # A mirror-reading SQL check holds the mirror lock only while it runs.
        return evaluate_post_publish_check_reading_mirror(
            conn,
            check,
            mirror,
            lambda: check.evaluate(conn, **kwargs),
            target_generation_id=target["generation_id"],
            catalog_version_id=target["catalog_version_id"],
            deployment_name=deployment_name,
        )

    summaries: list[dict[str, Any]] = []
    # The shared SQL mirror holds whatever generation last refreshed it. Each
    # mirror-reading check re-copies it from the generation being rechecked
    # under the mirror lock and holds the lock while it reads, so a
    # concurrent publish cannot swap it underneath that check.
    with chronos_sql_mirror_for_checks(
        conn,
        checks,
        store=ChronosGraphStore.from_connection(conn, ensure_schema=False),
        generation_id=native_generation_id,
        deployment_name=deployment_name,
    ) as mirror_guard:
        for check in checks:
            # The whole per-check body (evaluate -> summarize -> honest crash
            # summary -> runs block) is the shared runner; this path only
            # supplies HOW to evaluate (rerun passes scope_subject_ids). See
            # run_one_post_publish_check.
            summaries.append(
                run_one_post_publish_check(
                    conn,
                    check,
                    target_generation_id=target["generation_id"],
                    evaluate=lambda check=check: _evaluate(check, mirror_guard),
                    logger=log,
                )
            )
    return {
        "deployment_name": deployment_name,
        "generation_id": target["generation_id"],
        "catalog_version_id": target["catalog_version_id"],
        "status": target["status"],
        "native_generation_id": target.get("native_generation_id"),
        "node_count": target.get("node_count"),
        "edge_count": target.get("edge_count"),
        "scope_subject_ids": list(scope_subject_ids or []),
        "check_count": len(checks),
        "checks": summaries,
    }


def _resolve_post_publish_target(
    conn: psycopg.Connection[Any],
    *,
    deployment_name: str,
    generation_id: int | str | None,
) -> dict[str, Any]:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    if generation_id is None:
        checkpoint = store.latest_generation(
            deployment_name=deployment_name,
            status="published",
        )
    else:
        checkpoint = store.resolve_generation(
            generation_id,
            deployment_name=deployment_name,
            status="published",
        )
    if checkpoint is None:
        raise SubstrateError(
            "published generation not found for post-publish "
            "consistency rerun",
            data={
                "deployment_name": deployment_name,
                "generation_id": generation_id,
            },
            fix_hint=(
                "publish the deployment first or pass an existing "
                "published --generation"
            ),
        )
    metadata = dict(checkpoint.metadata or {})
    status = str(metadata.get("status") or "")
    if status != "published":
        raise SubstrateError(
            "post-publish consistency rerun requires a published generation",
            data={
                "deployment_name": deployment_name,
                "generation_id": generation_id,
                "status": status,
            },
            fix_hint="choose a generation whose status is 'published'",
        )
    raw_target = metadata.get("target_generation_id")
    if raw_target is None:
        text = str(generation_id or "")
        if text.isdigit():
            raw_target = int(text)
    if raw_target is None:
        raise SubstrateError(
            "published generation has no target_generation_id metadata",
            data={
                "deployment_name": deployment_name,
                "generation_id": checkpoint.checkpoint_id,
            },
            fix_hint="choose a Chronos generation produced by the publisher",
        )
    from okg.substrate.consistency.chronos_sql_mirror import (
        release_generation_session,
    )

    graph = store.checkout_generation(checkpoint.checkpoint_id)
    try:
        node_count = int(
            graph.query("SELECT COUNT(*) AS n FROM okg.graph_nodes")[0]["n"]
        )
        edge_count = int(
            graph.query("SELECT COUNT(*) AS n FROM okg.graph_edges")[0]["n"]
        )
    finally:
        # Release the history read lease now instead of leaving it to expire.
        release_generation_session(graph)
    return {
        "generation_id": int(raw_target),
        "native_generation_id": checkpoint.checkpoint_id,
        "catalog_version_id": int(metadata.get("catalog_version_id") or 1),
        "status": status,
        "node_count": node_count,
        "edge_count": edge_count,
    }


def _load_sql_checks(
    deployment_dir: Path | str,
    *,
    block_name: str,
    blocks_publish: bool,
    execution_phase: str,
    trigger_kind: str,
) -> list[DeterministicSqlConsistencyGate]:
    deployment_dir = Path(deployment_dir)
    manifest_path = deployment_dir / "deployment.yaml"
    if not manifest_path.is_file():
        return []
    with manifest_path.open() as f:
        manifest = fastyaml.safe_load(f) or {}
    specs = manifest.get(block_name) or []
    if not specs:
        return []
    if not isinstance(specs, list):
        raise ConsistencyGateLoadError(
            f"{manifest_path}: {block_name!r} must be a list",
            data={"path": str(manifest_path)},
        )
    gates: list[DeterministicSqlConsistencyGate] = []
    for idx, raw in enumerate(specs):
        gates.append(
            DeterministicSqlConsistencyGate(
                _parse_sql_gate(
                    raw,
                    where=f"{manifest_path}:{block_name}[{idx}]",
                ),
                blocks_publish=blocks_publish,
                execution_phase=execution_phase,
                trigger_kind=trigger_kind,
            )
        )
    if gates:
        warnings.warn(
            f"{manifest_path}: {len(gates)} {block_name} entry(ies) use the "
            f"frozen SQL path — {SQL_CHECK_FREEZE_POINTER}",
            DeprecationWarning,
            stacklevel=2,
        )
    return gates


_ALLOWED_GATE_KEYS = {
    "name",
    "type",
    "check_id",
    "predicate_family",
    "confidence_type",
    "candidate_sql",
    "coverage_sql",
    "evaluator_ref",
    "budget_ms",
    "timeout_ms",
    "materialization_chunk_size",
    "materialization_strategy",
    "relation_inputs",
    "scope_frontier",
    "candidate_rule_hash",
    "evaluator_hash",
    "compute_hash",
    "output_schema_hash",
    "renderer_hash",
    "dirty_rule_hash",
    "coverage_contract",
    "severity_policy",
    "check_kind",
    "backpressure",
    "block_on",
    "description",
}
_BLOCK_ON_POLICIES = {"any_open", "new_findings", "never"}
_BLOCK_ON_MIN_SEVERITIES = {"low", "medium", "high"}
_CONFIDENCE_TYPES = {
    "deterministic_boolean",
    "calibrated_probability",
    "thresholded_score",
    "ordinal_label",
}
_MATERIALIZATION_STRATEGIES = {"chunked_python", "sql_native"}


def _parse_sql_gate(raw: Any, *, where: str) -> SqlConsistencyGateSpec:
    if not isinstance(raw, dict):
        raise ConsistencyGateLoadError(
            f"{where}: gate entry must be a mapping",
            data={"where": where},
        )
    extras = set(raw) - _ALLOWED_GATE_KEYS
    if extras:
        raise ConsistencyGateLoadError(
            f"{where}: unknown keys {sorted(extras)}",
            data={"where": where, "extras": sorted(extras)},
        )
    gate_type = raw.get("type", "deterministic_sql")
    if gate_type not in {"deterministic_sql", "sql"}:
        raise ConsistencyGateLoadError(
            f"{where}: unsupported consistency gate type {gate_type!r}",
            data={"where": where, "type": gate_type},
            fix_hint="use type: deterministic_sql",
        )
    confidence_type = _required_str(raw, "confidence_type", where=where)
    if confidence_type not in _CONFIDENCE_TYPES:
        raise ConsistencyGateLoadError(
            f"{where}: unsupported confidence_type {confidence_type!r}",
            data={"where": where, "confidence_type": confidence_type},
        )
    candidate_sql = _required_str(raw, "candidate_sql", where=where)
    coverage_sql = _required_str(raw, "coverage_sql", where=where)
    _reject_write_sql(candidate_sql, field="candidate_sql", where=where)
    _reject_write_sql(coverage_sql, field="coverage_sql", where=where)
    relation_inputs = _parse_relation_inputs(raw, where=where)
    _validate_relation_placeholders(
        candidate_sql,
        coverage_sql,
        relation_inputs=relation_inputs,
        where=where,
    )
    timeout_ms = _positive_int(
        raw.get("timeout_ms", DEFAULT_SQL_GATE_TIMEOUT_MS),
        field="timeout_ms",
        where=where,
    )
    budget_ms = _positive_int(
        raw.get("budget_ms", timeout_ms),
        field="budget_ms",
        where=where,
    )
    materialization_chunk_size = _positive_int(
        raw.get(
            "materialization_chunk_size",
            DEFAULT_SQL_GATE_MATERIALIZATION_CHUNK_SIZE,
        ),
        field="materialization_chunk_size",
        where=where,
    )
    materialization_strategy = str(
        raw.get("materialization_strategy") or "chunked_python"
    )
    if materialization_strategy not in _MATERIALIZATION_STRATEGIES:
        raise ConsistencyGateLoadError(
            f"{where}: unsupported materialization_strategy "
            f"{materialization_strategy!r}",
            data={
                "where": where,
                "materialization_strategy": materialization_strategy,
            },
            fix_hint=(
                "use materialization_strategy: chunked_python "
                "or materialization_strategy: sql_native"
            ),
        )
    backpressure = _parse_backpressure(raw, where=where)
    scope_frontier = _parse_scope_frontier(raw, where=where)
    block_on = _parse_block_on(raw, where=where)
    check_kind = _parse_check_kind(raw, where=where)
    return SqlConsistencyGateSpec(
        name=_required_str(raw, "name", where=where),
        check_id=_required_str(raw, "check_id", where=where),
        predicate_family=_required_str(
            raw,
            "predicate_family",
            where=where,
        ),
        confidence_type=cast(ConfidenceType, confidence_type),
        candidate_sql=candidate_sql,
        coverage_sql=coverage_sql,
        evaluator_ref=str(raw.get("evaluator_ref") or SQL_GATE_EVALUATOR_REF),
        budget_ms=budget_ms,
        timeout_ms=timeout_ms,
        materialization_chunk_size=materialization_chunk_size,
        materialization_strategy=cast(
            MaterializationStrategy,
            materialization_strategy,
        ),
        relation_inputs=relation_inputs,
        scope_frontier=scope_frontier,
        health_lookback=backpressure["lookback"],
        max_consecutive_blocked=backpressure["max_consecutive_blocked"],
        backpressure_action=cast(BackpressureAction, backpressure["action"]),
        backpressure_probe_every=backpressure["probe_every"],
        candidate_rule_hash=_optional_str(raw, "candidate_rule_hash"),
        evaluator_hash=_optional_str(raw, "evaluator_hash"),
        compute_hash=_optional_str(raw, "compute_hash"),
        output_schema_hash=_optional_str(raw, "output_schema_hash"),
        renderer_hash=_optional_str(raw, "renderer_hash"),
        dirty_rule_hash=_optional_str(raw, "dirty_rule_hash"),
        coverage_contract=_mapping(raw, "coverage_contract"),
        severity_policy=_mapping(raw, "severity_policy"),
        block_on=block_on["policy"],
        block_on_min_severity=block_on["min_severity"],
        check_kind=check_kind,
    )


def _parse_relation_inputs(
    raw: Mapping[str, Any],
    *,
    where: str,
) -> dict[str, SqlGateRelationInput]:
    value = raw.get("relation_inputs")
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ConsistencyGateLoadError(
            f"{where}: 'relation_inputs' must be a mapping",
            data={"where": where, "field": "relation_inputs"},
        )
    out: dict[str, SqlGateRelationInput] = {}
    allowed = {"view_id", "slot_name", "definition_id"}
    for raw_alias, raw_spec in value.items():
        alias = str(raw_alias)
        _validate_relation_alias(alias, where=where)
        if not isinstance(raw_spec, Mapping):
            raise ConsistencyGateLoadError(
                f"{where}: relation input {alias!r} must be a mapping",
                data={"where": where, "field": f"relation_inputs.{alias}"},
            )
        extras = set(raw_spec) - allowed
        if extras:
            raise ConsistencyGateLoadError(
                f"{where}: relation input {alias!r} has unknown keys "
                f"{sorted(extras)}",
                data={
                    "where": where,
                    "field": f"relation_inputs.{alias}",
                    "extras": sorted(extras),
                },
            )
        out[alias] = SqlGateRelationInput(
            view_id=_required_str(
                raw_spec,
                "view_id",
                where=f"{where}:relation_inputs.{alias}",
            ),
            slot_name=_required_str(
                raw_spec,
                "slot_name",
                where=f"{where}:relation_inputs.{alias}",
            ),
            definition_id=_optional_str(raw_spec, "definition_id"),
        )
    return out


def _parse_scope_frontier(
    raw: Mapping[str, Any],
    *,
    where: str,
) -> SqlGateScopeFrontier | None:
    value = raw.get("scope_frontier")
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ConsistencyGateLoadError(
            f"{where}: 'scope_frontier' must be a mapping",
            data={"where": where, "field": "scope_frontier"},
        )
    allowed = {
        "source",
        "cursor_subject_ids_field",
        "require_scoped_cursor",
    }
    extras = set(value) - allowed
    if extras:
        raise ConsistencyGateLoadError(
            f"{where}: unknown scope_frontier keys {sorted(extras)}",
            data={"where": where, "field": "scope_frontier", "extras": sorted(extras)},
        )
    source = _required_str(
        value,
        "source",
        where=f"{where}:scope_frontier",
    )
    cursor_subject_ids_field = str(
        value.get("cursor_subject_ids_field")
        or "scoped_subject_node_ids"
    )
    if not cursor_subject_ids_field.strip():
        raise ConsistencyGateLoadError(
            f"{where}: scope_frontier.cursor_subject_ids_field must be non-empty",
            data={
                "where": where,
                "field": "scope_frontier.cursor_subject_ids_field",
            },
        )
    return SqlGateScopeFrontier(
        source=source,
        cursor_subject_ids_field=cursor_subject_ids_field,
        require_scoped_cursor=_bool(
            value.get("require_scoped_cursor", True)
        ),
    )


def _parse_backpressure(
    raw: Mapping[str, Any],
    *,
    where: str,
) -> dict[str, Any]:
    policy = _mapping(raw, "backpressure")
    allowed = {"lookback", "max_consecutive_blocked", "action", "probe_every"}
    extras = set(policy) - allowed
    if extras:
        raise ConsistencyGateLoadError(
            f"{where}: unknown backpressure keys {sorted(extras)}",
            data={"where": where, "extras": sorted(extras)},
        )
    action = str(policy.get("action") or "block_without_evaluator")
    if action not in {"block_without_evaluator", "run"}:
        raise ConsistencyGateLoadError(
            f"{where}: unsupported backpressure action {action!r}",
            data={"where": where, "action": action},
            fix_hint="use action: block_without_evaluator or action: run",
        )
    # probe_every is optional: absent => resolve at runtime from env/default
    # (see DeterministicSqlConsistencyGate._probe_every). Declared => a positive
    # int, validated loudly at load like the other backpressure knobs.
    probe_every_raw = policy.get("probe_every")
    probe_every = (
        None
        if probe_every_raw is None
        else _positive_int(
            probe_every_raw,
            field="backpressure.probe_every",
            where=where,
        )
    )
    return {
        "lookback": _positive_int(
            policy.get("lookback", 20),
            field="backpressure.lookback",
            where=where,
        ),
        "max_consecutive_blocked": _positive_int(
            policy.get("max_consecutive_blocked", 2),
            field="backpressure.max_consecutive_blocked",
            where=where,
        ),
        "action": action,
        "probe_every": probe_every,
    }


def _parse_check_kind(raw: Mapping[str, Any], *, where: str) -> str | None:
    """Parse + validate the optional check_kind taxonomy key.

    None (absent) is permitted -- the check registers legacy/NULL, auditably.
    An invalid kind fails LOUDLY at load (a ConsistencyGateLoadError, not the
    raw ValueError ConsistencyCheckDefinition.__post_init__ would raise later).
    """

    value = raw.get("check_kind")
    if value is None:
        return None
    if not isinstance(value, str) or value not in CHECK_KINDS:
        raise ConsistencyGateLoadError(
            f"{where}: unsupported check_kind {value!r}",
            data={"where": where, "check_kind": value},
            fix_hint=f"use one of {sorted(CHECK_KINDS)} or omit check_kind",
        )
    return value


def _parse_block_on(raw: Mapping[str, Any], *, where: str) -> dict[str, Any]:
    """Parse the declarative blocking policy (consistency-blocking-policy).

    Accepts ``block_on`` as either a bare string (``any_open`` — the default
    when absent — / ``new_findings`` / ``never``) or a mapping
    ``{policy: new_findings, min_severity: <low|medium|high>}`` (``min_severity``
    optional). ``min_severity`` is only meaningful with ``new_findings``.
    """

    value = raw.get("block_on")
    if value is None:
        return {"policy": "any_open", "min_severity": None}

    if isinstance(value, str):
        policy = value.strip()
        min_severity: str | None = None
    elif isinstance(value, Mapping):
        extras = set(value) - {"policy", "min_severity"}
        if extras:
            raise ConsistencyGateLoadError(
                f"{where}: unknown block_on keys {sorted(extras)}",
                data={"where": where, "field": "block_on", "extras": sorted(extras)},
            )
        policy = str(_required_value(value, "policy", where=f"{where}:block_on")).strip()
        raw_min_severity = value.get("min_severity")
        min_severity = (
            None if raw_min_severity is None else str(raw_min_severity).strip()
        )
    else:
        raise ConsistencyGateLoadError(
            f"{where}: 'block_on' must be a string or a mapping",
            data={"where": where, "field": "block_on"},
        )

    if policy not in _BLOCK_ON_POLICIES:
        raise ConsistencyGateLoadError(
            f"{where}: unsupported block_on policy {policy!r}",
            data={"where": where, "field": "block_on", "policy": policy},
            fix_hint="use block_on: any_open | new_findings | never",
        )
    if min_severity is not None:
        if policy != "new_findings":
            raise ConsistencyGateLoadError(
                f"{where}: block_on.min_severity is only allowed with "
                "policy: new_findings",
                data={"where": where, "field": "block_on.min_severity", "policy": policy},
                fix_hint="drop min_severity or set block_on.policy: new_findings",
            )
        if min_severity not in _BLOCK_ON_MIN_SEVERITIES:
            raise ConsistencyGateLoadError(
                f"{where}: unsupported block_on.min_severity {min_severity!r}",
                data={
                    "where": where,
                    "field": "block_on.min_severity",
                    "min_severity": min_severity,
                },
                fix_hint="use min_severity: low | medium | high",
            )
    return {"policy": policy, "min_severity": min_severity}


def _candidate_from_row(row: Mapping[str, Any]) -> ConsistencyCandidate:
    return ConsistencyCandidate(
        input_scope=_required_str(row, "input_scope", where="candidate_sql"),
        blocking_rule_hash=_required_str(
            row,
            "blocking_rule_hash",
            where="candidate_sql",
        ),
        participant_roles={
            str(role): str(anchor)
            for role, anchor in _required_mapping(
                row,
                "participant_roles",
                where="candidate_sql",
            ).items()
        },
        predicate_key=_required_str(row, "predicate_key", where="candidate_sql"),
        generation_policy=_required_str(
            row,
            "generation_policy",
            where="candidate_sql",
        ),
        coverage_status=str(row.get("coverage_status") or "candidate"),
        exclusion_reason=_optional_str(row, "exclusion_reason"),
        candidate_payload=_mapping(row, "candidate_payload"),
        symmetric_predicate=_bool(row.get("symmetric_predicate", False)),
    )


def _row_emits_observation(row: Mapping[str, Any]) -> bool:
    if row.get("emit_observation") is not None:
        return _bool(row.get("emit_observation"))
    return any(
        row.get(field_name) is not None
        for field_name in (
            "finding_event",
            "evaluation_event",
            "normalized_predicate",
            "normalized_values",
        )
    )


def _event_from_row(
    definition: ConsistencyCheckDefinition,
    candidate: ConsistencyCandidate,
    candidate_id: str,
    row: Mapping[str, Any],
) -> ObservationEvent:
    participant_roles = dict(candidate.participant_roles)
    normalized_predicate = _required_str(
        row,
        "normalized_predicate",
        where="candidate_sql",
    )
    normalized_values = _required_mapping(
        row,
        "normalized_values",
        where="candidate_sql",
    )
    finding_event = _optional_str(row, "finding_event")
    evaluation_event = _optional_str(row, "evaluation_event")
    if finding_event is None and evaluation_event is None:
        finding_event = "observed"
    observation_id = observation_id_for(
        definition,
        candidate_id=candidate_id,
        input_scope=candidate.input_scope,
        participant_roles=participant_roles,
        normalized_predicate=normalized_predicate,
        normalized_values=normalized_values,
        source_revision_policy=str(
            row.get("source_revision_policy") or candidate.generation_policy
        ),
        symmetric_predicate=candidate.symmetric_predicate,
    )
    return ObservationEvent(
        observation_id=observation_id,
        candidate_id=candidate_id,
        input_scope=candidate.input_scope,
        predicate_key=candidate.predicate_key,
        normalized_predicate=normalized_predicate,
        normalized_values=normalized_values,
        participant_roles=participant_roles,
        finding_event=cast(Any, finding_event),
        evaluation_event=cast(Any, evaluation_event),
        severity=_optional_str(row, "severity"),
        confidence=row.get("confidence"),
        confidence_type=definition.confidence_type,
        evidence=_mapping(row, "evidence"),
        payload=_mapping(row, "payload"),
        coverage_complete=_bool(row.get("observation_coverage_complete", True)),
        symmetric_predicate=candidate.symmetric_predicate,
    )


def _coverage_from_row(
    row: Mapping[str, Any],
    *,
    candidate_count: int,
    candidate_count_by_block: Mapping[str, int],
) -> CandidateCoverageReport:
    reported_candidate_count = row.get("candidate_count")
    if reported_candidate_count is None:
        reported_candidate_count = candidate_count
    reported_count_by_block = _mapping(row, "candidate_count_by_block")
    if (
        not reported_count_by_block
        and reported_candidate_count
        and int(reported_candidate_count) == candidate_count
    ):
        reported_count_by_block = dict(candidate_count_by_block)
    return CandidateCoverageReport(
        universe_key=_required_str(row, "universe_key", where="coverage_sql"),
        coverage_basis=_required_str(
            row,
            "coverage_basis",
            where="coverage_sql",
        ),
        coverage_complete=_bool(
            _required_value(row, "coverage_complete", where="coverage_sql")
        ),
        eligible_input_count=_required_int(
            row,
            "eligible_input_count",
            where="coverage_sql",
        ),
        candidate_count=int(reported_candidate_count),
        excluded_input_counts={
            str(key): int(value)
            for key, value in _mapping(row, "excluded_input_counts").items()
        },
        missing_text_count=_int(row.get("missing_text_count", 0)),
        missing_embedding_count=_int(row.get("missing_embedding_count", 0)),
        candidate_count_by_block={
            str(key): int(value)
            for key, value in reported_count_by_block.items()
        },
        block_count=_optional_int(row, "block_count"),
        largest_block_size=_optional_int(row, "largest_block_size"),
        p95_block_size=row.get("p95_block_size"),
        all_pairs_upper_bound=_optional_int(row, "all_pairs_upper_bound"),
        reduction_ratio=row.get("reduction_ratio"),
        evaluator_invocation_count=_required_int(
            row,
            "evaluator_invocation_count",
            where="coverage_sql",
        ),
        evaluator_cost=_mapping(row, "evaluator_cost"),
        false_negative_audit=_mapping(row, "false_negative_audit"),
        payload=_mapping(row, "payload"),
    )


def _candidate_count_by_block(
    candidate_rows: list[dict[str, Any]],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in candidate_rows:
        if str(row.get("coverage_status") or "candidate") != "candidate":
            continue
        block = str(row.get("blocking_rule_hash") or "unknown")
        counts[block] = counts.get(block, 0) + 1
    return dict(sorted(counts.items()))


def _install_sql_native_helpers(cur: psycopg.Cursor[Any]) -> None:
    cur.execute(
        """
        CREATE OR REPLACE FUNCTION pg_temp.okg_json_escape(value text)
        RETURNS text
        LANGUAGE plpgsql
        IMMUTABLE
        STRICT
        AS $$
        DECLARE
          out text := '"';
          ch text;
          code int;
          cp int;
          high int;
          low int;
        BEGIN
          FOR idx IN 1..char_length(value) LOOP
            ch := substr(value, idx, 1);
            code := ascii(ch);
            IF ch = '"' THEN
              out := out || chr(92) || '"';
            ELSIF ch = chr(92) THEN
              out := out || chr(92) || chr(92);
            ELSIF ch = chr(8) THEN
              out := out || chr(92) || 'b';
            ELSIF ch = chr(9) THEN
              out := out || chr(92) || 't';
            ELSIF ch = chr(10) THEN
              out := out || chr(92) || 'n';
            ELSIF ch = chr(12) THEN
              out := out || chr(92) || 'f';
            ELSIF ch = chr(13) THEN
              out := out || chr(92) || 'r';
            ELSIF code < 32 THEN
              out := out || chr(92) || 'u' || lpad(to_hex(code), 4, '0');
            ELSIF code < 128 THEN
              out := out || ch;
            ELSIF code <= 65535 THEN
              out := out || chr(92) || 'u' || lpad(to_hex(code), 4, '0');
            ELSE
              cp := code - 65536;
              high := 55296 + (cp / 1024);
              low := 56320 + (cp % 1024);
              out := out || chr(92) || 'u' || lpad(to_hex(high), 4, '0')
                         || chr(92) || 'u' || lpad(to_hex(low), 4, '0');
            END IF;
          END LOOP;
          RETURN out || '"';
        END
        $$;

        CREATE OR REPLACE FUNCTION pg_temp.okg_json_canonical(value jsonb)
        RETURNS text
        LANGUAGE sql
        IMMUTABLE
        AS $$
          SELECT CASE jsonb_typeof(value)
            WHEN 'object' THEN COALESCE(
              (
                SELECT '{' || string_agg(
                  pg_temp.okg_json_escape(key) || ':' ||
                  pg_temp.okg_json_canonical(val),
                  ',' ORDER BY key COLLATE "C"
                ) || '}'
                  FROM jsonb_each(value) AS item(key, val)
              ),
              '{}'
            )
            WHEN 'array' THEN COALESCE(
              (
                SELECT '[' || string_agg(
                  pg_temp.okg_json_canonical(val),
                  ',' ORDER BY ord
                ) || ']'
                  FROM jsonb_array_elements(value) WITH ORDINALITY
                    AS item(val, ord)
              ),
              '[]'
            )
            WHEN 'string' THEN pg_temp.okg_json_escape(value #>> '{}')
            ELSE value::text
          END
        $$;

        CREATE OR REPLACE FUNCTION pg_temp.okg_stable_hash(value jsonb)
        RETURNS text
        LANGUAGE sql
        IMMUTABLE
        AS $$
          SELECT encode(
            sha256(convert_to(pg_temp.okg_json_canonical(value), 'UTF8')),
            'hex'
          )
        $$;

        CREATE OR REPLACE FUNCTION pg_temp.okg_sha256_text(value text)
        RETURNS text
        LANGUAGE sql
        IMMUTABLE
        STRICT
        AS $$
          SELECT encode(sha256(convert_to(value, 'UTF8')), 'hex')
        $$;

        CREATE OR REPLACE FUNCTION pg_temp.okg_next_finding_status(
          current_status text,
          finding_event text,
          coverage_complete boolean
        )
        RETURNS text
        LANGUAGE sql
        IMMUTABLE
        AS $$
          SELECT CASE
            WHEN finding_event IS NULL THEN current_status
            WHEN finding_event = 'resolved' AND NOT coverage_complete THEN NULL
            WHEN current_status = 'absent'
             AND finding_event = 'observed' THEN 'open'
            WHEN current_status = 'open'
             AND finding_event = 'observed' THEN 'open'
            WHEN current_status = 'open'
             AND finding_event = 'resolved' THEN 'resolved'
            WHEN current_status = 'resolved'
             AND finding_event = 'observed' THEN 'open'
            WHEN current_status = 'resolved'
             AND finding_event = 'reopened' THEN 'open'
            WHEN current_status = 'resolved'
             AND finding_event = 'resolved' THEN 'resolved'
            ELSE NULL
          END
        $$;
        """,
    )


def _ensure_sql_native_optional_columns(cur: psycopg.Cursor[Any]) -> None:
    cur.execute(
        """
        ALTER TABLE pg_temp.okg_sql_gate_candidate_rows
          ADD COLUMN IF NOT EXISTS coverage_status text,
          ADD COLUMN IF NOT EXISTS exclusion_reason text,
          ADD COLUMN IF NOT EXISTS candidate_payload jsonb,
          ADD COLUMN IF NOT EXISTS symmetric_predicate boolean,
          ADD COLUMN IF NOT EXISTS emit_observation boolean,
          ADD COLUMN IF NOT EXISTS finding_event text,
          ADD COLUMN IF NOT EXISTS evaluation_event text,
          ADD COLUMN IF NOT EXISTS normalized_predicate text,
          ADD COLUMN IF NOT EXISTS normalized_values jsonb,
          ADD COLUMN IF NOT EXISTS source_revision_policy text,
          ADD COLUMN IF NOT EXISTS severity text,
          ADD COLUMN IF NOT EXISTS confidence numeric(8, 6),
          ADD COLUMN IF NOT EXISTS evidence jsonb,
          ADD COLUMN IF NOT EXISTS payload jsonb,
          ADD COLUMN IF NOT EXISTS observation_coverage_complete boolean
        """,
    )


def _reject_write_sql(sql: str, *, field: str, where: str) -> None:
    hit = sql_contains_writes(sql)
    if hit is not None:
        raise ConsistencyGateLoadError(
            f"{where}: {field} contains write-shaped SQL token {hit!r}",
            data={"where": where, "field": field, "token": hit},
            fix_hint="consistency gate SQL must be read-only SELECT SQL",
        )


def _relation_inputs_hash_payload(
    relation_inputs: Mapping[str, SqlGateRelationInput],
) -> dict[str, Any]:
    return {
        alias: relation_inputs[alias].to_hash_payload()
        for alias in sorted(relation_inputs)
    }


def _scope_frontier_hash_payload(
    scope_frontier: SqlGateScopeFrontier | None,
) -> dict[str, Any] | None:
    if scope_frontier is None:
        return None
    return scope_frontier.to_hash_payload()


def _validate_relation_alias(alias: str, *, where: str) -> None:
    if not _RELATION_ALIAS_RE.match(alias):
        raise ConsistencyGateLoadError(
            f"{where}: invalid relation input alias {alias!r}",
            data={"where": where, "field": "relation_inputs"},
            fix_hint=(
                "use aliases matching [A-Za-z_][A-Za-z0-9_]* so SQL "
                "placeholders are unambiguous"
            ),
        )


def _validate_relation_placeholders(
    candidate_sql: str,
    coverage_sql: str,
    *,
    relation_inputs: Mapping[str, SqlGateRelationInput],
    where: str,
) -> None:
    declared = set(relation_inputs)
    used = set(_RELATION_PLACEHOLDER_RE.findall(candidate_sql))
    used.update(_RELATION_PLACEHOLDER_RE.findall(coverage_sql))
    unknown = used - declared
    if unknown:
        raise ConsistencyGateLoadError(
            f"{where}: relation placeholders reference undeclared inputs "
            f"{sorted(unknown)}",
            data={"where": where, "unknown_relation_inputs": sorted(unknown)},
        )
    unused = declared - used
    if unused:
        raise ConsistencyGateLoadError(
            f"{where}: relation_inputs declared but unused {sorted(unused)}",
            data={"where": where, "unused_relation_inputs": sorted(unused)},
        )


def _render_bound_sql(
    *,
    candidate_sql: str,
    coverage_sql: str,
    bindings: Mapping[str, _SqlGateRelationBinding],
) -> tuple[str, str]:
    rendered_candidate, used_candidate = _replace_relation_placeholders(
        candidate_sql,
        bindings=bindings,
    )
    rendered_coverage, used_coverage = _replace_relation_placeholders(
        coverage_sql,
        bindings=bindings,
    )
    used = used_candidate | used_coverage
    unknown = used - set(bindings)
    if unknown:
        raise ValueError(
            "relation placeholders reference undeclared inputs "
            f"{sorted(unknown)}"
        )
    unused = set(bindings) - used
    if unused:
        raise ValueError(
            f"relation inputs declared but unused {sorted(unused)}"
        )
    _reject_write_sql(
        rendered_candidate,
        field="candidate_sql",
        where="bound consistency gate",
    )
    _reject_write_sql(
        rendered_coverage,
        field="coverage_sql",
        where="bound consistency gate",
    )
    return rendered_candidate, rendered_coverage


def _replace_relation_placeholders(
    sql: str,
    *,
    bindings: Mapping[str, _SqlGateRelationBinding],
) -> tuple[str, set[str]]:
    used: set[str] = set()

    def repl(match: re.Match[str]) -> str:
        alias = match.group(1)
        used.add(alias)
        binding = bindings.get(alias)
        if binding is None:
            return match.group(0)
        return binding.qualified_identifier

    return _RELATION_PLACEHOLDER_RE.sub(repl, sql), used


def _quote_relation_identifier(
    conn: psycopg.Connection[Any],
    *,
    schema_name: str,
    relation_name: str,
) -> str:
    return pg_sql.SQL("{}.{}").format(
        pg_sql.Identifier(schema_name),
        pg_sql.Identifier(relation_name),
    ).as_string(conn)


def _required_storage_str(
    storage_ref: Mapping[str, Any],
    field: str,
    *,
    alias: str,
) -> str:
    value = storage_ref.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"relation input {alias!r} storage_ref missing non-empty "
            f"{field!r}"
        )
    return value


def _relation_target_generation_id(
    storage_ref: Mapping[str, Any],
    *,
    alias: str,
) -> int:
    value = (
        storage_ref.get("target_generation_id")
        or storage_ref.get("upstream_target_generation_id")
    )
    if value is None:
        raise ValueError(
            f"relation input {alias!r} storage_ref missing "
            "'target_generation_id'"
        )
    return int(value)


def _optional_uuid(value: Any, *, alias: str, field: str) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except ValueError as exc:
        raise ValueError(
            f"relation input {alias!r} storage_ref field {field!r} "
            f"is not a UUID"
        ) from exc


def _relation_bindings_to_preflight(
    bindings: Mapping[str, _SqlGateRelationBinding],
) -> dict[str, Any]:
    return {
        alias: bindings[alias].to_preflight()
        for alias in sorted(bindings)
    }


def _relation_dependency_manifest_ids(
    bindings: Mapping[str, _SqlGateRelationBinding],
) -> list[UUID]:
    seen: set[UUID] = set()
    ordered: list[UUID] = []
    for alias in sorted(bindings):
        for manifest_id in bindings[alias].dependency_manifest_ids():
            if manifest_id in seen:
                continue
            seen.add(manifest_id)
            ordered.append(manifest_id)
    return ordered


def _set_run_dependency_manifest_ids(
    conn: psycopg.Connection[Any],
    run_id: UUID,
    dependency_manifest_ids: list[UUID],
) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.derived_artifact_runs
                   SET dependency_manifest_ids = %(dependency_manifest_ids)s
                 WHERE run_id = %(run_id)s
                """,
                {
                    "run_id": run_id,
                    "dependency_manifest_ids": dependency_manifest_ids,
                },
            )


def _fail_run(
    conn: psycopg.Connection[Any],
    run_id: UUID,
    exc: Exception,
) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.derived_artifact_runs
                   SET status = 'failed',
                       error = %(error)s,
                       completed_at = now()
                 WHERE run_id = %(run_id)s
                """,
                {
                    "run_id": run_id,
                    "error": Jsonb(
                        {
                            "type": type(exc).__name__,
                            "message": str(exc),
                        }
                    ),
                },
            )


def _complete_run(
    conn: psycopg.Connection[Any],
    run_id: UUID,
    metrics: Mapping[str, Any],
) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.derived_artifact_runs
                   SET status = 'completed',
                       metrics = %(metrics)s,
                       completed_at = now()
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": run_id, "metrics": Jsonb(dict(metrics))},
            )


def _update_run_phase_timings(
    conn: psycopg.Connection[Any],
    run_id: UUID,
    phase_timings_ms: Mapping[str, int],
) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.derived_artifact_runs
                   SET metrics = jsonb_set(
                         metrics,
                         '{phase_timings_ms}',
                         %(phase_timings_ms)s,
                         true
                       )
                 WHERE run_id = %(run_id)s
                """,
                {
                    "run_id": run_id,
                    "phase_timings_ms": Jsonb(dict(phase_timings_ms)),
                },
            )


def _block_run(
    conn: psycopg.Connection[Any],
    run_id: UUID,
    metrics: Mapping[str, Any],
) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.derived_artifact_runs
                   SET status = 'blocked',
                       metrics = %(metrics)s,
                       completed_at = now()
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": run_id, "metrics": Jsonb(dict(metrics))},
            )


def _is_query_timeout(exc: Exception) -> bool:
    return isinstance(exc, psycopg.errors.QueryCanceled)


def _health_to_dict(health: ConsistencyGateHealth) -> dict[str, Any]:
    return {
        "check_id": health.check_id,
        "gate_name": health.gate_name,
        "total_runs": health.total_runs,
        "p50_elapsed_ms": health.p50_elapsed_ms,
        "p99_elapsed_ms": health.p99_elapsed_ms,
        "timeout_rate": str(health.timeout_rate),
        "error_rate": str(health.error_rate),
        "blocked_rate": str(health.blocked_rate),
        "operational_blocked_rate": str(health.operational_blocked_rate),
        "consecutive_blocked": health.consecutive_blocked,
        "consecutive_operational_blocked": (
            health.consecutive_operational_blocked
        ),
        "max_consecutive_blocked": health.max_consecutive_blocked,
        "degraded": health.degraded,
        "degraded_reason": health.degraded_reason,
        "probe_every": health.probe_every,
        "short_circuits_since_probe": health.short_circuits_since_probe,
        "should_probe": health.should_probe,
        "probe_run_count": health.probe_run_count,
        "probe_pass_count": health.probe_pass_count,
        "probe_block_count": health.probe_block_count,
    }


def _required_str(
    raw: Mapping[str, Any],
    key: str,
    *,
    where: str,
) -> str:
    value = _required_value(raw, key, where=where)
    if not isinstance(value, str) or not value.strip():
        raise ConsistencyGateLoadError(
            f"{where}: {key!r} must be a non-empty string",
            data={"where": where, "field": key},
        )
    return value


def _optional_str(raw: Mapping[str, Any], key: str) -> str | None:
    value = raw.get(key)
    if value is None:
        return None
    return str(value)


def _required_mapping(
    raw: Mapping[str, Any],
    key: str,
    *,
    where: str,
) -> dict[str, Any]:
    value = _required_value(raw, key, where=where)
    if not isinstance(value, Mapping):
        raise ConsistencyGateLoadError(
            f"{where}: {key!r} must be a mapping",
            data={"where": where, "field": key},
        )
    return dict(value)


def _mapping(raw: Mapping[str, Any], key: str) -> dict[str, Any]:
    value = raw.get(key)
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ConsistencyGateLoadError(
            f"{key!r} must be a mapping",
            data={"field": key},
        )
    return dict(value)


def _required_value(raw: Mapping[str, Any], key: str, *, where: str) -> Any:
    if key not in raw or raw[key] is None:
        raise ConsistencyGateLoadError(
            f"{where}: missing required field {key!r}",
            data={"where": where, "field": key},
        )
    return raw[key]


def _positive_int(value: Any, *, field: str, where: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ConsistencyGateLoadError(
            f"{where}: {field!r} must be a positive integer",
            data={"where": where, "field": field},
        ) from exc
    if result <= 0:
        raise ConsistencyGateLoadError(
            f"{where}: {field!r} must be positive",
            data={"where": where, "field": field},
        )
    return result


def _required_int(
    raw: Mapping[str, Any],
    key: str,
    *,
    where: str,
) -> int:
    return _int(_required_value(raw, key, where=where))


def _optional_int(raw: Mapping[str, Any], key: str) -> int | None:
    value = raw.get(key)
    if value is None:
        return None
    return _int(value)


def _int(value: Any) -> int:
    if isinstance(value, Decimal):
        return int(value)
    return int(value)


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "t", "1", "yes", "y"}:
            return True
        if lowered in {"false", "f", "0", "no", "n"}:
            return False
    return bool(value)
