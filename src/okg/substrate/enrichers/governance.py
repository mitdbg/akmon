"""Governance helpers for publisher-run enricher judgments.

This module layers maintained-derivation judgment governance around the existing
enricher output boundary. It does not invoke enrichers and does not replace their
``edge_facts`` / ``project_new_edges`` materialization path.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import json
import uuid
from typing import Any

from okg.substrate.derived_artifacts.derivation_registry import (
    DeclaredDeps,
    DerivationRegistry,
    DerivationRow,
    DeterminismClass,
    ExecutionPhase,
    FailureMode,
    GovernanceProfile,
    Materialization,
    OverridePath,
)
from okg.substrate.derived_artifacts.maintained_derivation import (
    ChronosRegisteredTable,
    DerivationSpec,
    MaintainResult,
    ProjectionParityOracle,
    SentinelSpec,
)
from okg.substrate.enrichers.lineage import (
    declare_enricher_dependencies,
    enricher_derivation_id,
)


ENRICHER_GOVERNANCE_SCHEMA = "okg.enricher_judgment_governance.v1"
ENRICHER_PROJECTION_DELTA_SCHEMA = "okg.enricher_projection_deltas.v1"
ENRICHER_GOVERNANCE_STATUS_AVAILABLE = "available"
ENRICHER_GOVERNANCE_STATUS_UNAVAILABLE = "unavailable"
ENRICHER_FAILED_FALLBACK = "enricher_failed"
ENRICHER_SUPERSESSION_REASON = "enricher_supersession"
_DEFAULT_MATERIALIZATION = {
    "kind": "edge_facts",
    "projection": "project_new_edges",
}


@dataclass(frozen=True)
class EnricherSupersessionOutcome:
    projected_edges: dict[tuple[Any, ...], tuple[Any, ...]]
    projection_deltas: dict[str, Any]


def enricher_projection_parity_oracle() -> ProjectionParityOracle:
    """Return the judgment oracle for enricher output maintenance.

    The oracle reprojects persisted/frozen edge facts. It never calls the
    original producer, so nondeterministic enrichers can be replayed without
    re-paying or re-sampling the model.
    """

    return ProjectionParityOracle(
        lambda frozen, _checkout: reproject_frozen_edge_facts(frozen),
        key_columns=["edge_id"],
    )


def reproject_frozen_edge_facts(
    frozen_output: Mapping[str, Any] | Sequence[Mapping[str, Any]],
) -> dict[tuple[Any, ...], tuple[Any, ...]]:
    """Project active graph edges from frozen enricher ``edge_facts``.

    ``frozen_output`` accepts either a bare edge-fact sequence or a mapping with
    ``edge_facts``, optional ``superseded_dedupe_keys``, and optional
    ``replacement_edge_facts``. Supersession is applied to the frozen artifact;
    the producer itself is not invoked.
    """

    edge_facts, superseded, replacements = _normalize_frozen_output(
        frozen_output,
    )
    projected: dict[tuple[Any, ...], tuple[Any, ...]] = {}
    for raw in _ordered_edge_facts(edge_facts):
        fact = dict(raw)
        projection_key = _edge_projection_key(fact)
        if str(fact.get("op") or "I").upper() == "R":
            projected.pop(projection_key, None)
            continue
        if _edge_dedupe_key(fact)[0] in superseded:
            continue
        projected[projection_key] = _edge_value(fact)
    for raw in replacements:
        fact = dict(raw)
        if str(fact.get("op") or "I").upper() == "R":
            continue
        projected[_edge_projection_key(fact)] = _edge_value(fact)
    return projected


def supersede_enricher_edge_fact(
    frozen_edge_facts: Sequence[Mapping[str, Any]],
    *,
    target_dedupe_key: str,
    enricher_name: str,
    generation_id: int,
    decision_id: str | None = None,
    replacement_edge_fact: Mapping[str, Any] | None = None,
) -> EnricherSupersessionOutcome:
    """Supersede one frozen enricher edge and emit downstream deltas."""

    before = reproject_frozen_edge_facts(frozen_edge_facts)
    target_fact = _find_edge_fact(frozen_edge_facts, target_dedupe_key)
    after = reproject_frozen_edge_facts(
        {
            "edge_facts": list(frozen_edge_facts),
            "superseded_dedupe_keys": [target_dedupe_key],
            "replacement_edge_facts": (
                [replacement_edge_fact] if replacement_edge_fact else []
            ),
        }
    )
    before_counts = count_projected_edges_by_type(before)
    after_counts = count_projected_edges_by_type(after)
    edge_count_delta = {
        edge_type: after_counts.get(edge_type, 0) - before_counts.get(edge_type, 0)
        for edge_type in sorted(set(before_counts) | set(after_counts))
        if after_counts.get(edge_type, 0) != before_counts.get(edge_type, 0)
    }
    target_edge_id = _edge_projection_key(target_fact)[0]
    invalidations = [
        {
            "dependency_kind": "graph_edge",
            "dependency_ref": str(target_edge_id),
            "dirty_reason": ENRICHER_SUPERSESSION_REASON,
        }
    ]
    if decision_id:
        invalidations.append(
            {
                "dependency_kind": "decision",
                "dependency_ref": decision_id,
                "dirty_reason": ENRICHER_SUPERSESSION_REASON,
            }
        )
    projection_deltas = {
        "schema": ENRICHER_PROJECTION_DELTA_SCHEMA,
        "source": "enricher_supersession",
        "enricher_name": str(enricher_name),
        "generation_id": int(generation_id),
        "target_dedupe_key": target_dedupe_key,
        "superseded_dedupe_keys": [target_dedupe_key],
        "replacement_dedupe_keys": (
            [_edge_dedupe_key(dict(replacement_edge_fact))[0]]
            if replacement_edge_fact
            else []
        ),
        "affected_edge_types": [str(target_fact.get("edge_type"))],
        "edges_retracted": 1,
        "edges_added": 1 if replacement_edge_fact else 0,
        "edge_id": target_edge_id,
        "edge_count_delta": edge_count_delta,
        "lineage_invalidations": invalidations,
    }
    return EnricherSupersessionOutcome(
        projected_edges=after,
        projection_deltas=projection_deltas,
    )


def count_projected_edges_by_type(
    projected_edges: Mapping[tuple[Any, ...], tuple[Any, ...]],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in projected_edges.values():
        edge_type = str(value[1])
        counts[edge_type] = counts.get(edge_type, 0) + 1
    return counts


def apply_edge_count_projection_delta(
    prior_counts: Mapping[str, int],
    projection_deltas: Mapping[str, Any],
) -> dict[str, int]:
    counts = {str(k): int(v) for k, v in prior_counts.items()}
    for edge_type, delta in dict(
        projection_deltas.get("edge_count_delta") or {},
    ).items():
        next_value = counts.get(str(edge_type), 0) + int(delta)
        if next_value:
            counts[str(edge_type)] = next_value
        else:
            counts.pop(str(edge_type), None)
    return counts


def prepare_enricher_supersession(**kwargs: Any) -> dict[str, Any]:
    """Prepare a serializable per-output override request."""

    return dict(kwargs)


def commit_enricher_supersession(**kwargs: Any) -> EnricherSupersessionOutcome:
    """Commit a prepared per-output override over frozen edge facts."""

    return supersede_enricher_edge_fact(**kwargs)


def enricher_judgment_derivation_row(enricher: Any) -> DerivationRow:
    """Build the registry row that admits one enricher as a judgment."""

    declaration = declare_enricher_dependencies(enricher)
    dependency_kinds = declaration.kinds or frozenset({"component_hash"})
    sentinel = SentinelSpec(
        pk_columns=["dedupe_key"],
        cadence_column="governance_generation",
    )
    spec = DerivationSpec(
        derivation_id=enricher_derivation_id(enricher),
        sentinel=sentinel,
        oracle=enricher_projection_parity_oracle(),
        force_full_predicates=[],
        full_recount=_noop_maintain,
        incremental=_noop_maintain,
        self_heal_every=25,
    )
    return DerivationRow(
        spec=spec,
        declared_deps=DeclaredDeps(kinds=frozenset(dependency_kinds)),
        materializations=[
            Materialization(
                kind="graph_edges",
                storage=ChronosRegisteredTable(
                    table="okg.graph_edges",
                    ensure_fn=lambda _store: None,
                ),
                sentinel=sentinel,
            )
        ],
        governance=GovernanceProfile(
            determinism=DeterminismClass.JUDGMENT,
            override=OverridePath(
                prepare=prepare_enricher_supersession,
                commit=commit_enricher_supersession,
            ),
        ),
        execution_phase=ExecutionPhase.PRE_CHECKPOINT,
        failure_mode=FailureMode.NON_GATING,
    )


def validate_enricher_judgment_registration(enricher: Any) -> dict[str, Any]:
    """Validate and describe one enricher's registry judgment admission."""

    row = enricher_judgment_derivation_row(enricher)
    registry = DerivationRegistry()
    registry.register(row)
    return _registry_row_payload(row)


def merge_enricher_projection_deltas(
    base_projection_deltas: Mapping[str, Any],
    *projection_delta_payloads: Mapping[str, Any],
    enricher_runs: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Merge enricher supersession deltas into the common checkpoint receipt."""

    merged = dict(base_projection_deltas)
    invalidations = list(merged.get("lineage_invalidations") or [])
    edge_count_delta = {
        str(key): int(value)
        for key, value in dict(merged.get("edge_count_delta") or {}).items()
    }
    payloads: list[dict[str, Any]] = []
    for payload in projection_delta_payloads:
        if not isinstance(payload, Mapping):
            continue
        _merge_one_projection_delta(
            payload,
            invalidations=invalidations,
            edge_count_delta=edge_count_delta,
            payloads=payloads,
        )
    for run in enricher_runs:
        if not isinstance(run, Mapping):
            continue
        record = run.get("governance_record")
        if not isinstance(record, Mapping):
            continue
        payload = record.get("projection_deltas")
        if isinstance(payload, Mapping):
            _merge_one_projection_delta(
                payload,
                invalidations=invalidations,
                edge_count_delta=edge_count_delta,
                payloads=payloads,
            )
    if invalidations:
        merged["lineage_invalidations"] = invalidations
    if edge_count_delta:
        merged["edge_count_delta"] = edge_count_delta
    if payloads:
        merged["enricher_projection_deltas"] = payloads
    return merged


def enricher_governance_summary(
    enricher_runs: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Build a durable checkpoint summary for enricher governance records."""

    records = [
        dict(run["governance_record"])
        for run in enricher_runs
        if isinstance(run, Mapping)
        and isinstance(run.get("governance_record"), Mapping)
    ]
    status_counts: dict[str, int] = {}
    for record in records:
        status = str(record.get("governance_status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
    return {
        "schema": ENRICHER_GOVERNANCE_SCHEMA,
        "record_count": len(records),
        "status_counts": status_counts,
        "records": records,
    }


def record_enricher_governance_available(
    *,
    enricher_name: str,
    generation_id: int,
    n_edges_emitted: int,
    n_edges_skipped: int = 0,
    lineage_envelope: Mapping[str, Any] | None = None,
    registry_row: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    lineage = dict(lineage_envelope or {})
    return {
        "schema": ENRICHER_GOVERNANCE_SCHEMA,
        "enricher_name": str(enricher_name),
        "derivation_id": f"_enricher:{enricher_name}",
        "generation_id": int(generation_id),
        "determinism": "judgment",
        "parity": "projection",
        "governance_status": ENRICHER_GOVERNANCE_STATUS_AVAILABLE,
        "output_state": "maintained",
        "materialization": dict(
            lineage.get("materialization") or _DEFAULT_MATERIALIZATION,
        ),
        "registry_status": "admitted",
        "registry_row": dict(registry_row or {}),
        "edge_counts": {
            "emitted": int(n_edges_emitted),
            "skipped": int(n_edges_skipped),
        },
        "lineage_status": lineage.get("lineage_status"),
        "fallback_reason": lineage.get("fallback_reason"),
    }


def record_enricher_governance_failure(
    *,
    enricher_name: str,
    generation_id: int,
    failure_reason: str,
    lineage_envelope: Mapping[str, Any] | None = None,
    registry_row: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    lineage = dict(lineage_envelope or {})
    return {
        "schema": ENRICHER_GOVERNANCE_SCHEMA,
        "enricher_name": str(enricher_name),
        "derivation_id": f"_enricher:{enricher_name}",
        "generation_id": int(generation_id),
        "determinism": "judgment",
        "parity": "projection",
        "governance_status": ENRICHER_GOVERNANCE_STATUS_UNAVAILABLE,
        "fallback_reason": ENRICHER_FAILED_FALLBACK,
        "failure_reason": str(failure_reason),
        "output_state": "unavailable",
        "publish_behavior": "record_and_continue",
        "materialization": dict(
            lineage.get("materialization") or _DEFAULT_MATERIALIZATION,
        ),
        "registry_status": "admitted" if registry_row else "unavailable",
        "registry_row": dict(registry_row or {}),
        "lineage_status": lineage.get("lineage_status"),
        "lineage_fallback_reason": lineage.get("fallback_reason"),
    }


def _normalize_frozen_output(
    frozen_output: Mapping[str, Any] | Sequence[Mapping[str, Any]],
) -> tuple[
    Sequence[Mapping[str, Any]],
    frozenset[str],
    Sequence[Mapping[str, Any]],
]:
    if isinstance(frozen_output, Mapping):
        edge_facts = frozen_output.get("edge_facts") or ()
        superseded = frozenset(
            str(item) for item in frozen_output.get("superseded_dedupe_keys") or ()
        )
        replacements = frozen_output.get("replacement_edge_facts") or ()
        return edge_facts, superseded, replacements
    return frozen_output, frozenset(), ()


def _find_edge_fact(
    edge_facts: Sequence[Mapping[str, Any]],
    dedupe_key: str,
) -> Mapping[str, Any]:
    for fact in edge_facts:
        if _edge_dedupe_key(dict(fact))[0] == dedupe_key:
            return fact
    raise ValueError(f"cannot supersede unknown enricher edge {dedupe_key!r}")


def _ordered_edge_facts(
    edge_facts: Sequence[Mapping[str, Any]],
) -> tuple[Mapping[str, Any], ...]:
    facts = tuple(edge_facts)
    if facts and all(
        _positive_fact_id(fact.get("fact_id")) is not None for fact in facts
    ):
        return tuple(
            sorted(facts, key=lambda fact: _positive_fact_id(fact.get("fact_id")))
        )
    return facts


def _noop_maintain(_ctx: Any) -> MaintainResult:
    return MaintainResult([], [], 0, None, "full_recount")


def _registry_row_payload(row: DerivationRow) -> dict[str, Any]:
    return {
        "derivation_id": row.spec.derivation_id,
        "determinism": row.governance.determinism.value,
        "oracle": row.spec.oracle.__class__.__name__,
        "override": row.governance.override is not None,
        "materializations": [
            {
                "kind": materialization.kind,
                "storage_table": materialization.storage.table,
            }
            for materialization in row.materializations
        ],
        "declared_deps": sorted(row.declared_deps.kinds),
        "execution_phase": row.execution_phase.value,
        "failure_mode": row.failure_mode.value,
    }


def _merge_one_projection_delta(
    payload: Mapping[str, Any],
    *,
    invalidations: list[Any],
    edge_count_delta: dict[str, int],
    payloads: list[dict[str, Any]],
) -> None:
    payloads.append(dict(payload))
    invalidations.extend(list(payload.get("lineage_invalidations") or []))
    for edge_type, delta in dict(payload.get("edge_count_delta") or {}).items():
        key = str(edge_type)
        edge_count_delta[key] = edge_count_delta.get(key, 0) + int(delta)
        if edge_count_delta[key] == 0:
            edge_count_delta.pop(key)


def _edge_dedupe_key(fact: Mapping[str, Any]) -> tuple[str]:
    dedupe_key = fact.get("dedupe_key")
    if dedupe_key:
        return (str(dedupe_key),)
    src = fact.get("src_id") or fact.get("src")
    edge_type = fact.get("edge_type")
    dst = fact.get("dst_id") or fact.get("dst")
    if not (src and edge_type and dst):
        raise ValueError("edge fact requires dedupe_key or src_id/edge_type/dst_id")
    return (f"{src}|{edge_type}|{dst}",)


def _edge_projection_key(fact: Mapping[str, Any]) -> tuple[int]:
    edge_id = fact.get("edge_id")
    if edge_id is not None:
        return (int(edge_id),)
    src = fact.get("src_id") or fact.get("src")
    edge_type = fact.get("edge_type")
    dst = fact.get("dst_id") or fact.get("dst")
    if not (src and edge_type and dst):
        raise ValueError("edge fact requires edge_id or src_id/edge_type/dst_id")
    token = uuid.uuid5(uuid.NAMESPACE_URL, f"{src}|{edge_type}|{dst}").int
    return (token & ((1 << 63) - 1),)


def _positive_fact_id(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _edge_value(fact: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        str(fact.get("src_id") or fact.get("src")),
        str(fact.get("edge_type")),
        str(fact.get("dst_id") or fact.get("dst")),
        _json_key(fact.get("attrs") or {}),
        str(fact.get("source") or ""),
        _json_key(fact.get("source_record_id") or {}),
    )


def _json_key(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


__all__ = [
    "ENRICHER_FAILED_FALLBACK",
    "ENRICHER_GOVERNANCE_SCHEMA",
    "ENRICHER_GOVERNANCE_STATUS_AVAILABLE",
    "ENRICHER_GOVERNANCE_STATUS_UNAVAILABLE",
    "ENRICHER_PROJECTION_DELTA_SCHEMA",
    "ENRICHER_SUPERSESSION_REASON",
    "EnricherSupersessionOutcome",
    "apply_edge_count_projection_delta",
    "commit_enricher_supersession",
    "count_projected_edges_by_type",
    "enricher_governance_summary",
    "enricher_judgment_derivation_row",
    "enricher_projection_parity_oracle",
    "merge_enricher_projection_deltas",
    "prepare_enricher_supersession",
    "record_enricher_governance_available",
    "record_enricher_governance_failure",
    "reproject_frozen_edge_facts",
    "supersede_enricher_edge_fact",
    "validate_enricher_judgment_registration",
]
