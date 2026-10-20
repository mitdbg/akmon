"""Checkpoint-pinned, history-preserving retirement of one registered source."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from typing import Any

import psycopg

from okg.substrate.errors import SourceRetirementRefused, SubstrateError


PRODUCER_ID = "system:source_retirement"


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if value is None:
        return {}
    return {"value": value}


@dataclass(frozen=True)
class SourceRetirementPlan:
    deployment_name: str
    branch_name: str
    registry_source_id: str
    ownership_id: str
    source_subject_ids: tuple[str, ...]
    registry_digest: str
    pinned_checkpoint: str
    catalog_version_id: int
    branch_mutation_token: str
    nodes: tuple[dict[str, Any], ...]
    edges: tuple[dict[str, Any], ...]
    derived_support_frontier: tuple[dict[str, Any], ...]
    shared_source_collisions: tuple[dict[str, Any], ...]
    counts: dict[str, int]
    refusal_codes: tuple[str, ...]
    footprint_digest: str
    plan_digest: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> SourceRetirementPlan:
        values = dict(raw)
        for key in (
            "source_subject_ids",
            "nodes",
            "edges",
            "derived_support_frontier",
            "shared_source_collisions",
            "refusal_codes",
        ):
            values[key] = tuple(values.get(key) or [])
        return cls(**values)


@dataclass(frozen=True)
class SourceRetirementResult:
    pass_name: str
    applied: bool
    affected_count: int
    details: dict[str, Any]


def _unregistered_rerun_blocked_source(
    conn: psycopg.Connection,
    deployment_name: str,
    requested_aliases: set[str],
) -> str | None:
    """The ownership id of a rerun-blocked source named by ``requested_aliases``."""
    from okg.substrate.ingest.run_base import rerun_required

    names = sorted({
        str(row["source"]) for row in rerun_required(conn, deployment=deployment_name)
        if str(row["source"]) in requested_aliases
    })
    if len(names) != 1:
        return None
    return names[0]


def _registry_identity(
    deployment_name: str,
    requested_source: str,
    *,
    conn: psycopg.Connection | None = None,
) -> tuple[str, dict[str, Any], tuple[str, ...], str]:
    from okg.substrate.mcp.source_admin import _load_registry_yaml

    path, raw = _load_registry_yaml(deployment_name)
    sources = raw.get("sources") or {}
    if not isinstance(sources, Mapping):
        raise SubstrateError("source registry sources must be a mapping")

    requested = str(requested_source).strip()
    requested_aliases = {requested, requested.replace("_", "-"), requested.replace("-", "_")}
    matches: list[tuple[str, dict[str, Any], set[str]]] = []
    all_subjects: dict[str, set[str]] = {}
    for name, value in sources.items():
        if not isinstance(value, Mapping):
            continue
        entry = dict(value)
        subjects = {str(name).strip()}
        ownership = str(entry.get("ownership_id") or "").strip()
        if ownership:
            subjects.add(ownership)
        admission = entry.get("admission_policy")
        if isinstance(admission, Mapping):
            producer = str(admission.get("producer_id") or "").strip()
            if producer:
                subjects.add(producer)
        all_subjects[str(name)] = subjects
        aliases = set(subjects)
        for subject in tuple(subjects):
            aliases.add(subject.replace("_", "-"))
            aliases.add(subject.replace("-", "_"))
        if aliases & requested_aliases:
            matches.append((str(name), entry, subjects))

    if not matches and conn is not None:
        blocked = _unregistered_rerun_blocked_source(
            conn, deployment_name, requested_aliases,
        )
        if blocked is not None:
            # Issue #2993: a source whose failed run's partial rows were kept
            # (finish-undo --accept-current) blocks every publish until it
            # reruns or is retired. Once its registry entry is gone it can no
            # longer rerun, so retirement is its only exit: identify it by
            # the ownership id the block names, and retire exactly the rows
            # that source wrote.
            return (
                blocked,
                {"ownership_id": blocked},
                (blocked,),
                hashlib.sha256(path.read_bytes()).hexdigest(),
            )
    if not matches:
        raise SourceRetirementRefused(
            f"source {requested_source!r} is not registered for deployment "
            f"{deployment_name!r}",
            data={"registered_sources": sorted(str(name) for name in sources)},
        )
    if len(matches) != 1:
        raise SubstrateError(
            "source identity is ambiguous",
            data={"requested_source": requested_source, "matches": sorted(item[0] for item in matches)},
        )
    source_id, entry, subjects = matches[0]
    collisions = sorted(
        other
        for other, other_subjects in all_subjects.items()
        if other != source_id and subjects & other_subjects
    )
    if collisions:
        raise SubstrateError(
            "source identity overlaps another registry entry",
            data={"source": source_id, "colliding_sources": collisions},
        )
    ownership_id = str(entry.get("ownership_id") or "").strip()
    if not ownership_id:
        raise SubstrateError(
            "source retirement requires a canonical ownership_id",
            data={"source": source_id},
        )
    registry_digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return source_id, entry, tuple(sorted(subjects)), registry_digest


def _catalog_version_id(conn: psycopg.Connection, deployment_name: str) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT catalog_version_id FROM okg.catalog_versions "
            "WHERE status = 'applied' "
            "AND (deployment_name = %s OR deployment_name IS NULL) "
            "ORDER BY (deployment_name = %s) DESC NULLS LAST, "
            "catalog_version_id DESC LIMIT 1",
            (deployment_name, deployment_name),
        )
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def _source_nodes(graph: Any, subjects: tuple[str, ...]) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in graph.query(
            "SELECT node_id, subtype, attrs, source, source_record_id, "
            "source_revision, run_id, observed_at FROM okg.graph_nodes "
            "WHERE source = ANY(:subjects) ORDER BY node_id",
            {"subjects": list(subjects)},
        )
    ]


def _retirement_edges(
    graph: Any,
    subjects: tuple[str, ...],
    node_ids: list[str],
) -> list[dict[str, Any]]:
    if node_ids:
        where = (
            "source = ANY(:subjects) OR src = ANY(:node_ids) "
            "OR dst = ANY(:node_ids)"
        )
    else:
        where = "source = ANY(:subjects)"
    return [
        dict(row)
        for row in graph.query(
            "SELECT edge_id, src, dst, edge_type, attrs, provenance, confidence, "
            "source, source_record_id, source_revision, run_id, observed_at "
            f"FROM okg.graph_edges WHERE {where} ORDER BY edge_id",
            {"subjects": list(subjects), "node_ids": node_ids},
        )
    ]


def _derived_frontier(graph: Any, node_ids: set[str]) -> list[dict[str, Any]]:
    if not node_ids:
        return []
    rows = graph.query(
        "SELECT node_id, subtype, source, source_record_id "
        "FROM okg.graph_nodes "
        "WHERE (source >= :derived_lo AND source < :derived_hi) "
        "OR source_record_id->'derived_from' IS NOT NULL "
        "OR source_record_id->'derived_support' IS NOT NULL ORDER BY node_id",
        {"derived_lo": "_enricher:", "derived_hi": "_enricher;"},
    )
    frontier: list[dict[str, Any]] = []
    from okg.substrate.derived_node_liveness import derived_support_state

    for raw in rows:
        row = dict(raw)
        state = derived_support_state(row)
        members = {str(value) for value in state.get("members") or ()}
        affected = sorted(members & node_ids)
        if affected or str(row.get("node_id") or "") in node_ids:
            frontier.append(
                {
                    "node_id": str(row["node_id"]),
                    "subtype": str(row["subtype"]),
                    "source": row.get("source"),
                    "derived_from": sorted(members),
                    "affected_members": affected,
                    "status": str(state.get("status") or "not_managed"),
                    "support_policy": state.get("policy"),
                }
            )
    return frontier


def _shared_source_collisions(
    nodes: list[dict[str, Any]],
    subjects: tuple[str, ...],
) -> list[dict[str, Any]]:
    from okg.substrate.nomos.source_lineage import LINEAGE_ATTR, source_ids_from_lineage

    allowed = set(subjects)
    collisions: list[dict[str, Any]] = []
    for node in nodes:
        attrs = node.get("attrs")
        lineage = attrs.get(LINEAGE_ATTR) if isinstance(attrs, Mapping) else None
        if lineage is not None and not isinstance(lineage, Mapping):
            collisions.append(
                {"node_id": str(node["node_id"]), "reason": "malformed_lineage"}
            )
            continue
        extra = sorted(set(source_ids_from_lineage(lineage)) - allowed)
        if extra:
            collisions.append(
                {"node_id": str(node["node_id"]), "other_source_ids": extra}
            )
    return collisions


def _plan_from_graph(
    conn: psycopg.Connection,
    graph: Any,
    *,
    deployment_name: str,
    branch_name: str,
    requested_source: str,
    pinned_checkpoint: str,
    catalog_version_id: int,
    branch_mutation_token: str,
) -> SourceRetirementPlan:
    source_id, entry, subjects, registry_digest = _registry_identity(
        deployment_name, requested_source, conn=conn
    )
    nodes = _source_nodes(graph, subjects)
    node_ids = [str(row["node_id"]) for row in nodes]
    edges = _retirement_edges(graph, subjects, node_ids)
    frontier = _derived_frontier(graph, set(node_ids))
    collisions = _shared_source_collisions(nodes, subjects)
    refusal_codes: set[str] = set()
    if collisions:
        refusal_codes.add("shared_source_ownership_unresolved")
    if any(row.get("source") is None for row in [*nodes, *edges]):
        refusal_codes.add("unscoped_retirement_fact")

    footprint = {
        "nodes": nodes,
        "edges": edges,
        "derived_support_frontier": frontier,
        "shared_source_collisions": collisions,
    }
    footprint_digest = _digest(footprint)
    counts = {
        "nodes": len(nodes),
        "edges": len(edges),
        "derived_support_frontier": len(frontier),
        "shared_source_collisions": len(collisions),
    }
    payload = {
        "schema_version": "okg.source_retirement.plan.v1",
        "deployment_name": deployment_name,
        "branch_name": branch_name,
        "registry_source_id": source_id,
        "ownership_id": str(entry["ownership_id"]),
        "source_subject_ids": list(subjects),
        "registry_digest": registry_digest,
        "pinned_checkpoint": pinned_checkpoint,
        "catalog_version_id": catalog_version_id,
        "branch_mutation_token": branch_mutation_token,
        "footprint_digest": footprint_digest,
        "nodes": nodes,
        "edges": edges,
        "derived_support_frontier": frontier,
        "shared_source_collisions": collisions,
        "counts": counts,
        "refusal_codes": sorted(refusal_codes),
    }
    return SourceRetirementPlan(
        **{
            **{key: value for key, value in payload.items() if key != "schema_version"},
            "source_subject_ids": subjects,
            "nodes": tuple(nodes),
            "edges": tuple(edges),
            "derived_support_frontier": tuple(frontier),
            "shared_source_collisions": tuple(collisions),
            "counts": counts,
            "refusal_codes": tuple(sorted(refusal_codes)),
        },
        plan_digest=_digest(payload),
    )


def _combine_retirement_plans(
    prior: SourceRetirementPlan,
    current: SourceRetirementPlan,
) -> SourceRetirementPlan:
    """Carry the full reviewed footprint across an awaiting-publish continuation."""
    nodes_by_id = {
        str(row["node_id"]): dict(row) for row in (*prior.nodes, *current.nodes)
    }
    edges_by_id = {
        int(row["edge_id"]): dict(row) for row in (*prior.edges, *current.edges)
    }
    frontier_by_node = {
        str(row["node_id"]): dict(row)
        for row in (*prior.derived_support_frontier, *current.derived_support_frontier)
    }
    collisions_by_node = {
        str(row["node_id"]): dict(row)
        for row in (*prior.shared_source_collisions, *current.shared_source_collisions)
    }
    nodes = [nodes_by_id[key] for key in sorted(nodes_by_id)]
    edges = [edges_by_id[key] for key in sorted(edges_by_id)]
    frontier = [frontier_by_node[key] for key in sorted(frontier_by_node)]
    collisions = [collisions_by_node[key] for key in sorted(collisions_by_node)]
    refusal_codes = sorted(set(prior.refusal_codes) | set(current.refusal_codes))
    footprint = {
        "nodes": nodes,
        "edges": edges,
        "derived_support_frontier": frontier,
        "shared_source_collisions": collisions,
    }
    footprint_digest = _digest(footprint)
    counts = {
        "nodes": len(nodes),
        "edges": len(edges),
        "derived_support_frontier": len(frontier),
        "shared_source_collisions": len(collisions),
    }
    payload = {
        "schema_version": "okg.source_retirement.plan.v1",
        "deployment_name": current.deployment_name,
        "branch_name": current.branch_name,
        "registry_source_id": current.registry_source_id,
        "ownership_id": current.ownership_id,
        "source_subject_ids": list(current.source_subject_ids),
        "registry_digest": current.registry_digest,
        "pinned_checkpoint": current.pinned_checkpoint,
        "catalog_version_id": current.catalog_version_id,
        "branch_mutation_token": current.branch_mutation_token,
        "footprint_digest": footprint_digest,
        "nodes": nodes,
        "edges": edges,
        "derived_support_frontier": frontier,
        "shared_source_collisions": collisions,
        "counts": counts,
        "refusal_codes": refusal_codes,
    }
    return SourceRetirementPlan(
        **{
            **{key: value for key, value in payload.items() if key != "schema_version"},
            "source_subject_ids": current.source_subject_ids,
            "nodes": tuple(nodes),
            "edges": tuple(edges),
            "derived_support_frontier": tuple(frontier),
            "shared_source_collisions": tuple(collisions),
            "refusal_codes": tuple(refusal_codes),
        },
        plan_digest=_digest(payload),
    )


def _plan_source_retirement_locked(
    conn: psycopg.Connection,
    *,
    source: str,
    deployment_name: str,
    branch_name: str,
) -> SourceRetirementPlan:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.publisher import Publisher

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    latest = store.latest_generation(
        deployment_name=deployment_name,
        branch=branch_name,
        status="published",
    )
    if latest is None:
        raise SubstrateError("source retirement requires a published checkpoint")
    # Issue #2477: pin the plan to the catalog version the apply guard reads
    # (the deployment's latest APPLIED catalog), not the one the last
    # published checkpoint recorded. A failed `add-source` applies a newer
    # catalog and publishes nothing, so the two differ until some publish
    # succeeds; pinning the checkpoint's copy made every apply refuse
    # "catalog version drifted" and replanning could never clear it.
    catalog_version_id = _catalog_version_id(conn, deployment_name) or 0
    if catalog_version_id <= 0:
        raise SubstrateError(
            "source retirement found no applied catalog version for the "
            "deployment",
            data={"deployment": deployment_name},
            fix_hint=(
                f"run `okg catalog load --deployment {deployment_name} "
                "--apply`, then plan again"
            ),
        )
    token = Publisher.chronos_graph_branch_mutation_token(conn, branch_name=branch_name)
    plan = _plan_from_graph(
        conn,
        store.checkout(branch_name),
        deployment_name=deployment_name,
        branch_name=branch_name,
        requested_source=source,
        pinned_checkpoint=str(latest.checkpoint_id),
        catalog_version_id=catalog_version_id,
        branch_mutation_token=token,
    )
    observed = Publisher.chronos_graph_branch_mutation_token(conn, branch_name=branch_name)
    if observed != token:
        raise SubstrateError(
            "source retirement branch changed while planning",
            data={"planned_token": token, "observed_token": observed},
        )
    return plan


def plan_source_retirement(
    conn: psycopg.Connection,
    *,
    source: str,
    deployment_name: str = "default",
    branch_name: str = "default",
) -> SourceRetirementPlan:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    if not store._lock_branch_write_session(branch_name):
        raise SubstrateError(
            "source retirement planning requires PostgreSQL branch fencing",
            data={"branch": branch_name},
        )
    try:
        return _plan_source_retirement_locked(
            conn,
            source=source,
            deployment_name=deployment_name,
            branch_name=branch_name,
        )
    finally:
        store._unlock_branch_write_session(branch_name)


def _edge_fact(row: Mapping[str, Any]):
    from okg.substrate.library.sources.base import EdgeFact

    return EdgeFact(
        src=str(row["src"]),
        dst=str(row["dst"]),
        edge_type=str(row["edge_type"]),
        attrs=dict(row.get("attrs") or {}),
        provenance=str(row.get("provenance") or "explicit"),
        confidence=float(row.get("confidence") or 0.0),
        source_record_id=_json_object(row.get("source_record_id")),
        source_revision=_json_object(row.get("source_revision")),
        op="R",
    )


def _node_fact(row: Mapping[str, Any]):
    from okg.substrate.library.sources.base import NodeFact

    return NodeFact(
        node_id=str(row["node_id"]),
        subtype=str(row["subtype"]),
        attrs={},
        source_record_id=_json_object(row.get("source_record_id")),
        source_revision=_json_object(row.get("source_revision")),
        op="R",
    )


def _assert_plan_applicable(plan: SourceRetirementPlan) -> None:
    if plan.refusal_codes:
        raise SourceRetirementRefused(
            "source retirement plan is blocked before mutation",
            data={
                "refusal_codes": list(plan.refusal_codes),
                "derived_support_frontier": list(plan.derived_support_frontier),
                "shared_source_collisions": list(plan.shared_source_collisions),
                "mutation_started": False,
            },
            fix_hint=(
                "resolve source ownership or unscoped retirement facts, then "
                "generate and review a new plan"
            ),
        )


def _prepare_retirement_intent(
    conn: psycopg.Connection,
    plan: SourceRetirementPlan,
    *,
    actor: str,
) -> str:
    """Commit the reviewed intent before any graph chunk can be applied."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_retirement_receipts
              (deployment_name, branch_name, registry_source_id, ownership_id,
               source_subject_ids, registry_digest, pinned_checkpoint,
               catalog_version_id, branch_mutation_token, footprint_digest,
               plan_digest, plan, actor, status)
            VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s,
                    %s::jsonb, %s, 'planned')
            RETURNING retirement_id
            """,
            (
                plan.deployment_name,
                plan.branch_name,
                plan.registry_source_id,
                plan.ownership_id,
                json.dumps(list(plan.source_subject_ids)),
                plan.registry_digest,
                plan.pinned_checkpoint,
                plan.catalog_version_id,
                plan.branch_mutation_token,
                plan.footprint_digest,
                plan.plan_digest,
                json.dumps(plan.as_dict(), default=str),
                actor,
            ),
        )
        return str(cur.fetchone()[0])


def _replace_planned_retirement_intent(
    conn: psycopg.Connection,
    *,
    retirement_id: str,
    plan: SourceRetirementPlan,
    actor: str,
) -> None:
    """Supersede a mutation-free stale intent with an explicitly reviewed plan."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            UPDATE okg.source_retirement_receipts
            SET ownership_id = %s,
                source_subject_ids = %s::jsonb,
                registry_digest = %s,
                pinned_checkpoint = %s,
                catalog_version_id = %s,
                branch_mutation_token = %s,
                footprint_digest = %s,
                plan_digest = %s,
                plan = %s::jsonb,
                batch_receipts = '[]'::jsonb,
                actor = %s
            WHERE retirement_id = %s AND status = 'planned'
            """,
            (
                plan.ownership_id,
                json.dumps(list(plan.source_subject_ids)),
                plan.registry_digest,
                plan.pinned_checkpoint,
                plan.catalog_version_id,
                plan.branch_mutation_token,
                plan.footprint_digest,
                plan.plan_digest,
                json.dumps(plan.as_dict(), default=str),
                actor,
                retirement_id,
            ),
        )
        if cur.rowcount != 1:
            raise SourceRetirementRefused(
                "planned source retirement changed before it could be refreshed",
                data={
                    "retirement_id": retirement_id,
                    "refusal_codes": ["intent_state_changed"],
                    "mutation_started": False,
                },
            )


def _apply_plan(
    conn: psycopg.Connection,
    plan: SourceRetirementPlan,
    *,
    actor: str,
    retirement_id: str | None = None,
    receipt_plan: SourceRetirementPlan | None = None,
    resumable_status: str = "planned",
    fault_after_graph: bool = False,
) -> dict[str, Any]:
    from okg.substrate.fact_application import apply_bounded_fact_batch
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.publisher import Publisher

    _assert_plan_applicable(plan)
    retirement_id = retirement_id or _prepare_retirement_intent(conn, plan, actor=actor)
    final_plan = receipt_plan or plan
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False, autocommit=False)
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("LOCK TABLE okg.producer_profiles IN SHARE ROW EXCLUSIVE MODE")
            cur.execute(
                "LOCK TABLE okg.catalog_versions, okg.narrowings, "
                "okg.subtypes IN SHARE MODE"
            )
        store._lock_branch_write(plan.branch_name)
        token = Publisher.chronos_graph_branch_mutation_token(
            conn, branch_name=plan.branch_name
        )
        if token != plan.branch_mutation_token:
            raise SubstrateError(
                "source retirement branch changed; replan before mutation",
                data={"mutation_started": False},
            )
        latest = store.latest_generation(
            deployment_name=plan.deployment_name,
            branch=plan.branch_name,
            status="published",
        )
        if latest is None or str(latest.checkpoint_id) != plan.pinned_checkpoint:
            raise SubstrateError(
                "source retirement published checkpoint drifted; replan",
                data={"mutation_started": False},
            )
        current_catalog = _catalog_version_id(conn, plan.deployment_name)
        if current_catalog != plan.catalog_version_id:
            raise SubstrateError(
                "source retirement catalog version drifted; replan",
                data={
                    "mutation_started": False,
                    "planned_catalog_version_id": plan.catalog_version_id,
                    "current_catalog_version_id": current_catalog,
                },
                fix_hint=(
                    "the catalog was applied again after this plan was made; "
                    "run the same command without --apply to plan again"
                ),
            )
        session = store.checkout(plan.branch_name)
        current = _plan_from_graph(
            conn,
            session,
            deployment_name=plan.deployment_name,
            branch_name=plan.branch_name,
            requested_source=plan.registry_source_id,
            pinned_checkpoint=plan.pinned_checkpoint,
            catalog_version_id=plan.catalog_version_id,
            branch_mutation_token=token,
        )
        if current.plan_digest != plan.plan_digest:
            raise SubstrateError(
                "source retirement plan drifted; replan before mutation",
                data={
                    "planned_digest": plan.plan_digest,
                    "current_digest": current.plan_digest,
                    "mutation_started": False,
                },
            )
        observed_after_replan = Publisher.chronos_graph_branch_mutation_token(
            conn, branch_name=plan.branch_name
        )
        if observed_after_replan != token:
            raise SourceRetirementRefused(
                "source retirement branch changed during apply validation",
                data={
                    "planned_token": token,
                    "observed_token": observed_after_replan,
                    "refusal_codes": ["stale_plan"],
                    "mutation_started": False,
                },
                fix_hint="wait for the writer, generate a new plan, and retry",
            )
        _assert_plan_applicable(current)

        # A retired source must stop declaring shared nodes it does not own,
        # or a later retraction by their owner could pass them to it (issue
        # #1901). Withdraw first, so the retractions below cannot hand a
        # retired source's own node to another retired subject either.
        store.withdraw_node_declarations(
            branch=plan.branch_name,
            sources=plan.source_subject_ids,
            session=session,
        )
        # The same for ranked edges (item 92a): each edge the retired source
        # declared passes to its next-ranked declarer, or is deleted when
        # none remains, before the retractions below run.
        store.withdraw_edge_declarations(
            branch=plan.branch_name,
            sources=plan.source_subject_ids,
            session=session,
        )

        facts_by_source: dict[str, list[Any]] = defaultdict(list)
        for edge in plan.edges:
            facts_by_source[str(edge["source"])].append(_edge_fact(edge))
        for node in plan.nodes:
            facts_by_source[str(node["source"])].append(_node_fact(node))
        if not facts_by_source:
            facts_by_source[plan.ownership_id] = []

        batch_receipts: list[dict[str, Any]] = []
        for row_source in sorted(facts_by_source):
            result = apply_bounded_fact_batch(
                conn,
                store,
                facts_by_source[row_source],
                deployment_name=plan.deployment_name,
                producer_id=PRODUCER_ID,
                producer_kind="system",
                intent="source_retirement",
                catalog_version_id=plan.catalog_version_id,
                authority_scope={
                    "pinned_checkpoint": plan.pinned_checkpoint,
                    "plan_digest": plan.plan_digest,
                    "registry_source_id": plan.registry_source_id,
                    "row_source": row_source,
                },
                source=row_source,
                target_branch=plan.branch_name,
                idempotency_key=_digest([plan.plan_digest, row_source]),
                retractions_allowed=True,
                cross_source_retractions=False,
                session=session,
                zero_output_kind="authoritative_empty_snapshot",
                zero_output_reason="retired source has no live graph footprint",
            )
            batch_receipts.append(
                {
                    "source": row_source,
                    "admission_id": result.admission_id,
                    "run_id": result.run_id,
                    "chunk_count": result.summary.terminal_chunk_count,
                }
            )
        if fault_after_graph:
            raise RuntimeError("injected source-retirement fault after graph edits")

        # Issue #2993, option 2: a source whose failed run's partial rows were
        # kept (finish-undo --accept-current) blocks every publish until it
        # reruns. Retiring it removes all of its records -- those rows too --
        # in this same transaction, so the block is lifted with them.
        from okg.substrate.ingest.run_base import clear_rerun_required

        clear_rerun_required(
            conn,
            deployment=plan.deployment_name,
            sources=sorted({plan.ownership_id, *plan.source_subject_ids}),
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.source_retirement_receipts
                SET batch_receipts = batch_receipts || %s::jsonb,
                    status = 'awaiting_publish',
                    applied_at = now(),
                    registry_digest = %s,
                    pinned_checkpoint = %s,
                    catalog_version_id = %s,
                    branch_mutation_token = %s,
                    footprint_digest = %s,
                    plan_digest = %s,
                    plan = %s::jsonb
                WHERE retirement_id = %s AND status = %s
                RETURNING status, applied_at
                """,
                (
                    json.dumps(batch_receipts),
                    final_plan.registry_digest,
                    final_plan.pinned_checkpoint,
                    final_plan.catalog_version_id,
                    final_plan.branch_mutation_token,
                    final_plan.footprint_digest,
                    final_plan.plan_digest,
                    json.dumps(final_plan.as_dict(), default=str),
                    retirement_id,
                    resumable_status,
                ),
            )
            updated = cur.fetchone()
            if updated is None:
                raise SourceRetirementRefused(
                    "source retirement intent is no longer resumable",
                    data={
                        "retirement_id": retirement_id,
                        "refusal_codes": ["intent_state_changed"],
                        "mutation_started": False,
                    },
                )
            status, applied_at = updated
    return {
        "retirement_id": str(retirement_id),
        "status": str(status),
        "applied_at": applied_at.isoformat(),
        "plan_digest": final_plan.plan_digest,
        "batch_receipts": batch_receipts,
    }


def _check_confirm_source(
    confirm_source: str | None,
    *,
    registry_source_id: str,
    ownership_id: str,
) -> None:
    """Accept the registry name (`notes`) or its ownership id (`ws.notes`).

    The plan prints both; requiring only the bare name refused an operator
    who copied the ownership id the plan shows (issue #2477).
    """
    accepted = {registry_source_id}
    if ownership_id:
        accepted.add(ownership_id)
    if str(confirm_source or "").strip() in accepted:
        return
    raise ValueError(
        "source retirement --apply requires --confirm-source naming the "
        f"source: {registry_source_id!r}"
        + (f" or its ownership id {ownership_id!r}" if ownership_id else "")
        + f"; got {confirm_source!r}"
    )


def remediate_source_retirement(
    conn: psycopg.Connection,
    *,
    source_name: str,
    deployment_name: str = "default",
    branch_name: str = "default",
    expected_plan_digest: str | None = None,
    confirm_source: str | None = None,
    actor: str = "operator",
    apply: bool = False,
    _fault_after_graph: bool = False,
) -> SourceRetirementResult:
    canonical_source, _entry, _subjects, _registry_digest = _registry_identity(
        deployment_name, source_name, conn=conn
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT retirement_id, status, plan_digest, repair_generation_id, "
            "registry_source_id, plan FROM okg.source_retirement_receipts "
            "WHERE deployment_name = %s AND branch_name = %s "
            "AND registry_source_id = %s",
            (deployment_name, branch_name, canonical_source),
        )
        prior = cur.fetchone()
    if prior is not None:
        if apply and not expected_plan_digest:
            raise ValueError("source retirement replay --apply requires --expected-plan-digest")
        if apply:
            _check_confirm_source(
                confirm_source,
                registry_source_id=canonical_source,
                ownership_id=str(_entry.get("ownership_id") or ""),
            )
        prior_plan = SourceRetirementPlan.from_dict(dict(prior[5]))
        if str(prior[1]) == "planned":
            current_plan = plan_source_retirement(
                conn,
                source=canonical_source,
                deployment_name=deployment_name,
                branch_name=branch_name,
            )
            if not apply:
                return SourceRetirementResult(
                    "source_retirement",
                    False,
                    len(current_plan.nodes) + len(current_plan.edges),
                    {
                        "plan": current_plan.as_dict(),
                        "retirement_id": str(prior[0]),
                        "status": "planned",
                        "supersedes_plan_digest": (
                            prior_plan.plan_digest
                            if prior_plan.plan_digest != current_plan.plan_digest
                            else None
                        ),
                        "replayed": True,
                    },
                )
            if expected_plan_digest != current_plan.plan_digest:
                raise ValueError(
                    "source retirement plan digest changed; rerun dry-run and review it"
                )
            if prior_plan.plan_digest != current_plan.plan_digest:
                _replace_planned_retirement_intent(
                    conn,
                    retirement_id=str(prior[0]),
                    plan=current_plan,
                    actor=actor,
                )
            receipt = _apply_plan(
                conn,
                current_plan,
                actor=actor,
                retirement_id=str(prior[0]),
                fault_after_graph=_fault_after_graph,
            )
            return SourceRetirementResult(
                "source_retirement",
                True,
                len(current_plan.nodes) + len(current_plan.edges),
                {"plan": current_plan.as_dict(), "receipt": receipt, "replayed": True},
            )
        if str(prior[1]) == "awaiting_publish":
            current_plan = plan_source_retirement(
                conn,
                source=canonical_source,
                deployment_name=deployment_name,
                branch_name=branch_name,
            )
            if current_plan.nodes or current_plan.edges:
                continuation_plan = _combine_retirement_plans(prior_plan, current_plan)
                if not apply:
                    return SourceRetirementResult(
                        "source_retirement",
                        False,
                        len(current_plan.nodes) + len(current_plan.edges),
                        {
                            "plan": continuation_plan.as_dict(),
                            "retirement_id": str(prior[0]),
                            "status": "awaiting_publish",
                            "continuation_required": True,
                            "replayed": True,
                        },
                    )
                if expected_plan_digest != continuation_plan.plan_digest:
                    raise ValueError(
                        "source retirement plan digest changed; rerun dry-run and review it"
                    )
                receipt = _apply_plan(
                    conn,
                    current_plan,
                    actor=actor,
                    retirement_id=str(prior[0]),
                    receipt_plan=continuation_plan,
                    resumable_status="awaiting_publish",
                    fault_after_graph=_fault_after_graph,
                )
                return SourceRetirementResult(
                    "source_retirement",
                    True,
                    len(current_plan.nodes) + len(current_plan.edges),
                    {
                        "plan": continuation_plan.as_dict(),
                        "receipt": receipt,
                        "continuation": True,
                        "replayed": True,
                    },
                )
        if expected_plan_digest and str(prior[2]) != expected_plan_digest:
            raise ValueError("source already has a different applied retirement plan")
        return SourceRetirementResult(
            "source_retirement",
            True,
            0,
            {
                "retirement_id": str(prior[0]),
                "status": str(prior[1]),
                "plan_digest": str(prior[2]),
                "repair_generation_id": prior[3],
                "registry_source_id": str(prior[4]),
                "replayed": True,
            },
        )

    plan = plan_source_retirement(
        conn,
        source=canonical_source,
        deployment_name=deployment_name,
        branch_name=branch_name,
    )
    if plan.deployment_name != deployment_name or plan.branch_name != branch_name:
        raise SourceRetirementRefused(
            "source retirement plan crossed deployment or branch",
            data={
                "refusal_codes": ["cross_deployment_plan"],
                "mutation_started": False,
            },
        )
    if not apply:
        return SourceRetirementResult(
            "source_retirement", False, len(plan.nodes) + len(plan.edges), {"plan": plan.as_dict()}
        )
    if not expected_plan_digest:
        raise ValueError("source retirement --apply requires --expected-plan-digest")
    if expected_plan_digest != plan.plan_digest:
        raise ValueError("source retirement plan digest changed; rerun dry-run and review it")
    _check_confirm_source(
        confirm_source,
        registry_source_id=plan.registry_source_id,
        ownership_id=plan.ownership_id,
    )
    receipt = _apply_plan(
        conn, plan, actor=actor, fault_after_graph=_fault_after_graph
    )
    return SourceRetirementResult(
        "source_retirement",
        True,
        len(plan.nodes) + len(plan.edges),
        {"plan": plan.as_dict(), "receipt": receipt, "replayed": False},
    )


def mark_source_retirement_published(
    conn: psycopg.Connection,
    *,
    retirement_id: str,
    repair_generation_id: str,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore

    # Historical checkout acquires its own durable read lease, so perform the
    # immutable-generation proof outside the receipt row-lock transaction.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT deployment_name, branch_name, status, repair_generation_id, "
            "applied_at, plan "
            "FROM okg.source_retirement_receipts "
            "WHERE retirement_id = %s",
            (retirement_id,),
        )
        receipt = cur.fetchone()
        if receipt is None:
            raise ValueError("source retirement receipt does not exist")
        if str(receipt[2]) == "published":
            if str(receipt[3]) == repair_generation_id:
                return
            raise ValueError(
                "published source retirement receipt is already generation-bound"
            )
        cur.execute(
            "SELECT metadata::jsonb->>'deployment_name', "
            "metadata::jsonb->>'branch', metadata::jsonb->>'status', "
            "created_at::timestamptz "
            "FROM _chronos_branch_interval_checkpoints WHERE checkpoint_id = %s",
            (repair_generation_id,),
        )
        generation = cur.fetchone()
        if generation is None or str(generation[2]) != "published":
            raise ValueError("source retirement repair generation is not published")
        if (str(generation[0]), str(generation[1])) != (
            str(receipt[0]),
            str(receipt[1]),
        ):
            raise ValueError("source retirement generation belongs to another deployment or branch")
        if generation[3] < receipt[4]:
            raise ValueError("source retirement generation predates apply")
        snapshot = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(repair_generation_id, branch=str(receipt[1]))
        _assert_retirement_absent_from_snapshot(
            snapshot,
            dict(receipt[5]),
            retirement_id=retirement_id,
        )

    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "SELECT status, repair_generation_id, applied_at, plan_digest "
            "FROM okg.source_retirement_receipts "
            "WHERE retirement_id = %s FOR UPDATE",
            (retirement_id,),
        )
        locked = cur.fetchone()
        if locked is None:
            raise ValueError("source retirement receipt disappeared during binding")
        if str(locked[0]) == "published" and str(locked[1]) == repair_generation_id:
            return
        if str(locked[0]) != "awaiting_publish":
            raise ValueError("source retirement receipt was not awaiting publication")
        if locked[2] != receipt[4] or str(locked[3]) != str(receipt[5].get("plan_digest")):
            raise SourceRetirementRefused(
                "source retirement receipt changed during publication binding",
                data={
                    "retirement_id": retirement_id,
                    "refusal_codes": ["receipt_state_changed"],
                    "mutation_started": False,
                },
            )
        cur.execute(
            "UPDATE okg.source_retirement_receipts SET status = 'published', "
            "repair_generation_id = %s, published_at = now() "
            "WHERE retirement_id = %s AND status = 'awaiting_publish'",
            (repair_generation_id, retirement_id),
        )
        if cur.rowcount != 1:
            raise ValueError("source retirement receipt was not awaiting publication")


def _assert_retirement_absent_from_snapshot(
    snapshot: Any,
    plan: Mapping[str, Any],
    *,
    retirement_id: str,
) -> None:
    """Prove both the frozen footprint and current source ownership are absent."""
    node_ids = [str(row["node_id"]) for row in plan.get("nodes", [])]
    edge_ids = [int(row["edge_id"]) for row in plan.get("edges", [])]
    subjects = [str(value) for value in plan.get("source_subject_ids", [])]
    live_nodes = snapshot.query(
        "SELECT node_id FROM okg.graph_nodes "
        "WHERE source = ANY(:subjects) OR node_id = ANY(:node_ids) ORDER BY node_id",
        {"subjects": subjects, "node_ids": node_ids},
    )
    live_edges = snapshot.query(
        "SELECT edge_id FROM okg.graph_edges "
        "WHERE source = ANY(:subjects) OR edge_id = ANY(:edge_ids) "
        "ORDER BY edge_id",
        {"subjects": subjects, "edge_ids": edge_ids},
    )
    if live_nodes or live_edges:
        raise SourceRetirementRefused(
            "published generation still contains a retired source footprint",
            data={
                "retirement_id": retirement_id,
                "refusal_codes": ["retirement_footprint_still_live"],
                "remaining_node_ids": [str(row["node_id"]) for row in live_nodes],
                "remaining_edge_ids": [int(row["edge_id"]) for row in live_edges],
                "mutation_started": False,
            },
        )


def bind_source_retirements_to_published_generation(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch_name: str,
    generation_id: str,
) -> tuple[str, ...]:
    """Bind only retirements whose exact footprint is absent from the generation."""
    from okg.substrate.graph_store import ChronosGraphStore

    with conn.cursor() as cur:
        cur.execute(
            "SELECT metadata::jsonb->>'deployment_name', "
            "metadata::jsonb->>'branch', metadata::jsonb->>'status', "
            "created_at::timestamptz "
            "FROM _chronos_branch_interval_checkpoints WHERE checkpoint_id = %s",
            (generation_id,),
        )
        generation = cur.fetchone()
        if generation is None or str(generation[2]) != "published":
            raise ValueError("source retirement binding requires a published generation")
        if (str(generation[0]), str(generation[1])) != (
            deployment_name,
            branch_name,
        ):
            raise ValueError("source retirement binding crossed deployment or branch")
        cur.execute(
            "SELECT retirement_id, plan FROM okg.source_retirement_receipts "
            "WHERE deployment_name = %s AND branch_name = %s "
            "AND status = 'awaiting_publish' AND applied_at <= %s "
            "ORDER BY retirement_id",
            (deployment_name, branch_name, generation[3]),
        )
        pending = [(str(row[0]), dict(row[1])) for row in cur.fetchall()]
        if not pending:
            return ()

        snapshot = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(generation_id, branch=branch_name)
        bound: list[str] = []
        for retirement_id, plan in pending:
            _assert_retirement_absent_from_snapshot(
                snapshot,
                plan,
                retirement_id=retirement_id,
            )
            cur.execute(
                "UPDATE okg.source_retirement_receipts SET status = 'published', "
                "repair_generation_id = %s, published_at = now() "
                "WHERE retirement_id = %s AND status = 'awaiting_publish'",
                (generation_id, retirement_id),
            )
            if cur.rowcount != 1:
                raise SourceRetirementRefused(
                    "source retirement receipt changed during publication binding",
                    data={
                        "retirement_id": retirement_id,
                        "refusal_codes": ["receipt_state_changed"],
                        "mutation_started": False,
                    },
                )
            bound.append(retirement_id)
        return tuple(bound)


@contextmanager
def source_registry_removal_authority(
    conn: psycopg.Connection,
    *,
    source: str,
    deployment_name: str,
    branch_name: str = "default",
):
    """Hold the graph write fence while authorizing registry removal.

    The caller performs the filesystem mutation inside this context. This keeps
    the locked zero-footprint read and the authority transition in one critical
    section even though PostgreSQL cannot transact the registry file itself.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    if not store._lock_branch_write_session(branch_name):
        raise SubstrateError(
            "source removal requires PostgreSQL branch fencing",
            data={"branch": branch_name},
        )
    try:
        plan = _plan_source_retirement_locked(
            conn,
            source=source,
            deployment_name=deployment_name,
            branch_name=branch_name,
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT retirement_id, status, registry_digest, plan_digest, "
                "repair_generation_id FROM okg.source_retirement_receipts "
                "WHERE deployment_name = %s AND branch_name = %s "
                "AND registry_source_id = %s",
                (deployment_name, branch_name, plan.registry_source_id),
            )
            receipt = cur.fetchone()
        if receipt is None or str(receipt[1]) != "published":
            raise SubstrateError(
                "source registry removal requires a published retirement receipt",
                data={
                    "source": plan.registry_source_id,
                    "status": str(receipt[1]) if receipt is not None else "missing",
                },
                fix_hint=(
                    "run `okg remediate retire-source`, publish its repair "
                    "generation, and retry removal"
                ),
            )
        if str(receipt[2]) != plan.registry_digest:
            raise SubstrateError(
                "source registry changed after the applied retirement plan",
                data={"source": plan.registry_source_id},
                fix_hint="generate, review, apply, and publish a new retirement plan",
            )
        if plan.nodes or plan.edges:
            raise SubstrateError(
                "source registry removal found a live graph footprint",
                data={
                    "source": plan.registry_source_id,
                    "node_count": len(plan.nodes),
                    "edge_count": len(plan.edges),
                },
                fix_hint="retire and publish the remaining footprint before removal",
            )
        yield {
            "retirement_id": str(receipt[0]),
            "plan_digest": str(receipt[3]),
            "repair_generation_id": receipt[4],
            "registry_source_id": plan.registry_source_id,
            "registry_digest": plan.registry_digest,
        }
    finally:
        store._unlock_branch_write_session(branch_name)


__all__ = [
    "SourceRetirementPlan",
    "SourceRetirementResult",
    "bind_source_retirements_to_published_generation",
    "mark_source_retirement_published",
    "plan_source_retirement",
    "remediate_source_retirement",
    "source_registry_removal_authority",
]
