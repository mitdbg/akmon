"""Bridge accepted Poros candidates into substrate admission."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Mapping

import psycopg

from okg.substrate.exchange.canonical_json import canonical_json_bytes
from okg.substrate.exchange.constants import LEGACY_BUNDLE_SUBMIT_TYPE
from okg.substrate.exchange.graph_shape import classify_graph_body
from okg.substrate.exchange.inbox import PorosAdmissionCandidate
from okg.substrate.db.connection import connect as db_connect


POROS_ADMISSION_WORKFLOW = "poros_submit"


@dataclass(frozen=True)
class PorosAdmissionSubmitResult:
    deployment: str
    admission_id: str
    status: str
    inserted: bool
    idempotency_key: str
    source: str
    run_id: str
    payload_ref_inserted: bool
    emitted: dict[str, Any] | None
    fact_ref_count: int
    payload_rejected: bool = False
    compat_rejected: bool = False


def poros_candidate_to_admission_payload(
    candidate: PorosAdmissionCandidate,
    *,
    poros_provenance: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Convert a Poros graph body into review-admission payload shape."""
    body = candidate.body
    graph_id = str(
        body.get("graph_id") or body.get("bundle_id") or candidate.message_id
    )
    legacy_bundle_id = body.get("bundle_id")
    origin_authority_id = str(
        body.get("origin_authority_id") or candidate.sender,
    )
    origin_generation_id = body.get("origin_generation_id")
    classification = str(body.get("classification") or "unspecified")
    graph_profile = (
        body.get("graph_profile")
        or ("fact_bundle" if _is_legacy_bundle(candidate) else None)
    )
    root_node_ids = _root_node_ids(body)
    provenance = {
        "poros": {
            "sender": candidate.sender,
            "recipient": candidate.recipient,
            "message_id": candidate.message_id,
            "body_sha256": candidate.body_sha256,
            "type": candidate.envelope.get("type"),
            "signing_key_id": candidate.envelope.get("signing_key_id"),
            "graph_id": graph_id,
            "origin_authority_id": origin_authority_id,
            "origin_generation_id": origin_generation_id,
            "classification": classification,
        }
    }
    if graph_profile is not None:
        provenance["poros"]["graph_profile"] = str(graph_profile)
    if root_node_ids:
        provenance["poros"]["root_node_ids"] = root_node_ids
    if legacy_bundle_id is not None:
        provenance["poros"]["legacy_bundle_id"] = str(legacy_bundle_id)
    if poros_provenance is not None:
        provenance["poros"] = _deep_merge(
            dict(provenance["poros"]),
            dict(poros_provenance),
        )
    graph_shape = classify_graph_body(body)
    raw_nodes = _graph_items(body, "nodes", "node_facts")
    raw_edges = _graph_items(body, "edges", "edge_facts")
    routed_nodes, routed_edges = _route_graph_items_for_admission(
        candidate,
        nodes=raw_nodes,
        edges=raw_edges,
        root_node_ids=root_node_ids,
        root_pattern=("fact_bundle" if _is_legacy_bundle(candidate) else graph_shape.root_pattern),
    )
    nodes = [
        _normalize_node_for_admission(raw, provenance=provenance)
        for raw in routed_nodes
    ]
    edges = [
        _normalize_edge_for_admission(raw, provenance=provenance)
        for raw in routed_edges
    ]
    return {
        "nodes": nodes,
        "edges": edges,
        "poros": provenance["poros"],
    }


def submit_poros_candidate_to_admission(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    candidate: PorosAdmissionCandidate,
    admission_mode: str = "fast_track",
    producer_kind: str = "source",
    authority: Mapping[str, Any] | None = None,
    poros_provenance: Mapping[str, Any] | None = None,
    proposal_review_default: bool = True,
    allow_unsigned_compat: bool = False,
) -> PorosAdmissionSubmitResult:
    """Persist an accepted Poros candidate through admission helpers.

    This writes admission evidence and fact-log rows. It never writes
    directly to `nodes_live` or `edges_live`; callers run the publisher
    after this function when they want the imported graph materialized.

    Fail-closed compat admission (req.exchange-admission.fail-closed): when the
    receiver-owned producer profile has an empty output signature
    (``profile.status == "compat"``, surfaced historically as the
    ``producer_signature_missing`` warning), the envelope is REJECTED before
    any fact is emitted, unless ``allow_unsigned_compat`` is explicitly set by
    the caller from the deployment manifest opt-in
    (``admission.allow_unsigned_compat``). Both the rejection and the
    opted-in acceptance leave a durable audit record on the admission envelope
    naming the envelope and the reason. Absence of the opt-in is a refusal,
    never a benign default.
    """
    from okg.substrate.admission import (
        attach_fact_range_refs_for_run,
        attach_payload_ref,
        ensure_envelope,
        record_event,
        stable_idempotency_key,
        transition_envelope,
    )
    from okg.substrate.producer_authority import (
        ProducerPolicyViolation,
        materialize_producer_profile,
        merge_policy_evidence,
        policy_from_authority,
    )
    from okg.substrate.runtime.admission_workflows import (
        _ReviewPayloadRejected,
        _current_catalog_version_id,
        _emit_review_payload_facts,
        _load_any_envelope,
    )

    if admission_mode not in {"fast_track", "review", "quarantine"}:
        raise ValueError(f"unsupported Poros admission mode: {admission_mode}")
    graph_shape = classify_graph_body(candidate.body)
    effective_admission_mode = (
        "review"
        if (
            proposal_review_default
            and admission_mode == "fast_track"
            and graph_shape.root_pattern == "proposal"
        )
        else admission_mode
    )
    payload = poros_candidate_to_admission_payload(
        candidate,
        poros_provenance=poros_provenance,
    )
    payload = _preserve_conflicting_assertions(conn, payload, candidate)
    poros_evidence = dict(payload["poros"])
    source = f"poros:{candidate.sender}"
    run_id = str(uuid.uuid5(
        uuid.NAMESPACE_URL,
        "okg:poros:"
        f"{deployment_name}:{candidate.sender}:"
        f"{candidate.message_id}:{candidate.body_sha256}",
    ))
    idempotency_key = stable_idempotency_key(
        "poros",
        deployment_name,
        candidate.sender,
        candidate.message_id,
        candidate.body_sha256,
    )
    workflow_ref = {
        "workflow_name": POROS_ADMISSION_WORKFLOW,
        "message_id": candidate.message_id,
    }
    base_authority = {
        "poros": {
            "sender": candidate.sender,
            "recipient": candidate.recipient,
            "message_id": candidate.message_id,
            "body_sha256": candidate.body_sha256,
            **poros_evidence,
        },
        "producer_policy": _receiver_owned_producer_policy(
            candidate,
            payload=payload,
            admission_mode=effective_admission_mode,
            producer_kind=producer_kind,
        ),
    }
    authority_payload = _deep_merge(base_authority, dict(authority or {}))

    catalog_version_id = _current_catalog_version_id(conn)
    try:
        policy_payload = policy_from_authority(
            authority_payload,
            producer_id=candidate.sender,
            producer_kind=producer_kind,
        )
    except ProducerPolicyViolation:
        raise
    if (
        str(policy_payload["producer_id"]) != candidate.sender
        or str(policy_payload["producer_kind"]) != producer_kind
    ):
        raise ProducerPolicyViolation(
            code="producer_policy_invalid",
            message=(
                "Poros producer_policy identity must match the "
                "verified sender authority"
            ),
            verdict={"code": "producer_policy_invalid"},
        )
    profile = materialize_producer_profile(
        conn,
        deployment_name=deployment_name,
        producer_id=candidate.sender,
        producer_kind=producer_kind,
        trust_label=str(policy_payload["trust_label"]),
        admission_mode=str(policy_payload["admission_mode"]),
        authority_scope=policy_payload["authority_scope"],
        allowed_record_kinds=policy_payload["allowed_record_kinds"],
        output_signature=policy_payload["output_signature"],
        policy_clauses=policy_payload["policy_clauses"],
        output_scope_summary=policy_payload["output_scope_summary"],
        source_ref={
            "protocol": "poros",
            "message_id": candidate.message_id,
            "body_sha256": candidate.body_sha256,
            "graph_id": poros_evidence.get("graph_id"),
        },
        catalog_version_id=catalog_version_id,
    )
    producer_policy_summary = {
        **profile.evidence(),
        **(
            {"warnings": ["producer_signature_missing"]}
            if profile.status == "compat"
            else {}
        ),
    }
    authority_payload = {
        **authority_payload,
        "producer_policy": producer_policy_summary,
    }
    check_summary = {
        "poros": dict(authority_payload["poros"]),
        "producer_policy": producer_policy_summary,
    }
    initial_status = (
        "accepted" if effective_admission_mode == "fast_track" else "pending"
    )
    envelope = ensure_envelope(
        conn,
        deployment_name=deployment_name,
        producer_id=candidate.sender,
        producer_kind=producer_kind,
        intent="poros.graph.submit",
        admission_mode=effective_admission_mode,
        status=initial_status,
        idempotency_key=idempotency_key,
        catalog_version_id=catalog_version_id,
        source=source,
        run_id=run_id,
        authority=authority_payload,
        check_summary=check_summary,
        workflow_ref=workflow_ref,
    )
    payload_ref_inserted = attach_payload_ref(
        conn,
        admission_id=envelope.admission_id,
        payload=payload,
        ref_key=f"poros:{candidate.message_id}",
        workflow_ref=workflow_ref,
    )
    if envelope.inserted:
        record_event(
            conn,
            admission_id=envelope.admission_id,
            event_type="submitted",
            actor=candidate.sender,
            payload={
                "intent": "poros.graph.submit",
                "message_id": candidate.message_id,
                "body_sha256": candidate.body_sha256,
                "graph_id": poros_evidence.get("graph_id"),
            },
            workflow_ref=workflow_ref,
        )

    # Fail-closed compat admission. A profile whose output signature is empty
    # (``status == "compat"``) is the historical ``producer_signature_missing``
    # warn-through path; on the canonical target it is rejected before any
    # fact is emitted. Compat acceptance requires the explicit manifest opt-in.
    if profile.status == "compat":
        compat_audit = {
            "reason": "producer_signature_missing",
            "message_id": candidate.message_id,
            "body_sha256": candidate.body_sha256,
            "graph_id": poros_evidence.get("graph_id"),
            "producer_id": candidate.sender,
            "allow_unsigned_compat": bool(allow_unsigned_compat),
        }
        if not allow_unsigned_compat:
            result = transition_envelope(
                conn,
                admission_id=envelope.admission_id,
                status="rejected",
                actor=candidate.sender,
                reason="producer_signature_missing",
                verdict={
                    "code": "producer_signature_missing",
                    "producer_policy": {
                        "status": "compat",
                        "reason": "producer_signature_missing",
                        "message_id": candidate.message_id,
                        "body_sha256": candidate.body_sha256,
                    },
                },
                workflow_ref=workflow_ref,
            )
            return PorosAdmissionSubmitResult(
                deployment=deployment_name,
                admission_id=envelope.admission_id,
                status=str(result["status"]),
                inserted=envelope.inserted,
                idempotency_key=idempotency_key,
                source=source,
                run_id=run_id,
                payload_ref_inserted=payload_ref_inserted,
                emitted=None,
                fact_ref_count=0,
                payload_rejected=False,
                compat_rejected=True,
            )
        # Opted in: admit, but leave a durable audit record on the envelope.
        record_event(
            conn,
            admission_id=envelope.admission_id,
            event_type="submitted",
            actor=candidate.sender,
            reason="producer_signature_missing",
            payload=compat_audit,
            workflow_ref=workflow_ref,
        )

    emitted: dict[str, Any] | None = None
    fact_ref_count = 0
    payload_rejected = False
    status = envelope.status
    if effective_admission_mode == "fast_track":
        current = _load_any_envelope(
            conn,
            deployment=deployment_name,
            admission_id=envelope.admission_id,
        )
        try:
            emitted = _emit_review_payload_facts(
                conn,
                envelope=current,
                workflow_ref=workflow_ref,
            )
        except _ReviewPayloadRejected as exc:
            payload_rejected = True
            result = transition_envelope(
                conn,
                admission_id=envelope.admission_id,
                status="rejected",
                actor=candidate.sender,
                reason=exc.reason,
                verdict=exc.verdict,
                workflow_ref=workflow_ref,
            )
            status = str(result["status"])
        else:
            refs = attach_fact_range_refs_for_run(
                conn,
                admission_id=envelope.admission_id,
                source=emitted["source"],
                run_id=emitted["run_id"],
                workflow_ref=workflow_ref,
            )
            fact_ref_count = len(refs)
            result = transition_envelope(
                conn,
                admission_id=envelope.admission_id,
                status="accepted",
                actor=candidate.sender,
                reason="poros_fast_track",
                verdict=merge_policy_evidence(
                    {},
                    {
                        "source": emitted["source"],
                        "run_id": emitted["run_id"],
                        "node_facts_inserted": emitted[
                            "node_facts_inserted"
                        ],
                        "edge_facts_inserted": emitted[
                            "edge_facts_inserted"
                        ],
                        "fact_ref_count": fact_ref_count,
                    },
                    emitted.get("policy_evidence"),
                ),
                workflow_ref=workflow_ref,
            )
            status = str(result["status"])

    return PorosAdmissionSubmitResult(
        deployment=deployment_name,
        admission_id=envelope.admission_id,
        status=status,
        inserted=envelope.inserted,
        idempotency_key=idempotency_key,
        source=source,
        run_id=run_id,
        payload_ref_inserted=payload_ref_inserted,
        emitted=emitted,
        fact_ref_count=fact_ref_count,
        payload_rejected=payload_rejected,
    )


def _graph_items(
    body: Mapping[str, Any],
    primary: str,
    compatibility: str,
) -> list[dict[str, Any]]:
    value = body.get(primary)
    if value is None:
        value = body.get(compatibility, [])
    if not isinstance(value, list):
        raise ValueError(f"Poros graph field {primary!r} must be a list")
    out: list[dict[str, Any]] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise ValueError(
                f"Poros graph field {primary}[{index}] must be an object"
            )
        out.append(dict(item))
    return out


def _route_graph_items_for_admission(
    candidate: PorosAdmissionCandidate,
    *,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    root_node_ids: list[str],
    root_pattern: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if root_pattern in {"fact_bundle", "unknown"}:
        return nodes, edges
    roots = _root_nodes(nodes, root_node_ids)
    if root_pattern == "context":
        return [
            _with_route_attrs(
                node,
                poros_route="evidence",
                root_pattern=root_pattern,
                node_count=len(nodes),
                edge_count=len(edges),
            )
            for node in roots
        ], []
    if root_pattern == "claim":
        return [
            _with_route_attrs(
                node,
                poros_route="attributed_claim",
                root_pattern=root_pattern,
                node_count=len(nodes),
                edge_count=len(edges),
                claim_status="attributed_not_canonical",
            )
            for node in roots
        ], []
    if root_pattern == "dataset_reference":
        return [
            _with_route_attrs(
                node,
                poros_route="dataset_reference",
                root_pattern=root_pattern,
                node_count=len(nodes),
                edge_count=len(edges),
                body_import="not_imported_through_poros_v0",
            )
            for node in roots
        ], []
    if root_pattern == "proposal":
        return [
            _with_route_attrs(
                node,
                poros_route="proposal_review",
                root_pattern=root_pattern,
                node_count=len(nodes),
                edge_count=len(edges),
                review_required=True,
            )
            for node in roots
        ], []
    return [], []


def _root_nodes(
    nodes: list[dict[str, Any]],
    root_node_ids: list[str],
) -> list[dict[str, Any]]:
    if not root_node_ids:
        return [dict(node) for node in nodes]
    wanted = set(root_node_ids)
    return [
        dict(node)
        for node in nodes
        if isinstance(node.get("node_id"), str) and node["node_id"] in wanted
    ]


def _with_route_attrs(
    node: dict[str, Any],
    *,
    poros_route: str,
    root_pattern: str,
    node_count: int,
    edge_count: int,
    **extra_attrs: Any,
) -> dict[str, Any]:
    routed = dict(node)
    attrs = dict(routed.get("attrs") or {})
    attrs.update({
        "poros_route": poros_route,
        "poros_root_pattern": root_pattern,
        "poros_source_node_count": node_count,
        "poros_source_edge_count": edge_count,
    })
    attrs.update(extra_attrs)
    routed["attrs"] = attrs
    return routed


def _preserve_conflicting_assertions(
    conn: psycopg.Connection,
    payload: dict[str, Any],
    candidate: PorosAdmissionCandidate,
) -> dict[str, Any]:
    nodes = list(payload.get("nodes") or [])
    edges = list(payload.get("edges") or [])
    if not nodes and not edges:
        return payload
    # Chronos cutover: conflict probes read the CURRENT default-branch graph
    # state (the legacy reads used `valid_to_gen IS NULL` with no generation
    # pin -- "does this assertion exist now"), so check out the default branch.
    #
    # The checkout's `.query(...)` runs in autocommit mode and calls
    # `conn.commit()`, which psycopg forbids while `conn` is inside the open
    # `submit_poros_candidate_to_admission` transaction. Run the read-only
    # conflict probes over a separate, short-lived connection so the outer
    # transaction is untouched. `conn.info.dsn` redacts the password, so
    # reattach it from `conn.info.password` to reconnect.
    from psycopg.conninfo import make_conninfo

    from okg.substrate.graph_store import ChronosGraphStore

    conflict_nodes: list[dict[str, Any]] = []
    conflicted_node_ids: set[str] = set()
    kept_nodes: list[dict[str, Any]] = []
    kept_edges: list[dict[str, Any]] = []
    read_dsn = make_conninfo(conn.info.dsn, password=conn.info.password)
    with db_connect(read_dsn, autocommit=True) as read_conn:
        graph = ChronosGraphStore.from_connection(
            read_conn, ensure_schema=False
        ).checkout()
        for node in nodes:
            conflict = _node_conflict(graph, node)
            if conflict is None:
                kept_nodes.append(node)
                continue
            original_node_id = str(node["node_id"])
            conflicted_node_ids.add(original_node_id)
            conflict_nodes.append(
                _conflict_assertion_node(
                    candidate,
                    conflict_kind="node_attrs",
                    asserted_record={
                        "node_id": original_node_id,
                        "subtype": node.get("subtype"),
                        "attrs": dict(node.get("attrs") or {}),
                    },
                    existing_record=conflict,
                    source_record_id=node.get("source_record_id"),
                    source_revision=node.get("source_revision"),
                )
            )

        for edge in edges:
            if str(edge.get("src") or "") in conflicted_node_ids or str(
                edge.get("dst") or ""
            ) in conflicted_node_ids:
                continue
            conflict = _edge_conflict(graph, edge)
            if conflict is None:
                kept_edges.append(edge)
                continue
            conflict_nodes.append(
                _conflict_assertion_node(
                    candidate,
                    conflict_kind="edge_attrs",
                    asserted_record={
                        "src": edge.get("src"),
                        "edge_type": edge.get("edge_type"),
                        "dst": edge.get("dst"),
                        "attrs": dict(edge.get("attrs") or {}),
                        "confidence": edge.get("confidence"),
                        "provenance": edge.get("provenance"),
                    },
                    existing_record=conflict,
                    source_record_id=edge.get("source_record_id"),
                    source_revision=edge.get("source_revision"),
                )
            )
    if not conflict_nodes:
        return payload
    poros = dict(payload.get("poros") or {})
    poros["conflicts_detected"] = len(conflict_nodes)
    return {
        **payload,
        "nodes": [*kept_nodes, *conflict_nodes],
        "edges": kept_edges,
        "poros": poros,
    }


def _node_conflict(
    graph: Any,
    node: Mapping[str, Any],
) -> dict[str, Any] | None:
    node_id = node.get("node_id")
    if not node_id:
        return None
    rows = graph.query(
        """
        SELECT subtype, attrs
          FROM okg.graph_nodes
         WHERE node_id = :node_id
         ORDER BY node_id
         LIMIT 1
        """,
        {"node_id": str(node_id)},
    )
    if not rows:
        return None
    row = rows[0]
    incoming = {
        "subtype": str(node.get("subtype") or ""),
        "attrs": dict(node.get("attrs") or {}),
    }
    existing = {
        "subtype": str(row["subtype"] or ""),
        "attrs": dict(row["attrs"] or {}),
    }
    if _canonical_compare(incoming["subtype"]) == _canonical_compare(
        existing["subtype"]
    ) and _canonical_compare(incoming["attrs"]) == _canonical_compare(
        existing["attrs"]
    ):
        return None
    return {"node_id": str(node_id), **existing}


def _edge_conflict(
    graph: Any,
    edge: Mapping[str, Any],
) -> dict[str, Any] | None:
    src = edge.get("src")
    edge_type = edge.get("edge_type")
    dst = edge.get("dst")
    if not src or not edge_type or not dst:
        return None
    rows = graph.query(
        """
        SELECT attrs
          FROM okg.graph_edges
         WHERE src = :src
           AND edge_type = :edge_type
           AND dst = :dst
         ORDER BY edge_id
         LIMIT 1
        """,
        {"src": str(src), "edge_type": str(edge_type), "dst": str(dst)},
    )
    if not rows:
        return None
    row = rows[0]
    incoming = {
        "attrs": dict(edge.get("attrs") or {}),
        "provenance": str(edge.get("provenance") or "explicit"),
        "confidence": float(edge.get("confidence") or 1.0),
    }
    existing = {
        "attrs": dict(row["attrs"] or {}),
    }
    if _canonical_compare(incoming["attrs"]) == _canonical_compare(
        existing["attrs"]
    ):
        return None
    return {
        "src": str(src),
        "edge_type": str(edge_type),
        "dst": str(dst),
        **existing,
    }


def _conflict_assertion_node(
    candidate: PorosAdmissionCandidate,
    *,
    conflict_kind: str,
    asserted_record: Mapping[str, Any],
    existing_record: Mapping[str, Any],
    source_record_id: Any,
    source_revision: Any,
) -> dict[str, Any]:
    assertion_id = uuid.uuid5(
        uuid.NAMESPACE_URL,
        "okg:poros:conflict:"
        f"{candidate.message_id}:{candidate.body_sha256}:"
        f"{_canonical_compare(asserted_record)}",
    )
    return {
        "node_id": f"poros:conflict_assertion:{assertion_id}",
        "subtype": "poros_conflict_assertion",
        "attrs": {
            "conflict_kind": conflict_kind,
            "asserted_record": dict(asserted_record),
            "existing_record": dict(existing_record),
            "sender": candidate.sender,
            "recipient": candidate.recipient,
            "message_id": candidate.message_id,
            "body_sha256": candidate.body_sha256,
            "graph_id": candidate.body.get("graph_id") or candidate.body.get("bundle_id"),
            "resolution_status": "unresolved",
        },
        "source_record_id": source_record_id,
        "source_revision": source_revision,
    }


def _canonical_compare(value: Any) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def _receiver_owned_producer_policy(
    candidate: PorosAdmissionCandidate,
    *,
    payload: Mapping[str, Any],
    admission_mode: str,
    producer_kind: str,
) -> dict[str, Any]:
    body = candidate.body
    graph_shape = classify_graph_body(body)
    nodes = payload.get("nodes") if isinstance(payload.get("nodes"), list) else []
    edges = payload.get("edges") if isinstance(payload.get("edges"), list) else []
    node_subtypes = sorted({
        str(node.get("subtype"))
        for node in nodes
        if isinstance(node, Mapping) and node.get("subtype")
    })
    edge_narrowings = list(graph_shape.edge_narrowings)
    output_signature: dict[str, list[dict[str, str]]] = {}
    policy_clauses: list[dict[str, Any]] = []
    # Edge-bearing Poros graphs still rely on the catalog/narrowing gate for
    # v0 emission so unsupported edges produce durable rejected admission
    # evidence instead of failing during producer-profile materialization.
    if edge_narrowings:
        node_subtypes_for_signature: list[str] = []
    else:
        node_subtypes_for_signature = node_subtypes
    if node_subtypes_for_signature:
        output_signature["nodes"] = [
            {
                "subtype": subtype,
                "clause_id": f"poros.node.{_clause_token(subtype)}",
            }
            for subtype in node_subtypes_for_signature
        ]
        policy_clauses.extend(
            {
                "id": f"poros.node.{_clause_token(subtype)}",
                "effect": "allow",
                "record_kind": "node",
                "subtype": subtype,
            }
            for subtype in node_subtypes_for_signature
        )
    allowed_record_kinds: list[str] = []
    if nodes:
        allowed_record_kinds.append("node")
    if edges:
        allowed_record_kinds.append("edge")
    return {
        "producer_id": candidate.sender,
        "producer_kind": producer_kind,
        "trust_label": "poros-trusted",
        "admission_mode": admission_mode,
        "authority_scope": {
            "poros_sender": candidate.sender,
            "poros_recipient": candidate.recipient,
            "message_id": candidate.message_id,
            "body_sha256": candidate.body_sha256,
            "signing_key_id": candidate.envelope.get("signing_key_id"),
            "graph_id": body.get("graph_id") or body.get("bundle_id"),
            "graph_classification": str(body.get("classification") or "internal"),
            "requested_incorporation_mode": admission_mode,
            **graph_shape.to_resource_fields(),
            "sender_graph_profile": body.get("graph_profile"),
        },
        "allowed_record_kinds": allowed_record_kinds,
        "output_signature": output_signature,
        "policy_clauses": policy_clauses,
        "output_scope_summary": {
            "summary": (
                "Receiver-owned Poros producer policy derived from "
                "the verified sender, signing key, graph shape, "
                "classification, and incorporation mode."
            )
        },
    }


def _clause_token(value: str) -> str:
    token = "".join(ch if ch.isalnum() else "_" for ch in value.lower())
    return token.strip("_") or "unknown"


def _is_legacy_bundle(candidate: PorosAdmissionCandidate) -> bool:
    return (
        candidate.envelope.get("type") == LEGACY_BUNDLE_SUBMIT_TYPE
        or "bundle_id" in candidate.body
        or "node_facts" in candidate.body
        or "edge_facts" in candidate.body
    )


def _root_node_ids(body: Mapping[str, Any]) -> list[str]:
    value = body.get("root_node_ids", [])
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("Poros graph field 'root_node_ids' must be a list")
    out: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str) or not item:
            raise ValueError(
                f"Poros graph field root_node_ids[{index}] must be a string"
            )
        out.append(item)
    return out


def _normalize_node_for_admission(
    raw: dict[str, Any],
    *,
    provenance: dict[str, Any],
) -> dict[str, Any]:
    node = {
        "node_id": raw.get("node_id"),
        "subtype": raw.get("subtype"),
        "attrs": dict(raw.get("attrs") or {}),
        "op": raw.get("op", "I"),
        "source_record_id": _merge_poros_provenance(
            raw.get("source_record_id"),
            provenance,
        ),
        "source_revision": _merge_poros_provenance(
            raw.get("source_revision"),
            provenance,
        ),
    }
    return node


def _normalize_edge_for_admission(
    raw: dict[str, Any],
    *,
    provenance: dict[str, Any],
) -> dict[str, Any]:
    edge = {
        "src": raw.get("src"),
        "dst": raw.get("dst"),
        "edge_type": raw.get("edge_type"),
        "attrs": dict(raw.get("attrs") or {}),
        "provenance": raw.get("provenance", "explicit"),
        "confidence": raw.get("confidence", 1.0),
        "op": raw.get("op", "I"),
        "source_record_id": _merge_poros_provenance(
            raw.get("source_record_id"),
            provenance,
        ),
        "source_revision": _merge_poros_provenance(
            raw.get("source_revision"),
            provenance,
        ),
    }
    if raw.get("src_subtype") is not None:
        edge["src_subtype"] = raw.get("src_subtype")
    if raw.get("dst_subtype") is not None:
        edge["dst_subtype"] = raw.get("dst_subtype")
    return edge


def _merge_poros_provenance(
    value: Any,
    provenance: dict[str, Any],
) -> dict[str, Any]:
    base = dict(value or {}) if isinstance(value, Mapping) else {}
    return _deep_merge(base, provenance)


def _deep_merge(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    out = dict(left)
    for key, value in right.items():
        if isinstance(out.get(key), dict) and isinstance(value, Mapping):
            out[key] = _deep_merge(dict(out[key]), dict(value))
        else:
            out[key] = value
    return out
