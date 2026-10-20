"""Sender-side Poros graph export and outbox persistence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

import psycopg
from psycopg.types.json import Jsonb

from okg.substrate.exchange.canonical_json import canonical_json_bytes
from okg.substrate.exchange.constants import INLINE_SMALL_MAX_BYTES
from okg.substrate.exchange.envelope import (
    build_unsigned_envelope,
    format_timestamp,
    new_message_id,
    parse_timestamp,
)
from okg.substrate.exchange.hashing import sha256_b64url
from okg.substrate.exchange.object_store import LocalPorosObjectStore
from okg.substrate.exchange.payloads import build_detached_object_manifest
from okg.substrate.exchange.signing import sign_envelope
from okg.substrate.db.connection import connect as db_connect


class PorosOutboxError(ValueError):
    """Raised when a Poros outbox export cannot be proven safe."""


@dataclass(frozen=True)
class PorosOutboxExportResult:
    message_id: str
    sender_authority_id: str
    recipient_authority_id: str
    sender_generation_id: str
    graph_id: str
    signing_key_id: str
    body_sha256: str
    envelope_sha256: str
    classification: str
    graph_profile: str | None
    root_node_ids: tuple[str, ...]
    node_count: int
    edge_count: int
    payload_mode: str
    detached_object_count: int
    envelope: dict[str, Any]
    outbox_created_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "message_id": self.message_id,
            "sender_authority_id": self.sender_authority_id,
            "recipient_authority_id": self.recipient_authority_id,
            "sender_generation_id": self.sender_generation_id,
            "graph_id": self.graph_id,
            "signing_key_id": self.signing_key_id,
            "body_sha256": self.body_sha256,
            "envelope_sha256": self.envelope_sha256,
            "classification": self.classification,
            "graph_profile": self.graph_profile,
            "root_node_ids": list(self.root_node_ids),
            "node_count": self.node_count,
            "edge_count": self.edge_count,
            "payload_mode": self.payload_mode,
            "detached_object_count": self.detached_object_count,
            "outbox_created_at": self.outbox_created_at,
        }


@dataclass(frozen=True)
class PorosDeliveryAttemptResult:
    attempt_id: int
    message_id: str
    body_sha256: str
    status: str
    reason_code: str | None
    receipt_sha256: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "attempt_id": self.attempt_id,
            "message_id": self.message_id,
            "body_sha256": self.body_sha256,
            "status": self.status,
            "reason_code": self.reason_code,
            "receipt_sha256": self.receipt_sha256,
        }


def authorize_poros_signing_key_use(
    *,
    dsn: str,
    sender_authority_id: str,
    sender_generation_id: str,
    signing_key_id: str | None = None,
    signing_key_reference: str | None = None,
    nomos_subject: str | None = None,
    nomos_scope: str | None = None,
    use: str = "poros.sign_envelope",
) -> None:
    """Authorize Poros signing-key material access before reading or signing."""
    with db_connect(dsn) as conn:
        generation = _published_generation(conn, sender_generation_id)
        if generation is None:
            raise PorosOutboxError(
                f"sender generation {sender_generation_id} is not published"
            )
        deployment_name = str(generation["deployment_name"])
        _authorize_nomos_outbox_boundary(
            conn,
            deployment_name=deployment_name,
            scope=nomos_scope or f"deployment:{deployment_name}",
            subject=nomos_subject or sender_authority_id,
            surface="secret.use",
            classification="restricted",
            resource={
                "sender_authority_id": sender_authority_id,
                "sender_generation_id": sender_generation_id,
                "signing_key_id": signing_key_id,
                "signing_key_reference": signing_key_reference,
                "use": use,
            },
            target={
                "kind": "poros_signing_key_use",
                "signing_key_id": signing_key_id,
                "signing_key_reference": signing_key_reference,
                "use": use,
            },
            performed_obligations=("audit",),
        )


def export_published_subgraph_to_outbox(
    *,
    dsn: str,
    sender_authority_id: str,
    recipient_authority_id: str,
    sender_generation_id: str,
    graph_id: str,
    root_node_ids: Iterable[str],
    signing_private_key: bytes | str,
    signing_key_id: str,
    classification: str = "public",
    graph_profile: str | None = "context",
    message_id: str | None = None,
    ttl_seconds: int = 15 * 60,
    object_store_root: Path | str | None = None,
    detach_payload: bool = False,
    detached_chunk_size_bytes: int = 64 * 1024,
    nomos_subject: str | None = None,
    nomos_scope: str | None = None,
    signing_key_reference: str | None = None,
) -> PorosOutboxExportResult:
    """Snapshot visible rows from a published generation into Poros outbox."""
    roots = tuple(dict.fromkeys(str(node_id) for node_id in root_node_ids))
    if not roots:
        raise PorosOutboxError("Poros outbox export requires at least one root node")
    if ttl_seconds <= 0:
        raise PorosOutboxError("Poros outbox export ttl_seconds must be positive")
    if detached_chunk_size_bytes <= 0:
        raise PorosOutboxError("detached_chunk_size_bytes must be positive")
    if detach_payload and object_store_root is None:
        raise PorosOutboxError("detach_payload requires object_store_root")

    with db_connect(dsn) as conn:
        generation = _published_generation(conn, sender_generation_id)
        if generation is None:
            raise PorosOutboxError(
                f"sender generation {sender_generation_id} is not published"
            )
        deployment_name = str(generation["deployment_name"])
        scope = nomos_scope or f"deployment:{deployment_name}"
        subject = nomos_subject or sender_authority_id
        _authorize_nomos_outbox_boundary(
            conn,
            deployment_name=deployment_name,
            scope=scope,
            subject=subject,
            surface="graph.read.export_like",
            classification=classification,
            resource={
                "use": "poros.export_read",
                "sender_authority_id": sender_authority_id,
                "recipient_authority_id": recipient_authority_id,
                "sender_generation_id": sender_generation_id,
                "graph_id": graph_id,
                "root_node_ids": list(roots),
                "classification": classification,
                "graph_profile": graph_profile,
                "payload_mode": "detached" if detach_payload else "inline_or_detached",
            },
            target={
                "kind": "poros_export_read",
                "sender_generation_id": sender_generation_id,
                "graph_id": graph_id,
            },
            performed_obligations=("audit",),
        )
        # Chronos cutover: graph-content reads resolve through a Chronos
        # checkout pinned to the published sender generation
        # (okg.graph_nodes/graph_edges); the graph_generations metadata read in
        # _published_generation above stays on the raw cursor.
        from okg.substrate.graph_store import ChronosGraphStore

        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(sender_generation_id)
        from okg.substrate.mcp.session import open_session

        session = open_session(
            conn,
            deployment_name=deployment_name,
            requested_generation=sender_generation_id,
        )
        nodes = _visible_nodes(
            conn,
            session,
            graph,
            node_ids=roots,
            subject=subject,
        )
        found = {node["node_id"] for node in nodes}
        missing = [node_id for node_id in roots if node_id not in found]
        if missing:
            raise PorosOutboxError(
                "Poros outbox export root nodes are not live at generation "
                f"{sender_generation_id}: {missing}"
            )
        nodes_by_id = {node["node_id"]: node for node in nodes}
        # External-backed nodes (issue #83): a node's HANDLE points at the
        # sender's private systems + series store, which a peer cannot resolve.
        # Strip it before export so a received node never chases a dangling
        # pointer. The portable record is the stable metadata + (later) rollup
        # facts; the handle is local-only and the peer declares its own if it
        # has independent access.
        # Image values (#3046): reduce each to its bare reference. `png.id`
        # names a blob row in THIS okg, and only a receiver's own ingest
        # may add `png` or `error`, so a completed value would be refused
        # as forged. The receiver reads a bare `file` value as not ingested.
        ordered_nodes = [
            _strip_ingest_image_fields(
                _strip_external_handle(nodes_by_id[node_id])
            )
            for node_id in roots
        ]
        edges = [
            _strip_ingest_image_fields(edge)
            for edge in _visible_internal_edges(
                conn,
                session,
                graph,
                node_ids=roots,
                nodes_by_id=nodes_by_id,
                subject=subject,
            )
        ]
        created_at = datetime.now(UTC)
        actual_message_id = message_id or new_message_id()
        inline_body = {
            "graph_id": graph_id,
            "origin_authority_id": sender_authority_id,
            "origin_generation_id": sender_generation_id,
            "classification": classification,
            "root_node_ids": list(roots),
            "nodes": ordered_nodes,
            "edges": edges,
            "metadata": {
                "exported_at": format_timestamp(created_at),
                "node_count": len(ordered_nodes),
                "edge_count": len(edges),
                "sender_deployment": generation["deployment_name"],
            },
        }
        if graph_profile is not None:
            inline_body["graph_profile"] = graph_profile
        export_outcome = _authorize_nomos_outbox_boundary(
            conn,
            deployment_name=deployment_name,
            scope=scope,
            subject=subject,
            surface="graph.export",
            classification=classification,
            resource={
                "use": "poros.export",
                "sender_authority_id": sender_authority_id,
                "recipient_authority_id": recipient_authority_id,
                "sender_generation_id": sender_generation_id,
                "graph_id": graph_id,
                "root_node_ids": list(roots),
                "classification": classification,
                "graph_profile": graph_profile,
            },
            target={
                "kind": "poros_graph_export",
                "sender_generation_id": sender_generation_id,
                "graph_id": graph_id,
            },
            performed_obligations=("audit",),
            payload=inline_body,
        )
        if export_outcome is not None and isinstance(export_outcome.payload, Mapping):
            inline_body = dict(export_outcome.payload)
        _authorize_nomos_outbox_boundary(
            conn,
            deployment_name=deployment_name,
            scope=scope,
            subject=subject,
            surface="secret.use",
            classification="restricted",
            resource={
                "sender_authority_id": sender_authority_id,
                "signing_key_id": signing_key_id,
                "signing_key_reference": signing_key_reference,
                "use": "poros.sign_envelope",
            },
            target={
                "kind": "poros_signing_key_use",
                "signing_key_id": signing_key_id,
                "signing_key_reference": signing_key_reference,
            },
            performed_obligations=("audit",),
        )
        body, payload_mode = _body_for_payload_mode(
            inline_body,
            message_id=actual_message_id,
            graph_id=graph_id,
            classification=classification,
            object_store_root=object_store_root,
            detach_payload=detach_payload,
            detached_chunk_size_bytes=detached_chunk_size_bytes,
        )
        unsigned = build_unsigned_envelope(
            sender=sender_authority_id,
            recipient=recipient_authority_id,
            message_id=actual_message_id,
            created_at=created_at,
            ttl_seconds=ttl_seconds,
            body=body,
            payload_mode=payload_mode,
        )
        envelope = sign_envelope(
            unsigned,
            signing_private_key,
            kid=signing_key_id,
        )
        envelope_wire = canonical_json_bytes(envelope)
        envelope_sha256 = sha256_b64url(envelope_wire)
        expires_at = parse_timestamp(str(envelope["expires_at"]), field="expires_at")
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.poros_outbox_messages (
                    message_id, sender_authority_id, recipient_authority_id,
                    sender_generation_id, graph_id, signing_key_id,
                    body_sha256, envelope_sha256, payload_mode,
                    classification, graph_profile, root_node_ids,
                    node_count, edge_count, envelope, body, expires_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                RETURNING created_at
                """,
                (
                    envelope["message_id"],
                    sender_authority_id,
                    recipient_authority_id,
                    sender_generation_id,
                    graph_id,
                    signing_key_id,
                    envelope["body_sha256"],
                    envelope_sha256,
                    payload_mode,
                    classification,
                    graph_profile,
                    list(roots),
                    len(ordered_nodes),
                    len(edges),
                    Jsonb(envelope),
                    Jsonb(body),
                    expires_at,
                ),
            )
            (created_row,) = cur.fetchone()
        _persist_sender_detached_object_manifests(
            conn,
            sender_authority_id=sender_authority_id,
            message_id=str(envelope["message_id"]),
            body_sha256=str(envelope["body_sha256"]),
            envelope=envelope,
        )
    return PorosOutboxExportResult(
        message_id=str(envelope["message_id"]),
        sender_authority_id=sender_authority_id,
        recipient_authority_id=recipient_authority_id,
        sender_generation_id=sender_generation_id,
        graph_id=graph_id,
        signing_key_id=signing_key_id,
        body_sha256=str(envelope["body_sha256"]),
        envelope_sha256=envelope_sha256,
        classification=classification,
        graph_profile=graph_profile,
        root_node_ids=roots,
        node_count=len(ordered_nodes),
        edge_count=len(edges),
        payload_mode=payload_mode,
        detached_object_count=len(body.get("objects", [])),
        envelope=envelope,
        outbox_created_at=created_row.isoformat(),
    )


def _authorize_nomos_outbox_boundary(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    scope: str,
    subject: str,
    surface: str,
    classification: str,
    resource: Mapping[str, Any],
    target: Mapping[str, Any],
    performed_obligations: Iterable[str] = (),
    payload: Any | None = None,
):
    from okg.substrate.nomos import (
        NomosRuntimeRequest,
        deployment_requires_nomos_runtime,
        evaluate_runtime_boundary_with_active_policy,
    )

    if not deployment_requires_nomos_runtime(deployment_name):
        return None
    outcome = evaluate_runtime_boundary_with_active_policy(
        conn,
        deployment_name=deployment_name,
        scope=scope,
        request=NomosRuntimeRequest(
            surface=surface,
            subject=subject,
            resource=resource,
            classification=classification,
        ),
        target=target,
        performed_obligations=performed_obligations,
        payload=payload,
    )
    conn.commit()
    if outcome is not None and not outcome.allowed:
        raise PorosOutboxError(
            "Nomos denied "
            f"{surface}: {outcome.decision.reason_code or outcome.final_effect}"
        )
    return outcome


def _body_for_payload_mode(
    inline_body: dict[str, Any],
    *,
    message_id: str,
    graph_id: str,
    classification: str,
    object_store_root: Path | str | None,
    detach_payload: bool,
    detached_chunk_size_bytes: int,
) -> tuple[dict[str, Any], str]:
    inline_size = len(canonical_json_bytes(inline_body))
    should_detach = (
        detach_payload
        or (
            object_store_root is not None
            and inline_size > INLINE_SMALL_MAX_BYTES
        )
    )
    if not should_detach:
        return inline_body, "inline"
    if object_store_root is None:
        raise PorosOutboxError("detached Poros export requires object_store_root")
    object_store = LocalPorosObjectStore(object_store_root)
    object_id = f"{message_id}:graph-body"
    data = canonical_json_bytes(inline_body)
    stored = object_store.put_bytes(object_id=object_id, data=data)
    manifest = build_detached_object_manifest(
        object_id=object_id,
        media_type="application/vnd.okg.poros.graph+json",
        data=data,
        chunk_size_bytes=detached_chunk_size_bytes,
        classification=classification,
        locations=[{"uri": stored["uri"]}],
    )
    body = {
        "payload_mode": "detached",
        "graph_id": inline_body["graph_id"],
        "origin_authority_id": inline_body["origin_authority_id"],
        "origin_generation_id": inline_body["origin_generation_id"],
        "classification": classification,
        "root_node_ids": list(inline_body.get("root_node_ids") or []),
        "metadata": {
            **dict(inline_body.get("metadata") or {}),
            "detached_body_sha256": manifest["cleartext_sha256"],
            "detached_body_size_bytes": len(data),
        },
        "objects": [manifest],
    }
    if inline_body.get("graph_profile") is not None:
        body["graph_profile"] = inline_body["graph_profile"]
    if inline_body.get("metadata") is not None:
        body["metadata"]["inline_body_sha256"] = manifest["cleartext_sha256"]
    return body, "detached"


def record_delivery_attempt(
    *,
    dsn: str,
    message_id: str,
    body_sha256: str,
    envelope_sha256: str,
    status: str,
    reason_code: str | None = None,
    receipt_sha256: str | None = None,
    response: dict[str, Any] | None = None,
    transport: str = "local_cli",
) -> PorosDeliveryAttemptResult:
    """Record sender-side delivery evidence for an existing outbox row."""
    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT sender_authority_id, recipient_authority_id, body_sha256
              FROM okg.poros_outbox_messages
             WHERE message_id = %s
            """,
            (message_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise PorosOutboxError(
                f"cannot record delivery attempt for unknown message {message_id!r}"
            )
        sender_authority_id, recipient_authority_id, expected_hash = row
        if str(expected_hash) != body_sha256:
            raise PorosOutboxError(
                "delivery attempt body hash does not match sender outbox"
            )
        cur.execute(
            """
            INSERT INTO okg.poros_delivery_attempts (
                message_id, sender_authority_id, recipient_authority_id,
                body_sha256, envelope_sha256, transport, status,
                reason_code, receipt_sha256, response, completed_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            RETURNING attempt_id
            """,
            (
                message_id,
                sender_authority_id,
                recipient_authority_id,
                body_sha256,
                envelope_sha256,
                transport,
                status,
                reason_code,
                receipt_sha256,
                Jsonb(response or {}),
            ),
        )
        (attempt_id,) = cur.fetchone()
    return PorosDeliveryAttemptResult(
        attempt_id=int(attempt_id),
        message_id=message_id,
        body_sha256=body_sha256,
        status=status,
        reason_code=reason_code,
        receipt_sha256=receipt_sha256,
    )


def _published_generation(
    conn: psycopg.Connection,
    generation_id: int | str,
) -> dict[str, Any] | None:
    # Chronos cutover: the chronos-native publisher no longer writes
    # okg.graph_generations rows — a published generation is a Chronos
    # CHECKPOINT whose id is a STRING (`okg:<deployment>:branch:default:gen:
    # <ts>:<uuid>`). Resolve it from the checkpoint store instead of a bigint
    # graph_generations lookup (which crashes on a chronos string id with
    # `invalid input syntax for type bigint`). A bare-int compat id also
    # resolves via the store. The return contract is unchanged.
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    try:
        cp = store.get_generation(generation_id)
    except Exception:  # noqa: BLE001 — missing checkpoint -> "not published".
        cp = None
    if cp is None:
        return None
    meta = dict(cp.metadata or {})
    if meta.get("status") != "published":
        return None
    return {
        "generation_id": cp.checkpoint_id,
        "deployment_name": str(meta.get("deployment_name") or ""),
        "status": str(meta.get("status")),
    }


def _strip_external_handle(node: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of `node` with its external-backed HANDLE removed.

    The handle (issue #83) is a local-only pointer into the sender's systems;
    it must never travel to a peer as a dangling reference. Stable metadata and
    rollup facts remain.
    """
    from okg.substrate.external_backed.handle import HANDLE_ATTR

    attrs = node.get("attrs")
    if not isinstance(attrs, dict) or HANDLE_ATTR not in attrs:
        return node
    stripped = {k: v for k, v in attrs.items() if k != HANDLE_ATTR}
    return {**node, "attrs": stripped}


def _strip_ingest_image_fields(item: dict[str, Any]) -> dict[str, Any]:
    """Return `item` with every image value in its attrs reduced to the bare
    reference: ``png`` and ``error`` dropped (#3046).

    Those keys are local to this okg: ``png.id`` names a row in its
    ``okg.blobs``, and a receiver refuses a value carrying either as
    ``image_value_forged``. The reference alone travels.
    """
    from okg.substrate.images.values import strip_ingest_fields

    attrs = item.get("attrs")
    reduced = strip_ingest_fields(attrs)
    if reduced is attrs:
        return item
    return {**item, "attrs": reduced}


def _visible_nodes(
    conn: psycopg.Connection,
    session: Any,
    graph: Any,
    *,
    node_ids: tuple[str, ...],
    subject: str,
) -> list[dict[str, Any]]:
    rows = graph.query(
        """
        SELECT node_id, subtype, attrs, source, source_record_id, source_revision
          FROM okg.graph_nodes
         WHERE node_id = ANY(:node_ids)
        """,
        {"node_ids": list(node_ids)},
    )
    from okg.substrate.nomos.read_filter import filter_node_rows_for_session

    rows = filter_node_rows_for_session(
        conn,
        session,
        rows,
        action="graph.read.export_like",
        subject=subject,
        audit=True,
        surface="poros_export",
    )
    return [
        {
            "node_id": str(row["node_id"]),
            "subtype": str(row["subtype"]),
            "attrs": dict(row["attrs"] or {}),
        }
        for row in rows
    ]


def _visible_internal_edges(
    conn: psycopg.Connection,
    session: Any,
    graph: Any,
    *,
    node_ids: tuple[str, ...],
    nodes_by_id: dict[str, dict[str, Any]],
    subject: str,
) -> list[dict[str, Any]]:
    rows = graph.query(
        """
        SELECT edge_id, src, edge_type, dst, attrs,
               source, source_record_id, source_revision
          FROM okg.graph_edges
         WHERE src = ANY(:node_ids)
           AND dst = ANY(:node_ids)
         ORDER BY src, edge_type, dst
        """,
        {"node_ids": list(node_ids)},
    )
    from okg.substrate.nomos.read_filter import filter_edge_rows_for_session

    rows = filter_edge_rows_for_session(
        conn,
        session,
        rows,
        action="graph.read.export_like",
        subject=subject,
        audit=True,
        surface="poros_export",
    )
    edges: list[dict[str, Any]] = []
    for row in rows:
        src_id = str(row["src"])
        dst_id = str(row["dst"])
        edge = {
            "src": src_id,
            "edge_type": str(row["edge_type"]),
            "dst": dst_id,
            "attrs": dict(row["attrs"] or {}),
        }
        if src_id in nodes_by_id:
            edge["src_subtype"] = nodes_by_id[src_id]["subtype"]
        if dst_id in nodes_by_id:
            edge["dst_subtype"] = nodes_by_id[dst_id]["subtype"]
        edges.append(edge)
    return edges


def _persist_sender_detached_object_manifests(
    conn: psycopg.Connection,
    *,
    sender_authority_id: str,
    message_id: str,
    body_sha256: str,
    envelope: dict[str, Any],
) -> None:
    body = envelope.get("body")
    if not isinstance(body, dict) or not isinstance(body.get("objects"), list):
        return
    with conn.cursor() as cur:
        for raw in body["objects"]:
            if not isinstance(raw, dict):
                continue
            object_id = raw.get("object_id")
            if not isinstance(object_id, str) or not object_id:
                continue
            object_hash = (
                raw.get("cleartext_sha256")
                or raw.get("ciphertext_sha256")
                or raw.get("sha256")
                or ""
            )
            cur.execute(
                """
                INSERT INTO okg.poros_detached_object_manifests (
                    message_id, body_sha256, authority_id, side,
                    object_id, media_type, size_bytes, object_sha256,
                    chunk_count, classification, encrypted, manifest
                )
                VALUES (%s, %s, %s, 'sender', %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (side, authority_id, message_id, object_id)
                DO UPDATE SET
                    body_sha256 = EXCLUDED.body_sha256,
                    media_type = EXCLUDED.media_type,
                    size_bytes = EXCLUDED.size_bytes,
                    object_sha256 = EXCLUDED.object_sha256,
                    chunk_count = EXCLUDED.chunk_count,
                    classification = EXCLUDED.classification,
                    encrypted = EXCLUDED.encrypted,
                    manifest = EXCLUDED.manifest
                """,
                (
                    message_id,
                    body_sha256,
                    sender_authority_id,
                    object_id,
                    raw.get("media_type"),
                    int(raw.get("size_bytes") or raw.get("size") or 0),
                    str(object_hash),
                    len(raw.get("chunks") or []),
                    str(raw.get("classification") or "public"),
                    bool("encryption" in raw or "ciphertext_sha256" in raw),
                    Jsonb(raw),
                ),
            )
