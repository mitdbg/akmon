"""Build Poros refinement traces from live sender/receiver databases."""

from __future__ import annotations

from typing import Any

from okg.substrate.exchange.refinement import PorosRefinementError
from okg.substrate.db.connection import connect as db_connect


def build_trace_from_databases(
    *,
    sender_dsn: str,
    receiver_dsn: str,
    message_id: str,
) -> dict[str, Any]:
    with db_connect(sender_dsn, autocommit=True) as sender_conn:
        outbox = _load_outbox(sender_conn, message_id=message_id)
        delivery_attempts = _load_delivery_attempts(
            sender_conn,
            message_id=message_id,
        )
    with db_connect(receiver_dsn, autocommit=True) as receiver_conn:
        inbox = _load_inbox(receiver_conn, message_id=message_id)
        events = _load_events(receiver_conn, message_id=message_id)
        receiver_generation_id = _published_generation_for_message(events)
        live_rows = _load_live_rows(
            receiver_conn,
            message_id=message_id,
            receiver_generation_id=receiver_generation_id,
        )
    if outbox is not None:
        # receiver_authorized is the RECEIVER's decision, not the sender's, so
        # it is derived here (where the receiver events are in scope) from the
        # recorded Nomos decision rather than in _load_outbox on the sender DB.
        outbox["receiver_authorized"] = _receiver_authorized(events)
    inbox_trace = {
        key: value for key, value in inbox.items() if not key.startswith("_")
    }
    return {
        "outbox": [outbox] if outbox is not None else [],
        "delivery_attempts": delivery_attempts,
        "inbox": [inbox_trace],
        "receipts": [_receipt_row(inbox, events)],
        "nomos_decisions": _decision_rows(events, inbox),
        "promotion_decisions": _promotion_decision_rows(events, inbox),
        "admissions": _admission_rows(events, inbox),
        "generations": _generation_rows(events),
        "live_rows": live_rows,
        "proofs": [_proof_row(inbox)],
        "unsupported": _unsupported_events(events),
    }


def _load_outbox(conn, *, message_id: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT sender_authority_id, recipient_authority_id,
                   sender_generation_id, body_sha256, graph_id,
                   signing_key_id, payload_mode, classification,
                   graph_profile,
                   (
                     SELECT count(*)
                       FROM okg.poros_detached_object_manifests m
                      WHERE m.message_id = o.message_id
                        AND m.side = 'sender'
                   ) AS sender_manifest_count
              FROM okg.poros_outbox_messages AS o
             WHERE o.message_id = %s
            """,
            (message_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "message_id": message_id,
        "sender": row[0],
        "recipient": row[1],
        "sender_generation_id": row[2],
        "body_sha256": row[3],
        "graph_id": row[4],
        "signing_key_id": row[5],
        "detached": row[6] == "detached",
        "object_hash_valid": row[6] != "detached" or int(row[9] or 0) > 0,
        # sender_hint: the sender declared a claim/proposal-shaped graph
        # (anything other than neutral "context"), i.e. it is asserting
        # content whose incorporation only the receiver's policy may grant.
        "sender_hint": bool(row[8] and row[8] != "context"),
        # receiver_authorized is filled in build_trace_from_databases from the
        # receiver's recorded Nomos decision; a placeholder here would mask a
        # receiver deny, so it is deliberately left unset until then.
        "classification": row[7],
    }


def _receiver_authorized(events: list[dict[str, Any]]) -> bool:
    """Whether the receiver's policy authorized the message.

    Mirrors the model's ``HasReceiverAllow`` (``OriginalAllow \\/
    PromotionAllow``): authorization exists iff the receiver recorded a Nomos
    ``nomos_decided`` or ``promotion_decided`` event with effect ``allow``. A
    deny, review, or quarantine leaves the message unauthorized, so the
    sender-authority guard's ``not receiver_authorized`` conjunct
    (refinement.py) reflects a real receiver decision instead of the former
    hardcoded ``True``.
    """
    for event in events:
        if (
            event.get("event_type") in {"nomos_decided", "promotion_decided"}
            and event.get("status") == "allow"
        ):
            return True
    return False


def _load_delivery_attempts(conn, *, message_id: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, reason_code, receipt_sha256, transport,
                   body_sha256, envelope_sha256
              FROM okg.poros_delivery_attempts
             WHERE message_id = %s
             ORDER BY attempt_id
            """,
            (message_id,),
        )
        rows = cur.fetchall()
    return [
        {
            "message_id": message_id,
            "status": row[0],
            "reason_code": row[1],
            "receipt_sha256": row[2],
            "transport": row[3],
            "body_sha256": row[4],
            "envelope_sha256": row[5],
        }
        for row in rows
    ]


def _load_inbox(conn, *, message_id: str) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, reason_code, body_sha256, trusted,
                   signature_valid, recipient_valid, body_hash_valid,
                   fresh, receipt, envelope
              FROM okg.poros_inbox_messages
             WHERE message_id = %s
            """,
            (message_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise PorosRefinementError(
            f"missing receiver inbox evidence for {message_id!r}"
        )
    return {
        "message_id": message_id,
        "status": row[0],
        "reason_code": row[1],
        "body_sha256": row[2],
        "trusted": bool(row[3]),
        "signature_valid": bool(row[4]),
        "recipient_valid": bool(row[5]),
        "body_hash_valid": bool(row[6]),
        "fresh": bool(row[7]),
        "detached_object_valid": row[0] == "accepted_for_admission",
        "_receipt": row[8],
        "_envelope": row[9],
    }


def _load_events(conn, *, message_id: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_type, status, reason_code, nomos_decision_id,
                   policy_bundle_id, policy_fingerprint, nomos_input_hash,
                   admission_id::text, receiver_generation_id::text, payload
              FROM okg.poros_incorporation_events
             WHERE message_id = %s
             ORDER BY event_id
            """,
            (message_id,),
        )
        rows = cur.fetchall()
    return [
        {
            "message_id": message_id,
            "event_type": row[0],
            "status": row[1],
            "reason_code": row[2],
            "nomos_decision_id": row[3],
            "policy_bundle_id": row[4],
            "policy_fingerprint": row[5],
            "nomos_input_hash": row[6],
            "admission_id": row[7],
            "receiver_generation_id": row[8],
            "payload": row[9] if isinstance(row[9], dict) else {},
        }
        for row in rows
    ]


def _published_generation_for_message(
    events: list[dict[str, Any]],
) -> str:
    """Receiver generation a message published into, from its events.

    The Chronos graph (``okg.graph_nodes`` / ``okg.graph_edges``)
    carries no per-row generation column; the message's receiver
    generation is taken from its ``published`` event instead.
    """
    for event in events:
        if event.get("event_type") == "published":
            gen = event.get("receiver_generation_id")
            if gen is not None:
                return str(gen)
    return ""


def _load_live_rows(
    conn,
    *,
    message_id: str,
    receiver_generation_id: str,
) -> list[dict[str, Any]]:
    from okg.substrate.graph_store import ChronosGraphStore

    rows: list[dict[str, Any]] = []
    # Read the facts this message produced from the audit log, then
    # confirm each landed in the Chronos graph (the live projection no
    # longer exists). The receiver generation comes from the message's
    # published event, not a per-row bitemporal column.
    graph = ChronosGraphStore.from_connection(
        conn, ensure_schema=False,
    ).checkout()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT nf.node_id, nf.source_record_id
              FROM okg.node_facts nf
             WHERE nf.source_record_id->'poros'->>'message_id' = %s
             ORDER BY nf.fact_id
            """,
            (message_id,),
        )
        node_fact_rows = cur.fetchall()
        cur.execute(
            """
            SELECT ef.src, ef.edge_type, ef.dst, ef.source_record_id
              FROM okg.edge_facts ef
             WHERE ef.source_record_id->'poros'->>'message_id' = %s
             ORDER BY ef.fact_id
            """,
            (message_id,),
        )
        edge_fact_rows = cur.fetchall()

    for node_id, source_record_id in node_fact_rows:
        present = graph.query(
            "SELECT 1 AS ok FROM okg.graph_nodes WHERE node_id = :node_id LIMIT 1",
            {"node_id": node_id},
        )
        if not present:
            continue
        rows.append(
            _live_row(
                row_id=f"node:{node_id}",
                row_kind="node",
                source_record_id=source_record_id,
                receiver_generation_id=receiver_generation_id,
            )
        )
    for src, edge_type, dst, source_record_id in edge_fact_rows:
        present = graph.query(
            """
            SELECT 1 AS ok
              FROM okg.graph_edges
             WHERE src = :src AND edge_type = :edge_type AND dst = :dst
             LIMIT 1
            """,
            {"src": src, "edge_type": edge_type, "dst": dst},
        )
        if not present:
            continue
        rows.append(
            _live_row(
                row_id=f"edge:{src}:{edge_type}:{dst}",
                row_kind="edge",
                source_record_id=source_record_id,
                receiver_generation_id=receiver_generation_id,
            )
        )
    return rows


def _live_row(
    *,
    row_id: str,
    row_kind: str,
    source_record_id: dict[str, Any],
    receiver_generation_id: str,
) -> dict[str, Any]:
    poros = dict(source_record_id.get("poros") or {})
    return {
        "row_id": row_id,
        "row_kind": row_kind,
        "message_id": str(poros["message_id"]),
        "admission_id": str(source_record_id["admission_id"]),
        "sender_generation_id": str(poros["origin_generation_id"]),
        "receiver_generation_id": receiver_generation_id,
        "body_sha256": str(poros["body_sha256"]),
    }


def _receipt_row(
    inbox: dict[str, Any],
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    receipt = inbox.get("_receipt")
    receipt = receipt if isinstance(receipt, dict) else {}
    admitted = next(
        (event for event in events if event["event_type"] == "admitted"),
        {},
    )
    return {
        "message_id": str(receipt.get("in_reply_to") or inbox["message_id"]),
        "status": str(receipt.get("status") or inbox["status"]),
        "received_body_sha256": str(
            receipt.get("received_body_sha256") or inbox["body_sha256"]
        ),
        "admission_id": admitted.get("admission_id"),
    }


def _decision_rows(
    events: list[dict[str, Any]],
    inbox: dict[str, Any],
) -> list[dict[str, Any]]:
    rows = []
    for event in events:
        if event["event_type"] != "nomos_decided":
            continue
        payload = event["payload"]
        rows.append(
            {
                "message_id": inbox["message_id"],
                "decision_id": event["nomos_decision_id"],
                "effect": event["status"],
                "input_hash": event["nomos_input_hash"],
                "policy_bundle_id": event["policy_bundle_id"],
                "policy_fingerprint": event["policy_fingerprint"]
                or payload.get("policy_fingerprint"),
                "uses_receiver_policy": True,
                "classification": payload.get("classification", "public"),
                "obligations": payload.get("obligations", []),
                "reason_code": event["reason_code"],
            }
        )
    return rows


def _promotion_decision_rows(
    events: list[dict[str, Any]],
    inbox: dict[str, Any],
) -> list[dict[str, Any]]:
    rows = []
    for event in events:
        if event["event_type"] != "promotion_decided":
            continue
        payload = event["payload"]
        rows.append(
            {
                "message_id": inbox["message_id"],
                "decision_id": event["nomos_decision_id"],
                "effect": event["status"],
                "input_hash": event["nomos_input_hash"],
                "policy_bundle_id": event["policy_bundle_id"],
                "policy_fingerprint": event["policy_fingerprint"]
                or payload.get("policy_fingerprint"),
                "uses_receiver_policy": True,
                "classification": payload.get("classification", "public"),
                "obligations": payload.get("obligations", []),
                "reason_code": event["reason_code"],
            }
        )
    return rows


def _admission_rows(
    events: list[dict[str, Any]],
    inbox: dict[str, Any],
) -> list[dict[str, Any]]:
    rows = []
    for event in events:
        if event["event_type"] != "admitted":
            continue
        admission = event["payload"].get("admission") or {}
        rows.append(
            {
                "message_id": inbox["message_id"],
                "admission_id": event["admission_id"],
                "status": event["status"],
                "body_sha256": inbox["body_sha256"],
                "source": admission.get("source"),
                "run_id": admission.get("run_id"),
            }
        )
    return rows


def _generation_rows(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for event in events:
        if event["event_type"] != "published":
            continue
        rows.append(
            {
                "message_id": event["payload"].get("publish", {}).get(
                    "message_id",
                    "",
                )
                or str(event["message_id"]),
                "generation_id": event["receiver_generation_id"],
                "status": event["status"],
                "admission_id": event["admission_id"],
            }
        )
    return rows


def _unsupported_events(events: list[dict[str, Any]]) -> list[str]:
    supported = {
        "preflight_accepted",
        "preflight_rejected",
        "duplicate",
        "nomos_decided",
        "admitted",
        "published",
        "review",
        "quarantined",
        "promotion_decided",
        "promotion_denied",
        "promoted",
        "rejection_decided",
        "rejection_denied",
        "rejected",
    }
    return sorted(
        {
            str(event.get("event_type"))
            for event in events
            if event.get("event_type") not in supported
        }
    )


def _proof_row(inbox: dict[str, Any]) -> dict[str, Any]:
    envelope = inbox.get("_envelope")
    body = envelope.get("body") if isinstance(envelope, dict) else {}
    body = body if isinstance(body, dict) else {}
    classification = str(body.get("classification") or "public")
    return {
        "message_id": inbox["message_id"],
        "visibility": "public",
        "classification": classification,
        "redacted": classification in {"restricted", "private"},
        "declassified": False,
    }
