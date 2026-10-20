"""Runnable Poros mailbox incorporation demo.

This module is intentionally outside the lightweight mailbox import path: it
requires the full Poros receiver, Nomos, publisher, and Postgres substrate.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from okg.substrate.exchange import (
    build_unsigned_envelope,
    generate_ed25519_keypair,
    sign_envelope,
    submit_poros_envelope_bytes,
)
from okg.substrate.exchange.canonical_json import canonical_json_bytes
from okg.substrate.federation.mailbox import MailboxService, PostgresMailboxStore
from okg.substrate.nomos import compile_policy_graph
from okg.substrate.db.connection import connect as db_connect


SENDER_AUTHORITY = "okg:lab-a"
RECEIVER_AUTHORITY = "okg:mit"


def run_mailbox_incorporation_demo(
    dsn: str,
    *,
    policy_dir: Path | str | None = None,
) -> dict[str, Any]:
    """Send two mailbox messages and return receiver-side evidence.

    The accepted message is public context and is acknowledged only after
    Poros verification, Nomos allow, admission, publish, and live-row proof.
    The denied message is restricted context and stays audit-only.
    """
    if policy_dir is None:
        with tempfile.TemporaryDirectory(prefix="poros-mailbox-demo-") as tmp:
            return _run_mailbox_incorporation_demo(dsn, policy_dir=Path(tmp))
    return _run_mailbox_incorporation_demo(dsn, policy_dir=Path(policy_dir))


def _run_mailbox_incorporation_demo(
    dsn: str,
    *,
    policy_dir: Path,
) -> dict[str, Any]:
    run_id = uuid.uuid4().hex[:10]
    sender_keys = generate_ed25519_keypair(f"lab-a-mailbox-demo-{run_id}")
    receiver_keys = generate_ed25519_keypair(f"mit-mailbox-demo-{run_id}")
    trust_pins = {
        "pins": [
            {
                "authority_id": SENDER_AUTHORITY,
                "kid": sender_keys.kid,
                "use": "sign",
                "alg": "Ed25519",
                "public_key": sender_keys.public_key,
            }
        ]
    }
    policy_path = _write_allow_public_context_policy(policy_dir)
    accepted_message_id = f"mailbox-demo-{run_id}-accepted"
    denied_message_id = f"mailbox-demo-{run_id}-denied-by-policy"
    accepted = _signed_envelope(
        sender_keys,
        message_id=accepted_message_id,
        classification="public",
    )
    denied = _signed_envelope(
        sender_keys,
        message_id=denied_message_id,
        classification="restricted",
    )
    accepted_node_id = str(accepted["body"]["root_node_ids"][0])
    denied_node_id = str(denied["body"]["root_node_ids"][0])

    before = {
        "accepted_live_count": _live_count(dsn, accepted_node_id),
        "denied_live_count": _live_count(dsn, denied_node_id),
    }
    with db_connect(dsn) as conn:
        PostgresMailboxStore.install_schema(conn)
        service = MailboxService(store=PostgresMailboxStore(conn))

        service.submit_envelope(accepted)
        accepted_lease = service.poll(RECEIVER_AUTHORITY, lease_seconds=120)[0]
        accepted_outcome = submit_poros_envelope_bytes(
            canonical_json_bytes(dict(accepted_lease.envelope or {})),
            trust_pins=trust_pins,
            receiver_keys=receiver_keys.__dict__,
            receiver=RECEIVER_AUTHORITY,
            dsn=dsn,
            deployment="default",
            admission_mode="fast_track",
            persist_preflight=True,
            apply=True,
            publish=True,
            nomos_bundle=policy_path,
        )
        if accepted_outcome.exit_code != 0:
            raise RuntimeError(
                "accepted mailbox demo message did not publish: "
                + json.dumps(accepted_outcome.payload, sort_keys=True)
            )
        service.ack_message(
            accepted_lease.message_id,
            lease_token=accepted_lease.lease_token or "",
            receipt_evidence=_receipt_evidence(accepted_outcome.payload),
        )

        service.submit_envelope(denied)
        denied_lease = service.poll(RECEIVER_AUTHORITY, lease_seconds=120)[0]
        denied_outcome = submit_poros_envelope_bytes(
            canonical_json_bytes(dict(denied_lease.envelope or {})),
            trust_pins=trust_pins,
            receiver_keys=receiver_keys.__dict__,
            receiver=RECEIVER_AUTHORITY,
            dsn=dsn,
            deployment="default",
            admission_mode="fast_track",
            persist_preflight=True,
            apply=True,
            publish=True,
            nomos_bundle=policy_path,
        )
        if denied_outcome.exit_code == 0:
            raise RuntimeError("denied mailbox demo message unexpectedly published")
        service.deny_message(
            denied_lease.message_id,
            lease_token=denied_lease.lease_token or "",
            reason_code=denied_outcome.payload["nomos_decision"]["reason_code"],
            denial_evidence={
                "poros_status": denied_outcome.payload["status"],
                "preflight": denied_outcome.payload["preflight"],
                "nomos_decision": {
                    "decision_id": denied_outcome.payload["nomos_decision"][
                        "decision_id"
                    ],
                    "effect": denied_outcome.payload["nomos_decision"]["effect"],
                    "reason_code": denied_outcome.payload["nomos_decision"][
                        "reason_code"
                    ],
                },
            },
        )

    after = {
        "accepted_live_count": _live_count(dsn, accepted_node_id),
        "denied_live_count": _live_count(dsn, denied_node_id),
    }
    return {
        "run_id": run_id,
        "sender_authority": SENDER_AUTHORITY,
        "receiver_authority": RECEIVER_AUTHORITY,
        "policy_bundle": str(policy_path),
        "before": before,
        "after": after,
        "accepted": {
            "message_id": accepted_message_id,
            "node_id": accepted_node_id,
            "mailbox": _mailbox_record(dsn, accepted_message_id),
            "poros_status": accepted_outcome.payload["status"],
            "nomos_decision": _decision_summary(
                accepted_outcome.payload["nomos_decision"]
            ),
            "admission": accepted_outcome.payload["admission"],
            "publish": accepted_outcome.payload["publish"],
            "live_counts": accepted_outcome.payload["live_counts"],
            "incorporation_events": _incorporation_events(dsn, accepted_message_id),
        },
        "denied": {
            "message_id": denied_message_id,
            "node_id": denied_node_id,
            "mailbox": _mailbox_record(dsn, denied_message_id),
            "poros_status": denied_outcome.payload["status"],
            "nomos_decision": _decision_summary(
                denied_outcome.payload["nomos_decision"]
            ),
            "admission": denied_outcome.payload["admission"],
            "publish": denied_outcome.payload["publish"],
            "live_counts": denied_outcome.payload["live_counts"],
            "incorporation_events": _incorporation_events(dsn, denied_message_id),
        },
    }


def _signed_envelope(
    sender_keys: Any,
    *,
    message_id: str,
    classification: str,
) -> dict[str, Any]:
    unsigned = build_unsigned_envelope(
        sender=SENDER_AUTHORITY,
        recipient=RECEIVER_AUTHORITY,
        message_id=message_id,
        created_at=datetime.now(UTC).replace(microsecond=0),
        ttl_seconds=900,
        body=_graph_body(message_id, classification=classification),
    )
    return sign_envelope(unsigned, sender_keys.private_key, kid=sender_keys.kid)


def _graph_body(message_id: str, *, classification: str) -> dict[str, Any]:
    node_id = f"poros:mailbox-demo:{message_id}"
    return {
        "graph_id": f"poros:graph:mailbox-demo:{message_id}",
        "origin_authority_id": SENDER_AUTHORITY,
        "origin_generation_id": 101,
        "classification": classification,
        "graph_profile": "context",
        "root_node_ids": [node_id],
        "nodes": [
            {
                "node_id": node_id,
                "subtype": "context_packet",
                "attrs": {
                    "text": (
                        "Mailbox demo context packet transferred from a "
                        "personal OKG into the receiver graph."
                    )
                },
            }
        ],
        "edges": [],
    }


def _write_allow_public_context_policy(out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    policy_graph = {
        "policy_bundle_id": "nomos:test:mailbox-incorporation-demo",
        "policy_version": "v1",
        "source_generation_id": 0,
        "scope": {"deployment": "default"},
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_lab_a_public_context",
                "effect": "allow",
                "priority": 100,
                "subjects": [SENDER_AUTHORITY],
                "actions": ["poros.graph.submit"],
                "match": {
                    "classification": "public",
                    "graph_profile": "context",
                    "root_subtypes": ["context_packet"],
                    "requested_incorporation_mode": "fast_track",
                },
                "reason_code": "nomos.test.mailbox_demo.public_context_allowed",
            }
        ],
    }
    bundle = compile_policy_graph(policy_graph).to_dict()
    path = out_dir / "compiled-policy.json"
    path.write_text(
        json.dumps(bundle, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _receipt_evidence(payload: dict[str, Any]) -> dict[str, Any]:
    preflight = payload["preflight"]
    admission = payload["admission"]
    publish = payload["publish"]
    decision = payload["nomos_decision"]
    return {
        "poros_status": payload["status"],
        "poros_inbox_id": preflight["inbox_id"],
        "receipt_sha256": preflight["receipt_sha256"],
        "nomos_decision_id": decision["decision_id"],
        "admission_id": admission["admission_id"],
        "receiver_generation_id": publish["generation_id"],
        "live_counts": payload["live_counts"],
    }


def _decision_summary(decision: dict[str, Any] | None) -> dict[str, Any] | None:
    if decision is None:
        return None
    return {
        "decision_id": decision.get("decision_id"),
        "effect": decision.get("effect"),
        "reason_code": decision.get("reason_code"),
        "policy_bundle_id": decision.get("policy_bundle_id"),
    }


def _live_count(dsn: str, node_id: str) -> int:
    from okg.substrate.graph_store import ChronosGraphStore

    # ensure_schema=True: the "before" snapshot can run prior to the
    # first publish that initializes the Chronos branch metadata, so
    # ensure the store/branch exist (idempotent) before checkout.
    store = ChronosGraphStore.connect(dsn)
    try:
        rows = store.checkout().query(
            """
            SELECT count(*) AS n
              FROM okg.graph_nodes
             WHERE node_id = :node_id
            """,
            {"node_id": node_id},
        )
        return int(rows[0]["n"] or 0)
    finally:
        store.close()


def _mailbox_record(dsn: str, message_id: str) -> dict[str, Any]:
    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, payload_mode, packet_sha256, body_sha256,
                   receipt_evidence, deny_reason_code, denial_evidence
              FROM okg.federation_mailbox_messages
             WHERE message_id = %s
            """,
            (message_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"mailbox record missing for {message_id}")
    return {
        "status": row[0],
        "payload_mode": row[1],
        "packet_sha256": row[2],
        "body_sha256": row[3],
        "receipt_evidence": row[4],
        "deny_reason_code": row[5],
        "denial_evidence": row[6],
    }


def _incorporation_events(dsn: str, message_id: str) -> list[dict[str, Any]]:
    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_type, status, reason_code, admission_id,
                   receiver_generation_id
              FROM okg.poros_incorporation_events
             WHERE message_id = %s
             ORDER BY event_id
            """,
            (message_id,),
        )
        rows = cur.fetchall()
    return [
        {
            "event_type": row[0],
            "status": row[1],
            "reason_code": row[2],
            "admission_id": str(row[3]) if row[3] is not None else None,
            "receiver_generation_id": row[4],
        }
        for row in rows
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the Poros mailbox receiver-owned incorporation demo.",
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("OKG_TEST_DSN") or os.environ.get("OKG_DSN"),
        help="Postgres DSN for an initialized OKG substrate database.",
    )
    parser.add_argument(
        "--policy-dir",
        type=Path,
        default=None,
        help="Directory where the compiled demo Nomos bundle should be written.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional path for the JSON evidence bundle.",
    )
    args = parser.parse_args(argv)
    if not args.dsn:
        parser.error("--dsn is required unless OKG_TEST_DSN or OKG_DSN is set")
    evidence = run_mailbox_incorporation_demo(
        str(args.dsn),
        policy_dir=args.policy_dir,
    )
    rendered = json.dumps(evidence, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
