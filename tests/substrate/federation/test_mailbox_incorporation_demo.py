from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
import pytest

from okg.substrate.exchange import (
    build_unsigned_envelope,
    generate_ed25519_keypair,
    sign_envelope,
    submit_poros_envelope_bytes,
)
from okg.substrate.exchange.canonical_json import canonical_json_bytes
from okg.substrate.federation.mailbox import MailboxService, PostgresMailboxStore
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.nomos import compile_policy_graph


pytestmark = pytest.mark.postgres

SENDER_AUTHORITY = "okg:lab-a"
RECEIVER_AUTHORITY = "okg:mit"


from tests.pytest.catalog_fixture_builders import (  # noqa: E402
    bind_factory_test_deployment,
    seed_catalog,
)

# Node subtypes the Poros messages in these tests carry. The migrated test
# catalog declares no subtypes, and every publish runs the node-subtype
# conformance gate, so a message node is admitted but its publish is
# blocked until its subtype is in the catalog the explicit test writer binds.
_POROS_TEST_SUBTYPES = (
    "context_packet",
    "concept",
    "proposal",
    "person",
    "organization",
    "course",
)


@pytest.fixture
def _explicit_factory_publish_authority(
    monkeypatch, tmp_path, clean_runtime: str,
) -> None:
    """Explicit publish authority for the product publish path.

    The demo's receiver builds its publisher through ``build_deployment_publisher``, which
    refuses the synthetic ``default`` deployment these tests drive: it has no
    catalog writer authority and no committed deployment directory. Bind
    the explicit pytest catalog writer and a committed synthetic
    deployment directory instead, as the merge-back tests do (#2866),
    and declare the message subtypes in the catalog that writer binds.
    """
    _seed_poros_test_subtypes(clean_runtime)
    bind_factory_test_deployment(monkeypatch, tmp_path, "default")


def _seed_poros_test_subtypes(dsn: str) -> None:
    """Apply a test catalog that declares ``_POROS_TEST_SUBTYPES``."""
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=list(_POROS_TEST_SUBTYPES))


def test_mailbox_top_level_import_does_not_require_postgres_driver() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    env = dict(os.environ)
    env["PYTHONPATH"] = (
        str(repo_root / "src")
        + os.pathsep
        + env.get("PYTHONPATH", "")
    )
    script = """
import importlib.abc
import sys

class BlockPsycopg(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "psycopg" or fullname.startswith("psycopg."):
            raise ModuleNotFoundError("blocked psycopg")
        return None

sys.meta_path.insert(0, BlockPsycopg())
import okg.substrate.federation.mailbox as mailbox

assert mailbox.InMemoryMailboxStore
assert mailbox.MailboxPollingClient
assert mailbox.MailboxService
assert mailbox.MemoryObjectStore
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        env=env,
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.usefixtures("_explicit_factory_publish_authority")
def test_mailbox_delivery_then_receiver_owned_incorporation(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    sender_keys = generate_ed25519_keypair("lab-a-mailbox-demo-sign")
    receiver_keys = generate_ed25519_keypair("mit-mailbox-demo-sign")
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
    policy_path = _write_allow_public_context_policy(tmp_path / "nomos")

    accepted = _signed_envelope(
        sender_keys,
        message_id="mailbox-demo-accepted",
        classification="public",
    )
    denied = _signed_envelope(
        sender_keys,
        message_id="mailbox-demo-denied-by-policy",
        classification="restricted",
    )
    accepted_node_id = str(accepted["body"]["root_node_ids"][0])
    denied_node_id = str(denied["body"]["root_node_ids"][0])

    with psycopg.connect(clean_runtime) as conn:
        service = MailboxService(store=PostgresMailboxStore(conn))

        service.submit_envelope(accepted)
        leased = service.poll(RECEIVER_AUTHORITY, lease_seconds=120)
        assert [record.message_id for record in leased] == ["mailbox-demo-accepted"]
        assert _live_count(clean_runtime, accepted_node_id) == 0

        accepted_outcome = submit_poros_envelope_bytes(
            canonical_json_bytes(dict(leased[0].envelope or {})),
            trust_pins=trust_pins,
            receiver_keys=receiver_keys.__dict__,
            receiver=RECEIVER_AUTHORITY,
            dsn=clean_runtime,
            deployment="default",
            admission_mode="fast_track",
            persist_preflight=True,
            apply=True,
            publish=True,
            nomos_bundle=policy_path,
        )
        assert accepted_outcome.exit_code == 0, accepted_outcome.payload
        assert accepted_outcome.payload["nomos_decision"]["effect"] == "allow"
        assert accepted_outcome.payload["admission"]["status"] == "accepted"
        assert accepted_outcome.payload["publish"]["status"] == "published"
        assert accepted_outcome.payload["live_counts"] == {
            "nodes_live": 1,
            "edges_live": 0,
        }

        service.ack_message(
            leased[0].message_id,
            lease_token=leased[0].lease_token or "",
            receipt_evidence=_receipt_evidence(accepted_outcome.payload),
        )

        service.submit_envelope(denied)
        denied_lease = service.poll(RECEIVER_AUTHORITY, lease_seconds=120)
        assert [record.message_id for record in denied_lease] == [
            "mailbox-demo-denied-by-policy"
        ]
        denied_outcome = submit_poros_envelope_bytes(
            canonical_json_bytes(dict(denied_lease[0].envelope or {})),
            trust_pins=trust_pins,
            receiver_keys=receiver_keys.__dict__,
            receiver=RECEIVER_AUTHORITY,
            dsn=clean_runtime,
            deployment="default",
            admission_mode="fast_track",
            persist_preflight=True,
            apply=True,
            publish=True,
            nomos_bundle=policy_path,
        )
        assert denied_outcome.exit_code == 1
        assert denied_outcome.payload["nomos_decision"]["effect"] == "deny"
        assert denied_outcome.payload["admission"] is None
        assert _live_count(clean_runtime, denied_node_id) == 0

        service.deny_message(
            denied_lease[0].message_id,
            lease_token=denied_lease[0].lease_token or "",
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

    assert _live_count(clean_runtime, accepted_node_id) == 1
    assert _live_count(clean_runtime, denied_node_id) == 0
    assert _mailbox_statuses(clean_runtime) == {
        "mailbox-demo-accepted": "acknowledged",
        "mailbox-demo-denied-by-policy": "denied",
    }
    assert _mailbox_event_types(clean_runtime, "mailbox-demo-accepted") == [
        "submitted",
        "claimed",
        "acknowledged",
    ]
    assert _mailbox_event_types(clean_runtime, "mailbox-demo-denied-by-policy") == [
        "submitted",
        "claimed",
        "denied",
    ]
    assert _incorporation_event_types(clean_runtime, "mailbox-demo-accepted") == [
        "preflight_accepted",
        "nomos_decided",
        "admitted",
        "published",
    ]
    assert _incorporation_event_types(clean_runtime, "mailbox-demo-denied-by-policy") == [
        "preflight_accepted",
        "nomos_decided",
    ]


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


def _live_count(dsn: str, node_id: str) -> int:
    # Post-chronos-cutover the live graph lives only in
    # okg.graph_nodes, read via a checkout (the legacy
    # okg.nodes_live projection was dropped).
    with psycopg.connect(dsn) as conn:
        rows = (
            ChronosGraphStore.from_connection(conn, ensure_schema=False)
            .checkout()
            .query(
                "SELECT count(*) AS n FROM okg.graph_nodes "
                "WHERE node_id = %(node_id)s",
                {"node_id": node_id},
            )
        )
        return int(rows[0]["n"] or 0)


def _mailbox_statuses(dsn: str) -> dict[str, str]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT message_id, status
              FROM okg.federation_mailbox_messages
             WHERE message_id LIKE 'mailbox-demo-%'
             ORDER BY message_id
            """
        )
        return {str(row[0]): str(row[1]) for row in cur.fetchall()}


def _mailbox_event_types(dsn: str, message_id: str) -> list[str]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_type
              FROM okg.federation_mailbox_events
             WHERE message_id = %s
             ORDER BY event_id
            """,
            (message_id,),
        )
        return [str(row[0]) for row in cur.fetchall()]


def _incorporation_event_types(dsn: str, message_id: str) -> list[str]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_type
              FROM okg.poros_incorporation_events
             WHERE message_id = %s
             ORDER BY event_id
            """,
            (message_id,),
        )
        return [str(row[0]) for row in cur.fetchall()]
