from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from psycopg.types.json import Jsonb

from okg.substrate.exchange.authority import (
    KeyDescriptor,
    TrustPin,
    TrustPinRegistry,
    build_authority_document,
)
from okg.substrate.exchange.canonical_json import canonical_json_bytes
from okg.substrate.exchange.envelope import build_unsigned_envelope
from okg.substrate.exchange.harness import describe_generation
from okg.substrate.exchange.http import build_poros_router
from okg.substrate.exchange.inbox import PorosInbox
from okg.substrate.exchange.object_store import LocalPorosObjectStore
from okg.substrate.exchange.outbox import export_published_subgraph_to_outbox
from okg.substrate.exchange.payloads import build_detached_object_manifest
from okg.substrate.exchange.sender_delivery import deliver_next_outbox_messages
from okg.substrate.exchange.signing import (
    derive_ed25519_public_key,
    generate_ed25519_keypair,
    sign_document,
    sign_envelope,
)
from okg.substrate.nomos import compile_policy_graph
from okg.substrate.nomos.demo import demo_policy_graph
from okg.substrate.publisher import Publisher
from tests.pytest.chronos_reset import reset_chronos_graph
from tests.substrate.conftest import (
    _DERIVED_RUNTIME_TABLES,
    _RUNTIME_TABLES,
    _clean_runtime_subset,
    _ensure_test_db_exists,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


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

    The Poros receiver builds its publisher through ``build_deployment_publisher``, which
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


@pytest.fixture(autouse=True)
def _allow_test_receiver(monkeypatch):
    """The outbound-delivery egress guard (centralize-egress-url-validation)
    refuses non-public hosts. These tests POST to a mocked `receiver.local`
    test receiver, so allow it via the explicit operator-exception allowlist."""
    monkeypatch.setenv("OKG_EGRESS_ALLOW_HOSTS", "receiver.local")


SENDER_PRIVATE = b"\x03" * 32
SENDER_PUBLIC = derive_ed25519_public_key(SENDER_PRIVATE)
RECEIVER_PRIVATE = b"\x04" * 32
RECEIVER_PUBLIC = derive_ed25519_public_key(RECEIVER_PRIVATE)


def _client() -> TestClient:
    from okg.substrate.exchange.authority import build_authority_document

    authority = build_authority_document(
        authority_id="okg:example",
        base_url="https://okg.example.org",
        signing_keys=[
            KeyDescriptor(
                kid="example-sign-1",
                use="sign",
                alg="Ed25519",
                public_key=RECEIVER_PUBLIC,
            )
        ],
        latest_public_generation={"generation_id": 42},
    )
    inbox = PorosInbox(
        receiver_authority_id="okg:example",
        trust_registry=TrustPinRegistry.from_pins(
            [
                TrustPin(
                    authority_id="okg:lab-a",
                    kid="lab-a-sign-1",
                    public_key=SENDER_PUBLIC,
                )
            ]
        ),
        receiver_private_key=RECEIVER_PRIVATE,
        receiver_signing_kid="example-sign-1",
    )
    app = FastAPI()
    app.include_router(build_poros_router(authority_document=authority, inbox=inbox))
    return TestClient(app)


def _signed_envelope() -> dict:
    unsigned = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:example",
        message_id="msg-http",
        created_at=datetime.now(UTC),
        ttl_seconds=900,
        body={"graph_id": "poros:graph:http", "classification": "public"},
    )
    return sign_envelope(unsigned, SENDER_PRIVATE, kid="lab-a-sign-1")


def _graph_effect_counts(dsn: str) -> tuple[int, int, int, int, int]:
    # Chronos cutover: okg.nodes_live/edges_live were dropped. The live
    # graph is the Chronos default-branch checkout over
    # okg.graph_nodes/graph_edges; count those instead.
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              (SELECT count(*) FROM okg.node_facts),
              (SELECT count(*) FROM okg.edge_facts),
              (SELECT count(*) FROM okg.admission_envelopes)
            """
        )
        node_facts, edge_facts, envelopes = (
            int(value or 0) for value in cur.fetchone()
        )
    with psycopg.connect(dsn, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout("default")
        nodes_live = int(
            graph.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"] or 0
        )
        edges_live = int(
            graph.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"] or 0
        )
    return (node_facts, edge_facts, nodes_live, edges_live, envelopes)


def _live_node_count(dsn: str, node_id: str) -> int:
    """Count a node in the deployment's latest PUBLISHED generation.

    Chronos cutover replacement for the legacy
    ``SELECT count(*) FROM okg.nodes_live WHERE node_id = %s``. Legacy
    nodes_live was the PUBLISHED view -- a node became live only when
    published at the latest generation -- so the chronos equivalent is the
    node's presence in the checkout of the latest published checkpoint, NOT
    the default-branch head (which already holds admitted-but-unpublished
    facts). These tests publish to deployment "default".
    """
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        cp = store.latest_generation(
            deployment_name="default", status="published",
        )
        if cp is None:
            return 0
        rows = store.checkout_generation(cp.checkpoint_id).query(
            "SELECT count(*) AS n FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": node_id},
        )
    return int(rows[0]["n"] or 0)


def _node_visible_at_generation(
    dsn: str, *, generation_id: int, node_id: str
) -> bool:
    """Whether a node is visible in the Chronos checkout pinned to a
    published generation.

    Chronos cutover replacement for the legacy assertion that
    ``nodes_live.valid_from_gen`` equals the publish generation: the node
    becoming live at generation G is now expressed as the node being
    present in ``checkout_generation(G)``'s okg.graph_nodes.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout_generation(generation_id)
        rows = graph.query(
            "SELECT count(*) AS n FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": node_id},
        )
    return int(rows[0]["n"] or 0) > 0


def _seed_chronos_node(
    dsn: str,
    node_id: str,
    subtype: str,
    attrs: dict[str, object],
) -> None:
    # Post-chronos-cutover a published generation is a Chronos checkpoint of
    # the default-branch graph head (okg.graph_nodes), and the dropped
    # projector no longer mirrors okg.node_facts into that graph. Seed the
    # node directly into the Chronos default-branch graph so the checkpoint
    # taken by `Publisher.cycle()` contains it and
    # `export_published_subgraph_to_outbox` finds its root nodes live.
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    with psycopg.connect(dsn) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(node_id, subtype, dict(attrs), {"id": node_id}, {"v": 1})],
            source="default",
            strict_endpoints=False,
        )


def _auxiliary_runtime_dsn(base_dsn: str, suffix: str) -> str:
    info = psycopg.conninfo.conninfo_to_dict(base_dsn)
    base_name = info.get("dbname") or "okg_test"
    max_base_len = max(1, 63 - len(suffix) - 1)
    info["dbname"] = f"{base_name[:max_base_len]}_{suffix}"
    return psycopg.conninfo.make_conninfo(**info)


def _prepare_auxiliary_clean_runtime(base_dsn: str, suffix: str) -> str:
    dsn = _auxiliary_runtime_dsn(base_dsn, suffix)
    try:
        _ensure_test_db_exists(dsn)
    except RuntimeError as exc:
        pytest.skip(f"Auxiliary Poros test DB unavailable: {exc}")

    from okg.substrate.db import migrate as _migrate_mod

    original = dict(_migrate_mod.REQUIRED_EXTENSIONS)
    _migrate_mod.REQUIRED_EXTENSIONS.pop("pg_cron", None)
    try:
        _migrate_mod.migrate(dsn)
    finally:
        _migrate_mod.REQUIRED_EXTENSIONS.clear()
        _migrate_mod.REQUIRED_EXTENSIONS.update(original)

    _clean_runtime_subset(
        dsn,
        _RUNTIME_TABLES,
        operation="clean auxiliary Poros receiver runtime",
        reset_generations=True,
        clean_derived_schema=True,
    )
    _clean_runtime_subset(
        dsn,
        _DERIVED_RUNTIME_TABLES,
        operation="clean auxiliary Poros receiver derived runtime",
    )
    reset_chronos_graph(dsn)
    return dsn


def _poros_row_count(dsn: str, table: str, message_id: str) -> int:
    assert table in {
        "poros_outbox_messages",
        "poros_delivery_attempts",
        "poros_inbox_messages",
    }
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM okg.{table} WHERE message_id = %s",
            (message_id,),
        )
        return int(cur.fetchone()[0] or 0)


def _write_poros_review_policy_bundle(tmp_path: Path) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    policy = demo_policy_graph()
    policy["rules"] = [
        *policy["rules"],
        {
            "rule_id": "review_internal_poros_context",
            "effect": "review",
            "priority": 95,
            "subjects": ["okg:lab-a"],
            "actions": ["poros.graph.submit"],
            "match": {
                "classification": "internal",
                "root_pattern": "context",
                "graph_profile": "context",
                "node_subtypes": ["context_packet"],
                "root_subtypes": ["context_packet"],
                "requested_incorporation_mode": "fast_track",
            },
            "reason_code": "nomos.demo.internal_poros_context_requires_review",
        },
        {
            "rule_id": "allow_receiver_poros_promotion",
            "effect": "allow",
            "priority": 110,
            "subjects": ["okg:mit-reviewer"],
            "actions": ["poros.graph.promote"],
            "match": {"original_effect": ["review", "quarantine"]},
            "reason_code": "nomos.demo.receiver_poros_promotion_allowed",
        },
    ]
    bundle = compile_policy_graph(policy).to_dict()
    bundle_path = tmp_path / "compiled-policy.json"
    bundle_path.write_text(
        json.dumps(bundle, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return bundle_path


def test_well_known_authority_document_endpoint() -> None:
    response = _client().get("/.well-known/okg.json")

    assert response.status_code == 200
    doc = response.json()
    assert doc["authority_id"] == "okg:example"
    assert doc["endpoints"]["inbox"] == "https://okg.example.org/poros/v1/inbox"
    assert doc["latest_public_generation"] == {"generation_id": 42}
    assert "private_key" not in response.text


def test_http_inbox_accepts_valid_public_envelope() -> None:
    response = _client().post(
        "/poros/v1/inbox",
        content=canonical_json_bytes(_signed_envelope()),
    )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "accepted_for_admission"
    assert body["receipt"]["status"] == "accepted_for_admission"


def test_http_inbox_rejects_invalid_signature_with_receipt() -> None:
    envelope = _signed_envelope()
    envelope["body"]["graph_id"] = "poros:graph:tampered"

    response = _client().post(
        "/poros/v1/inbox",
        content=canonical_json_bytes(envelope),
    )

    assert response.status_code == 400
    body = response.json()
    assert body["status"] == "rejected_preflight"
    assert body["reason_code"] == "invalid_signature"
    assert body["receipt"]["status"] == "rejected_preflight"


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("raw", "reason_code"),
    [
        (b"{", "malformed_json"),
        (
            b'{"poros":"0","sender":"okg:lab-a","sender":"okg:lab-b"}',
            "duplicate_key",
        ),
    ],
)
def test_http_receiver_service_rejects_unparseable_json_without_graph_effects(
    clean_runtime: str,
    raw: bytes,
    reason_code: str,
) -> None:
    receiver = generate_ed25519_keypair("mit-demo-sign")
    authority = build_authority_document(
        authority_id="okg:mit-demo",
        base_url="http://receiver.local",
        signing_keys=[
            KeyDescriptor(
                kid=receiver.kid,
                use="sign",
                alg="Ed25519",
                public_key=receiver.public_key,
            )
        ],
    )
    app = FastAPI()
    app.include_router(
        build_poros_router(
            authority_document=authority,
            trust_pins={"pins": []},
            receiver_keys=receiver.__dict__,
            receiver="okg:mit-demo",
            dsn=clean_runtime,
            deployment="default",
            persist_preflight=True,
            apply=True,
            publish=True,
        )
    )
    before = _graph_effect_counts(clean_runtime)

    response = TestClient(app).post("/poros/v1/inbox", content=raw)

    assert response.status_code == 400
    payload = response.json()
    assert payload["status"] == "error"
    assert payload["reason_code"] == reason_code
    assert _graph_effect_counts(clean_runtime) == before
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.poros_inbox_messages")
        assert int(cur.fetchone()[0] or 0) == 0
        cur.execute("SELECT count(*) FROM okg.poros_incorporation_events")
        assert int(cur.fetchone()[0] or 0) == 0


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("case", "reason_code"),
    [
        ("untrusted", "missing_trust_pin"),
        ("wrong_recipient", "recipient_mismatch"),
        ("expired", "expired_message"),
        ("invalid_signature", "invalid_signature"),
        ("body_hash_mismatch", "body_hash_mismatch"),
    ],
)
def test_http_receiver_service_persists_preflight_failures_without_graph_effects(
    clean_runtime: str,
    case: str,
    reason_code: str,
) -> None:
    from okg.substrate.exchange.refinement import build_refinement_state_from_trace
    from scripts.check_poros_refinement_from_db import build_trace_from_databases

    sender = generate_ed25519_keypair("lab-a-sign-demo")
    receiver = generate_ed25519_keypair("mit-demo-sign")
    message_id = f"http-failure-{case}"
    node_id = f"poros:http:failure:{case}"
    created_at = datetime.now(UTC).replace(microsecond=0)
    ttl_seconds = 900
    recipient = "okg:mit-demo"
    trust_pins = {
        "pins": [
            {
                "authority_id": "okg:lab-a",
                "kid": sender.kid,
                "use": "sign",
                "alg": "Ed25519",
                "public_key": sender.public_key,
            }
        ]
    }
    if case == "untrusted":
        trust_pins = {"pins": []}
    if case == "wrong_recipient":
        recipient = "okg:other"
    if case == "expired":
        created_at = datetime.now(UTC).replace(microsecond=0) - timedelta(hours=2)
        ttl_seconds = 1

    unsigned = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient=recipient,
        message_id=message_id,
        created_at=created_at,
        ttl_seconds=ttl_seconds,
        body={
            "graph_id": f"poros:graph:http-failure:{case}",
            "origin_authority_id": "okg:lab-a",
            "origin_generation_id": 7,
            "classification": "public",
            "graph_profile": "context",
            "root_node_ids": [node_id],
            "nodes": [
                {
                    "node_id": node_id,
                    "subtype": "context_packet",
                    "attrs": {"text": f"failure fixture {case}"},
                }
            ],
            "edges": [],
        },
    )
    if case == "body_hash_mismatch":
        unsigned["body_sha256"] = "0" * 43
        envelope = sign_document(unsigned, sender.private_key, kid=sender.kid)
    else:
        envelope = sign_envelope(unsigned, sender.private_key, kid=sender.kid)
    if case == "invalid_signature":
        envelope["body"]["nodes"][0]["attrs"]["text"] = "tampered after signing"

    authority = build_authority_document(
        authority_id="okg:mit-demo",
        base_url="http://receiver.local",
        signing_keys=[
            KeyDescriptor(
                kid=receiver.kid,
                use="sign",
                alg="Ed25519",
                public_key=receiver.public_key,
            )
        ],
    )
    app = FastAPI()
    app.include_router(
        build_poros_router(
            authority_document=authority,
            trust_pins=trust_pins,
            receiver_keys=receiver.__dict__,
            receiver="okg:mit-demo",
            dsn=clean_runtime,
            deployment="default",
            persist_preflight=True,
            apply=True,
            publish=True,
        )
    )
    before = _graph_effect_counts(clean_runtime)

    response = TestClient(app).post(
        "/poros/v1/inbox",
        content=canonical_json_bytes(envelope),
    )

    assert response.status_code == 400
    payload = response.json()
    assert payload["status"] == "rejected_preflight"
    assert payload["reason_code"] == reason_code
    assert payload["preflight"]["status"] == "rejected_preflight"
    assert payload["preflight"]["reason_code"] == reason_code
    assert payload["receipt"]["status"] == "rejected_preflight"
    assert payload["nomos_decision"] is None
    assert payload["admission"] is None
    assert payload["publish"] is None
    assert _graph_effect_counts(clean_runtime) == before
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, reason_code
              FROM okg.poros_inbox_messages
             WHERE message_id = %s
            """,
            (message_id,),
        )
        assert cur.fetchone() == ("rejected_preflight", reason_code)
        cur.execute(
            """
            SELECT event_type, status, reason_code
              FROM okg.poros_incorporation_events
             WHERE message_id = %s
            """,
            (message_id,),
        )
        assert cur.fetchall() == [
            ("preflight_rejected", "rejected_preflight", reason_code)
        ]
    trace = build_trace_from_databases(
        sender_dsn=clean_runtime,
        receiver_dsn=clean_runtime,
        message_id=message_id,
    )
    state = build_refinement_state_from_trace(trace)
    assert state["outbox"] == []
    assert state["blockedState"] == [message_id]
    assert state["admissions"] == []
    assert state["liveRows"] == []


@pytest.mark.postgres
@pytest.mark.usefixtures("_explicit_factory_publish_authority")
def test_http_receiver_service_persists_admits_and_publishes(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.nomos import run_demo

    sender = generate_ed25519_keypair("lab-a-sign-demo")
    receiver = generate_ed25519_keypair("mit-demo-sign")
    node_id = "poros:http:context"
    unsigned = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit-demo",
        message_id="http-service-message",
        created_at=datetime.now(UTC).replace(microsecond=0),
        ttl_seconds=900,
        body={
            "graph_id": "poros:graph:http-service",
            "origin_authority_id": "okg:lab-a",
            "origin_generation_id": 7,
            "classification": "public",
            "graph_profile": "context",
            "root_node_ids": [node_id],
            "nodes": [
                {
                    "node_id": node_id,
                    "subtype": "context_packet",
                    "attrs": {"text": "HTTP service context"},
                }
            ],
            "edges": [],
        },
    )
    envelope = sign_envelope(unsigned, sender.private_key, kid=sender.kid)
    nomos_dir = tmp_path / "nomos"
    run_demo(nomos_dir)

    authority = build_authority_document(
        authority_id="okg:mit-demo",
        base_url="http://receiver.local",
        signing_keys=[
            KeyDescriptor(
                kid=receiver.kid,
                use="sign",
                alg="Ed25519",
                public_key=receiver.public_key,
            )
        ],
    )
    app = FastAPI()
    app.include_router(
        build_poros_router(
            authority_document=authority,
            trust_pins={
                "pins": [
                    {
                        "authority_id": "okg:lab-a",
                        "kid": sender.kid,
                        "use": "sign",
                        "alg": "Ed25519",
                        "public_key": sender.public_key,
                    }
                ]
            },
            receiver_keys=receiver.__dict__,
            receiver="okg:mit-demo",
            dsn=clean_runtime,
            deployment="default",
            persist_preflight=True,
            apply=True,
            publish=True,
            nomos_bundle=nomos_dir / "compiled-policy.json",
        )
    )

    client = TestClient(app)
    response = client.post(
        "/poros/v1/inbox",
        content=canonical_json_bytes(envelope),
    )

    assert response.status_code == 202
    payload = response.json()
    assert payload["preflight"]["status"] == "accepted_for_admission"
    assert payload["nomos_decision"]["effect"] == "allow"
    assert payload["admission"]["status"] == "accepted"
    assert payload["publish"]["status"] == "published"
    assert payload["live_counts"] == {"nodes_live": 1, "edges_live": 0}
    generation = describe_generation(clean_runtime, payload["publish"]["generation_id"])
    assert generation["status"] == "published"

    duplicate = client.post(
        "/poros/v1/inbox",
        content=canonical_json_bytes(envelope),
    )
    assert duplicate.status_code == 202
    duplicate_payload = duplicate.json()
    assert duplicate_payload["preflight"]["status"] == "duplicate"
    assert duplicate_payload["nomos_decision"] is None
    assert duplicate_payload["admission"] is None
    assert duplicate_payload["publish"] is None

    assert _live_node_count(clean_runtime, node_id) == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, reason_code
              FROM okg.poros_inbox_messages
             WHERE message_id = 'http-service-message'
            """
        )
        assert cur.fetchone() == ("accepted_for_admission", None)
        cur.execute(
            """
            SELECT event_type, count(*)
              FROM okg.poros_incorporation_events
             WHERE message_id = 'http-service-message'
             GROUP BY event_type
            """
        )
        event_counts = {event_type: int(count) for event_type, count in cur.fetchall()}
        assert event_counts == {
            "admitted": 1,
            "duplicate": 1,
            "nomos_decided": 1,
            "preflight_accepted": 1,
            "published": 1,
        }


@pytest.mark.postgres
@pytest.mark.usefixtures("_explicit_factory_publish_authority")
def test_http_receiver_service_exported_outbox_message_refines_published_flow(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.exchange.refinement import build_refinement_state_from_trace
    from okg.substrate.nomos import run_demo
    from scripts.check_poros_refinement_from_db import build_trace_from_databases

    sender = generate_ed25519_keypair("lab-a-sign-outbox-refinement")
    receiver = generate_ed25519_keypair("mit-demo-sign-outbox-refinement")
    receiver_runtime = _prepare_auxiliary_clean_runtime(
        clean_runtime,
        "poros_receiver",
    )
    # The receiver publishes into its own database, so its catalog needs the
    # message subtype too (the fixture seeded only the sender's).
    _seed_poros_test_subtypes(receiver_runtime)
    assert psycopg.conninfo.conninfo_to_dict(clean_runtime)[
        "dbname"
    ] != psycopg.conninfo.conninfo_to_dict(receiver_runtime)["dbname"]
    message_id = "http-outbox-refinement-message"
    graph_id = "poros:graph:http-outbox-refinement"
    node_id = "poros:http:outbox-refinement:root"
    _seed_chronos_node(
        clean_runtime,
        node_id,
        "context_packet",
        {"text": "Sender outbox refinement context"},
    )
    sender_publish = Publisher(
        clean_runtime, "poros-sender",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "poros-sender"),
    ).cycle()
    assert sender_publish["status"] == "published"

    export = export_published_subgraph_to_outbox(
        dsn=clean_runtime,
        sender_authority_id="okg:lab-a",
        recipient_authority_id="okg:mit-demo",
        sender_generation_id=sender_publish["generation_id"],
        graph_id=graph_id,
        root_node_ids=[node_id],
        signing_private_key=sender.private_key,
        signing_key_id=sender.kid,
        graph_profile="context",
        message_id=message_id,
    )

    nomos_dir = tmp_path / "nomos-outbox-refinement"
    run_demo(nomos_dir)
    authority = build_authority_document(
        authority_id="okg:mit-demo",
        base_url="http://receiver.local",
        signing_keys=[
            KeyDescriptor(
                kid=receiver.kid,
                use="sign",
                alg="Ed25519",
                public_key=receiver.public_key,
            )
        ],
    )
    app = FastAPI()
    app.include_router(
        build_poros_router(
            authority_document=authority,
            trust_pins={
                "pins": [
                    {
                        "authority_id": "okg:lab-a",
                        "kid": sender.kid,
                        "use": "sign",
                        "alg": "Ed25519",
                        "public_key": sender.public_key,
                    }
                ]
            },
            receiver_keys=receiver.__dict__,
            receiver="okg:mit-demo",
            dsn=receiver_runtime,
            sender_dsn=clean_runtime,
            deployment="default",
            persist_preflight=True,
            apply=True,
            publish=True,
            nomos_bundle=nomos_dir / "compiled-policy.json",
        )
    )

    response = TestClient(app).post(
        "/poros/v1/inbox",
        content=canonical_json_bytes(export.envelope),
    )

    assert response.status_code == 202
    payload = response.json()
    assert payload["preflight"]["status"] == "accepted_for_admission"
    assert payload["delivery"]["status"] == "accepted_for_admission"
    assert payload["delivery"]["message_id"] == message_id
    assert payload["nomos_decision"]["effect"] == "allow"
    assert payload["admission"]["status"] == "accepted"
    assert payload["publish"]["status"] == "published"
    assert payload["live_counts"] == {"nodes_live": 1, "edges_live": 0}
    assert _poros_row_count(clean_runtime, "poros_outbox_messages", message_id) == 1
    assert _poros_row_count(clean_runtime, "poros_delivery_attempts", message_id) == 1
    assert _poros_row_count(receiver_runtime, "poros_outbox_messages", message_id) == 0
    assert (
        _poros_row_count(receiver_runtime, "poros_delivery_attempts", message_id)
        == 0
    )
    assert _poros_row_count(receiver_runtime, "poros_inbox_messages", message_id) == 1

    trace = build_trace_from_databases(
        sender_dsn=clean_runtime,
        receiver_dsn=receiver_runtime,
        message_id=message_id,
    )
    state = build_refinement_state_from_trace(trace)
    row_id = f"node:{node_id}"
    assert state["outbox"] == [message_id]
    assert state["inbox"] == [message_id]
    assert state["verified"] == [message_id]
    assert state["admissions"] == [message_id]
    assert state["publishedMessages"] == [message_id]
    assert state["liveRows"] == [row_id]
    assert state["outboxEvidence"][message_id]["sender_generation_id"] == str(
        sender_publish["generation_id"]
    )
    assert state["outboxEvidence"][message_id]["body_sha256"] == export.body_sha256
    assert state["inboxEvidence"][message_id]["body_sha256"] == export.body_sha256
    assert state["rowSenderGeneration"][row_id] == str(sender_publish["generation_id"])
    assert state["rowReceiverGeneration"][row_id] == str(
        payload["publish"]["generation_id"]
    )
    assert state["rowBodyHash"][row_id] == export.body_sha256
    assert (
        state["deliveryAttempts"][message_id][0]["body_sha256"]
        == export.body_sha256
    )
    assert (
        state["deliveryAttempts"][message_id][0]["envelope_sha256"]
        == export.envelope_sha256
    )
    assert state["deliveryAttempts"][message_id][0]["status"] == (
        "accepted_for_admission"
    )
    assert state["deliveryAttempts"][message_id][0]["transport"] == (
        "receiver_service_callback"
    )


@pytest.mark.postgres
@pytest.mark.usefixtures("_explicit_factory_publish_authority")
def test_http_receiver_service_fetches_valid_detached_object_and_publishes(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.nomos import run_demo

    sender = generate_ed25519_keypair("lab-a-sign-detached-valid")
    receiver = generate_ed25519_keypair("mit-demo-sign-detached-valid")
    object_store = LocalPorosObjectStore(tmp_path / "objects")
    detached_bytes = (
        b'{"kind":"poros-detached-evidence","text":"valid detached object"}'
    )
    stored = object_store.put_bytes(
        object_id="obj:http-detached-valid",
        data=detached_bytes,
    )
    manifest = build_detached_object_manifest(
        object_id="obj:http-detached-valid",
        media_type="application/vnd.okg.graph+json",
        data=detached_bytes,
        chunk_size_bytes=17,
        locations=[{"uri": stored["uri"]}],
    )
    node_id = "poros:http:detached:valid"
    unsigned = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit-demo",
        message_id="http-detached-valid-message",
        created_at=datetime.now(UTC).replace(microsecond=0),
        ttl_seconds=900,
        payload_mode="detached",
        body={
            "payload_mode": "detached",
            "graph_id": "poros:graph:http-detached-valid",
            "origin_authority_id": "okg:lab-a",
            "origin_generation_id": 7,
            "classification": "public",
            "graph_profile": "context",
            "root_node_ids": [node_id],
            "nodes": [
                {
                    "node_id": node_id,
                    "subtype": "context_packet",
                    "attrs": {"text": "valid detached HTTP object"},
                }
            ],
            "edges": [],
            "objects": [manifest],
        },
    )
    envelope = sign_envelope(unsigned, sender.private_key, kid=sender.kid)
    nomos_dir = tmp_path / "nomos-detached-valid"
    run_demo(nomos_dir)
    authority = build_authority_document(
        authority_id="okg:mit-demo",
        base_url="http://receiver.local",
        signing_keys=[
            KeyDescriptor(
                kid=receiver.kid,
                use="sign",
                alg="Ed25519",
                public_key=receiver.public_key,
            )
        ],
    )
    app = FastAPI()
    app.include_router(
        build_poros_router(
            authority_document=authority,
            trust_pins={
                "pins": [
                    {
                        "authority_id": "okg:lab-a",
                        "kid": sender.kid,
                        "use": "sign",
                        "alg": "Ed25519",
                        "public_key": sender.public_key,
                    }
                ]
            },
            receiver_keys=receiver.__dict__,
            receiver="okg:mit-demo",
            dsn=clean_runtime,
            deployment="default",
            persist_preflight=True,
            apply=True,
            publish=True,
            nomos_bundle=nomos_dir / "compiled-policy.json",
            object_store_root=object_store.root,
        )
    )
    before = _graph_effect_counts(clean_runtime)

    response = TestClient(app).post(
        "/poros/v1/inbox",
        content=canonical_json_bytes(envelope),
    )

    assert response.status_code == 202
    payload = response.json()
    assert payload["preflight"]["status"] == "accepted_for_admission"
    assert payload["nomos_decision"]["effect"] == "allow"
    assert payload["admission"]["status"] == "accepted"
    assert payload["publish"]["status"] == "published"
    assert payload["live_counts"] == {"nodes_live": 1, "edges_live": 0}
    assert _graph_effect_counts(clean_runtime) == (
        before[0] + 1,
        before[1],
        before[2] + 1,
        before[3],
        before[4] + 1,
    )
    generation = describe_generation(clean_runtime, payload["publish"]["generation_id"])
    assert generation["status"] == "published"
    assert _live_node_count(clean_runtime, node_id) == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT side, authority_id, object_sha256, size_bytes,
                   chunk_count, classification
              FROM okg.poros_detached_object_manifests
             WHERE message_id = %s
            """,
            ("http-detached-valid-message",),
        )
        assert cur.fetchone() == (
            "receiver",
            "okg:mit-demo",
            manifest["cleartext_sha256"],
            len(detached_bytes),
            len(manifest["chunks"]),
            "public",
        )


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("case", "reason_code"),
    [
        ("object_hash", "object_hash_mismatch"),
        ("chunk_hash", "chunk_hash_mismatch"),
    ],
)
def test_http_receiver_service_rejects_tampered_detached_object_without_graph_effects(
    clean_runtime: str,
    tmp_path: Path,
    case: str,
    reason_code: str,
) -> None:
    from okg.substrate.exchange.refinement import build_refinement_state_from_trace
    from scripts.check_poros_refinement_from_db import build_trace_from_databases

    sender = generate_ed25519_keypair(f"lab-a-sign-detached-{case}")
    receiver = generate_ed25519_keypair(f"mit-demo-sign-detached-{case}")
    object_store = LocalPorosObjectStore(tmp_path / f"objects-{case}")
    detached_bytes = (
        b'{"kind":"poros-detached-evidence","text":"tamper fixture"}'
    )
    stored = object_store.put_bytes(
        object_id=f"obj:http-detached-{case}",
        data=detached_bytes,
    )
    manifest = build_detached_object_manifest(
        object_id=f"obj:http-detached-{case}",
        media_type="application/vnd.okg.graph+json",
        data=detached_bytes,
        chunk_size_bytes=17,
        locations=[{"uri": stored["uri"]}],
    )
    if case == "object_hash":
        wrong = build_detached_object_manifest(
            object_id=f"obj:http-detached-{case}:wrong",
            media_type="application/vnd.okg.graph+json",
            data=b"wrong-object",
            chunk_size_bytes=17,
        )
        manifest["cleartext_sha256"] = wrong["cleartext_sha256"]
    else:
        wrong = build_detached_object_manifest(
            object_id=f"obj:http-detached-{case}:wrong",
            media_type="application/vnd.okg.graph+json",
            data=b"wrong-chunk",
            chunk_size_bytes=17,
        )
        manifest["chunks"][0]["cleartext_sha256"] = wrong["chunks"][0][
            "cleartext_sha256"
        ]
    node_id = f"poros:http:detached:{case}"
    unsigned = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit-demo",
        message_id=f"http-detached-{case}-message",
        created_at=datetime.now(UTC).replace(microsecond=0),
        ttl_seconds=900,
        payload_mode="detached",
        body={
            "payload_mode": "detached",
            "graph_id": f"poros:graph:http-detached-{case}",
            "origin_authority_id": "okg:lab-a",
            "origin_generation_id": 7,
            "classification": "public",
            "graph_profile": "context",
            "root_node_ids": [node_id],
            "nodes": [
                {
                    "node_id": node_id,
                    "subtype": "context_packet",
                    "attrs": {"text": f"tampered detached object {case}"},
                }
            ],
            "edges": [],
            "objects": [manifest],
        },
    )
    envelope = sign_envelope(unsigned, sender.private_key, kid=sender.kid)
    authority = build_authority_document(
        authority_id="okg:mit-demo",
        base_url="http://receiver.local",
        signing_keys=[
            KeyDescriptor(
                kid=receiver.kid,
                use="sign",
                alg="Ed25519",
                public_key=receiver.public_key,
            )
        ],
    )
    app = FastAPI()
    app.include_router(
        build_poros_router(
            authority_document=authority,
            trust_pins={
                "pins": [
                    {
                        "authority_id": "okg:lab-a",
                        "kid": sender.kid,
                        "use": "sign",
                        "alg": "Ed25519",
                        "public_key": sender.public_key,
                    }
                ]
            },
            receiver_keys=receiver.__dict__,
            receiver="okg:mit-demo",
            dsn=clean_runtime,
            deployment="default",
            persist_preflight=True,
            apply=True,
            publish=True,
            object_store_root=object_store.root,
        )
    )
    before = _graph_effect_counts(clean_runtime)

    response = TestClient(app).post(
        "/poros/v1/inbox",
        content=canonical_json_bytes(envelope),
    )

    assert response.status_code == 400
    payload = response.json()
    assert payload["status"] == "rejected_preflight"
    assert payload["reason_code"] == reason_code
    assert payload["preflight"]["status"] == "rejected_preflight"
    assert payload["preflight"]["reason_code"] == reason_code
    assert payload["nomos_decision"] is None
    assert payload["admission"] is None
    assert payload["publish"] is None
    assert _graph_effect_counts(clean_runtime) == before
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, reason_code
              FROM okg.poros_inbox_messages
             WHERE message_id = %s
            """,
            (f"http-detached-{case}-message",),
        )
        assert cur.fetchone() == ("rejected_preflight", reason_code)
        cur.execute(
            """
            SELECT event_type, status, reason_code
              FROM okg.poros_incorporation_events
             WHERE message_id = %s
            """,
            (f"http-detached-{case}-message",),
        )
        assert cur.fetchall() == [
            ("preflight_rejected", "rejected_preflight", reason_code)
        ]
        cur.execute(
            """
            SELECT object_id, object_sha256, chunk_count
              FROM okg.poros_detached_object_manifests
             WHERE message_id = %s
            """,
            (f"http-detached-{case}-message",),
        )
        assert cur.fetchone() == (
            f"obj:http-detached-{case}",
            manifest["cleartext_sha256"],
            len(manifest["chunks"]),
        )
    trace = build_trace_from_databases(
        sender_dsn=clean_runtime,
        receiver_dsn=clean_runtime,
        message_id=f"http-detached-{case}-message",
    )
    state = build_refinement_state_from_trace(trace)
    assert state["blockedState"] == [f"http-detached-{case}-message"]
    assert state["admissions"] == []
    assert state["liveRows"] == []


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("mode", "apply", "publish", "proof_state", "live_rows"),
    [
        ("preflight", False, False, "duplicate", 0),
        ("admitted", True, False, "admitted-unpublished", 0),
        ("published", True, True, "published", 1),
    ],
)
@pytest.mark.usefixtures("_explicit_factory_publish_authority")
def test_http_receiver_service_replay_is_idempotent_at_each_lifecycle_state(
    clean_runtime: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    mode: str,
    apply: bool,
    publish: bool,
    proof_state: str,
    live_rows: int,
) -> None:
    from okg.substrate.exchange.refinement import build_refinement_state_from_trace
    from okg.substrate.cli import main as cli
    from okg.substrate.nomos import run_demo
    from scripts.check_poros_refinement_from_db import build_trace_from_databases

    sender = generate_ed25519_keypair(f"lab-a-sign-{mode}")
    receiver = generate_ed25519_keypair(f"mit-demo-sign-{mode}")
    message_id = f"http-replay-{mode}-message"
    node_id = f"poros:http:replay:{mode}"
    unsigned = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit-demo",
        message_id=message_id,
        created_at=datetime.now(UTC).replace(microsecond=0),
        ttl_seconds=900,
        body={
            "graph_id": f"poros:graph:http-replay:{mode}",
            "origin_authority_id": "okg:lab-a",
            "origin_generation_id": 7,
            "classification": "public",
            "graph_profile": "context",
            "root_node_ids": [node_id],
            "nodes": [
                {
                    "node_id": node_id,
                    "subtype": "context_packet",
                    "attrs": {"text": f"replay fixture {mode}"},
                }
            ],
            "edges": [],
        },
    )
    envelope = sign_envelope(unsigned, sender.private_key, kid=sender.kid)
    nomos_dir = tmp_path / f"nomos-{mode}"
    run_demo(nomos_dir)
    capsys.readouterr()

    authority = build_authority_document(
        authority_id="okg:mit-demo",
        base_url="http://receiver.local",
        signing_keys=[
            KeyDescriptor(
                kid=receiver.kid,
                use="sign",
                alg="Ed25519",
                public_key=receiver.public_key,
            )
        ],
    )
    app = FastAPI()
    app.include_router(
        build_poros_router(
            authority_document=authority,
            trust_pins={
                "pins": [
                    {
                        "authority_id": "okg:lab-a",
                        "kid": sender.kid,
                        "use": "sign",
                        "alg": "Ed25519",
                        "public_key": sender.public_key,
                    }
                ]
            },
            receiver_keys=receiver.__dict__,
            receiver="okg:mit-demo",
            dsn=clean_runtime,
            deployment="default",
            persist_preflight=True,
            apply=apply,
            publish=publish,
            nomos_bundle=nomos_dir / "compiled-policy.json",
        )
    )
    client = TestClient(app)
    first = client.post("/poros/v1/inbox", content=canonical_json_bytes(envelope))
    assert first.status_code == 202
    first_payload = first.json()
    assert first_payload["preflight"]["status"] == "accepted_for_admission"
    if apply:
        assert first_payload["admission"]["status"] == "accepted"
    else:
        assert first_payload["admission"] is None
    if publish:
        assert first_payload["publish"]["status"] == "published"
    else:
        assert first_payload["publish"] is None
    after_first = _graph_effect_counts(clean_runtime)

    duplicate = client.post("/poros/v1/inbox", content=canonical_json_bytes(envelope))

    assert duplicate.status_code == 202
    duplicate_payload = duplicate.json()
    assert duplicate_payload["status"] == "duplicate"
    assert duplicate_payload["preflight"]["status"] == "duplicate"
    assert duplicate_payload["nomos_decision"] is None
    assert duplicate_payload["admission"] is None
    assert duplicate_payload["publish"] is None
    assert _graph_effect_counts(clean_runtime) == after_first

    assert cli.main(
        [
            "poros",
            "proof",
            "--dsn",
            clean_runtime,
            "--message-id",
            message_id,
            "--json",
        ]
    ) == 0
    proof = json.loads(capsys.readouterr().out)
    assert proof["state"] == proof_state
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_type, count(*)
              FROM okg.poros_incorporation_events
             WHERE message_id = %s
             GROUP BY event_type
            """,
            (message_id,),
        )
        event_counts = {event_type: int(count) for event_type, count in cur.fetchall()}
        expected = {
            "duplicate": 1,
            "nomos_decided": 1,
            "preflight_accepted": 1,
        }
        if apply:
            expected["admitted"] = 1
        if publish:
            expected["published"] = 1
        assert event_counts == expected
    assert _live_node_count(clean_runtime, node_id) == live_rows
    if mode == "preflight":
        trace = build_trace_from_databases(
            sender_dsn=clean_runtime,
            receiver_dsn=clean_runtime,
            message_id=message_id,
        )
        state = build_refinement_state_from_trace(trace)
        assert state["verified"] == [message_id]
        assert state["admissions"] == []
        assert state["publishedMessages"] == []
        assert state["liveRows"] == []


@pytest.mark.postgres
def test_http_receiver_service_denied_graph_shape_is_non_live_and_refines(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.exchange.refinement import build_refinement_state_from_trace
    from okg.substrate.nomos import run_demo
    from scripts.check_poros_refinement_from_db import build_trace_from_databases

    sender = generate_ed25519_keypair("lab-a-sign-deny")
    receiver = generate_ed25519_keypair("mit-demo-sign-deny")
    node_id = "poros:http:deny:root"
    run_id = str(uuid.uuid4())
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts (
                node_id, subtype, attrs, source, source_record_id,
                source_revision, observed_at, dedupe_key, run_id, op
            )
            VALUES (
                %s, 'concept', %s, 'poros_denied_shape_test', %s, %s,
                now(), %s, %s::uuid, 'I'
            )
            """,
            (
                node_id,
                Jsonb({"text": "Denied Poros shape root"}),
                Jsonb({"id": node_id}),
                Jsonb({"v": 1}),
                f"poros-deny:{run_id}:{node_id}",
                run_id,
            ),
        )
    _seed_chronos_node(
        clean_runtime,
        node_id,
        "concept",
        {"text": "Denied Poros shape root"},
    )
    publish = Publisher(clean_runtime, "poros-sender", catalog_identity=bind_test_catalog_writer(clean_runtime, "poros-sender")).cycle()
    assert publish["status"] == "published"
    export = export_published_subgraph_to_outbox(
        dsn=clean_runtime,
        sender_authority_id="okg:lab-a",
        recipient_authority_id="okg:mit-demo",
        sender_generation_id=publish["generation_id"],
        graph_id="poros:graph:denied-shape",
        root_node_ids=[node_id],
        signing_private_key=sender.private_key,
        signing_key_id=sender.kid,
        graph_profile="context",
        message_id="http-denied-shape-message",
    )
    nomos_dir = tmp_path / "nomos-denied"
    run_demo(nomos_dir)
    authority = build_authority_document(
        authority_id="okg:mit-demo",
        base_url="http://receiver.local",
        signing_keys=[
            KeyDescriptor(
                kid=receiver.kid,
                use="sign",
                alg="Ed25519",
                public_key=receiver.public_key,
            )
        ],
    )
    app = FastAPI()
    app.include_router(
        build_poros_router(
            authority_document=authority,
            trust_pins={
                "pins": [
                    {
                        "authority_id": "okg:lab-a",
                        "kid": sender.kid,
                        "use": "sign",
                        "alg": "Ed25519",
                        "public_key": sender.public_key,
                    }
                ]
            },
            receiver_keys=receiver.__dict__,
            receiver="okg:mit-demo",
            dsn=clean_runtime,
            deployment="default",
            persist_preflight=True,
            apply=True,
            publish=True,
            nomos_bundle=nomos_dir / "compiled-policy.json",
        )
    )
    before = _graph_effect_counts(clean_runtime)

    response = TestClient(app).post(
        "/poros/v1/inbox",
        content=canonical_json_bytes(export.envelope),
    )

    assert response.status_code == 202
    payload = response.json()
    assert payload["preflight"]["status"] == "accepted_for_admission"
    assert payload["nomos_decision"]["effect"] == "deny"
    assert payload["admission"] is None
    assert payload["publish"] is None
    assert _graph_effect_counts(clean_runtime) == before
    trace = build_trace_from_databases(
        sender_dsn=clean_runtime,
        receiver_dsn=clean_runtime,
        message_id=export.message_id,
    )
    state = build_refinement_state_from_trace(trace)
    assert state["blockedState"] == [export.message_id]
    assert state["admissions"] == []
    assert state["liveRows"] == []
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_type, status, reason_code
              FROM okg.poros_incorporation_events
             WHERE message_id = %s
             ORDER BY event_id
            """,
            (export.message_id,),
        )
        assert cur.fetchall() == [
            ("preflight_accepted", "accepted_for_admission", None),
            ("nomos_decided", "deny", "nomos.default_deny"),
        ]


@pytest.mark.postgres
def test_http_sender_hinted_denial_refines_receiver_unauthorized(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """A sender-hinted message the receiver DENIES refines to receiver_authorized=False.

    The sender exports a claim-shaped graph (graph_profile != "context"), so the
    outbox trace carries sender_hint=True -- the sender is asserting content whose
    incorporation only the receiver's policy may grant. The receiver denies it.
    Before wave 7 the refinement builder hardcoded receiver_authorized=True, so the
    sender-authority guard's `not receiver_authorized` conjunct (the model's
    NoSenderPolicyAuthority) was dead against every real trace and exercised only by
    a fixture that hand-set the boolean. This test drives the real
    export -> inbox -> Nomos-deny -> build_trace_from_databases path and asserts the
    built trace derives sender_hint=True AND receiver_authorized=False from the
    recorded Nomos decision, and that the message lands in blockedState, never
    admitted. It turns red if receiver_authorized reverts to the hardcoded True.
    The guard's raise-on-violation is covered by the fixture refinement test, which
    this derivation now makes reachable from real state.
    """
    from okg.substrate.exchange.refinement import build_refinement_state_from_trace
    from okg.substrate.exchange.refinement_db import build_trace_from_databases
    from okg.substrate.nomos import run_demo

    sender = generate_ed25519_keypair("lab-a-sign-hint-deny")
    receiver = generate_ed25519_keypair("mit-demo-sign-hint-deny")
    node_id = "poros:http:hint-deny:root"
    run_id = str(uuid.uuid4())
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts (
                node_id, subtype, attrs, source, source_record_id,
                source_revision, observed_at, dedupe_key, run_id, op
            )
            VALUES (
                %s, 'concept', %s, 'poros_hint_deny_test', %s, %s,
                now(), %s, %s::uuid, 'I'
            )
            """,
            (
                node_id,
                Jsonb({"text": "Sender-hinted denied root"}),
                Jsonb({"id": node_id}),
                Jsonb({"v": 1}),
                f"poros-hint-deny:{run_id}:{node_id}",
                run_id,
            ),
        )
    _seed_chronos_node(
        clean_runtime,
        node_id,
        "concept",
        {"text": "Sender-hinted denied root"},
    )
    publish = Publisher(clean_runtime, "poros-sender", catalog_identity=bind_test_catalog_writer(clean_runtime, "poros-sender")).cycle()
    assert publish["status"] == "published"
    export = export_published_subgraph_to_outbox(
        dsn=clean_runtime,
        sender_authority_id="okg:lab-a",
        recipient_authority_id="okg:mit-demo",
        sender_generation_id=publish["generation_id"],
        graph_id="poros:graph:hint-denied",
        root_node_ids=[node_id],
        signing_private_key=sender.private_key,
        signing_key_id=sender.kid,
        graph_profile="claim",
        message_id="http-sender-hint-denied-message",
    )
    nomos_dir = tmp_path / "nomos-hint-denied"
    run_demo(nomos_dir)
    authority = build_authority_document(
        authority_id="okg:mit-demo",
        base_url="http://receiver.local",
        signing_keys=[
            KeyDescriptor(
                kid=receiver.kid,
                use="sign",
                alg="Ed25519",
                public_key=receiver.public_key,
            )
        ],
    )
    app = FastAPI()
    app.include_router(
        build_poros_router(
            authority_document=authority,
            trust_pins={
                "pins": [
                    {
                        "authority_id": "okg:lab-a",
                        "kid": sender.kid,
                        "use": "sign",
                        "alg": "Ed25519",
                        "public_key": sender.public_key,
                    }
                ]
            },
            receiver_keys=receiver.__dict__,
            receiver="okg:mit-demo",
            dsn=clean_runtime,
            deployment="default",
            persist_preflight=True,
            apply=True,
            publish=True,
            nomos_bundle=nomos_dir / "compiled-policy.json",
        )
    )
    before = _graph_effect_counts(clean_runtime)

    response = TestClient(app).post(
        "/poros/v1/inbox",
        content=canonical_json_bytes(export.envelope),
    )

    assert response.status_code == 202
    payload = response.json()
    assert payload["preflight"]["status"] == "accepted_for_admission"
    assert payload["nomos_decision"]["effect"] == "deny"
    assert payload["admission"] is None
    assert payload["publish"] is None
    assert _graph_effect_counts(clean_runtime) == before

    trace = build_trace_from_databases(
        sender_dsn=clean_runtime,
        receiver_dsn=clean_runtime,
        message_id=export.message_id,
    )
    outbox_row = trace["outbox"][0]
    # The mapping-fidelity assertions: both are derived from real exchange state.
    # sender_hint from the sender's declared claim profile, receiver_authorized
    # from the receiver's recorded Nomos deny -- NOT the former hardcoded True.
    assert outbox_row["sender_hint"] is True
    assert outbox_row["receiver_authorized"] is False

    state = build_refinement_state_from_trace(trace)
    assert state["blockedState"] == [export.message_id]
    assert state["admissions"] == []
    assert state["liveRows"] == []


@pytest.mark.postgres
@pytest.mark.parametrize(
    (
        "effect",
        "root_subtype",
        "classification",
        "expected_state",
        "expected_event",
        "expected_reason",
    ),
    [
        (
            "review",
            "context_packet",
            "internal",
            "review",
            "review",
            "nomos.demo.internal_poros_context_requires_review",
        ),
        (
            "quarantine",
            "proposal",
            "public",
            "quarantined",
            "quarantined",
            "nomos.demo.profile_hint_cannot_broaden",
        ),
    ],
)
def test_http_receiver_service_review_and_quarantine_are_non_live_visible(
    clean_runtime: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    effect: str,
    root_subtype: str,
    classification: str,
    expected_state: str,
    expected_event: str,
    expected_reason: str,
) -> None:
    from okg.substrate.cli import main as cli
    from okg.substrate.exchange.refinement import build_refinement_state_from_trace
    from scripts.check_poros_refinement_from_db import build_trace_from_databases

    sender = generate_ed25519_keypair(f"lab-a-sign-{effect}")
    receiver = generate_ed25519_keypair(f"mit-demo-sign-{effect}")
    message_id = f"http-{effect}-message"
    graph_id = f"poros:graph:http-{effect}"
    node_id = f"poros:http:{effect}:root"
    unsigned = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit-demo",
        message_id=message_id,
        created_at=datetime.now(UTC).replace(microsecond=0),
        ttl_seconds=900,
        body={
            "graph_id": graph_id,
            "origin_authority_id": "okg:lab-a",
            "origin_generation_id": 7,
            "classification": classification,
            "graph_profile": "context",
            "root_node_ids": [node_id],
            "nodes": [
                {
                    "node_id": node_id,
                    "subtype": root_subtype,
                    "attrs": {
                        "text": f"{effect} payload text must not appear in summary"
                    },
                }
            ],
            "edges": [],
        },
    )
    envelope = sign_envelope(unsigned, sender.private_key, kid=sender.kid)
    bundle_path = _write_poros_review_policy_bundle(tmp_path / "nomos")
    authority = build_authority_document(
        authority_id="okg:mit-demo",
        base_url="http://receiver.local",
        signing_keys=[
            KeyDescriptor(
                kid=receiver.kid,
                use="sign",
                alg="Ed25519",
                public_key=receiver.public_key,
            )
        ],
    )
    app = FastAPI()
    app.include_router(
        build_poros_router(
            authority_document=authority,
            trust_pins={
                "pins": [
                    {
                        "authority_id": "okg:lab-a",
                        "kid": sender.kid,
                        "use": "sign",
                        "alg": "Ed25519",
                        "public_key": sender.public_key,
                    }
                ]
            },
            receiver_keys=receiver.__dict__,
            receiver="okg:mit-demo",
            dsn=clean_runtime,
            deployment="default",
            persist_preflight=True,
            apply=True,
            publish=True,
            nomos_bundle=bundle_path,
        )
    )
    before = _graph_effect_counts(clean_runtime)

    response = TestClient(app).post(
        "/poros/v1/inbox",
        content=canonical_json_bytes(envelope),
    )

    assert response.status_code == 202
    payload = response.json()
    assert payload["preflight"]["status"] == "accepted_for_admission"
    assert payload["nomos_decision"]["effect"] == effect
    assert payload["nomos_decision"]["policy_fingerprint"].startswith("sha256:")
    assert payload["admission"] is None
    assert payload["publish"] is None
    assert _graph_effect_counts(clean_runtime) == before

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_type, status, reason_code, payload
              FROM okg.poros_incorporation_events
             WHERE message_id = %s
             ORDER BY event_id
            """,
            (message_id,),
        )
        events = cur.fetchall()
        cur.execute("SELECT count(*) FROM okg.node_facts WHERE node_id = %s", (node_id,))
        node_facts = int(cur.fetchone()[0] or 0)
    nodes_live = _live_node_count(clean_runtime, node_id)

    assert [(row[0], row[1], row[2]) for row in events] == [
        ("preflight_accepted", "accepted_for_admission", None),
        ("nomos_decided", effect, expected_reason),
        (expected_event, effect, expected_reason),
    ]
    lifecycle_payload = events[-1][3]
    graph_summary = lifecycle_payload["graph"]
    assert graph_summary["graph_id"] == graph_id
    assert graph_summary["classification"] == classification
    assert graph_summary["node_count"] == 1
    assert graph_summary["edge_count"] == 0
    assert "nodes" not in graph_summary
    assert "payload text must not appear" not in json.dumps(graph_summary)
    assert (
        lifecycle_payload["nomos_decision"]["policy_fingerprint"]
        == payload["nomos_decision"]["policy_fingerprint"]
    )
    assert node_facts == 0
    assert nodes_live == 0

    assert cli.main(
        [
            "poros",
            "status",
            "--dsn",
            clean_runtime,
            "--side",
            "receiver",
            "--message-id",
            message_id,
            "--json",
        ]
    ) == 0
    by_message = json.loads(capsys.readouterr().out)
    status_row = by_message["messages"][0]
    assert status_row["state"] == expected_state
    assert status_row["policy_fingerprint"] == payload["nomos_decision"][
        "policy_fingerprint"
    ]
    assert status_row["graph"]["graph_id"] == graph_id
    assert status_row["graph"]["node_count"] == 1

    assert cli.main(
        [
            "poros",
            "status",
            "--dsn",
            clean_runtime,
            "--side",
            "receiver",
            "--graph-id",
            graph_id,
            "--json",
        ]
    ) == 0
    by_graph = json.loads(capsys.readouterr().out)
    assert by_graph["messages"][0]["message_id"] == message_id
    trace = build_trace_from_databases(
        sender_dsn=clean_runtime,
        receiver_dsn=clean_runtime,
        message_id=message_id,
    )
    state = build_refinement_state_from_trace(trace)
    assert state["blockedState"] == [message_id]
    assert state["admissions"] == []
    assert state["liveRows"] == []


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("effect", "root_subtype", "classification", "expected_event"),
    [
        ("review", "context_packet", "internal", "review"),
        ("quarantine", "proposal", "public", "quarantined"),
    ],
)
@pytest.mark.usefixtures("_explicit_factory_publish_authority")
def test_receiver_owned_promotion_publishes_reviewed_or_quarantined_message(
    clean_runtime: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    effect: str,
    root_subtype: str,
    classification: str,
    expected_event: str,
) -> None:
    from okg.substrate.cli import main as cli
    from okg.substrate.exchange.refinement import (
        PorosRefinementError,
        build_refinement_state_from_trace,
    )
    from scripts.check_poros_refinement_from_db import build_trace_from_databases

    sender = generate_ed25519_keypair(f"lab-a-sign-promote-{effect}")
    receiver = generate_ed25519_keypair(f"mit-demo-sign-promote-{effect}")
    message_id = f"http-promote-{effect}-message"
    graph_id = f"poros:graph:http-promote-{effect}"
    node_id = f"poros:http:promote:{effect}:root"
    unsigned = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit-demo",
        message_id=message_id,
        created_at=datetime.now(UTC).replace(microsecond=0),
        ttl_seconds=900,
        body={
            "graph_id": graph_id,
            "origin_authority_id": "okg:lab-a",
            "origin_generation_id": 7,
            "classification": classification,
            "graph_profile": "context",
            "root_node_ids": [node_id],
            "nodes": [
                {
                    "node_id": node_id,
                    "subtype": root_subtype,
                    "attrs": {"text": f"promoted {effect} payload"},
                }
            ],
            "edges": [],
        },
    )
    envelope = sign_envelope(unsigned, sender.private_key, kid=sender.kid)
    bundle_path = _write_poros_review_policy_bundle(tmp_path / f"nomos-{effect}")
    authority = build_authority_document(
        authority_id="okg:mit-demo",
        base_url="http://receiver.local",
        signing_keys=[
            KeyDescriptor(
                kid=receiver.kid,
                use="sign",
                alg="Ed25519",
                public_key=receiver.public_key,
            )
        ],
    )
    app = FastAPI()
    app.include_router(
        build_poros_router(
            authority_document=authority,
            trust_pins={
                "pins": [
                    {
                        "authority_id": "okg:lab-a",
                        "kid": sender.kid,
                        "use": "sign",
                        "alg": "Ed25519",
                        "public_key": sender.public_key,
                    }
                ]
            },
            receiver_keys=receiver.__dict__,
            receiver="okg:mit-demo",
            dsn=clean_runtime,
            deployment="default",
            persist_preflight=True,
            apply=True,
            publish=True,
            nomos_bundle=bundle_path,
        )
    )
    before = _graph_effect_counts(clean_runtime)
    response = TestClient(app).post(
        "/poros/v1/inbox",
        content=canonical_json_bytes(envelope),
    )
    assert response.status_code == 202
    review_payload = response.json()
    assert review_payload["nomos_decision"]["effect"] == effect
    assert review_payload["admission"] is None
    assert review_payload["publish"] is None
    assert _graph_effect_counts(clean_runtime) == before

    assert cli.main(
        [
            "poros",
            "promote",
            "--dsn",
            clean_runtime,
            "--deployment",
            "default",
            "--actor",
            "okg:mit-reviewer",
            "--nomos-bundle",
            str(bundle_path),
            "--message-id",
            message_id,
            "--publish",
            "--json",
        ]
    ) == 0
    promoted = json.loads(capsys.readouterr().out)

    assert promoted["status"] == "promoted"
    assert promoted["promotion_decision"]["effect"] == "allow"
    assert (
        promoted["promotion_decision"]["reason_code"]
        == "nomos.demo.receiver_poros_promotion_allowed"
    )
    assert promoted["original_decision"]["effect"] == effect
    assert promoted["admission"]["status"] == "accepted"
    assert promoted["publish"]["status"] == "published"
    assert promoted["live_counts"] == {"nodes_live": 1, "edges_live": 0}
    assert _graph_effect_counts(clean_runtime) == (
        before[0] + 1,
        before[1],
        before[2] + 1,
        before[3],
        before[4] + 1,
    )
    generation = describe_generation(clean_runtime, promoted["publish"]["generation_id"])
    assert generation["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT nf.source_record_id->'poros',
                   nf.source_revision->'poros',
                   nf.source_record_id->>'admission_id'
              FROM okg.node_facts nf
             WHERE nf.node_id = %s
            """,
            (node_id,),
        )
        fact_poros, revision_poros, admission_id = cur.fetchone()
        cur.execute(
            """
            SELECT event_type, status, reason_code
              FROM okg.poros_incorporation_events
             WHERE message_id = %s
             ORDER BY event_id
            """,
            (message_id,),
        )
        events = cur.fetchall()

    assert fact_poros["message_id"] == message_id
    assert fact_poros["original_nomos_effect"] == effect
    assert fact_poros["original_nomos_decision_id"] == review_payload[
        "nomos_decision"
    ]["decision_id"]
    assert fact_poros["promotion_decision_id"] == promoted[
        "promotion_decision"
    ]["decision_id"]
    assert fact_poros["promotion_actor"] == "okg:mit-reviewer"
    assert revision_poros["promotion_decision_id"] == fact_poros[
        "promotion_decision_id"
    ]
    assert admission_id == promoted["admission"]["admission_id"]
    assert _node_visible_at_generation(
        clean_runtime,
        generation_id=promoted["publish"]["generation_id"],
        node_id=node_id,
    )
    assert [event[0] for event in events] == [
        "preflight_accepted",
        "nomos_decided",
        expected_event,
        "promotion_decided",
        "admitted",
        "published",
        "promoted",
    ]

    assert cli.main(
        [
            "poros",
            "proof",
            "--dsn",
            clean_runtime,
            "--message-id",
            message_id,
            "--json",
        ]
    ) == 0
    proof = json.loads(capsys.readouterr().out)
    assert proof["state"] == "promoted"
    assert proof["imported_nodes"][0]["node_id"] == node_id
    assert proof["lifecycle"]["promotion_decision"]["status"] == "allow"
    assert proof["lifecycle"]["promotion_result"]["event_type"] == "promoted"
    assert proof["lifecycle"]["publication"]["receiver_generation_id"] == (
        promoted["publish"]["generation_id"]
    )

    assert cli.main(
        [
            "poros",
            "status",
            "--dsn",
            clean_runtime,
            "--side",
            "receiver",
            "--graph-id",
            graph_id,
            "--json",
        ]
    ) == 0
    status = json.loads(capsys.readouterr().out)
    status_row = status["messages"][0]
    assert status_row["state"] == proof["state"]
    assert status_row["event_types"] == [
        event["event_type"] for event in proof["events"]
    ]
    assert status_row["promotion_effect"] == "allow"
    assert status_row["promotion_decision_id"] == promoted[
        "promotion_decision"
    ]["decision_id"]
    trace = build_trace_from_databases(
        sender_dsn=clean_runtime,
        receiver_dsn=clean_runtime,
        message_id=message_id,
    )
    with pytest.raises(PorosRefinementError, match="lacks sender outbox evidence"):
        build_refinement_state_from_trace(trace)


def _post_promotable_fixture(
    *,
    clean_runtime: str,
    tmp_path: Path,
    effect: str = "review",
) -> dict[str, Any]:
    sender = generate_ed25519_keypair(f"lab-a-sign-negative-{effect}")
    receiver = generate_ed25519_keypair(f"mit-demo-sign-negative-{effect}")
    message_id = f"http-negative-{effect}-message"
    graph_id = f"poros:graph:http-negative-{effect}"
    node_id = f"poros:http:negative:{effect}:root"
    root_subtype = "proposal" if effect == "quarantine" else "context_packet"
    classification = "public" if effect == "quarantine" else "internal"
    unsigned = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit-demo",
        message_id=message_id,
        created_at=datetime.now(UTC).replace(microsecond=0),
        ttl_seconds=900,
        body={
            "graph_id": graph_id,
            "origin_authority_id": "okg:lab-a",
            "origin_generation_id": 7,
            "classification": classification,
            "graph_profile": "context",
            "root_node_ids": [node_id],
            "nodes": [
                {
                    "node_id": node_id,
                    "subtype": root_subtype,
                    "attrs": {"text": f"negative {effect} payload"},
                }
            ],
            "edges": [],
        },
    )
    envelope = sign_envelope(unsigned, sender.private_key, kid=sender.kid)
    bundle_path = _write_poros_review_policy_bundle(tmp_path / f"nomos-{effect}")
    authority = build_authority_document(
        authority_id="okg:mit-demo",
        base_url="http://receiver.local",
        signing_keys=[
            KeyDescriptor(
                kid=receiver.kid,
                use="sign",
                alg="Ed25519",
                public_key=receiver.public_key,
            )
        ],
    )
    app = FastAPI()
    app.include_router(
        build_poros_router(
            authority_document=authority,
            trust_pins={
                "pins": [
                    {
                        "authority_id": "okg:lab-a",
                        "kid": sender.kid,
                        "use": "sign",
                        "alg": "Ed25519",
                        "public_key": sender.public_key,
                    }
                ]
            },
            receiver_keys=receiver.__dict__,
            receiver="okg:mit-demo",
            dsn=clean_runtime,
            deployment="default",
            persist_preflight=True,
            apply=True,
            publish=True,
            nomos_bundle=bundle_path,
        )
    )
    response = TestClient(app).post(
        "/poros/v1/inbox",
        content=canonical_json_bytes(envelope),
    )
    assert response.status_code == 202
    payload = response.json()
    assert payload["nomos_decision"]["effect"] == effect
    assert payload["admission"] is None
    return {
        "bundle_path": bundle_path,
        "envelope": envelope,
        "message_id": message_id,
        "graph_id": graph_id,
        "node_id": node_id,
        "effect": effect,
    }


@pytest.mark.postgres
@pytest.mark.parametrize("case", ["unauthorized", "mutated_payload"])
def test_promotion_negative_authorization_and_mutated_payload_are_non_live(
    clean_runtime: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    case: str,
) -> None:
    from okg.substrate.cli import main as cli

    fixture = _post_promotable_fixture(
        clean_runtime=clean_runtime,
        tmp_path=tmp_path / case,
    )
    before = _graph_effect_counts(clean_runtime)
    cmd = [
        "poros",
        "promote",
        "--dsn",
        clean_runtime,
        "--deployment",
        "default",
        "--actor",
        "okg:mit-reviewer",
        "--nomos-bundle",
        str(fixture["bundle_path"]),
        "--message-id",
        fixture["message_id"],
        "--publish",
        "--json",
    ]
    if case == "unauthorized":
        cmd[cmd.index("okg:mit-reviewer")] = "okg:unknown-operator"
        expected_denial_status = "deny"
        expected_reason = "nomos.default_deny"
    else:
        mutated = dict(fixture["envelope"])
        mutated["body"] = dict(mutated["body"])
        mutated["body"]["nodes"] = [dict(mutated["body"]["nodes"][0])]
        mutated["body"]["nodes"][0]["attrs"] = {"text": "mutated"}
        mutated_path = tmp_path / case / "mutated-envelope.json"
        mutated_path.write_text(json.dumps(mutated), encoding="utf-8")
        cmd.extend(["--envelope", str(mutated_path)])
        expected_denial_status = "payload_mismatch"
        expected_reason = "poros_promotion_payload_mismatch"

    assert cli.main(cmd) == 1
    denied = json.loads(capsys.readouterr().out)

    assert denied["status"] == "promotion_denied"
    assert denied["admission"] is None
    assert denied["publish"] is None
    assert _graph_effect_counts(clean_runtime) == before
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_type, status, reason_code
              FROM okg.poros_incorporation_events
             WHERE message_id = %s
             ORDER BY event_id
            """,
            (fixture["message_id"],),
        )
        events = cur.fetchall()
    nodes_live = _live_node_count(clean_runtime, fixture["node_id"])

    assert events[-1] == (
        "promotion_denied",
        expected_denial_status,
        expected_reason,
    )
    if case == "unauthorized":
        assert events[-2][0] == "promotion_decided"
        assert denied["promotion_decision"]["effect"] == "deny"
    else:
        assert denied["reason_code"] == expected_reason
        assert all(event[0] != "promotion_decided" for event in events)
    assert nodes_live == 0


@pytest.mark.postgres
@pytest.mark.usefixtures("_explicit_factory_publish_authority")
def test_stale_promotion_is_durable_and_creates_no_second_live_delta(
    clean_runtime: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from okg.substrate.cli import main as cli

    fixture = _post_promotable_fixture(
        clean_runtime=clean_runtime,
        tmp_path=tmp_path / "stale",
    )
    promote_cmd = [
        "poros",
        "promote",
        "--dsn",
        clean_runtime,
        "--deployment",
        "default",
        "--actor",
        "okg:mit-reviewer",
        "--nomos-bundle",
        str(fixture["bundle_path"]),
        "--message-id",
        fixture["message_id"],
        "--publish",
        "--json",
    ]
    assert cli.main(promote_cmd) == 0
    promoted = json.loads(capsys.readouterr().out)
    assert promoted["status"] == "promoted"
    after_first = _graph_effect_counts(clean_runtime)

    assert cli.main(promote_cmd) == 1
    stale = json.loads(capsys.readouterr().out)

    assert stale["status"] == "promotion_denied"
    assert stale["reason_code"] == "poros_promotion_stale"
    assert stale["admission"] is None
    assert stale["publish"] is None
    assert _graph_effect_counts(clean_runtime) == after_first
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_type, status, reason_code
              FROM okg.poros_incorporation_events
             WHERE message_id = %s
             ORDER BY event_id
            """,
            (fixture["message_id"],),
        )
        events = cur.fetchall()
    nodes_live = _live_node_count(clean_runtime, fixture["node_id"])

    assert events[-2][0] == "promoted"
    assert events[-1] == ("promotion_denied", "stale", "poros_promotion_stale")
    assert nodes_live == 1


@pytest.mark.postgres
def test_sender_delivery_worker_records_http_receipt_and_outbox_state(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from okg.substrate.cli import main as cli

    sender = generate_ed25519_keypair("lab-a-sign-1")
    receiver = generate_ed25519_keypair("mit-sign-1")
    node_id = "poros:delivery:root"
    run_id = str(uuid.uuid4())
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts (
                node_id, subtype, attrs, source, source_record_id,
                source_revision, observed_at, dedupe_key, run_id, op
            )
            VALUES (
                %s, 'concept', %s, 'poros_delivery_test', %s, %s,
                now(), %s, %s::uuid, 'I'
            )
            """,
            (
                node_id,
                Jsonb({"text": "Poros delivery root"}),
                Jsonb({"id": node_id}),
                Jsonb({"v": 1}),
                f"poros-delivery:{run_id}:{node_id}",
                run_id,
            ),
        )
    _seed_chronos_node(
        clean_runtime,
        node_id,
        "concept",
        {"text": "Poros delivery root"},
    )
    publish = Publisher(clean_runtime, "poros-sender", catalog_identity=bind_test_catalog_writer(clean_runtime, "poros-sender")).cycle()
    assert publish["status"] == "published"
    export = export_published_subgraph_to_outbox(
        dsn=clean_runtime,
        sender_authority_id="okg:lab-a",
        recipient_authority_id="okg:mit",
        sender_generation_id=publish["generation_id"],
        graph_id="poros:graph:delivery",
        root_node_ids=[node_id],
        signing_private_key=sender.private_key,
        signing_key_id=sender.kid,
        message_id="delivery-worker-message",
    )
    inbox = PorosInbox(
        receiver_authority_id="okg:mit",
        trust_registry=TrustPinRegistry.from_pins(
            [
                TrustPin(
                    authority_id="okg:lab-a",
                    kid=sender.kid,
                    public_key=sender.public_key,
                )
            ]
        ),
        receiver_private_key=receiver.private_key,
        receiver_signing_kid=receiver.kid,
    )

    class FakeResponse:
        def __init__(self, payload: dict[str, Any]) -> None:
            self._payload = payload
            # The redirect-SSRF guard reads status_code before .json().
            self.status_code = 200

        def json(self) -> dict[str, Any]:
            return self._payload

    def fake_post(*_args: Any, **kwargs: Any) -> FakeResponse:
        result = inbox.handle_bytes(kwargs["data"] if "data" in kwargs else _args[1])
        return FakeResponse(
            {
                "status": result.status,
                "reason_code": result.reason_code,
                "receipt": result.receipt,
            }
        )

    monkeypatch.setattr("requests.post", fake_post)

    results = deliver_next_outbox_messages(
        dsn=clean_runtime,
        receiver_inbox_url="http://receiver.local/poros/v1/inbox",
        receiver_public_key=receiver.public_key,
        message_id=export.message_id,
    )

    assert len(results) == 1
    assert results[0].status == "accepted_for_admission"
    assert results[0].outbox_status == "delivered"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM okg.poros_outbox_messages WHERE message_id = %s",
            (export.message_id,),
        )
        assert cur.fetchone()[0] == "delivered"
        cur.execute(
            """
            SELECT status, transport, receipt_sha256 IS NOT NULL
              FROM okg.poros_delivery_attempts
             WHERE message_id = %s
            """,
            (export.message_id,),
        )
        assert cur.fetchone() == ("accepted_for_admission", "http", True)
        cur.execute(
            """
            UPDATE okg.poros_outbox_messages
               SET status = 'failed',
                   updated_at = now() - interval '1 second'
             WHERE message_id = %s
            """,
            (export.message_id,),
        )

    duplicate_results = deliver_next_outbox_messages(
        dsn=clean_runtime,
        receiver_inbox_url="http://receiver.local/poros/v1/inbox",
        receiver_public_key=receiver.public_key,
        message_id=export.message_id,
    )

    assert len(duplicate_results) == 1
    assert duplicate_results[0].status == "duplicate"
    assert duplicate_results[0].outbox_status == "delivered"
    assert cli.main(
        [
            "poros",
            "status",
            "--dsn",
            clean_runtime,
            "--side",
            "sender",
            "--graph-id",
            "poros:graph:delivery",
            "--json",
        ]
    ) == 0
    delivered_status = json.loads(capsys.readouterr().out)
    delivered_message = delivered_status["messages"][0]
    delivered_attempts = delivered_status["attempts"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, max_delivery_attempts, next_retry_at IS NULL,
                   last_error, terminal_failure_reason
              FROM okg.poros_outbox_messages
             WHERE message_id = %s
            """,
            (export.message_id,),
        )
        outbox_row = cur.fetchone()
        cur.execute(
            """
            SELECT attempt_id, status, reason_code, receipt_sha256 IS NOT NULL,
                   transport
              FROM okg.poros_delivery_attempts
             WHERE message_id = %s
             ORDER BY attempted_at DESC, attempt_id DESC
            """,
            (export.message_id,),
        )
        attempt_rows = cur.fetchall()
    assert delivered_message["state"] == outbox_row[0]
    assert delivered_message["max_delivery_attempts"] == outbox_row[1]
    assert (delivered_message["next_retry_at"] is None) is outbox_row[2]
    assert delivered_message["last_error"] == outbox_row[3]
    assert delivered_message["terminal_failure_reason"] == outbox_row[4]
    assert delivered_message["latest_attempt"]["attempt_id"] == attempt_rows[0][0]
    assert delivered_message["latest_attempt"]["status"] == "duplicate"
    assert [attempt["status"] for attempt in delivered_attempts] == [
        row[1] for row in attempt_rows
    ]
    assert [attempt["reason_code"] for attempt in delivered_attempts] == [
        row[2] for row in attempt_rows
    ]
    assert [attempt["receipt_sha256"] is not None for attempt in delivered_attempts] == [
        row[3] for row in attempt_rows
    ]
    assert [attempt["transport"] for attempt in delivered_attempts] == [
        row[4] for row in attempt_rows
    ]

    invalid_key_export = export_published_subgraph_to_outbox(
        dsn=clean_runtime,
        sender_authority_id="okg:lab-a",
        recipient_authority_id="okg:mit",
        sender_generation_id=publish["generation_id"],
        graph_id="poros:graph:delivery-invalid-receipt",
        root_node_ids=[node_id],
        signing_private_key=sender.private_key,
        signing_key_id=sender.kid,
        message_id="delivery-worker-invalid-receipt-message",
    )
    wrong_receiver = generate_ed25519_keypair("wrong-mit-sign")
    failed_results = deliver_next_outbox_messages(
        dsn=clean_runtime,
        receiver_inbox_url="http://receiver.local/poros/v1/inbox",
        receiver_public_key=wrong_receiver.public_key,
        message_id=invalid_key_export.message_id,
        max_attempts=1,
    )

    assert len(failed_results) == 1
    assert failed_results[0].status == "failed"
    assert failed_results[0].reason_code == "delivery_failed"
    assert failed_results[0].receipt_sha256 is None
    assert failed_results[0].outbox_status == "terminal_failed"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, count(*)
              FROM okg.poros_delivery_attempts
             WHERE message_id = %s
             GROUP BY status
            """,
            (invalid_key_export.message_id,),
        )
        attempts_by_status = {
            status: int(count) for status, count in cur.fetchall()
        }
        assert attempts_by_status == {"failed": 1}
        cur.execute(
            """
            SELECT status, terminal_failure_reason
              FROM okg.poros_outbox_messages
             WHERE message_id = %s
            """,
            (invalid_key_export.message_id,),
        )
        assert cur.fetchone() == ("terminal_failed", "delivery_failed")
    assert cli.main(
        [
            "poros",
            "status",
            "--dsn",
            clean_runtime,
            "--side",
            "sender",
            "--message-id",
            invalid_key_export.message_id,
            "--json",
        ]
    ) == 0
    failed_status = json.loads(capsys.readouterr().out)
    assert failed_status["messages"][0]["state"] == "terminal_failed"
    assert failed_status["messages"][0]["latest_attempt"]["status"] == "failed"
    assert failed_status["messages"][0]["terminal_failure_reason"] == (
        "delivery_failed"
    )


@pytest.mark.postgres
def test_sender_delivery_worker_recovers_stale_claim_and_records_transport_failure(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sender = generate_ed25519_keypair("lab-a-sign-stale")
    receiver = generate_ed25519_keypair("mit-sign-stale")
    node_id = "poros:delivery:stale-root"
    run_id = str(uuid.uuid4())
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts (
                node_id, subtype, attrs, source, source_record_id,
                source_revision, observed_at, dedupe_key, run_id, op
            )
            VALUES (
                %s, 'concept', %s, 'poros_stale_delivery_test', %s, %s,
                now(), %s, %s::uuid, 'I'
            )
            """,
            (
                node_id,
                Jsonb({"text": "Poros stale delivery root"}),
                Jsonb({"id": node_id}),
                Jsonb({"v": 1}),
                f"poros-stale-delivery:{run_id}:{node_id}",
                run_id,
            ),
        )
    _seed_chronos_node(
        clean_runtime,
        node_id,
        "concept",
        {"text": "Poros stale delivery root"},
    )
    publish = Publisher(clean_runtime, "poros-sender", catalog_identity=bind_test_catalog_writer(clean_runtime, "poros-sender")).cycle()
    assert publish["status"] == "published"
    export = export_published_subgraph_to_outbox(
        dsn=clean_runtime,
        sender_authority_id="okg:lab-a",
        recipient_authority_id="okg:mit",
        sender_generation_id=publish["generation_id"],
        graph_id="poros:graph:stale-delivery",
        root_node_ids=[node_id],
        signing_private_key=sender.private_key,
        signing_key_id=sender.kid,
        message_id="stale-delivery-message",
    )
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE okg.poros_outbox_messages
               SET status = 'delivering',
                   updated_at = now() - interval '10 minutes'
             WHERE message_id = %s
            """,
            (export.message_id,),
        )
    before = _graph_effect_counts(clean_runtime)

    def fake_post(*_args: Any, **_kwargs: Any) -> None:
        raise TimeoutError("receiver unavailable")

    monkeypatch.setattr("requests.post", fake_post)

    results = deliver_next_outbox_messages(
        dsn=clean_runtime,
        receiver_inbox_url="http://receiver.local/poros/v1/inbox",
        receiver_public_key=receiver.public_key,
        message_id=export.message_id,
        stale_after_seconds=1,
        max_attempts=1,
    )

    assert len(results) == 1
    assert results[0].status == "failed"
    assert results[0].reason_code == "delivery_failed"
    assert results[0].outbox_status == "terminal_failed"
    assert _graph_effect_counts(clean_runtime) == before
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, terminal_failure_reason
              FROM okg.poros_outbox_messages
             WHERE message_id = %s
            """,
            (export.message_id,),
        )
        assert cur.fetchone() == ("terminal_failed", "delivery_failed")
        cur.execute(
            """
            SELECT status, reason_code, transport, receipt_sha256
              FROM okg.poros_delivery_attempts
             WHERE message_id = %s
            """,
            (export.message_id,),
        )
        assert cur.fetchone() == ("failed", "delivery_failed", "http", None)


@pytest.mark.postgres
def test_sender_delivery_worker_stops_at_bounded_retry_and_surfaces_status(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from okg.substrate.cli import main as cli
    from okg.substrate.cli.poros import _load_sender_delivery_proof

    sender = generate_ed25519_keypair("lab-a-sign-retry-bound")
    receiver = generate_ed25519_keypair("mit-sign-retry-bound")
    node_id = "poros:delivery:retry-bound-root"
    run_id = str(uuid.uuid4())
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts (
                node_id, subtype, attrs, source, source_record_id,
                source_revision, observed_at, dedupe_key, run_id, op
            )
            VALUES (
                %s, 'concept', %s, 'poros_retry_bound_test', %s, %s,
                now(), %s, %s::uuid, 'I'
            )
            """,
            (
                node_id,
                Jsonb({"text": "Poros bounded retry root"}),
                Jsonb({"id": node_id}),
                Jsonb({"v": 1}),
                f"poros-retry-bound:{run_id}:{node_id}",
                run_id,
            ),
        )
    _seed_chronos_node(
        clean_runtime,
        node_id,
        "concept",
        {"text": "Poros bounded retry root"},
    )
    publish = Publisher(clean_runtime, "poros-sender", catalog_identity=bind_test_catalog_writer(clean_runtime, "poros-sender")).cycle()
    assert publish["status"] == "published"
    export = export_published_subgraph_to_outbox(
        dsn=clean_runtime,
        sender_authority_id="okg:lab-a",
        recipient_authority_id="okg:mit",
        sender_generation_id=publish["generation_id"],
        graph_id="poros:graph:retry-bound",
        root_node_ids=[node_id],
        signing_private_key=sender.private_key,
        signing_key_id=sender.kid,
        message_id="retry-bound-message",
    )
    before = _graph_effect_counts(clean_runtime)

    def fake_post(*_args: Any, **_kwargs: Any) -> None:
        raise TimeoutError("receiver unavailable")

    monkeypatch.setattr("requests.post", fake_post)

    first = deliver_next_outbox_messages(
        dsn=clean_runtime,
        receiver_inbox_url="http://receiver.local/poros/v1/inbox",
        receiver_public_key=receiver.public_key,
        message_id=export.message_id,
        max_attempts=2,
        retry_delay_seconds=0,
    )
    assert len(first) == 1
    assert first[0].outbox_status == "retrying"
    assert first[0].attempt_count == 1
    assert first[0].next_retry_at is not None
    assert cli.main(
        [
            "poros",
            "status",
            "--dsn",
            clean_runtime,
            "--side",
            "sender",
            "--message-id",
            export.message_id,
            "--json",
        ]
    ) == 0
    retrying_status = json.loads(capsys.readouterr().out)
    assert retrying_status["messages"][0]["state"] == "retrying"
    assert retrying_status["messages"][0]["attempt_count"] == 1
    assert retrying_status["messages"][0]["latest_attempt"]["status"] == "failed"
    assert retrying_status["messages"][0]["next_retry_at"] is not None
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT o.status, o.next_retry_at IS NOT NULL,
                   a.status, a.reason_code
              FROM okg.poros_outbox_messages o
              JOIN okg.poros_delivery_attempts a
                ON a.message_id = o.message_id
             WHERE o.message_id = %s
             ORDER BY a.attempted_at DESC, a.attempt_id DESC
             LIMIT 1
            """,
            (export.message_id,),
        )
        assert cur.fetchone() == ("retrying", True, "failed", "delivery_failed")
    second = deliver_next_outbox_messages(
        dsn=clean_runtime,
        receiver_inbox_url="http://receiver.local/poros/v1/inbox",
        receiver_public_key=receiver.public_key,
        message_id=export.message_id,
        max_attempts=2,
        retry_delay_seconds=0,
    )
    third = deliver_next_outbox_messages(
        dsn=clean_runtime,
        receiver_inbox_url="http://receiver.local/poros/v1/inbox",
        receiver_public_key=receiver.public_key,
        message_id=export.message_id,
        max_attempts=2,
        retry_delay_seconds=0,
    )
    assert len(second) == 1
    assert second[0].outbox_status == "terminal_failed"
    assert second[0].attempt_count == 2
    assert second[0].terminal_failure_reason == "delivery_failed"
    assert third == []
    assert _graph_effect_counts(clean_runtime) == before
    assert cli.main(
        [
            "poros",
            "status",
            "--dsn",
            clean_runtime,
            "--side",
            "sender",
            "--message-id",
            export.message_id,
            "--json",
        ]
    ) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["messages"][0]["state"] == "terminal_failed"
    assert status["messages"][0]["attempt_count"] == 2
    assert status["messages"][0]["max_delivery_attempts"] == 2
    assert status["messages"][0]["latest_attempt"]["status"] == "failed"
    assert status["messages"][0]["latest_attempt"]["reason_code"] == (
        "delivery_failed"
    )
    assert status["messages"][0]["terminal_failure_reason"] == "delivery_failed"
    assert [attempt["status"] for attempt in status["attempts"]] == [
        "failed",
        "failed",
    ]
    sender_proof = _load_sender_delivery_proof(
        clean_runtime,
        message_id=export.message_id,
    )
    assert sender_proof["outbox"]["state"] == "terminal_failed"
    assert sender_proof["outbox"]["receiver_publication_claim"] == (
        "not_proven_by_sender_delivery"
    )
    assert len(sender_proof["delivery_attempts"]) == 2


@pytest.mark.postgres
def test_sender_delivery_transient_retries_but_non_retryable_terminates(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient transport error is RETRYABLE while attempts remain; a
    non-retryable error (SSRF-refused URL) is TERMINAL on the first attempt
    even when max_attempts > 1.

    This pins the classification fix in
    ``sender_delivery.deliver_outbox_item``: the generic ``except Exception``
    must not blanket-terminalize transient failures (regression for the
    bounded-retry bug), and the dedicated SSRF/nomos branches must keep
    terminalizing non-retryable failures (over-broadening guard).
    """
    sender = generate_ed25519_keypair("lab-a-sign-classify")
    receiver = generate_ed25519_keypair("mit-sign-classify")

    def _seed_outbox(message_id: str, node_id: str) -> Any:
        run_id = str(uuid.uuid4())
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.node_facts (
                    node_id, subtype, attrs, source, source_record_id,
                    source_revision, observed_at, dedupe_key, run_id, op
                )
                VALUES (
                    %s, 'concept', %s, 'poros_classify_test', %s, %s,
                    now(), %s, %s::uuid, 'I'
                )
                """,
                (
                    node_id,
                    Jsonb({"text": "Poros classify root"}),
                    Jsonb({"id": node_id}),
                    Jsonb({"v": 1}),
                    f"poros-classify:{run_id}:{node_id}",
                    run_id,
                ),
            )
        _seed_chronos_node(
            clean_runtime, node_id, "concept", {"text": "Poros classify root"}
        )
        publish = Publisher(
            clean_runtime, "poros-sender",
            catalog_identity=bind_test_catalog_writer(clean_runtime, "poros-sender"),
        ).cycle()
        assert publish["status"] == "published"
        return export_published_subgraph_to_outbox(
            dsn=clean_runtime,
            sender_authority_id="okg:lab-a",
            recipient_authority_id="okg:mit",
            sender_generation_id=publish["generation_id"],
            graph_id=f"poros:graph:classify:{message_id}",
            root_node_ids=[node_id],
            signing_private_key=sender.private_key,
            signing_key_id=sender.kid,
            message_id=message_id,
        )

    # --- Transient transport failure retries while attempts remain. ---
    transient = _seed_outbox(
        "classify-transient-message", "poros:delivery:classify-transient"
    )

    def fake_post(*_args: Any, **_kwargs: Any) -> None:
        raise TimeoutError("receiver momentarily unavailable")

    monkeypatch.setattr("requests.post", fake_post)
    transient_result = deliver_next_outbox_messages(
        dsn=clean_runtime,
        receiver_inbox_url="http://receiver.local/poros/v1/inbox",
        receiver_public_key=receiver.public_key,
        message_id=transient.message_id,
        max_attempts=2,
        retry_delay_seconds=0,
    )
    assert len(transient_result) == 1
    assert transient_result[0].status == "failed"
    assert transient_result[0].reason_code == "delivery_failed"
    # The retry path fires: status is 'retrying', not 'terminal_failed'.
    assert transient_result[0].outbox_status == "retrying"
    assert transient_result[0].attempt_count == 1
    assert transient_result[0].next_retry_at is not None
    assert transient_result[0].terminal_failure_reason is None

    # --- Non-retryable failure (SSRF-refused URL) terminates immediately. ---
    # The autouse allowlist only permits 'receiver.local'; a loopback target
    # is refused by the egress guard as UnsafeFetchURL (a non-retryable
    # destination verdict), so it must NOT become 'retrying' even though
    # max_attempts=2 leaves an attempt budget.
    non_retryable = _seed_outbox(
        "classify-ssrf-message", "poros:delivery:classify-ssrf"
    )
    ssrf_result = deliver_next_outbox_messages(
        dsn=clean_runtime,
        receiver_inbox_url="http://127.0.0.1:9/poros/v1/inbox",
        receiver_public_key=receiver.public_key,
        message_id=non_retryable.message_id,
        max_attempts=2,
        retry_delay_seconds=0,
    )
    assert len(ssrf_result) == 1
    assert ssrf_result[0].status == "failed"
    assert ssrf_result[0].reason_code == "unsafe_delivery_url"
    assert ssrf_result[0].outbox_status == "terminal_failed"
    assert ssrf_result[0].next_retry_at is None
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, next_retry_at IS NOT NULL
              FROM okg.poros_outbox_messages
             WHERE message_id = %s
            """,
            (non_retryable.message_id,),
        )
        # Stays terminal on the first attempt; no retry scheduled.
        assert cur.fetchone() == ("terminal_failed", False)
