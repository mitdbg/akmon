from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.substrate.cli.federation import (
    cmd_connect,
    cmd_inbox,
    cmd_proof,
    cmd_pull,
    cmd_send,
)
from okg.substrate.cli.main import _build_parser
from okg.substrate.exchange import generate_ed25519_keypair
from okg.substrate.exchange.readiness import public_key_fingerprint


pytestmark = pytest.mark.postgres


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

    The connection demo's receiver builds its publisher through ``build_deployment_publisher``, which
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


def test_connection_top_level_argparse_routes() -> None:
    denied = _build_parser().parse_args(
        [
            "connect",
            "deny",
            "federation:connection:one",
            "--dsn",
            "postgres://x",
            "--actor",
            "okg:operator",
        ]
    )
    assert denied.command == "connect"
    assert denied.connect_subcmd == "deny"

    promoted = _build_parser().parse_args(
        [
            "inbox",
            "promote",
            "message-one",
            "--dsn",
            "postgres://x",
            "--actor",
            "okg:operator",
            "--nomos-bundle",
            "policy.json",
        ]
    )
    assert promoted.command == "inbox"
    assert promoted.inbox_subcmd == "promote"

    demo = _build_parser().parse_args(
        [
            "connect",
            "demo",
            "--dsn",
            "postgres://x",
            "--mailbox-url",
            "http://mailbox.example",
        ]
    )
    assert demo.command == "connect"
    assert demo.connect_subcmd == "demo"
    assert demo.mailbox_url == "http://mailbox.example"

    serve = _build_parser().parse_args(
        [
            "federation",
            "mailbox",
            "serve",
            "--dsn",
            "postgres://x",
            "--object-store-root",
            "/tmp/okg-mailbox-objects",
        ]
    )
    assert serve.command == "federation"
    assert serve.federation_subcmd == "mailbox"
    assert serve.mailbox_subcmd == "serve"
    assert serve.object_store_root == "/tmp/okg-mailbox-objects"


def test_connection_lifecycle_discovers_approves_sends_and_denies_wrong_scope(
    clean_runtime: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    receiver_keys = generate_ed25519_keypair("mit-shared-sign")
    sender_keys = generate_ed25519_keypair("personal-sign")
    discovery_path = tmp_path / "mit-discovery.json"
    discovery_path.write_text(
        json.dumps(_discovery(receiver_keys)),
        encoding="utf-8",
    )

    discover_args = _build_parser().parse_args(
        [
            "connect",
            "--json",
            "discover",
            str(discovery_path),
            "--dsn",
            clean_runtime,
            "--local-authority",
            "okg:personal:riley",
        ]
    )
    assert cmd_connect(discover_args, as_json=True) == 0
    discovered = json.loads(capsys.readouterr().out)
    assert discovered["status"] == "candidate_pending"
    assert discovered["trust_granted"] is False

    request_args = _build_parser().parse_args(
        [
            "connect",
            "--json",
            "request",
            "--dsn",
            clean_runtime,
            "--local-authority",
            "okg:personal:riley",
            "--to",
            "okg://mit-shared-okg-fixture",
            "--board",
            "proposals",
        ]
    )
    assert cmd_connect(request_args, as_json=True) == 0
    requested = json.loads(capsys.readouterr().out)
    request_id = requested["request"]["request_id"]
    assert requested["request"]["status"] == "pending"
    assert requested["request"]["requested_action"] == "federation.message.send"

    approve_args = _build_parser().parse_args(
        [
            "connect",
            "--json",
            "approve",
            request_id,
            "--dsn",
            clean_runtime,
            "--actor",
            "okg:personal:riley",
            "--nomos-decision-id",
            "nomos:decision:allow-send",
        ]
    )
    assert cmd_connect(approve_args, as_json=True) == 0
    approved = json.loads(capsys.readouterr().out)
    assert approved["status"] == "approved"
    assert approved["trust_pin"]["remote_authority_id"] == "okg:mit-shared-okg-fixture"
    assert approved["trust_pin"]["allowed_boards"] == [
        "okg://mit-shared-okg-fixture/boards/proposals"
    ]

    graph_path = tmp_path / "proposal-graph.json"
    graph_path.write_text(
        json.dumps(
            {
                "graph_id": "poros:graph:proposal:one",
                "nodes": [
                    {
                        "node_id": "proposal:one",
                        "subtype": "context_packet",
                        "attrs": {"text": "Please add this public DSG fact."},
                    }
                ],
                "edges": [],
            }
        ),
        encoding="utf-8",
    )
    sender_key_path = tmp_path / "sender.keys.json"
    sender_key_path.write_text(json.dumps(sender_keys.__dict__), encoding="utf-8")
    envelope_path = tmp_path / "proposal-envelope.json"
    send_args = _build_parser().parse_args(
        [
            "send",
            "--json",
            "--dsn",
            clean_runtime,
            "--local-authority",
            "okg:personal:riley",
            "--to",
            "okg://mit-shared-okg-fixture",
            "--board",
            "proposals",
            "--graph",
            str(graph_path),
            "--signing-keys",
            str(sender_key_path),
            "--envelope-out",
            str(envelope_path),
        ]
    )
    assert cmd_send(send_args, as_json=True) == 0
    sent = json.loads(capsys.readouterr().out)
    assert sent["status"] == "route_allowed"
    assert sent["route"]["decision"] == "allow"
    assert sent["live_row_delta"] == {"edges_live": 0, "nodes_live": 0}
    assert envelope_path.exists()

    denied_args = _build_parser().parse_args(
        [
            "send",
            "--json",
            "--dsn",
            clean_runtime,
            "--local-authority",
            "okg:personal:riley",
            "--to",
            "okg://mit-shared-okg-fixture",
            "--board",
            "private",
            "--graph",
            str(graph_path),
        ]
    )
    assert cmd_send(denied_args, as_json=True) == 1
    denied = json.loads(capsys.readouterr().out)
    assert denied["status"] == "route_denied"
    assert denied["route"]["reason_code"] == "federation.target_not_scoped"
    assert denied["live_row_delta"] == {"edges_live": 0, "nodes_live": 0}


def test_connection_pull_route_and_exposure_readiness(
    clean_runtime: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    receiver_keys = generate_ed25519_keypair("mit-shared-pull")
    discovery_path = tmp_path / "mit-discovery.json"
    discovery_path.write_text(json.dumps(_discovery(receiver_keys)), encoding="utf-8")

    args = _build_parser().parse_args(
        [
            "connect",
            "--json",
            "discover",
            str(discovery_path),
            "--dsn",
            clean_runtime,
            "--local-authority",
            "okg:personal:riley",
        ]
    )
    assert cmd_connect(args, as_json=True) == 0
    capsys.readouterr()

    request_args = _build_parser().parse_args(
        [
            "connect",
            "--json",
            "request",
            "--dsn",
            clean_runtime,
            "--local-authority",
            "okg:personal:riley",
            "--to",
            "okg://mit-shared-okg-fixture",
            "--surface",
            "public-context",
        ]
    )
    assert cmd_connect(request_args, as_json=True) == 0
    request_id = json.loads(capsys.readouterr().out)["request"]["request_id"]

    approve_args = _build_parser().parse_args(
        [
            "connect",
            "--json",
            "approve",
            request_id,
            "--dsn",
            clean_runtime,
            "--actor",
            "okg:personal:riley",
            "--nomos-decision-id",
            "nomos:decision:allow-pull",
        ]
    )
    assert cmd_connect(approve_args, as_json=True) == 0
    capsys.readouterr()

    pull_args = _build_parser().parse_args(
        [
            "pull",
            "--json",
            "--dsn",
            clean_runtime,
            "--local-authority",
            "okg:personal:riley",
            "--from",
            "okg://mit-shared-okg-fixture",
            "--surface",
            "public-context",
        ]
    )
    assert cmd_pull(pull_args, as_json=True) == 0
    pulled = json.loads(capsys.readouterr().out)
    assert pulled["status"] == "route_allowed"
    assert pulled["route"]["target_address"] == (
        "okg://mit-shared-okg-fixture/pull/public-context"
    )

    unsafe_args = _build_parser().parse_args(
        ["connect", "--json", "readiness", "--exposure", "world-open"]
    )
    assert cmd_connect(unsafe_args, as_json=True) == 1
    unsafe = json.loads(capsys.readouterr().out)
    assert "dns_tls" in unsafe["missing"]
    assert "nomos_receive_policy" in unsafe["missing"]

    safe_args = _build_parser().parse_args(
        [
            "connect",
            "--json",
            "readiness",
            "--exposure",
            "world-open",
            "--dns-tls",
            "--auth-or-rate-limit",
            "--quotas",
            "--retention",
            "--object-store-encryption",
            "--key-rotation",
            "--audit-logs",
            "--backups",
            "--monitoring",
            "--nomos-receive-policy",
        ]
    )
    assert cmd_connect(safe_args, as_json=True) == 0
    assert json.loads(capsys.readouterr().out)["ready"] is True


@pytest.mark.usefixtures("_explicit_factory_publish_authority")
def test_high_level_inbox_and_proof_wrap_poros_evidence(
    clean_runtime: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from okg.substrate.federation.mailbox.demo import run_mailbox_incorporation_demo

    evidence = run_mailbox_incorporation_demo(
        clean_runtime,
        policy_dir=tmp_path / "policy",
    )
    message_id = evidence["accepted"]["message_id"]

    list_args = _build_parser().parse_args(
        [
            "inbox",
            "--json",
            "list",
            "--dsn",
            clean_runtime,
            "--receiver",
            "okg:mit",
        ]
    )
    assert cmd_inbox(list_args, as_json=True) == 0
    inbox = json.loads(capsys.readouterr().out)
    assert message_id in {message["message_id"] for message in inbox["messages"]}

    review_args = _build_parser().parse_args(
        ["inbox", "--json", "review", message_id, "--dsn", clean_runtime]
    )
    assert cmd_inbox(review_args, as_json=True) == 0
    review = json.loads(capsys.readouterr().out)
    assert review["status"] == "review"
    assert review["message"]["message_id"] == message_id

    proof_args = _build_parser().parse_args(
        ["proof", "--json", message_id, "--dsn", clean_runtime]
    )
    assert cmd_proof(proof_args, as_json=True) == 0
    proof = json.loads(capsys.readouterr().out)
    assert proof["message_id"] == message_id
    assert proof["state"] == "published"
    assert proof["graph"]["node_count"] == 1
    assert proof["graph"]["edge_count"] == 0


@pytest.mark.usefixtures("_explicit_factory_publish_authority")
def test_connect_demo_runs_complete_connection_lifecycle(
    clean_runtime: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    args = _build_parser().parse_args(
        [
            "connect",
            "--json",
            "demo",
            "--dsn",
            clean_runtime,
            "--policy-dir",
            str(tmp_path / "policy"),
            "--output",
            str(tmp_path / "evidence.json"),
        ]
    )
    assert cmd_connect(args, as_json=True) == 0
    payload = json.loads(capsys.readouterr().out)
    evidence = json.loads(Path(payload["evidence_path"]).read_text(encoding="utf-8"))

    assert evidence["status"] == "ok"
    assert evidence["mailbox_transport"]["mode"] == "local_postgres"
    assert evidence["discovery"]["trust_granted"] is False
    assert evidence["connection"]["unapproved_route"]["decision"] == "deny"
    assert evidence["connection"]["request"]["status"] == "pending"
    assert evidence["connection"]["approved_request"]["status"] == "approved"
    assert evidence["connection"]["allowed_route"]["decision"] == "allow"
    assert evidence["transfer"]["accepted"]["mailbox_action"] == "acknowledged"
    assert evidence["transfer"]["accepted"]["publish"]["status"] == "published"
    assert evidence["transfer"]["proof"]["state"] == "published"
    assert evidence["transfer"]["after"]["accepted_live_count"] == 1

    assert evidence["denials"]["out_of_scope"]["route"]["decision"] == "deny"
    assert evidence["denials"]["disabled"]["route"]["reason_code"] == (
        "federation.peer_not_active"
    )
    assert evidence["denials"]["tampered"]["mailbox_action"] == "denied"
    assert evidence["denials"]["expired"]["mailbox_action"] == "not_delivered_expired"
    assert evidence["denials"]["oversized"]["mailbox_action"] == "denied_before_store"
    for denial in evidence["denials"].values():
        assert denial["live_row_delta"] == {"edges_live": 0, "nodes_live": 0}


@pytest.mark.usefixtures("_explicit_factory_publish_authority")
def test_connect_demo_can_use_http_mailbox_transport(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    from okg.substrate.federation.connection_demo import (
        SHARED_AUTHORITY,
        run_connection_lifecycle_demo,
    )
    from okg.substrate.federation.mailbox import (
        InMemoryMailboxStore,
        MailboxPollingClient,
        MailboxService,
        build_mailbox_app,
    )
    from okg.substrate.federation.mailbox.auth import static_token_authenticator

    service = MailboxService(store=InMemoryMailboxStore())
    app = build_mailbox_app(
        service,
        authenticator=static_token_authenticator({SHARED_AUTHORITY: "tok"}),
    )
    client = MailboxPollingClient(
        "http://testserver",
        client=TestClient(app),
        principal=SHARED_AUTHORITY,
        token="tok",
    )

    evidence = run_connection_lifecycle_demo(
        clean_runtime,
        policy_dir=tmp_path / "policy",
        mailbox_client=client,
    )

    assert evidence["status"] == "ok"
    assert evidence["mailbox_transport"]["mode"] == "http"
    assert evidence["transfer"]["accepted"]["mailbox_action"] == "acknowledged"
    assert evidence["transfer"]["accepted"]["publish"]["status"] == "published"
    assert evidence["transfer"]["proof"]["state"] == "published"
    assert evidence["denials"]["tampered"]["mailbox_action"] == "denied"
    assert evidence["denials"]["expired"]["mailbox_action"] == "not_delivered_expired"
    assert evidence["denials"]["oversized"]["mailbox_action"] == "denied_before_store"
    for denial in evidence["denials"].values():
        assert denial["live_row_delta"] == {"edges_live": 0, "nodes_live": 0}


def _discovery(keypair: object) -> dict:
    public_key = str(getattr(keypair, "public_key"))
    return {
        "authority_id": "okg:mit-shared-okg-fixture",
        "display_label": "MIT Shared OKG Fixture",
        "keys": [
            {
                "kid": str(getattr(keypair, "kid")),
                "public_key": public_key,
                "fingerprint": public_key_fingerprint(public_key),
            }
        ],
        "capabilities": [
            "federation.discovery",
            "federation.message.send",
            "federation.graph.pull",
        ],
        "boards": [
            {
                "address": "okg://mit-shared-okg-fixture/boards/proposals",
                "visibility": "public",
                "message_profiles": ["proposal"],
            }
        ],
        "pull_surfaces": [
            {
                "address": "okg://mit-shared-okg-fixture/pull/public-context",
                "visibility": "public",
                "graph_profile": "context",
                "catalog_reference": "catalog:mit:v1",
                "latest_generation_id": 42,
                "max_packet_bytes": 100_000,
            }
        ],
        "mailboxes": [
            {
                "address": "okg://mit-shared-okg-fixture/mailboxes/inbox",
                "queue_ref": "https://mailbox.example/mit-shared",
                "max_inline_bytes": 100_000,
                "max_object_bytes": 1_000_000_000,
            }
        ],
        "endpoints": {"mailbox": "https://mailbox.example/mit-shared"},
        "visibility_tiers": ["public"],
        "latest_public_generation_id": 42,
        "catalog_reference": "catalog:mit:v1",
    }
