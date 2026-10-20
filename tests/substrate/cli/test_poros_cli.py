from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest
from psycopg.types.json import Jsonb

from okg.substrate.cli import main as cli
from okg.substrate.cli.main import _build_parser
from okg.substrate.exchange import (
    build_unsigned_envelope,
    export_published_subgraph_to_outbox,
    generate_ed25519_keypair,
    sign_envelope,
    verify_envelope,
)
from okg.substrate.exchange.base64url import b64url_encode
from okg.substrate.exchange.canonical_json import canonical_json_bytes
from okg.substrate.exchange.hashing import sha256_b64url
from okg.substrate.exchange.object_store import LocalPorosObjectStore
from okg.substrate.exchange.payloads import (
    build_detached_object_manifest,
    validate_detached_object_bytes,
)
from okg.substrate.exchange.signing import verify_receipt
from okg.substrate.exchange.signing import sign_receipt
from okg.substrate.nomos import (
    PermissionGrant,
    SubjectBinding,
    activate_policy_bundle,
    compile_policy_graph,
    institutional_provider_fixture,
    upsert_identity_provider,
    upsert_permission_grant,
    upsert_subject_binding,
)
from okg.substrate.nomos.demo import demo_policy_graph
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


# ---------------------------------------------------------------------------
# Chronos-cutover live-graph read helpers.
#
# okg.nodes_live/edges_live were dropped; the live graph is the Chronos
# default-branch checkout over okg.graph_nodes/graph_edges. These helpers
# replace the legacy ``SELECT ... FROM okg.nodes_live`` reads. The chronos
# checkout exposes no ``valid_from_gen``/``valid_to_gen`` columns: a node
# being live "at" a generation G is presence in checkout_generation(G), and
# "currently live" (legacy ``valid_to_gen IS NULL``) is presence in the
# default-branch checkout.
# ---------------------------------------------------------------------------


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

    ``okg poros submit`` builds its publisher through ``build_deployment_publisher``, which
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


def _live_graph_counts(dsn: str) -> tuple[int, int]:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout("default")
        nodes = int(
            graph.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"] or 0
        )
        edges = int(
            graph.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"] or 0
        )
    return nodes, edges


def _live_node_count(dsn: str, node_id: str) -> int:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout("default")
        rows = graph.query(
            "SELECT count(*) AS n FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": node_id},
        )
    return int(rows[0]["n"] or 0)


def _live_node_count_like(dsn: str, pattern: str) -> int:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout("default")
        rows = graph.query(
            "SELECT count(*) AS n FROM okg.graph_nodes WHERE node_id LIKE :pattern",
            {"pattern": pattern},
        )
    return int(rows[0]["n"] or 0)


def _node_visible_at_generation(
    dsn: str, *, generation_id: int, node_id: str
) -> bool:
    """Whether a node is present in the Chronos checkout pinned to a
    published generation — the cutover equivalent of the legacy
    ``nodes_live.valid_from_gen == <generation>`` assertion.
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


def test_poros_demo_argparse() -> None:
    args = _build_parser().parse_args(
        [
            "poros",
            "demo",
            "--out",
            "/tmp/poros-demo",
            "--sender",
            "okg:lab-a",
            "--receiver",
            "okg:mit",
        ]
    )

    assert args.command == "poros"
    assert args.poros_subcmd == "demo"
    assert args.sender == "okg:lab-a"
    assert args.receiver == "okg:mit"


def test_poros_submit_argparse() -> None:
    args = _build_parser().parse_args(
        [
            "poros",
            "submit",
            "--envelope",
            "envelope.wire.json",
            "--trust-pins",
            "trust-pins.json",
            "--receiver-keys",
            "receiver.keys.json",
            "--deployment",
            "default",
            "--dsn",
            "postgres://x",
            "--sender-dsn",
            "postgres://sender",
            "--persist-preflight",
            "--apply",
            "--publish",
            "--nomos-bundle",
            "compiled-policy.json",
        ]
    )

    assert args.command == "poros"
    assert args.poros_subcmd == "submit"
    assert args.sender_dsn == "postgres://sender"
    assert args.persist_preflight is True
    assert args.apply is True
    assert args.publish is True
    assert args.nomos_bundle == "compiled-policy.json"


def test_poros_serve_argparse_accepts_sender_dsn() -> None:
    args = _build_parser().parse_args(
        [
            "poros",
            "serve",
            "--dsn",
            "postgres://receiver",
            "--sender-dsn",
            "postgres://sender",
            "--trust-pins",
            "trust-pins.json",
            "--receiver-keys",
            "receiver.keys.json",
            "--receiver",
            "okg:mit",
            "--apply",
            "--publish",
        ]
    )

    assert args.command == "poros"
    assert args.poros_subcmd == "serve"
    assert args.dsn == "postgres://receiver"
    assert args.sender_dsn == "postgres://sender"
    assert args.apply is True
    assert args.publish is True


def test_poros_status_argparse() -> None:
    args = _build_parser().parse_args(
        [
            "poros",
            "status",
            "--dsn",
            "postgres://receiver",
            "--side",
            "receiver",
            "--graph-id",
            "poros:graph:one",
            "--limit",
            "5",
        ]
    )

    assert args.command == "poros"
    assert args.poros_subcmd == "status"
    assert args.graph_id == "poros:graph:one"
    assert args.limit == 5


def test_poros_promote_argparse() -> None:
    args = _build_parser().parse_args(
        [
            "poros",
            "promote",
            "--dsn",
            "postgres://receiver",
            "--deployment",
            "default",
            "--actor",
            "okg:mit-reviewer",
            "--nomos-bundle",
            "compiled-policy.json",
            "--message-id",
            "msg-1",
            "--publish",
        ]
    )

    assert args.command == "poros"
    assert args.poros_subcmd == "promote"
    assert args.actor == "okg:mit-reviewer"
    assert args.message_id == "msg-1"
    assert args.publish is True


def test_poros_outbox_export_argparse() -> None:
    args = _build_parser().parse_args(
        [
            "poros",
            "outbox-export",
            "--dsn",
            "postgres://x",
            "--sender",
            "okg:lab-a",
            "--recipient",
            "okg:mit",
            "--sender-generation-id",
            "7",
            "--graph-id",
            "poros:graph:test",
            "--root-node-id",
            "node:one",
            "--signing-keys",
            "sender.keys.json",
        ]
    )

    assert args.command == "poros"
    assert args.poros_subcmd == "outbox-export"
    # Chronos cutover: a published generation is a Chronos checkpoint with a
    # NON-NUMERIC string id, so --sender-generation-id parses as TEXT (str),
    # not int. The string is stored verbatim in the TEXT outbox column.
    assert args.sender_generation_id == "7"
    assert args.root_node_id == ["node:one"]


def test_poros_harness_init_argparse() -> None:
    args = _build_parser().parse_args(
        [
            "poros",
            "harness-init",
            "--sender-dsn",
            "postgres://sender",
            "--receiver-dsn",
            "postgres://receiver",
            "--skip-migrate",
        ]
    )

    assert args.command == "poros"
    assert args.poros_subcmd == "harness-init"
    assert args.sender_dsn == "postgres://sender"
    assert args.receiver_dsn == "postgres://receiver"
    assert args.skip_migrate is True


def test_poros_hardening_harness_argparse() -> None:
    args = _build_parser().parse_args(
        [
            "poros",
            "hardening-harness",
            "--sender-dsn",
            "postgres://sender",
            "--receiver-dsn",
            "postgres://receiver",
            "--out",
            "/tmp/poros-hardening",
            "--skip-migrate",
            "--overwrite",
            "--allow-dirty",
        ]
    )

    assert args.command == "poros"
    assert args.poros_subcmd == "hardening-harness"
    assert args.sender_dsn == "postgres://sender"
    assert args.receiver_dsn == "postgres://receiver"
    assert args.out == "/tmp/poros-hardening"
    assert args.skip_migrate is True
    assert args.overwrite is True
    assert args.allow_dirty is True


def test_poros_readiness_harness_argparse() -> None:
    args = _build_parser().parse_args(
        [
            "poros",
            "readiness-harness",
            "--sender-dsn",
            "postgres://sender",
            "--receiver-dsn",
            "postgres://receiver",
            "--out",
            "/tmp/poros-readiness",
            "--skip-migrate",
            "--overwrite",
        ]
    )

    assert args.command == "poros"
    assert args.poros_subcmd == "readiness-harness"
    assert args.sender_dsn == "postgres://sender"
    assert args.receiver_dsn == "postgres://receiver"
    assert args.out == "/tmp/poros-readiness"
    assert args.skip_migrate is True
    assert args.overwrite is True


def test_poros_readiness_init_and_peer_argparse() -> None:
    args = _build_parser().parse_args(
        [
            "poros",
            "init",
            "--manifest",
            "/tmp/poros.manifest.json",
            "--authority",
            "okg:lab-a",
            "--role",
            "sender",
            "--role",
            "receiver",
            "--receiver-endpoint",
            "http://127.0.0.1:8765/poros/v1/inbox",
        ]
    )

    assert args.command == "poros"
    assert args.poros_subcmd == "init"
    assert args.authority == "okg:lab-a"
    assert args.role == ["sender", "receiver"]

    peer_args = _build_parser().parse_args(
        [
            "poros",
            "peer",
            "add",
            "--manifest",
            "/tmp/poros.manifest.json",
            "--authority",
            "okg:mit-demo",
            "--kid",
            "mit-sign",
            "--public-key",
            "abc",
        ]
    )

    assert peer_args.command == "poros"
    assert peer_args.poros_subcmd == "peer"
    assert peer_args.peer_subcmd == "add"
    assert peer_args.authority == "okg:mit-demo"

    send_args = _build_parser().parse_args(
        [
            "poros",
            "send",
            "--manifest",
            "/tmp/poros.manifest.json",
            "--dsn",
            "postgres://sender",
            "--peer",
            "okg:mit-demo",
            "--sender-generation-id",
            "3",
            "--graph-id",
            "poros:graph:test",
            "--root-node-id",
            "node:one",
            "--no-deliver",
        ]
    )

    assert send_args.command == "poros"
    assert send_args.poros_subcmd == "send"
    assert send_args.peer == "okg:mit-demo"
    assert send_args.deliver is False

    receive_args = _build_parser().parse_args(
        [
            "poros",
            "receive",
            "--manifest",
            "/tmp/poros.manifest.json",
            "--dsn",
            "postgres://receiver",
            "--level",
            "pilot",
            "--trust-pins-out",
            "/tmp/trust-pins.json",
            "--apply",
            "--publish",
        ]
    )

    assert receive_args.command == "poros"
    assert receive_args.poros_subcmd == "receive"
    assert receive_args.level == "pilot"
    assert receive_args.apply is True
    assert receive_args.publish is True


def test_poros_proof_argparse() -> None:
    args = _build_parser().parse_args(
        [
            "poros",
            "proof",
            "--dsn",
            "postgres://receiver",
            "--message-id",
            "msg-1",
            "--mode",
            "audit",
        ]
    )

    assert args.command == "poros"
    assert args.poros_subcmd == "proof"
    assert args.message_id == "msg-1"
    assert args.mode == "audit"


def test_poros_init_peer_and_readiness_cli(
    tmp_path: Path,
    capsys,
) -> None:
    manifest = tmp_path / "poros.manifest.json"
    policy = tmp_path / "policy.bundle.json"
    policy.write_text('{"policy_bundle_id":"demo"}\n', encoding="utf-8")
    peer_key = generate_ed25519_keypair("mit-sign-v1")
    peer_key_v2 = generate_ed25519_keypair("mit-sign-v2")

    rc = cli.main(
        [
            "poros",
            "--json",
            "init",
            "--manifest",
            str(manifest),
            "--authority",
            "okg:lab-a",
            "--role",
            "sender",
            "--role",
            "receiver",
            "--receiver-endpoint",
            "http://127.0.0.1:8765/poros/v1/inbox",
            "--nomos-bundle",
            str(policy),
        ]
    )
    out = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert out["status"] == "ok"
    assert manifest.is_file()
    assert Path(out["key_path"]).is_file()
    assert "private_key_path" not in out["manifest"]["signing_keys"][0]
    assert out["readiness"]["ready"] is False

    rc = cli.main(
        [
            "poros",
            "--json",
            "peer",
            "add",
            "--manifest",
            str(manifest),
            "--authority",
            "okg:mit-demo",
            "--kid",
            peer_key.kid,
            f"--public-key={peer_key.public_key}",
            "--endpoint",
            "http://127.0.0.1:8765/poros/v1/inbox",
        ]
    )
    add_out = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert add_out["peer"]["authority_id"] == "okg:mit-demo"
    assert add_out["trust_pins"]["pins"][0]["kid"] == peer_key.kid

    rc = cli.main(
        [
            "poros",
            "--json",
            "peer",
            "list",
            "--manifest",
            str(manifest),
        ]
    )
    list_out = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert list_out["peers"][0]["authority_id"] == "okg:mit-demo"

    rc = cli.main(
        [
            "poros",
            "--json",
            "peer",
            "inspect",
            "--manifest",
            str(manifest),
            "--authority",
            "okg:mit-demo",
        ]
    )
    inspect_out = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert inspect_out["peer"]["authority_id"] == "okg:mit-demo"

    rc = cli.main(
        [
            "poros",
            "--json",
            "readiness",
            "--manifest",
            str(manifest),
        ]
    )
    ready_out = json.loads(capsys.readouterr().out)
    assert rc == 1
    assert ready_out["ready"] is False
    failed = {check["id"] for check in ready_out["checks"] if not check["ok"]}
    assert "evidence.transfer_proof" in failed

    trust_pins_out = tmp_path / "receiver-trust-pins.json"
    rc = cli.main(
        [
            "poros",
            "--json",
            "receive",
            "--manifest",
            str(manifest),
            "--dsn",
            "postgres://receiver",
            "--trust-pins-out",
            str(trust_pins_out),
            "--apply",
            "--publish",
        ]
    )
    receive_out = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert receive_out["ready"] is True
    assert trust_pins_out.is_file()
    assert receive_out["serve_command"][:4] == [
        "okg",
        "poros",
        "serve",
        "--dsn",
    ]
    assert "--nomos-bundle" in receive_out["serve_command"]

    manifest_payload = json.loads(manifest.read_text(encoding="utf-8"))
    proof_generation_id = "okg:receiver:branch:default:gen:5"
    manifest_payload["readiness_evidence"] = {
        "database_identities": {
            "sender": {"database": "okg_sender"},
            "receiver": {"database": "okg_receiver"},
            "separate_databases": True,
        },
        "latest_transfer_proof": {
            "proof_kind": "poros_cli_test",
            "proof_version": 1,
            "freshness": {
                "fresh": True,
                "source": "test",
                "observed_at": datetime.now(UTC).isoformat(),
                "max_age_seconds": 3600,
            },
            "observed_at": datetime.now(UTC).isoformat(),
            "message_id": "msg-1",
            "graph_id": "poros:graph:demo",
            "state": "published",
            "sender_outbox_status": "delivered",
            "receiver_generation": {
                "generation_id": proof_generation_id,
                "status": "published",
            },
            "read_surface": {
                "kind": "chronos_generation",
                "generation_id": proof_generation_id,
            },
            "nomos_decision": {
                "event_type": "nomos_decided",
                "status": "allow",
                "nomos_decision_id": "nomos:decision:msg-1",
                "policy_fingerprint": "sha256:policy",
                "nomos_input_hash": "sha256:input",
            },
            "admission": {
                "event_type": "admitted",
                "status": "accepted",
                "admission_id": "admission-msg-1",
            },
            "publication": {
                "event_type": "published",
                "status": "published",
                "receiver_generation_id": proof_generation_id,
            },
            "expected_live_row_delta": {
                "nodes_live": 1,
                "edges_live": 0,
            },
            "observed_live_row_delta": {
                "nodes_live": 1,
                "edges_live": 0,
            },
            "live_row_delta": {
                "nodes_live": 1,
                "edges_live": 0,
            },
        },
    }
    manifest.write_text(
        json.dumps(manifest_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    rc = cli.main(
        [
            "poros",
            "--json",
            "readiness",
            "--manifest",
            str(manifest),
        ]
    )
    ready_with_evidence = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert ready_with_evidence["ready"] is True

    rc = cli.main(
        [
            "poros",
            "--json",
            "peer",
            "rotate-key",
            "--manifest",
            str(manifest),
            "--authority",
            "okg:mit-demo",
            "--old-kid",
            peer_key.kid,
            "--new-kid",
            peer_key_v2.kid,
            f"--public-key={peer_key_v2.public_key}",
        ]
    )
    rotate_out = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert rotate_out["trust_pins"]["pins"][0]["kid"] == peer_key_v2.kid
    old_key = next(
        key for key in rotate_out["peer"]["keys"] if key["kid"] == peer_key.kid
    )
    assert old_key["status"] == "retired"
    assert old_key["replaced_by"] == peer_key_v2.kid

    rc = cli.main(
        [
            "poros",
            "--json",
            "peer",
            "disable",
            "--manifest",
            str(manifest),
            "--authority",
            "okg:mit-demo",
        ]
    )
    disable_out = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert disable_out["peer"]["status"] == "disabled"

    rc = cli.main(
        [
            "poros",
            "--json",
            "readiness",
            "--manifest",
            str(manifest),
        ]
    )
    not_ready = json.loads(capsys.readouterr().out)
    assert rc == 1
    assert not_ready["ready"] is False


def test_poros_receive_fails_without_receiver_policy(
    tmp_path: Path,
    capsys,
) -> None:
    manifest = tmp_path / "poros.manifest.json"
    peer_key = generate_ed25519_keypair("lab-a-sign")
    rc = cli.main(
        [
            "poros",
            "--json",
            "init",
            "--manifest",
            str(manifest),
            "--authority",
            "okg:mit-demo",
            "--role",
            "receiver",
            "--receiver-endpoint",
            "http://127.0.0.1:8765/poros/v1/inbox",
            "--object-store-root",
            str(tmp_path / "objects"),
        ]
    )
    assert rc == 0
    capsys.readouterr()
    rc = cli.main(
        [
            "poros",
            "--json",
            "peer",
            "add",
            "--manifest",
            str(manifest),
            "--authority",
            "okg:lab-a",
            "--kid",
            peer_key.kid,
            f"--public-key={peer_key.public_key}",
        ]
    )
    assert rc == 0
    capsys.readouterr()

    rc = cli.main(
        [
            "poros",
            "--json",
            "receive",
            "--manifest",
            str(manifest),
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert rc == 1
    assert payload["ready"] is False
    failed = {check["id"] for check in payload["checks"] if not check["ok"]}
    assert "receiver.nomos_bundle" in failed


@pytest.mark.postgres
def test_poros_send_workflow_exports_and_delivers_with_manifest_peer(
    clean_runtime: str,
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    node_id = "poros:test:send-workflow-root"
    run_id = str(uuid.uuid4())
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts (
                node_id, subtype, attrs, source, source_record_id,
                source_revision, observed_at, dedupe_key, run_id, op
            )
            VALUES (
                %s, 'context_packet', %s, 'poros_send_workflow_test',
                %s, %s, now(), %s, %s::uuid, 'I'
            )
            """,
            (
                node_id,
                Jsonb({"text": "Poros send workflow root"}),
                Jsonb({"id": node_id}),
                Jsonb({"v": 1}),
                f"poros-send-workflow:{run_id}:{node_id}",
                run_id,
            ),
        )
    _seed_chronos_node(
        clean_runtime,
        node_id,
        "context_packet",
        {"text": "Poros send workflow root"},
    )
    publish = Publisher(clean_runtime, "poros-sender", catalog_identity=bind_test_catalog_writer(clean_runtime, "poros-sender")).cycle()
    assert publish["status"] == "published", publish

    manifest = tmp_path / "poros.manifest.json"
    receiver_key = generate_ed25519_keypair("mit-send-workflow-sign")
    rc = cli.main(
        [
            "poros",
            "--json",
            "init",
            "--manifest",
            str(manifest),
            "--authority",
            "okg:lab-a",
            "--role",
            "sender",
            "--object-store-root",
            str(tmp_path / "objects"),
        ]
    )
    assert rc == 0
    capsys.readouterr()
    rc = cli.main(
        [
            "poros",
            "--json",
            "peer",
            "add",
            "--manifest",
            str(manifest),
            "--authority",
            "okg:mit-demo",
            "--kid",
            receiver_key.kid,
            f"--public-key={receiver_key.public_key}",
            "--endpoint",
            "http://receiver.example/poros/v1/inbox",
        ]
    )
    assert rc == 0
    capsys.readouterr()

    class _FakeResponse:
        def __init__(self, payload: dict[str, object]) -> None:
            self._payload = payload
            # The redirect-SSRF guard reads status_code before .json(); a 2xx
            # is the non-redirect success path.
            self.status_code = 200
            self.headers: dict[str, str] = {}

        def json(self) -> dict[str, object]:
            return self._payload

    def _fake_post(url, *, data, headers, timeout, allow_redirects=True):  # noqa: ANN001
        assert url == "http://receiver.example/poros/v1/inbox"
        assert headers["content-type"] == "application/json"
        assert timeout == 10.0
        assert allow_redirects is False
        envelope = json.loads(data)
        receipt = sign_receipt(
            {
                "status": "accepted_for_admission",
                "message_id": envelope["message_id"],
                "body_sha256": envelope["body_sha256"],
                "receiver": "okg:mit-demo",
            },
            receiver_key.private_key,
            kid=receiver_key.kid,
        )
        return _FakeResponse(
            {
                "status": "accepted_for_admission",
                "reason_code": "accepted",
                "receipt": receipt,
            }
        )

    monkeypatch.setattr("requests.post", _fake_post)
    # The outbound-delivery egress guard rejects the non-resolvable
    # `receiver.example` test host. The send path uses a mocked `requests.post`,
    # so neutralize the SSRF URL check the way the poros_http suite does for its
    # mocked receiver (mirror of the `_allow_test_receiver` fixture).
    monkeypatch.setattr(
        "okg.substrate.exchange.sender_delivery.assert_public_http_url",
        lambda *args, **kwargs: None,
    )

    rc = cli.main(
        [
            "poros",
            "--json",
            "send",
            "--manifest",
            str(manifest),
            "--dsn",
            clean_runtime,
            "--peer",
            "okg:mit-demo",
            "--sender-generation-id",
            str(publish["generation_id"]),
            "--graph-id",
            "poros:graph:send-workflow",
            "--root-node-id",
            node_id,
            "--message-id",
            "send-workflow-message",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["export"]["message_id"] == "send-workflow-message"
    assert payload["export"]["sender_authority_id"] == "okg:lab-a"
    assert payload["export"]["recipient_authority_id"] == "okg:mit-demo"
    assert payload["delivery"][0]["status"] == "accepted_for_admission"
    assert payload["delivery"][0]["outbox_status"] == "delivered"

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT sender_authority_id, recipient_authority_id, graph_id,
                   signing_key_id, status
              FROM okg.poros_outbox_messages
             WHERE message_id = 'send-workflow-message'
            """
        )
        outbox = cur.fetchone()
        cur.execute(
            """
            SELECT status, reason_code, receipt_sha256, transport
              FROM okg.poros_delivery_attempts
             WHERE message_id = 'send-workflow-message'
            """
        )
        attempt = cur.fetchone()
    assert outbox == (
        "okg:lab-a",
        "okg:mit-demo",
        "poros:graph:send-workflow",
        "poros-signing",
        "delivered",
    )
    assert attempt[0] == "accepted_for_admission"
    assert attempt[1] == "accepted"
    assert attempt[2]
    assert attempt[3] == "http"


@pytest.mark.postgres
def test_poros_send_workflow_rejects_disabled_peer_before_outbox(
    clean_runtime: str,
    tmp_path: Path,
    capsys,
) -> None:
    manifest = tmp_path / "poros.manifest.json"
    peer_key = generate_ed25519_keypair("mit-disabled-sign")
    rc = cli.main(
        [
            "poros",
            "--json",
            "init",
            "--manifest",
            str(manifest),
            "--authority",
            "okg:lab-a",
            "--role",
            "sender",
            "--object-store-root",
            str(tmp_path / "objects"),
        ]
    )
    assert rc == 0
    capsys.readouterr()
    rc = cli.main(
        [
            "poros",
            "--json",
            "peer",
            "add",
            "--manifest",
            str(manifest),
            "--authority",
            "okg:mit-demo",
            "--kid",
            peer_key.kid,
            f"--public-key={peer_key.public_key}",
        ]
    )
    assert rc == 0
    capsys.readouterr()
    rc = cli.main(
        [
            "poros",
            "--json",
            "peer",
            "disable",
            "--manifest",
            str(manifest),
            "--authority",
            "okg:mit-demo",
        ]
    )
    assert rc == 0
    capsys.readouterr()

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.poros_outbox_messages")
        before = int(cur.fetchone()[0] or 0)
    rc = cli.main(
        [
            "poros",
            "--json",
            "send",
            "--manifest",
            str(manifest),
            "--dsn",
            clean_runtime,
            "--peer",
            "okg:mit-demo",
            "--sender-generation-id",
            "1",
            "--graph-id",
            "poros:graph:disabled-peer",
            "--root-node-id",
            "node:missing",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert rc == 1
    assert payload["reason_code"] == "peer_disabled"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.poros_outbox_messages")
        after = int(cur.fetchone()[0] or 0)
    assert after == before


def test_poros_demo_writes_artifacts_and_verifiable_receipt(
    tmp_path: Path,
    capsys,
) -> None:
    rc = cli.main(
        [
            "poros",
            "demo",
            "--out",
            str(tmp_path),
            "--message-id",
            "demo-cli-message",
            "--json",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "accepted_for_admission"
    assert payload["receipt_verified"] is True
    assert payload["admission_candidate_created"] is True
    assert payload["message_id"] == "demo-cli-message"

    artifacts = payload["artifacts"]
    for path in artifacts.values():
        assert Path(path).is_file()

    receiver_keys = json.loads(Path(artifacts["receiver_private_key"]).read_text())
    receipt = json.loads(Path(artifacts["receipt_pretty"]).read_text())
    verify_receipt(receipt, receiver_keys["public_key"])

    candidate = json.loads(Path(artifacts["admission_candidate"]).read_text())
    assert candidate["message_id"] == "demo-cli-message"
    assert candidate["body"]["graph_id"] == "poros:graph:poros-demo"
    assert candidate["body"]["nodes"][0]["subtype"] == "context_packet"


def test_poros_demo_refuses_to_overwrite_existing_artifacts(
    tmp_path: Path,
    capsys,
) -> None:
    (tmp_path / "existing.txt").write_text("keep", encoding="utf-8")

    rc = cli.main(["poros", "demo", "--out", str(tmp_path), "--json"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "error"
    assert "already contains files" in payload["error"]


def _write_demo(tmp_path: Path) -> None:
    rc = cli.main(
        [
            "poros",
            "demo",
            "--out",
            str(tmp_path),
            "--message-id",
            "submit-cli-message",
            "--overwrite",
            "--json",
        ]
    )
    assert rc == 0


def test_poros_submit_preflight_only_from_demo_artifacts(
    tmp_path: Path,
    capsys,
) -> None:
    _write_demo(tmp_path)
    capsys.readouterr()

    rc = cli.main(
        [
            "poros",
            "submit",
            "--envelope",
            str(tmp_path / "envelope.wire.json"),
            "--trust-pins",
            str(tmp_path / "trust-pins.json"),
            "--receiver-keys",
            str(tmp_path / "receiver.keys.json"),
            "--json",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "accepted_for_admission"
    assert payload["receipt_verified"] is True
    assert payload["admission"] is None
    assert payload["publish"] is None


def test_poros_submit_can_evaluate_nomos_policy(
    tmp_path: Path,
    capsys,
) -> None:
    from okg.substrate.nomos import run_demo

    poros_dir = tmp_path / "poros"
    nomos_dir = tmp_path / "nomos"
    _write_demo(poros_dir)
    run_demo(nomos_dir)
    capsys.readouterr()

    rc = cli.main(
        [
            "poros",
            "submit",
            "--envelope",
            str(poros_dir / "envelope.wire.json"),
            "--trust-pins",
            str(poros_dir / "trust-pins.json"),
            "--receiver-keys",
            str(poros_dir / "receiver.keys.json"),
            "--nomos-bundle",
            str(nomos_dir / "compiled-policy.json"),
            "--json",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "accepted_for_admission"
    assert payload["nomos_decision"]["effect"] == "allow"
    assert payload["nomos_decision"]["matched_rule_ids"] == [
        "allow_public_poros_context"
    ]
    assert payload["nomos_decision"]["resource"]["signing_key_id"] == (
        "lab-a-sign-demo"
    )
    assert payload["nomos_decision"]["resource"]["sender_authority_id"] == (
        "okg:lab-a"
    )
    assert payload["nomos_decision"]["resource"]["graph_classification"] == "public"
    assert payload["nomos_decision"]["resource"]["requested_incorporation_mode"] == (
        "fast_track"
    )
    assert payload["nomos_decision"]["resource"]["root_pattern"] == "context"
    assert payload["nomos_decision"]["resource"]["node_subtypes"] == [
        "context_packet"
    ]
    assert payload["nomos_decision"]["resource"]["edge_narrowings"] == []


def test_poros_submit_refuses_database_mutation_without_apply(
    tmp_path: Path,
    capsys,
) -> None:
    _write_demo(tmp_path)
    capsys.readouterr()

    rc = cli.main(
        [
            "poros",
            "submit",
            "--envelope",
            str(tmp_path / "envelope.wire.json"),
            "--trust-pins",
            str(tmp_path / "trust-pins.json"),
            "--receiver-keys",
            str(tmp_path / "receiver.keys.json"),
            "--dsn",
            "postgres://does-not-connect",
            "--json",
        ]
    )

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "accepted_for_admission"
    assert "--apply" in payload["mutation_error"]


@pytest.mark.postgres
def test_poros_submit_persists_preflight_without_fact_side_effects(
    clean_runtime: str,
    tmp_path: Path,
    capsys,
) -> None:
    _write_demo(tmp_path)
    capsys.readouterr()

    rc = cli.main(
        [
            "poros",
            "submit",
            "--envelope",
            str(tmp_path / "envelope.wire.json"),
            "--trust-pins",
            str(tmp_path / "trust-pins.json"),
            "--receiver-keys",
            str(tmp_path / "receiver.keys.json"),
            "--dsn",
            clean_runtime,
            "--persist-preflight",
            "--json",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "accepted_for_admission"
    assert payload["admission"] is None
    assert payload["publish"] is None
    assert payload["preflight"]["status"] == "accepted_for_admission"
    assert payload["preflight"]["receipt_sha256"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, receipt IS NOT NULL, body_sha256
              FROM okg.poros_inbox_messages
             WHERE message_id = 'submit-cli-message'
            """
        )
        row = cur.fetchone()
        cur.execute("SELECT count(*) FROM okg.node_facts")
        node_fact_count = int(cur.fetchone()[0] or 0)
        cur.execute("SELECT count(*) FROM okg.edge_facts")
        edge_fact_count = int(cur.fetchone()[0] or 0)
    nodes_live_count, edges_live_count = _live_graph_counts(clean_runtime)

    assert row == (
        "accepted_for_admission",
        True,
        payload["body_sha256"],
    )
    assert node_fact_count == 0
    assert edge_fact_count == 0
    assert nodes_live_count == 0
    assert edges_live_count == 0


def _write_exported_message(
    dsn: str,
    tmp_path: Path,
    *,
    message_id: str,
) -> tuple[Path, Path, str]:
    node_id = f"poros:test:{message_id}:root"
    run_id = str(uuid.uuid4())
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts (
                node_id, subtype, attrs, source, source_record_id,
                source_revision, observed_at, dedupe_key, run_id, op
            )
            VALUES (
                %s, 'concept', %s, 'poros_test', %s, %s,
                now(), %s, %s::uuid, 'I'
            )
            """,
            (
                node_id,
                Jsonb({"text": "Poros transport ledger root"}),
                Jsonb({"id": node_id}),
                Jsonb({"v": 1}),
                f"poros-test:{run_id}:{node_id}",
                run_id,
            ),
        )
    _seed_chronos_node(
        dsn,
        node_id,
        "concept",
        {"text": "Poros transport ledger root"},
    )
    publish = Publisher(dsn, "poros-sender", catalog_identity=bind_test_catalog_writer(dsn, "poros-sender")).cycle()
    assert publish["status"] == "published", publish

    sender = generate_ed25519_keypair("lab-a-sign-1")
    receiver = generate_ed25519_keypair("mit-sign-1")
    sender_keys_path = tmp_path / "sender.keys.json"
    receiver_keys_path = tmp_path / "receiver.keys.json"
    trust_pins_path = tmp_path / "trust-pins.json"
    sender_keys_path.write_text(json.dumps(sender.__dict__), encoding="utf-8")
    receiver_keys_path.write_text(json.dumps(receiver.__dict__), encoding="utf-8")
    trust_pins_path.write_text(
        json.dumps(
            {
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
        ),
        encoding="utf-8",
    )
    exported = export_published_subgraph_to_outbox(
        dsn=dsn,
        sender_authority_id="okg:lab-a",
        recipient_authority_id="okg:mit",
        sender_generation_id=publish["generation_id"],
        graph_id="poros:graph:transport-ledger",
        root_node_ids=[node_id],
        signing_private_key=sender.private_key,
        signing_key_id=sender.kid,
        message_id=message_id,
    )
    envelope_path = tmp_path / "envelope.wire.json"
    envelope_path.write_bytes(canonical_json_bytes(exported.envelope))
    return envelope_path, sender_keys_path, exported.body_sha256


def _write_preflight_fixture(
    tmp_path: Path,
    *,
    case: str,
) -> tuple[Path, str, str]:
    sender = generate_ed25519_keypair("lab-a-sign-1")
    receiver = generate_ed25519_keypair("mit-sign-1")
    (tmp_path / "receiver.keys.json").write_text(
        json.dumps(receiver.__dict__),
        encoding="utf-8",
    )
    (tmp_path / "trust-pins.json").write_text(
        json.dumps(
            {
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
        ),
        encoding="utf-8",
    )
    created_at = (
        datetime(2020, 1, 1, tzinfo=UTC)
        if case == "expired"
        else datetime.now(UTC).replace(microsecond=0)
    )
    unsigned = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit",
        message_id=f"bad-preflight-{case}",
        created_at=created_at,
        ttl_seconds=1 if case == "expired" else 15 * 60,
        body={
            "graph_id": f"poros:graph:bad-preflight:{case}",
            "origin_authority_id": "okg:lab-a",
            "origin_generation_id": 1,
            "classification": "public",
            "graph_profile": "context",
            "root_node_ids": [f"poros:test:{case}"],
            "nodes": [
                {
                    "node_id": f"poros:test:{case}",
                    "subtype": "context_packet",
                    "attrs": {"text": case},
                }
            ],
            "edges": [],
        },
    )
    envelope = sign_envelope(unsigned, sender.private_key, kid=sender.kid)
    if case == "tampered":
        envelope["body"]["nodes"][0]["attrs"]["text"] = "tampered-after-signing"
    envelope_path = tmp_path / "envelope.wire.json"
    envelope_path.write_bytes(canonical_json_bytes(envelope))
    return envelope_path, sender.public_key, str(envelope.get("body_sha256"))


def _write_signed_graph_fixture(
    tmp_path: Path,
    *,
    message_id: str,
    root_subtype: str,
    graph_profile: str,
    classification: str = "public",
) -> tuple[Path, str]:
    sender = generate_ed25519_keypair("lab-a-sign-demo")
    receiver = generate_ed25519_keypair("mit-demo-sign")
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "receiver.keys.json").write_text(
        json.dumps(receiver.__dict__),
        encoding="utf-8",
    )
    (tmp_path / "trust-pins.json").write_text(
        json.dumps(
            {
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
        ),
        encoding="utf-8",
    )
    node_id = f"poros:test:{message_id}:root"
    unsigned = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit-demo",
        message_id=message_id,
        created_at=datetime.now(UTC).replace(microsecond=0),
        ttl_seconds=15 * 60,
        body={
            "graph_id": f"poros:graph:{message_id}",
            "origin_authority_id": "okg:lab-a",
            "origin_generation_id": 1,
            "classification": classification,
            "graph_profile": graph_profile,
            "root_node_ids": [node_id],
            "nodes": [
                {
                    "node_id": node_id,
                    "subtype": root_subtype,
                    "attrs": {"text": f"{root_subtype} payload"},
                }
            ],
            "edges": [],
        },
    )
    envelope = sign_envelope(unsigned, sender.private_key, kid=sender.kid)
    envelope_path = tmp_path / "envelope.wire.json"
    envelope_path.write_bytes(canonical_json_bytes(envelope))
    return envelope_path, node_id


def _cern_oidc_claim(provider_id: str, issuer: str) -> dict:
    # This claim reaches Nomos through a path that supplies no clock, so it
    # is evaluated against real time (issue #1007). Live timestamps, as a
    # real login would produce.
    now = datetime.now(tz=UTC)
    return {
        "provider_id": provider_id,
        "verification_status": "valid",
        "issuer": issuer,
        "claims": {
            "sub": "cern-user-123",
            "aud": "akmon-cms",
            "cern_roles": ["cms-operator"],
            "acr": "mfa",
            "exp": (now + timedelta(hours=1)).isoformat(),
            "auth_time": (now - timedelta(minutes=1)).isoformat(),
        },
    }


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


def _write_detached_preflight_fixture(
    tmp_path: Path,
    *,
    case: str,
) -> tuple[Path, str]:
    sender = generate_ed25519_keypair("lab-a-sign-1")
    receiver = generate_ed25519_keypair("mit-sign-1")
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "receiver.keys.json").write_text(
        json.dumps(receiver.__dict__),
        encoding="utf-8",
    )
    (tmp_path / "trust-pins.json").write_text(
        json.dumps(
            {
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
        ),
        encoding="utf-8",
    )
    data = b'{"nodes":[{"node_id":"detached:should-not-parse"}],"edges":[]}'
    obj = build_detached_object_manifest(
        object_id=f"obj:{case}",
        media_type="application/vnd.okg.graph+json",
        data=data,
        chunk_size_bytes=17,
        locations=[{"uri": "s3://private-bucket/secret-object"}],
    )
    obj["inline_bytes_b64url"] = b64url_encode(data)
    if case == "object_hash":
        obj["cleartext_sha256"] = sha256_b64url(b"wrong-object")
    elif case == "chunk_hash":
        obj["chunks"][0]["cleartext_sha256"] = sha256_b64url(b"wrong-chunk")
    else:
        raise ValueError(f"unsupported detached fixture: {case}")
    unsigned = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit",
        message_id=f"detached-{case}",
        created_at=datetime.now(UTC).replace(microsecond=0),
        ttl_seconds=15 * 60,
        payload_mode="detached",
        body={
            "payload_mode": "detached",
            "graph_id": f"poros:graph:detached:{case}",
            "origin_authority_id": "okg:lab-a",
            "origin_generation_id": 1,
            "classification": "public",
            "objects": [obj],
        },
    )
    envelope = sign_envelope(unsigned, sender.private_key, kid=sender.kid)
    envelope_path = tmp_path / "envelope.wire.json"
    envelope_path.write_bytes(canonical_json_bytes(envelope))
    return envelope_path, str(envelope["body_sha256"])


def _write_restricted_detached_fixture(tmp_path: Path) -> tuple[Path, str]:
    sender = generate_ed25519_keypair("lab-a-sign-restricted")
    receiver = generate_ed25519_keypair("mit-sign-restricted")
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "receiver.keys.json").write_text(
        json.dumps(receiver.__dict__),
        encoding="utf-8",
    )
    (tmp_path / "trust-pins.json").write_text(
        json.dumps(
            {
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
        ),
        encoding="utf-8",
    )
    data = (
        b'{"nodes":[{"node_id":"detached:restricted",'
        b'"attrs":{"embedding":[1,2,3]}}],"edges":[]}'
    )
    obj = build_detached_object_manifest(
        object_id="obj:restricted-secret",
        media_type="application/vnd.okg.graph+json",
        data=data,
        chunk_size_bytes=17,
        classification="restricted",
        locations=[{"uri": "s3://private-bucket/restricted-secret"}],
    )
    obj["inline_bytes_b64url"] = b64url_encode(data)
    unsigned = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit",
        message_id="detached-restricted",
        created_at=datetime.now(UTC).replace(microsecond=0),
        ttl_seconds=15 * 60,
        payload_mode="detached",
        body={
            "payload_mode": "detached",
            "graph_id": "poros:graph:detached:restricted",
            "origin_authority_id": "okg:lab-a",
            "origin_generation_id": 1,
            "classification": "restricted",
            "objects": [obj],
        },
    )
    envelope = sign_envelope(unsigned, sender.private_key, kid=sender.kid)
    envelope_path = tmp_path / "restricted-envelope.wire.json"
    envelope_path.write_bytes(canonical_json_bytes(envelope))
    return envelope_path, str(envelope["body_sha256"])


@pytest.mark.postgres
def test_poros_transport_ledgers_and_duplicate_are_durable(
    clean_runtime: str,
    tmp_path: Path,
    capsys,
) -> None:
    envelope_path, keys_path, body_hash = _write_exported_message(
        clean_runtime,
        tmp_path,
        message_id="transport-ledger-message",
    )

    first_rc = cli.main(
        [
            "poros",
            "submit",
            "--envelope",
            str(envelope_path),
            "--trust-pins",
            str(tmp_path / "trust-pins.json"),
            "--receiver-keys",
            str(tmp_path / "receiver.keys.json"),
            "--dsn",
            clean_runtime,
            "--sender-dsn",
            clean_runtime,
            "--persist-preflight",
            "--json",
        ]
    )
    assert first_rc == 0
    first = json.loads(capsys.readouterr().out)
    assert first["preflight"]["status"] == "accepted_for_admission"
    assert first["delivery"]["status"] == "accepted_for_admission"

    second_rc = cli.main(
        [
            "poros",
            "submit",
            "--envelope",
            str(envelope_path),
            "--trust-pins",
            str(tmp_path / "trust-pins.json"),
            "--receiver-keys",
            str(tmp_path / "receiver.keys.json"),
            "--dsn",
            clean_runtime,
            "--sender-dsn",
            clean_runtime,
            "--persist-preflight",
            "--apply",
            "--json",
        ]
    )
    assert second_rc == 1
    second = json.loads(capsys.readouterr().out)
    assert second["status"] == "duplicate"
    assert second["preflight"]["status"] == "duplicate"
    assert second["admission"] is None

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              (SELECT count(*) FROM okg.poros_outbox_messages
                WHERE message_id = 'transport-ledger-message'
                  AND body_sha256 = %s),
              (SELECT count(*) FROM okg.poros_delivery_attempts
                WHERE message_id = 'transport-ledger-message'
                  AND body_sha256 = %s),
              (SELECT count(*) FROM okg.poros_inbox_messages
                WHERE message_id = 'transport-ledger-message'
                  AND body_sha256 = %s),
              (SELECT count(*) FROM okg.poros_replay_cache
                WHERE message_id = 'transport-ledger-message'
                  AND body_sha256 = %s
                  AND duplicate_count = 1),
              (SELECT count(*) FROM okg.poros_incorporation_events
                WHERE message_id = 'transport-ledger-message'
                  AND body_sha256 = %s),
              (SELECT count(*) FROM okg.admission_envelopes)
            """,
            (body_hash, body_hash, body_hash, body_hash, body_hash),
        )
        counts = tuple(int(value or 0) for value in cur.fetchone())
    live_transport_ledger = _live_node_count_like(
        clean_runtime, "poros:test:transport-ledger%"
    )
    assert counts[:4] == (1, 2, 1, 1)
    assert counts[4] >= 2
    assert counts[5] == 0
    assert live_transport_ledger == 1
    assert keys_path.is_file()


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("case", "reason_code"),
    [
        ("unpinned", "missing_trust_pin"),
        ("wrong_recipient", "recipient_mismatch"),
        ("expired", "expired_message"),
        ("tampered", "invalid_signature"),
    ],
)
def test_poros_bad_preflights_persist_rejections_without_admission(
    clean_runtime: str,
    tmp_path: Path,
    capsys,
    case: str,
    reason_code: str,
) -> None:
    envelope_path, _, _ = _write_preflight_fixture(tmp_path, case=case)
    trust_pins = tmp_path / "trust-pins.json"
    if case == "unpinned":
        trust_pins.write_text('{"pins":[]}', encoding="utf-8")

    cmd = [
        "poros",
        "submit",
        "--envelope",
        str(envelope_path),
        "--trust-pins",
        str(trust_pins),
        "--receiver-keys",
        str(tmp_path / "receiver.keys.json"),
        "--dsn",
        clean_runtime,
        "--persist-preflight",
        "--json",
    ]
    if case == "wrong_recipient":
        cmd.extend(["--receiver", "okg:other"])
    rc = cli.main(cmd)

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "rejected_preflight"
    assert payload["reason_code"] == reason_code
    assert payload["preflight"]["status"] == "rejected_preflight"

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.admission_envelopes")
        admission_count = int(cur.fetchone()[0] or 0)
        cur.execute("SELECT count(*) FROM okg.node_facts")
        node_fact_count = int(cur.fetchone()[0] or 0)
    nodes_live_count, _ = _live_graph_counts(clean_runtime)
    assert admission_count == 0
    assert node_fact_count == 0
    assert nodes_live_count == 0


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("case", "reason_code"),
    [
        ("object_hash", "object_hash_mismatch"),
        ("chunk_hash", "chunk_hash_mismatch"),
    ],
)
def test_detached_object_tamper_fails_before_admission(
    clean_runtime: str,
    tmp_path: Path,
    capsys,
    case: str,
    reason_code: str,
) -> None:
    envelope_path, body_hash = _write_detached_preflight_fixture(
        tmp_path,
        case=case,
    )

    rc = cli.main(
        [
            "poros",
            "submit",
            "--envelope",
            str(envelope_path),
            "--trust-pins",
            str(tmp_path / "trust-pins.json"),
            "--receiver-keys",
            str(tmp_path / "receiver.keys.json"),
            "--dsn",
            clean_runtime,
            "--persist-preflight",
            "--json",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "rejected_preflight"
    assert payload["reason_code"] == reason_code
    assert payload["preflight"]["body_sha256"] == body_hash

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.admission_envelopes")
        admission_count = int(cur.fetchone()[0] or 0)
        cur.execute("SELECT count(*) FROM okg.node_facts")
        node_fact_count = int(cur.fetchone()[0] or 0)
        cur.execute(
            """
            SELECT count(*)
              FROM okg.poros_detached_object_manifests
             WHERE message_id = %s
            """,
            (f"detached-{case}",),
        )
        manifest_count = int(cur.fetchone()[0] or 0)
    nodes_live_count, _ = _live_graph_counts(clean_runtime)

    assert admission_count == 0
    assert node_fact_count == 0
    assert nodes_live_count == 0
    assert manifest_count == 1


@pytest.mark.postgres
def test_poros_submit_persists_nomos_decision_event(
    clean_runtime: str,
    tmp_path: Path,
    capsys,
) -> None:
    from okg.substrate.nomos import run_demo

    poros_dir = tmp_path / "poros"
    nomos_dir = tmp_path / "nomos"
    _write_demo(poros_dir)
    run_demo(nomos_dir)
    capsys.readouterr()

    rc = cli.main(
        [
            "poros",
            "submit",
            "--envelope",
            str(poros_dir / "envelope.wire.json"),
            "--trust-pins",
            str(poros_dir / "trust-pins.json"),
            "--receiver-keys",
            str(poros_dir / "receiver.keys.json"),
            "--dsn",
            clean_runtime,
            "--persist-preflight",
            "--nomos-bundle",
            str(nomos_dir / "compiled-policy.json"),
            "--json",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    decision = payload["nomos_decision"]
    assert decision["effect"] == "allow"
    assert decision["reason_code"] == "nomos.demo.public_poros_context_allowed"

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, reason_code, nomos_decision_id,
                   policy_bundle_id, nomos_input_hash,
                   payload->>'decision_id'
              FROM okg.poros_incorporation_events
             WHERE message_id = 'submit-cli-message'
               AND event_type = 'nomos_decided'
            """
        )
        row = cur.fetchone()

    assert row == (
        "allow",
        "nomos.demo.public_poros_context_allowed",
        decision["decision_id"],
        decision["policy_bundle_id"],
        decision["input_hash"],
        decision["decision_id"],
    )


@pytest.mark.postgres
@pytest.mark.usefixtures("_explicit_factory_publish_authority")
def test_poros_policy_bundle_fingerprint_surfaces_in_status_and_proof(
    clean_runtime: str,
    tmp_path: Path,
    capsys,
) -> None:
    bundle_path = _write_poros_review_policy_bundle(tmp_path / "nomos")
    effects = {
        "allow": {
            "root_subtype": "context_packet",
            "classification": "public",
            "expected_state": "published",
            "expected_rc": 0,
        },
        "deny": {
            "root_subtype": "concept",
            "classification": "public",
            "expected_state": "blocked",
            "expected_rc": 1,
        },
        "review": {
            "root_subtype": "context_packet",
            "classification": "internal",
            "expected_state": "review",
            "expected_rc": 1,
        },
        "quarantine": {
            "root_subtype": "proposal",
            "classification": "public",
            "expected_state": "quarantined",
            "expected_rc": 1,
        },
    }
    observed: dict[str, dict[str, str]] = {}

    for effect, config in effects.items():
        message_id = f"policy-surface-{effect}-message"
        poros_dir = tmp_path / effect
        envelope_path, _ = _write_signed_graph_fixture(
            poros_dir,
            message_id=message_id,
            root_subtype=str(config["root_subtype"]),
            graph_profile="context",
            classification=str(config["classification"]),
        )
        rc = cli.main(
            [
                "poros",
                "submit",
                "--envelope",
                str(envelope_path),
                "--trust-pins",
                str(poros_dir / "trust-pins.json"),
                "--receiver-keys",
                str(poros_dir / "receiver.keys.json"),
                "--deployment",
                "default",
                "--dsn",
                clean_runtime,
                "--persist-preflight",
                "--apply",
                "--publish",
                "--nomos-bundle",
                str(bundle_path),
                "--json",
            ]
        )
        assert rc == config["expected_rc"]
        payload = json.loads(capsys.readouterr().out)
        decision = payload["nomos_decision"]
        assert decision["effect"] == effect
        assert decision["policy_bundle_id"] == "nomos:demo:baseline"
        assert decision["policy_fingerprint"].startswith("sha256:")
        observed[effect] = {
            "message_id": message_id,
            "graph_id": f"poros:graph:{message_id}",
            "policy_bundle_id": decision["policy_bundle_id"],
            "policy_fingerprint": decision["policy_fingerprint"],
            "input_hash": decision["input_hash"],
            "decision_id": decision["decision_id"],
            "expected_state": str(config["expected_state"]),
        }

    for effect, expected in observed.items():
        assert cli.main(
            [
                "poros",
                "proof",
                "--dsn",
                clean_runtime,
                "--message-id",
                expected["message_id"],
                "--json",
            ]
        ) == 0
        proof = json.loads(capsys.readouterr().out)
        assert proof["state"] == expected["expected_state"]
        nomos_event = next(
            event for event in proof["events"]
            if event["event_type"] == "nomos_decided"
        )
        assert nomos_event["status"] == effect
        assert nomos_event["nomos_decision_id"] == expected["decision_id"]
        assert nomos_event["policy_bundle_id"] == expected["policy_bundle_id"]
        assert nomos_event["policy_fingerprint"] == expected["policy_fingerprint"]
        assert nomos_event["nomos_input_hash"] == expected["input_hash"]
        assert (
            nomos_event["payload"]["policy_fingerprint"]
            == expected["policy_fingerprint"]
        )

        assert cli.main(
            [
                "poros",
                "status",
                "--dsn",
                clean_runtime,
                "--side",
                "receiver",
                "--message-id",
                expected["message_id"],
                "--json",
            ]
        ) == 0
        by_message = json.loads(capsys.readouterr().out)
        status_row = by_message["messages"][0]
        assert status_row["state"] == expected["expected_state"]
        assert status_row["nomos_effect"] == effect
        assert status_row["nomos_decision_id"] == expected["decision_id"]
        assert status_row["policy_bundle_id"] == expected["policy_bundle_id"]
        assert status_row["policy_fingerprint"] == expected["policy_fingerprint"]
        assert status_row["graph"]["graph_id"] == expected["graph_id"]

        assert cli.main(
            [
                "poros",
                "status",
                "--dsn",
                clean_runtime,
                "--side",
                "receiver",
                "--graph-id",
                expected["graph_id"],
                "--json",
            ]
        ) == 0
        by_graph = json.loads(capsys.readouterr().out)
        assert by_graph["messages"][0]["message_id"] == expected["message_id"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, policy_bundle_id, policy_fingerprint
              FROM okg.poros_incorporation_events
             WHERE event_type = 'nomos_decided'
             ORDER BY status
            """
        )
        rows = cur.fetchall()
    assert {row[0] for row in rows} == set(effects)
    assert all(row[1] == "nomos:demo:baseline" for row in rows)
    assert all(str(row[2]).startswith("sha256:") for row in rows)


@pytest.mark.postgres
def test_sender_graph_profile_hint_cannot_broaden_nomos_policy(
    clean_runtime: str,
    tmp_path: Path,
    capsys,
) -> None:
    from okg.substrate.nomos import run_demo

    poros_dir = tmp_path / "misleading"
    nomos_dir = tmp_path / "nomos"
    envelope_path, node_id = _write_signed_graph_fixture(
        poros_dir,
        message_id="misleading-profile-message",
        root_subtype="proposal",
        graph_profile="context",
    )
    run_demo(nomos_dir)
    capsys.readouterr()

    rc = cli.main(
        [
            "poros",
            "submit",
            "--envelope",
            str(envelope_path),
            "--trust-pins",
            str(poros_dir / "trust-pins.json"),
            "--receiver-keys",
            str(poros_dir / "receiver.keys.json"),
            "--dsn",
            clean_runtime,
            "--persist-preflight",
            "--apply",
            "--publish",
            "--nomos-bundle",
            str(nomos_dir / "compiled-policy.json"),
            "--json",
        ]
    )

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    decision = payload["nomos_decision"]
    assert decision["effect"] == "quarantine"
    assert decision["matched_rule_ids"] == ["quarantine_proposal_shaped_poros"]
    assert decision["resource"]["root_pattern"] == "proposal"
    assert decision["resource"]["node_subtypes"] == ["proposal"]
    assert decision["resource"]["graph_profile"] == "proposal"
    assert decision["resource"]["sender_graph_profile"] == "context"
    assert decision["resource"]["requested_incorporation_mode"] == "fast_track"
    assert decision["resource"]["root_pattern_vocabulary"] == (
        "poros-root-patterns-v1"
    )
    assert decision["resource"]["edge_narrowings"] == []
    assert payload["admission"] is None
    assert payload["publish"] is None

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, reason_code
              FROM okg.poros_incorporation_events
             WHERE message_id = 'misleading-profile-message'
               AND event_type = 'nomos_decided'
            """
        )
        event = cur.fetchone()
        cur.execute("SELECT count(*) FROM okg.node_facts WHERE node_id = %s", (node_id,))
        node_facts = int(cur.fetchone()[0] or 0)
    nodes_live = _live_node_count(clean_runtime, node_id)

    assert event == (
        "quarantine",
        "nomos.demo.profile_hint_cannot_broaden",
    )
    assert node_facts == 0
    assert nodes_live == 0


@pytest.mark.postgres
def test_non_public_fast_track_requires_receiver_policy(
    clean_runtime: str,
    tmp_path: Path,
    capsys,
) -> None:
    poros_dir = tmp_path / "non-public"
    envelope_path, node_id = _write_signed_graph_fixture(
        poros_dir,
        message_id="non-public-no-policy-message",
        root_subtype="context_packet",
        graph_profile="context",
        classification="internal",
    )

    rc = cli.main(
        [
            "poros",
            "submit",
            "--envelope",
            str(envelope_path),
            "--trust-pins",
            str(poros_dir / "trust-pins.json"),
            "--receiver-keys",
            str(poros_dir / "receiver.keys.json"),
            "--deployment",
            "default",
            "--dsn",
            clean_runtime,
            "--persist-preflight",
            "--apply",
            "--publish",
            "--json",
        ]
    )

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["nomos_decision"] is None
    assert "requires an explicit receiver Nomos" in payload["mutation_error"]
    assert payload["admission"] is None
    assert payload["publish"] is None

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_type, status, reason_code,
                   payload->'classification_gate'->>'classification'
              FROM okg.poros_incorporation_events
             WHERE message_id = 'non-public-no-policy-message'
               AND event_type = 'quarantined'
            """
        )
        event = cur.fetchone()
        cur.execute("SELECT count(*) FROM okg.node_facts WHERE node_id = %s", (node_id,))
        node_facts = int(cur.fetchone()[0] or 0)
    nodes_live = _live_node_count(clean_runtime, node_id)

    assert event == (
        "quarantined",
        "quarantine",
        "poros.classification_requires_receiver_policy",
        "internal",
    )
    assert node_facts == 0
    assert nodes_live == 0


@pytest.mark.postgres
@pytest.mark.usefixtures("_explicit_factory_publish_authority")
def test_nomos_allowed_path_publishes_and_quarantined_path_does_not(
    clean_runtime: str,
    tmp_path: Path,
    capsys,
) -> None:
    from okg.substrate.nomos import run_demo

    nomos_dir = tmp_path / "nomos"
    run_demo(nomos_dir)
    allowed_dir = tmp_path / "allowed"
    quarantined_dir = tmp_path / "quarantined"
    allowed_envelope, allowed_node_id = _write_signed_graph_fixture(
        allowed_dir,
        message_id="allowed-context-message",
        root_subtype="context_packet",
        graph_profile="context",
    )
    quarantined_envelope, quarantined_node_id = _write_signed_graph_fixture(
        quarantined_dir,
        message_id="quarantined-proposal-message",
        root_subtype="proposal",
        graph_profile="context",
    )
    capsys.readouterr()

    allowed_rc = cli.main(
        [
            "poros",
            "submit",
            "--envelope",
            str(allowed_envelope),
            "--trust-pins",
            str(allowed_dir / "trust-pins.json"),
            "--receiver-keys",
            str(allowed_dir / "receiver.keys.json"),
            "--deployment",
            "default",
            "--dsn",
            clean_runtime,
            "--persist-preflight",
            "--apply",
            "--publish",
            "--nomos-bundle",
            str(nomos_dir / "compiled-policy.json"),
            "--json",
        ]
    )
    assert allowed_rc == 0
    allowed = json.loads(capsys.readouterr().out)
    assert allowed["nomos_decision"]["effect"] == "allow"
    assert allowed["admission"]["status"] == "accepted"
    assert allowed["publish"]["status"] == "published"
    assert allowed["live_counts"] == {"nodes_live": 1, "edges_live": 0}

    quarantined_rc = cli.main(
        [
            "poros",
            "submit",
            "--envelope",
            str(quarantined_envelope),
            "--trust-pins",
            str(quarantined_dir / "trust-pins.json"),
            "--receiver-keys",
            str(quarantined_dir / "receiver.keys.json"),
            "--deployment",
            "default",
            "--dsn",
            clean_runtime,
            "--persist-preflight",
            "--apply",
            "--publish",
            "--nomos-bundle",
            str(nomos_dir / "compiled-policy.json"),
            "--json",
        ]
    )
    assert quarantined_rc == 1
    quarantined = json.loads(capsys.readouterr().out)
    assert quarantined["nomos_decision"]["effect"] == "quarantine"
    assert quarantined["admission"] is None
    assert quarantined["publish"] is None

    allowed_live = _live_node_count(clean_runtime, allowed_node_id)
    quarantined_live = _live_node_count(clean_runtime, quarantined_node_id)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, reason_code
              FROM okg.poros_incorporation_events
             WHERE message_id = 'quarantined-proposal-message'
               AND event_type = 'nomos_decided'
            """
        )
        quarantine_event = cur.fetchone()
        # Chronos cutover: a published generation is a checkpoint with a string
        # id and no okg.graph_generations row; resolve its status from the
        # checkpoint metadata (a bigint lookup crashes on the string id).
        from okg.substrate.graph_store import ChronosGraphStore

        _cp = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).get_generation(allowed["publish"]["generation_id"])
        generation_status = (_cp.metadata or {}).get("status")

    assert allowed_live == 1
    assert quarantined_live == 0
    assert quarantine_event == (
        "quarantine",
        "nomos.demo.profile_hint_cannot_broaden",
    )
    assert generation_status == "published"


@pytest.mark.postgres
def test_poros_submit_can_be_gated_by_institutional_nomos_identity(
    clean_runtime: str,
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    deployment = "institutional-pipeline"
    # The submit publishes through the deployment factory; give this test's
    # synthetic deployment the writer authority and committed directory the
    # factory requires, and the message subtypes its catalog must declare
    # (see `_explicit_factory_publish_authority`).
    _seed_poros_test_subtypes(clean_runtime)
    bind_factory_test_deployment(monkeypatch, tmp_path, deployment)
    scope = f"deployment:{deployment}"
    provider = institutional_provider_fixture(
        "cern-oidc",
        audience="akmon-cms",
        provider_id="cern-sso",
    )
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:poros-institutional-identity",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_cern_operator_context_submit",
                "effect": "allow",
                "priority": 100,
                "subjects": ["subject:cern:operator"],
                "actions": ["poros.graph.submit"],
                "match": {
                    "deployment": deployment,
                    "scope": scope,
                    "identity_provider_id": provider.provider_id,
                    "root_pattern": "context",
                    "graph_profile": "context",
                    "requested_incorporation_mode": "fast_track",
                },
                "reason_code": "nomos.test.cern_operator_submit_allowed",
            }
        ],
    })
    allowed_dir = tmp_path / "allowed-identity"
    denied_dir = tmp_path / "denied-identity"
    allowed_envelope, allowed_node_id = _write_signed_graph_fixture(
        allowed_dir,
        message_id="cern-identity-allowed",
        root_subtype="context_packet",
        graph_profile="context",
    )
    denied_envelope, denied_node_id = _write_signed_graph_fixture(
        denied_dir,
        message_id="cern-identity-denied",
        root_subtype="context_packet",
        graph_profile="context",
    )
    allowed_claim_path = tmp_path / "cern-allowed-claim.json"
    denied_claim_path = tmp_path / "cern-denied-claim.json"
    allowed_claim_path.write_text(
        json.dumps(_cern_oidc_claim(provider.provider_id, provider.issuer)),
        encoding="utf-8",
    )
    denied_claim = _cern_oidc_claim(provider.provider_id, provider.issuer)
    denied_claim["claims"]["aud"] = "wrong-audience"
    denied_claim_path.write_text(json.dumps(denied_claim), encoding="utf-8")

    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            scope=scope,
            activated_by="test:poros-institutional-identity",
        )
        upsert_identity_provider(conn, deployment_name=deployment, provider=provider)
        upsert_subject_binding(
            conn,
            deployment_name=deployment,
            binding=SubjectBinding(
                binding_id="binding:cern:operator",
                provider_id=provider.provider_id,
                issuer=provider.issuer,
                claim_path="roles",
                claim_value="cms-operator",
                subject="subject:cern:operator",
                scope=scope,
                required_assurance=("mfa",),
                priority=10,
            ),
        )
        upsert_permission_grant(
            conn,
            deployment_name=deployment,
            grant=PermissionGrant(
                grant_id="grant:cern:operator-submit",
                subject="subject:cern:operator",
                actions=("poros.graph.submit",),
                scope=scope,
                classifications=("public",),
                priority=10,
            ),
        )
        conn.commit()

    allowed_rc = cli.main([
        "poros",
        "submit",
        "--envelope",
        str(allowed_envelope),
        "--trust-pins",
        str(allowed_dir / "trust-pins.json"),
        "--receiver-keys",
        str(allowed_dir / "receiver.keys.json"),
        "--deployment",
        deployment,
        "--dsn",
        clean_runtime,
        "--persist-preflight",
        "--apply",
        "--publish",
        "--nomos-identity-claim",
        str(allowed_claim_path),
        "--json",
    ])
    assert allowed_rc == 0
    allowed = json.loads(capsys.readouterr().out)
    assert allowed["nomos_decision"]["effect"] == "allow"
    assert allowed["admission"]["status"] == "accepted"
    assert allowed["publish"]["status"] == "published"

    denied_rc = cli.main([
        "poros",
        "submit",
        "--envelope",
        str(denied_envelope),
        "--trust-pins",
        str(denied_dir / "trust-pins.json"),
        "--receiver-keys",
        str(denied_dir / "receiver.keys.json"),
        "--deployment",
        deployment,
        "--dsn",
        clean_runtime,
        "--persist-preflight",
        "--apply",
        "--publish",
        "--nomos-identity-claim",
        str(denied_claim_path),
        "--json",
    ])
    assert denied_rc == 1
    denied = json.loads(capsys.readouterr().out)
    assert denied["nomos_decision"]["effect"] == "deny"
    assert denied["nomos_decision"]["reason_code"] == (
        "nomos.identity.audience_mismatch"
    )
    assert denied["admission"] is None
    assert denied["publish"] is None

    allowed_live = _live_node_count(clean_runtime, allowed_node_id)
    denied_live = _live_node_count(clean_runtime, denied_node_id)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.node_facts WHERE node_id = %s",
            (denied_node_id,),
        )
        denied_facts = int(cur.fetchone()[0] or 0)
        cur.execute(
            """
            SELECT count(*)
              FROM okg.nomos_decisions
             WHERE deployment_name = %s
               AND effect = 'deny'
               AND reason_code = 'nomos.identity.audience_mismatch'
            """,
            (deployment,),
        )
        denied_decisions = int(cur.fetchone()[0] or 0)

    assert allowed_live == 1
    assert denied_facts == 0
    assert denied_live == 0
    assert denied_decisions == 1


@pytest.mark.postgres
@pytest.mark.usefixtures("_explicit_factory_publish_authority")
def test_poros_imported_fact_and_live_rows_keep_full_provenance(
    clean_runtime: str,
    tmp_path: Path,
    capsys,
) -> None:
    from okg.substrate.nomos import run_demo

    poros_dir = tmp_path / "poros"
    nomos_dir = tmp_path / "nomos"
    envelope_path, node_id = _write_signed_graph_fixture(
        poros_dir,
        message_id="provenance-context-message",
        root_subtype="context_packet",
        graph_profile="context",
    )
    run_demo(nomos_dir)
    capsys.readouterr()

    rc = cli.main(
        [
            "poros",
            "submit",
            "--envelope",
            str(envelope_path),
            "--trust-pins",
            str(poros_dir / "trust-pins.json"),
            "--receiver-keys",
            str(poros_dir / "receiver.keys.json"),
            "--deployment",
            "default",
            "--dsn",
            clean_runtime,
            "--persist-preflight",
            "--apply",
            "--publish",
            "--nomos-bundle",
            str(nomos_dir / "compiled-policy.json"),
            "--json",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    decision = payload["nomos_decision"]
    admission_id = payload["admission"]["admission_id"]
    receiver_generation_id = payload["publish"]["generation_id"]

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
        fact_poros, revision_poros, fact_admission_id = cur.fetchone()
        cur.execute(
            """
            SELECT event_type, status, admission_id::text,
                   receiver_generation_id
              FROM okg.poros_incorporation_events
             WHERE message_id = 'provenance-context-message'
               AND event_type IN ('admitted', 'published')
             ORDER BY event_type
            """
        )
        lifecycle_events = cur.fetchall()

    expected_poros = {
        "sender": "okg:lab-a",
        "message_id": "provenance-context-message",
        "body_sha256": payload["body_sha256"],
        "graph_id": "poros:graph:provenance-context-message",
        "signing_key_id": "lab-a-sign-demo",
        "origin_generation_id": 1,
        "nomos_decision_id": decision["decision_id"],
        "policy_bundle_id": decision["policy_bundle_id"],
        "nomos_input_hash": decision["input_hash"],
    }
    for key, value in expected_poros.items():
        assert fact_poros[key] == value
        assert revision_poros[key] == value
    assert fact_admission_id == admission_id
    assert _node_visible_at_generation(
        clean_runtime,
        generation_id=receiver_generation_id,
        node_id=node_id,
    )
    assert lifecycle_events == [
        ("admitted", "accepted", admission_id, None),
        ("published", "published", admission_id, receiver_generation_id),
    ]


@pytest.mark.postgres
@pytest.mark.usefixtures("_explicit_factory_publish_authority")
def test_poros_proof_reports_published_chain(
    clean_runtime: str,
    tmp_path: Path,
    capsys,
) -> None:
    from okg.substrate.nomos import run_demo

    poros_dir = tmp_path / "poros"
    nomos_dir = tmp_path / "nomos"
    envelope_path, node_id = _write_signed_graph_fixture(
        poros_dir,
        message_id="proof-published-message",
        root_subtype="context_packet",
        graph_profile="context",
    )
    run_demo(nomos_dir)
    capsys.readouterr()

    assert cli.main(
        [
            "poros",
            "submit",
            "--envelope",
            str(envelope_path),
            "--trust-pins",
            str(poros_dir / "trust-pins.json"),
            "--receiver-keys",
            str(poros_dir / "receiver.keys.json"),
            "--deployment",
            "default",
            "--dsn",
            clean_runtime,
            "--persist-preflight",
            "--apply",
            "--publish",
            "--nomos-bundle",
            str(nomos_dir / "compiled-policy.json"),
            "--json",
        ]
    ) == 0
    submit_payload = json.loads(capsys.readouterr().out)

    rc = cli.main(
        [
            "poros",
            "proof",
            "--dsn",
            clean_runtime,
            "--message-id",
            "proof-published-message",
            "--json",
        ]
    )

    assert rc == 0
    proof = json.loads(capsys.readouterr().out)
    assert proof["state"] == "published"
    assert proof["graph"]["origin_generation_id"] == 1
    assert proof["imported_nodes"][0]["node_id"] == node_id
    # Chronos cutover: the per-generation visibility window
    # (`valid_from_gen`) was dropped from the proof bundle.
    assert proof["events"][-1]["event_type"] == "published"
    assert proof["lifecycle"]["admission"]["admission_id"] == (
        submit_payload["admission"]["admission_id"]
    )
    assert proof["lifecycle"]["publication"]["receiver_generation_id"] == (
        submit_payload["publish"]["generation_id"]
    )
    assert proof["detached_objects"] == []
    assert proof["body_sha256"] == submit_payload["body_sha256"]
    assert "body" not in proof

    assert cli.main(
        [
            "poros",
            "proof",
            "--dsn",
            clean_runtime,
            "--graph-id",
            "poros:graph:proof-published-message",
            "--json",
        ]
    ) == 0
    by_graph = json.loads(capsys.readouterr().out)
    assert by_graph["message_id"] == "proof-published-message"

    assert cli.main(
        [
            "poros",
            "proof",
            "--dsn",
            clean_runtime,
            "--body-sha256",
            submit_payload["body_sha256"],
            "--json",
        ]
    ) == 0
    by_hash = json.loads(capsys.readouterr().out)
    assert by_hash["message_id"] == "proof-published-message"


@pytest.mark.postgres
def test_poros_proof_reports_rejection_and_redacts_detached_bytes(
    clean_runtime: str,
    tmp_path: Path,
    capsys,
) -> None:
    envelope_path, _ = _write_detached_preflight_fixture(
        tmp_path,
        case="object_hash",
    )
    assert cli.main(
        [
            "poros",
            "submit",
            "--envelope",
            str(envelope_path),
            "--trust-pins",
            str(tmp_path / "trust-pins.json"),
            "--receiver-keys",
            str(tmp_path / "receiver.keys.json"),
            "--dsn",
            clean_runtime,
            "--persist-preflight",
            "--json",
        ]
    ) == 0
    capsys.readouterr()

    rc = cli.main(
        [
            "poros",
            "proof",
            "--dsn",
            clean_runtime,
            "--message-id",
            "detached-object_hash",
            "--json",
        ]
    )

    assert rc == 0
    raw = capsys.readouterr().out
    proof = json.loads(raw)
    assert proof["state"] == "preflight-rejected"
    assert proof["preflight"]["reason_code"] == "object_hash_mismatch"
    assert proof["graph"]["objects"][0]["bytes_redacted"] is True
    assert proof["detached_objects"][0]["manifest_redacted"] is True
    assert proof["detached_objects"][0]["metadata_redacted"] is True
    assert "object_id" not in proof["detached_objects"][0]
    assert proof["lifecycle"]["preflight_rejected"]["reason_code"] == (
        "object_hash_mismatch"
    )
    assert proof["redaction"]["applied"] is True
    assert "inline_bytes_b64url" not in raw
    assert "wrong-object" not in raw
    assert "s3://private-bucket/secret-object" not in raw

    assert cli.main(
        [
            "poros",
            "proof",
            "--dsn",
            clean_runtime,
            "--message-id",
            "detached-object_hash",
            "--mode",
            "audit",
            "--json",
        ]
    ) == 0
    audit = json.loads(capsys.readouterr().out)
    assert "body" not in audit
    assert audit["redaction"]["audit_denied_reason"] == (
        "missing_allowed_nomos_decision"
    )


@pytest.mark.postgres
def test_poros_proof_redacts_restricted_detached_metadata_until_audit_allowed(
    clean_runtime: str,
    tmp_path: Path,
    capsys,
) -> None:
    envelope_path, body_hash = _write_restricted_detached_fixture(tmp_path)
    assert cli.main(
        [
            "poros",
            "submit",
            "--envelope",
            str(envelope_path),
            "--trust-pins",
            str(tmp_path / "trust-pins.json"),
            "--receiver-keys",
            str(tmp_path / "receiver.keys.json"),
            "--dsn",
            clean_runtime,
            "--persist-preflight",
            "--json",
        ]
    ) == 0
    capsys.readouterr()

    assert cli.main(
        [
            "poros",
            "proof",
            "--dsn",
            clean_runtime,
            "--message-id",
            "detached-restricted",
            "--json",
        ]
    ) == 0
    raw_public = capsys.readouterr().out
    public = json.loads(raw_public)
    public_obj = public["graph"]["objects"][0]
    public_manifest = public["detached_objects"][0]
    assert public["redaction"]["applied"] is True
    assert public_obj["metadata_redacted"] is True
    assert public_obj["locations_redacted"] is True
    assert public_obj["bytes_redacted"] is True
    assert public_manifest["metadata_redacted"] is True
    assert public_manifest["locations_redacted"] is True
    assert public_manifest["bytes_redacted"] is True
    assert "object_id" not in public_obj
    assert "object_id" not in public_manifest
    assert "object_sha256" not in public_obj
    assert "object_sha256" not in public_manifest
    assert "media_type" not in public_obj
    assert "size_bytes" not in public_obj
    assert "inline_bytes_b64url" not in raw_public
    assert "s3://private-bucket/restricted-secret" not in raw_public
    assert "obj:restricted-secret" not in raw_public

    assert cli.main(
        [
            "poros",
            "proof",
            "--dsn",
            clean_runtime,
            "--message-id",
            "detached-restricted",
            "--mode",
            "audit",
            "--json",
        ]
    ) == 0
    audit_denied = json.loads(capsys.readouterr().out)
    assert "body" not in audit_denied
    assert audit_denied["graph"]["objects"][0]["metadata_redacted"] is True
    assert audit_denied["redaction"]["audit_denied_reason"] == (
        "missing_allowed_nomos_decision"
    )

    decision_id = "decision:detached-restricted:redact"
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.nomos_decisions (
              deployment_name, decision_id, input_hash, subject, action,
              resource, classification, context, effect, reason_code,
              matched_rule_ids, obligations, enforcement_point, target
            )
            VALUES (
              'default', %s, %s, 'okg:mit', 'poros.proof.audit',
              %s, 'restricted', %s, 'redact',
              'nomos.demo.restricted_export_redacted',
              ARRAY['test']::TEXT[], %s, 'test.poros.proof', %s
            )
            ON CONFLICT (deployment_name, decision_id)
            DO UPDATE SET target = EXCLUDED.target
            """,
            (
                decision_id,
                f"hash:{decision_id}",
                Jsonb({"message_id": "detached-restricted"}),
                Jsonb({"test": "restricted_detached_proof"}),
                Jsonb([{"type": "redaction"}]),
                Jsonb({"message_id": "detached-restricted"}),
            ),
        )
        cur.execute(
            """
            INSERT INTO okg.poros_incorporation_events (
                receiver_authority_id, sender_authority_id, message_id,
                body_sha256, event_type, status, reason_code,
                nomos_decision_id, policy_bundle_id, nomos_input_hash,
                payload
            )
            VALUES (
                'okg:mit', 'okg:lab-a', 'detached-restricted',
                %s, 'nomos_decided', 'redact',
                'nomos.demo.restricted_export_redacted',
                %s,
                'nomos:demo:baseline', %s, %s
            )
            """,
            (
                body_hash,
                decision_id,
                sha256_b64url(body_hash.encode("utf-8")),
                Jsonb(
                    {
                        "classification": "restricted",
                        "obligations": [{"type": "redaction"}],
                    }
                ),
            ),
        )

    assert cli.main(
        [
            "poros",
            "proof",
            "--dsn",
            clean_runtime,
            "--message-id",
            "detached-restricted",
            "--mode",
            "audit",
            "--json",
        ]
    ) == 0
    raw_audit = capsys.readouterr().out
    audit_allowed = json.loads(raw_audit)
    audit_obj = audit_allowed["graph"]["objects"][0]
    audit_manifest = audit_allowed["detached_objects"][0]
    assert "audit_denied_reason" not in audit_allowed["redaction"]
    assert audit_allowed["body"]["objects"][0]["object_id"] == (
        "obj:restricted-secret"
    )
    assert audit_obj["object_id"] == "obj:restricted-secret"
    assert audit_manifest["object_id"] == "obj:restricted-secret"
    assert audit_obj["object_sha256"] == audit_allowed["body"]["objects"][0][
        "cleartext_sha256"
    ]
    assert audit_manifest["object_sha256"] == audit_allowed["body"]["objects"][0][
        "cleartext_sha256"
    ]
    assert "s3://private-bucket/restricted-secret" in raw_audit


@pytest.mark.postgres
@pytest.mark.usefixtures("_explicit_factory_publish_authority")
def test_poros_proof_reports_intermediate_states_distinctly(
    clean_runtime: str,
    tmp_path: Path,
    capsys,
) -> None:
    from okg.substrate.nomos import run_demo

    nomos_dir = tmp_path / "nomos"
    run_demo(nomos_dir)
    capsys.readouterr()

    def submit_graph(
        name: str,
        *,
        root_subtype: str = "context_packet",
        apply: bool = False,
        publish: bool = False,
        admission_mode: str | None = None,
        expected_rc: int = 0,
    ) -> None:
        poros_dir = tmp_path / name
        envelope_path, _ = _write_signed_graph_fixture(
            poros_dir,
            message_id=f"{name}-message",
            root_subtype=root_subtype,
            graph_profile="context",
        )
        cmd = [
            "poros",
            "submit",
            "--envelope",
            str(envelope_path),
            "--trust-pins",
            str(poros_dir / "trust-pins.json"),
            "--receiver-keys",
            str(poros_dir / "receiver.keys.json"),
            "--deployment",
            "default",
            "--dsn",
            clean_runtime,
            "--persist-preflight",
            "--nomos-bundle",
            str(nomos_dir / "compiled-policy.json"),
            "--json",
        ]
        if apply:
            cmd.append("--apply")
        if publish:
            cmd.append("--publish")
        if admission_mode is not None:
            cmd.extend(["--admission-mode", admission_mode])
        assert cli.main(cmd) == expected_rc
        capsys.readouterr()

    rejected_dir = tmp_path / "rejected"
    rejected_dir.mkdir(parents=True, exist_ok=True)
    rejected_envelope, _, _ = _write_preflight_fixture(
        rejected_dir,
        case="tampered",
    )
    assert cli.main(
        [
            "poros",
            "submit",
            "--envelope",
            str(rejected_envelope),
            "--trust-pins",
            str(rejected_dir / "trust-pins.json"),
            "--receiver-keys",
            str(rejected_dir / "receiver.keys.json"),
            "--deployment",
            "default",
            "--dsn",
            clean_runtime,
            "--persist-preflight",
            "--json",
        ]
    ) == 0
    capsys.readouterr()
    submit_graph("duplicate")
    duplicate_dir = tmp_path / "duplicate"
    assert cli.main(
        [
            "poros",
            "submit",
            "--envelope",
            str(duplicate_dir / "envelope.wire.json"),
            "--trust-pins",
            str(duplicate_dir / "trust-pins.json"),
            "--receiver-keys",
            str(duplicate_dir / "receiver.keys.json"),
            "--deployment",
            "default",
            "--dsn",
            clean_runtime,
            "--persist-preflight",
            "--apply",
            "--nomos-bundle",
            str(nomos_dir / "compiled-policy.json"),
            "--json",
        ]
    ) == 1
    capsys.readouterr()
    submit_graph("review", apply=True, publish=True, admission_mode="review")
    submit_graph(
        "quarantine",
        root_subtype="proposal",
        apply=True,
        publish=True,
        expected_rc=1,
    )
    submit_graph("admitted", apply=True, publish=False)
    submit_graph("published", apply=True, publish=True)

    expected = {
        "bad-preflight-tampered": "preflight-rejected",
        "duplicate-message": "duplicate",
        "review-message": "review",
        "quarantine-message": "quarantined",
        "admitted-message": "admitted-unpublished",
        "published-message": "published",
    }
    for message_id, state in expected.items():
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
        assert proof["state"] == state
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
        status = json.loads(capsys.readouterr().out)
        status_row = status["messages"][0]
        assert status_row["state"] == proof["state"]
        assert status_row["event_types"] == [
            event["event_type"] for event in proof["events"]
        ]
        with (
            psycopg.connect(clean_runtime, autocommit=True) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(
                """
                SELECT status, reason_code, body_sha256
                  FROM okg.poros_inbox_messages
                 WHERE message_id = %s
                """,
                (message_id,),
            )
            inbox_row = cur.fetchone()
        assert status_row["preflight_status"] == inbox_row[0]
        assert status_row["reason_code"] == inbox_row[1]
        assert status_row["body_sha256"] == inbox_row[2]


@pytest.mark.postgres
def test_review_mode_records_audit_without_fact_or_live_rows(
    clean_runtime: str,
    tmp_path: Path,
    capsys,
) -> None:
    from okg.substrate.nomos import run_demo

    poros_dir = tmp_path / "review"
    nomos_dir = tmp_path / "nomos"
    envelope_path, node_id = _write_signed_graph_fixture(
        poros_dir,
        message_id="review-context-message",
        root_subtype="context_packet",
        graph_profile="context",
    )
    run_demo(nomos_dir)
    capsys.readouterr()

    rc = cli.main(
        [
            "poros",
            "submit",
            "--envelope",
            str(envelope_path),
            "--trust-pins",
            str(poros_dir / "trust-pins.json"),
            "--receiver-keys",
            str(poros_dir / "receiver.keys.json"),
            "--deployment",
            "default",
            "--dsn",
            clean_runtime,
            "--persist-preflight",
            "--apply",
            "--publish",
            "--admission-mode",
            "review",
            "--nomos-bundle",
            str(nomos_dir / "compiled-policy.json"),
            "--json",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["nomos_decision"]["effect"] == "allow"
    assert payload["admission"]["status"] == "pending"
    assert payload["publish"] is None

    nodes_live = _live_node_count(clean_runtime, node_id)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.node_facts WHERE node_id = %s", (node_id,))
        node_facts = int(cur.fetchone()[0] or 0)
        cur.execute(
            """
            SELECT event_type, status, admission_id::text
              FROM okg.poros_incorporation_events
             WHERE message_id = 'review-context-message'
               AND event_type = 'review'
            """
        )
        event = cur.fetchone()

    assert node_facts == 0
    assert nodes_live == 0
    assert event == (
        "review",
        "pending",
        payload["admission"]["admission_id"],
    )


@pytest.mark.postgres
def test_poros_outbox_export_persists_signed_record(
    clean_runtime: str,
    tmp_path: Path,
    capsys,
) -> None:
    node_id = "poros:test:export-root"
    run_id = str(uuid.uuid4())
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts (
                node_id, subtype, attrs, source, source_record_id,
                source_revision, observed_at, dedupe_key, run_id, op
            )
            VALUES (
                %s, 'concept', %s, 'poros_test', %s, %s,
                now(), %s, %s::uuid, 'I'
            )
            """,
            (
                node_id,
                Jsonb({"text": "Poros export test root"}),
                Jsonb({"id": node_id}),
                Jsonb({"v": 1}),
                f"poros-test:{run_id}:{node_id}",
                run_id,
            ),
        )
    _seed_chronos_node(
        clean_runtime,
        node_id,
        "concept",
        {"text": "Poros export test root"},
    )
    publish = Publisher(clean_runtime, "poros-sender", catalog_identity=bind_test_catalog_writer(clean_runtime, "poros-sender")).cycle()
    assert publish["status"] == "published", publish

    keypair = generate_ed25519_keypair("lab-a-sign-1")
    keys_path = tmp_path / "sender.keys.json"
    keys_path.write_text(
        json.dumps(keypair.__dict__, sort_keys=True),
        encoding="utf-8",
    )
    envelope_path = tmp_path / "envelope.wire.json"

    rc = cli.main(
        [
            "poros",
            "outbox-export",
            "--dsn",
            clean_runtime,
            "--sender",
            "okg:lab-a",
            "--recipient",
            "okg:mit",
            "--sender-generation-id",
            str(publish["generation_id"]),
            "--graph-id",
            "poros:graph:test-export",
            "--root-node-id",
            node_id,
            "--signing-keys",
            str(keys_path),
            "--message-id",
            "outbox-export-message",
            "--envelope-out",
            str(envelope_path),
            "--json",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["message_id"] == "outbox-export-message"
    assert payload["sender_authority_id"] == "okg:lab-a"
    assert payload["recipient_authority_id"] == "okg:mit"
    assert payload["sender_generation_id"] == publish["generation_id"]
    assert payload["graph_id"] == "poros:graph:test-export"
    assert payload["signing_key_id"] == "lab-a-sign-1"
    assert payload["node_count"] == 1
    assert payload["edge_count"] == 0

    envelope = json.loads(envelope_path.read_text(encoding="utf-8"))
    verify_envelope(envelope, keypair.public_key)
    assert envelope["body_sha256"] == payload["body_sha256"]
    assert envelope["body"]["origin_generation_id"] == publish["generation_id"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT sender_authority_id, sender_generation_id,
                   recipient_authority_id, message_id, graph_id,
                   signing_key_id, body_sha256, node_count, edge_count
              FROM okg.poros_outbox_messages
             WHERE message_id = 'outbox-export-message'
            """
        )
        row = cur.fetchone()
    assert row == (
        "okg:lab-a",
        publish["generation_id"],
        "okg:mit",
        "outbox-export-message",
        "poros:graph:test-export",
        "lab-a-sign-1",
        payload["body_sha256"],
        1,
        0,
    )


@pytest.mark.postgres
def test_poros_outbox_export_writes_detached_object_manifest(
    clean_runtime: str,
    tmp_path: Path,
    capsys,
) -> None:
    node_id = "poros:test:detached-export-root"
    run_id = str(uuid.uuid4())
    large_text = "Poros detached export payload " + ("x" * (120 * 1024))
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts (
                node_id, subtype, attrs, source, source_record_id,
                source_revision, observed_at, dedupe_key, run_id, op
            )
            VALUES (
                %s, 'context_packet', %s, 'poros_detached_export_test',
                %s, %s, now(), %s, %s::uuid, 'I'
            )
            """,
            (
                node_id,
                Jsonb({"text": large_text}),
                Jsonb({"id": node_id}),
                Jsonb({"v": 1}),
                f"poros-detached-export:{run_id}:{node_id}",
                run_id,
            ),
        )
    _seed_chronos_node(
        clean_runtime,
        node_id,
        "context_packet",
        {"text": large_text},
    )
    publish = Publisher(clean_runtime, "poros-sender", catalog_identity=bind_test_catalog_writer(clean_runtime, "poros-sender")).cycle()
    assert publish["status"] == "published", publish

    keypair = generate_ed25519_keypair("lab-a-sign-detached-export")
    keys_path = tmp_path / "sender.keys.json"
    keys_path.write_text(
        json.dumps(keypair.__dict__, sort_keys=True),
        encoding="utf-8",
    )
    envelope_path = tmp_path / "detached-envelope.wire.json"
    object_store_root = tmp_path / "objects"

    rc = cli.main(
        [
            "poros",
            "outbox-export",
            "--dsn",
            clean_runtime,
            "--sender",
            "okg:lab-a",
            "--recipient",
            "okg:mit",
            "--sender-generation-id",
            str(publish["generation_id"]),
            "--graph-id",
            "poros:graph:detached-export",
            "--root-node-id",
            node_id,
            "--signing-keys",
            str(keys_path),
            "--message-id",
            "detached-outbox-export-message",
            "--detach",
            "--object-store-root",
            str(object_store_root),
            "--detached-chunk-size-bytes",
            "4096",
            "--envelope-out",
            str(envelope_path),
            "--json",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["message_id"] == "detached-outbox-export-message"
    assert payload["payload_mode"] == "detached"
    assert payload["detached_object_count"] == 1

    envelope = json.loads(envelope_path.read_text(encoding="utf-8"))
    verify_envelope(envelope, keypair.public_key)
    assert envelope["payload_mode"] == "detached"
    assert envelope["body"]["payload_mode"] == "detached"
    assert "nodes" not in envelope["body"]
    obj = envelope["body"]["objects"][0]
    assert obj["object_id"] == "detached-outbox-export-message:graph-body"
    object_store = LocalPorosObjectStore(object_store_root)
    detached_bytes = object_store.fetch_bytes(obj)
    validate_detached_object_bytes(obj, detached_bytes)
    detached_body = json.loads(detached_bytes)
    assert detached_body["nodes"][0]["attrs"]["text"] == large_text
    assert envelope["body_sha256"] == payload["body_sha256"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT payload_mode, body, envelope
              FROM okg.poros_outbox_messages
             WHERE message_id = 'detached-outbox-export-message'
            """
        )
        outbox_payload_mode, outbox_body, outbox_envelope = cur.fetchone()
        cur.execute(
            """
            SELECT side, authority_id, object_id, object_sha256,
                   size_bytes, chunk_count, classification
              FROM okg.poros_detached_object_manifests
             WHERE message_id = 'detached-outbox-export-message'
            """
        )
        manifest_row = cur.fetchone()
    assert outbox_payload_mode == "detached"
    assert outbox_body["objects"][0]["cleartext_sha256"] == obj["cleartext_sha256"]
    assert outbox_envelope["body_sha256"] == payload["body_sha256"]
    assert manifest_row == (
        "sender",
        "okg:lab-a",
        obj["object_id"],
        obj["cleartext_sha256"],
        len(detached_bytes),
        len(obj["chunks"]),
        "public",
    )


@pytest.mark.postgres
@pytest.mark.usefixtures("_explicit_factory_publish_authority")
def test_poros_submit_apply_publish(
    clean_runtime: str,
    tmp_path: Path,
    capsys,
) -> None:
    _write_demo(tmp_path)
    capsys.readouterr()

    rc = cli.main(
        [
            "poros",
            "submit",
            "--envelope",
            str(tmp_path / "envelope.wire.json"),
            "--trust-pins",
            str(tmp_path / "trust-pins.json"),
            "--receiver-keys",
            str(tmp_path / "receiver.keys.json"),
            "--deployment",
            "default",
            "--dsn",
            clean_runtime,
            "--apply",
            "--publish",
            "--json",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["admission"]["status"] == "accepted"
    assert payload["publish"]["status"] == "published"
    assert payload["live_counts"]["nodes_live"] == 1
