from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

import psycopg
import pytest

from okg.substrate.cli.federation import cmd_federation, pull_mailbox_messages
from okg.substrate.cli.main import _build_parser
from okg.substrate.exchange import (
    build_unsigned_envelope,
    generate_ed25519_keypair,
    sign_envelope,
)
from okg.substrate.exchange.canonical_json import canonical_json_bytes
from okg.substrate.exchange.envelope import format_timestamp
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.federation.mailbox import (
    FileObjectStore,
    InMemoryMailboxStore,
    MailboxPollingClient,
    MailboxService,
    build_mailbox_app,
)
from okg.substrate.federation.mailbox.auth import static_token_authenticator
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

    The mailbox CLI's receiver builds its publisher through ``build_deployment_publisher``, which
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


class FakeMailboxClient:
    def __init__(self, service: MailboxService) -> None:
        self.service = service

    def poll(
        self,
        recipient_authority_id: str,
        *,
        limit: int = 10,
        lease_seconds: int = 60,
    ) -> list[dict[str, Any]]:
        return [
            record.to_dict(include_payload=True)
            for record in self.service.poll(
                recipient_authority_id,
                limit=limit,
                lease_seconds=lease_seconds,
            )
        ]

    def ack(
        self,
        message_id: str,
        *,
        lease_token: str,
        receipt_evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        return self.service.ack_message(
            message_id,
            lease_token=lease_token,
            receipt_evidence=receipt_evidence,
        ).to_dict()

    def deny(
        self,
        message_id: str,
        *,
        lease_token: str,
        reason_code: str,
        denial_evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        return self.service.deny_message(
            message_id,
            lease_token=lease_token,
            reason_code=reason_code,
            denial_evidence=denial_evidence,
        ).to_dict()


@pytest.mark.usefixtures("_explicit_factory_publish_authority")
def test_federation_mailbox_demo_cli_emits_evidence(
    clean_runtime: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output = tmp_path / "evidence.json"
    args = _build_parser().parse_args(
        [
            "federation",
            "--json",
            "mailbox",
            "demo",
            "--dsn",
            clean_runtime,
            "--policy-dir",
            str(tmp_path / "policy"),
            "--output",
            str(output),
        ]
    )

    assert cmd_federation(args, as_json=True) == 0

    payload = json.loads(capsys.readouterr().out)
    evidence = json.loads(output.read_text(encoding="utf-8"))
    assert payload["evidence_path"] == str(output)
    assert evidence["accepted"]["mailbox"]["status"] == "acknowledged"
    assert evidence["accepted"]["live_counts"] == {"edges_live": 0, "nodes_live": 1}
    assert evidence["denied"]["mailbox"]["status"] == "denied"
    assert evidence["denied"]["nomos_decision"]["effect"] == "deny"
    assert evidence["after"]["denied_live_count"] == 0


@pytest.mark.usefixtures("_explicit_factory_publish_authority")
def test_federation_mailbox_pull_acknowledges_allowed_and_denies_policy_blocked(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    sender_keys = generate_ed25519_keypair("lab-a-mailbox-cli-sign")
    receiver_keys = generate_ed25519_keypair("mit-mailbox-cli-sign")
    accepted = _signed_envelope(
        sender_keys,
        message_id="mailbox-cli-accepted",
        classification="public",
    )
    denied = _signed_envelope(
        sender_keys,
        message_id="mailbox-cli-denied",
        classification="restricted",
    )
    accepted_node_id = str(accepted["body"]["root_node_ids"][0])
    denied_node_id = str(denied["body"]["root_node_ids"][0])

    service = MailboxService(store=InMemoryMailboxStore())
    service.submit_envelope(accepted)
    service.submit_envelope(denied)

    result = pull_mailbox_messages(
        client=FakeMailboxClient(service),
        receiver=RECEIVER_AUTHORITY,
        trust_pins=_trust_pins(sender_keys),
        receiver_keys=receiver_keys.__dict__,
        dsn=clean_runtime,
        deployment="default",
        nomos_bundle=_write_allow_public_context_policy(tmp_path / "nomos"),
        admission_mode="fast_track",
        limit=10,
        lease_seconds=120,
        apply=True,
        publish=True,
    )

    assert result["polled"] == 2
    assert result["acknowledged"] == 1
    assert result["denied"] == 1
    assert result["failed"] == 0
    by_message = {item["message_id"]: item for item in result["messages"]}
    assert by_message["mailbox-cli-accepted"]["mailbox_action"] == "acknowledged"
    assert by_message["mailbox-cli-accepted"]["publish_status"] == "published"
    assert by_message["mailbox-cli-accepted"]["live_counts"] == {
        "edges_live": 0,
        "nodes_live": 1,
    }
    assert by_message["mailbox-cli-denied"]["mailbox_action"] == "denied"
    assert by_message["mailbox-cli-denied"]["nomos_effect"] == "deny"
    accepted_record = service.get_message("mailbox-cli-accepted")
    denied_record = service.get_message("mailbox-cli-denied")
    assert accepted_record is not None
    assert denied_record is not None
    assert accepted_record.acknowledged
    assert denied_record.denied
    assert _live_count(clean_runtime, accepted_node_id) == 1
    assert _live_count(clean_runtime, denied_node_id) == 0


def test_federation_mailbox_pull_denies_detached_pointer_without_fetcher() -> None:
    service = MailboxService(store=InMemoryMailboxStore())
    service.submit_pointer(
        {
            "message_id": "mailbox-cli-detached",
            "sender_authority_id": SENDER_AUTHORITY,
            "recipient_authority_id": RECEIVER_AUTHORITY,
            "object_uri": "memory://mailbox/detached",
            "object_sha256": "sha256:not-fetched",
            "size_bytes": 12,
            "expires_at": format_timestamp(datetime.now(UTC) + timedelta(minutes=5)),
        }
    )

    result = pull_mailbox_messages(
        client=FakeMailboxClient(service),
        receiver=RECEIVER_AUTHORITY,
        trust_pins={"pins": []},
        receiver_keys={"kid": "unused", "private_key": "unused"},
        dsn="postgres://unused",
        deployment="default",
        nomos_bundle=None,
        admission_mode="fast_track",
        limit=10,
        lease_seconds=120,
        apply=False,
        publish=False,
    )

    assert result["polled"] == 1
    assert result["denied"] == 1
    assert result["failed"] == 0
    message = service.get_message("mailbox-cli-detached")
    assert message is not None
    assert message.denied
    assert message.deny_reason_code == "mailbox_detached_object_fetch_not_configured"


@pytest.mark.usefixtures("_explicit_factory_publish_authority")
def test_federation_mailbox_pull_admits_detached_pointer_with_file_object_store(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    sender_keys = generate_ed25519_keypair("lab-a-detached-sign")
    receiver_keys = generate_ed25519_keypair("mit-detached-sign")
    envelope = _signed_envelope(
        sender_keys,
        message_id="mailbox-cli-detached-accepted",
        classification="public",
    )
    node_id = str(envelope["body"]["root_node_ids"][0])
    object_store_root = tmp_path / "objects"
    stored = FileObjectStore(object_store_root).put_bytes(
        "mailbox-cli-detached-accepted",
        canonical_json_bytes(envelope),
        metadata={"message_id": "mailbox-cli-detached-accepted"},
    )

    service = MailboxService(store=InMemoryMailboxStore())
    service.submit_pointer(
        {
            "message_id": "mailbox-cli-detached-accepted",
            "sender_authority_id": SENDER_AUTHORITY,
            "recipient_authority_id": RECEIVER_AUTHORITY,
            "object_uri": stored.uri,
            "object_sha256": "sha256:" + stored.sha256,
            "size_bytes": stored.size_bytes,
            "expires_at": format_timestamp(datetime.now(UTC) + timedelta(minutes=5)),
            "metadata": stored.metadata,
        }
    )

    result = pull_mailbox_messages(
        client=FakeMailboxClient(service),
        receiver=RECEIVER_AUTHORITY,
        trust_pins=_trust_pins(sender_keys),
        receiver_keys=receiver_keys.__dict__,
        dsn=clean_runtime,
        deployment="default",
        nomos_bundle=_write_allow_public_context_policy(tmp_path / "nomos"),
        admission_mode="fast_track",
        limit=10,
        lease_seconds=120,
        apply=True,
        publish=True,
        object_store_root=object_store_root,
    )

    assert result["polled"] == 1
    assert result["acknowledged"] == 1
    assert result["failed"] == 0
    message = service.get_message("mailbox-cli-detached-accepted")
    assert message is not None
    assert message.acknowledged
    assert result["messages"][0]["publish_status"] == "published"
    assert result["messages"][0]["live_counts"] == {
        "edges_live": 0,
        "nodes_live": 1,
    }
    assert _live_count(clean_runtime, node_id) == 1


@pytest.mark.usefixtures("_explicit_factory_publish_authority")
def test_federation_mailbox_pull_fetches_mailbox_owned_local_object(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    sender_keys = generate_ed25519_keypair("lab-a-mailbox-owned-object")
    receiver_keys = generate_ed25519_keypair("mit-mailbox-owned-object")
    envelope = _signed_envelope(
        sender_keys,
        message_id="mailbox-cli-owned-object",
        classification="public",
    )
    node_id = str(envelope["body"]["root_node_ids"][0])
    service = MailboxService(
        store=InMemoryMailboxStore(),
        object_store=FileObjectStore(tmp_path / "mailbox-objects"),
    )
    client = MailboxPollingClient(
        "http://testserver",
        client=TestClient(build_mailbox_app(
            service,
            authenticator=static_token_authenticator({RECEIVER_AUTHORITY: "tok"}),
        )),
        principal=RECEIVER_AUTHORITY,
        token="tok",
    )
    client.submit_object(
        message_id="mailbox-cli-owned-object",
        data=canonical_json_bytes(envelope),
        sender_authority_id=SENDER_AUTHORITY,
        recipient_authority_id=RECEIVER_AUTHORITY,
        expires_at=format_timestamp(datetime.now(UTC) + timedelta(minutes=5)),
        metadata={"kind": "mailbox-owned-local-object"},
    )

    result = pull_mailbox_messages(
        client=client,
        receiver=RECEIVER_AUTHORITY,
        trust_pins=_trust_pins(sender_keys),
        receiver_keys=receiver_keys.__dict__,
        dsn=clean_runtime,
        deployment="default",
        nomos_bundle=_write_allow_public_context_policy(tmp_path / "nomos"),
        admission_mode="fast_track",
        limit=10,
        lease_seconds=120,
        apply=True,
        publish=True,
    )

    assert result["polled"] == 1
    assert result["acknowledged"] == 1
    assert result["failed"] == 0
    assert result["messages"][0]["mailbox_action"] == "acknowledged"
    assert result["messages"][0]["publish_status"] == "published"
    assert result["messages"][0]["live_counts"] == {
        "edges_live": 0,
        "nodes_live": 1,
    }
    assert _live_count(clean_runtime, node_id) == 1


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
    node_id = f"poros:mailbox-cli:{message_id}"
    return {
        "graph_id": f"poros:graph:mailbox-cli:{message_id}",
        "origin_authority_id": SENDER_AUTHORITY,
        "origin_generation_id": 101,
        "classification": classification,
        "graph_profile": "context",
        "root_node_ids": [node_id],
        "nodes": [
            {
                "node_id": node_id,
                "subtype": "context_packet",
                "attrs": {"text": "Mailbox CLI UX test context packet."},
            }
        ],
        "edges": [],
    }


def _trust_pins(sender_keys: Any) -> dict[str, Any]:
    return {
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


def _write_allow_public_context_policy(out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    policy_graph = {
        "policy_bundle_id": "nomos:test:mailbox-cli-ux",
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
                "reason_code": "nomos.test.mailbox_cli.public_context_allowed",
            }
        ],
    }
    path = out_dir / "compiled-policy.json"
    path.write_text(
        json.dumps(
            compile_policy_graph(policy_graph).to_dict(),
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


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
