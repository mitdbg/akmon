from __future__ import annotations

from typing import Any

import psycopg
import pytest
from psycopg.types.json import Jsonb

from okg.substrate.exchange import generate_ed25519_keypair
from okg.substrate.exchange.sender_delivery import deliver_next_outbox_messages
from okg.substrate.exchange.outbox import (
    PorosOutboxError,
    export_published_subgraph_to_outbox,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


SUBJECT = "okg:lab-a"


def test_poros_outbox_nomos_read_denial_blocks_before_signing(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = _publish_fixture_graph(clean_runtime)
    _activate_policy(clean_runtime, rules=[])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setattr(
        "okg.substrate.exchange.outbox.sign_envelope",
        _forbidden_sign_envelope,
    )

    with pytest.raises(
        PorosOutboxError,
        match="Nomos denied graph.read.export_like",
    ):
        _export(clean_runtime, generation_id=generation_id)

    assert _outbox_count(clean_runtime) == 0
    assert _decision_rows(clean_runtime) == [
        ("graph.read.export_like", "deny", "poros_export_read"),
    ]


def test_poros_outbox_source_lineage_denial_hides_root_before_export(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = _publish_fixture_graph(clean_runtime)
    _activate_policy(
        clean_runtime,
        rules=[
            _allow_rule(
                "allow_export_read_boundary",
                "graph.read.export_like",
                match={"use": "poros.export_read"},
            ),
            _allow_rule(
                "allow_public_docs_source_read",
                "graph.read.export_like",
                match={"source_id": "public_docs"},
            ),
            _allow_rule("allow_export", "graph.export"),
            _allow_rule("allow_secret_use", "secret.use"),
        ],
    )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setattr(
        "okg.substrate.exchange.outbox.sign_envelope",
        _forbidden_sign_envelope,
    )

    with pytest.raises(PorosOutboxError, match="root nodes are not live"):
        _export(clean_runtime, generation_id=generation_id)

    assert _outbox_count(clean_runtime) == 0
    assert _decision_rows(clean_runtime) == [
        ("graph.read.export_like", "allow", "poros_export_read"),
        ("graph.read.export_like", "deny", "poros_export_node_source_read"),
    ]


def test_poros_outbox_nomos_secret_use_denial_blocks_before_signing(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = _publish_fixture_graph(clean_runtime)
    _activate_policy(
        clean_runtime,
        rules=[
            _allow_rule("allow_export_read", "graph.read.export_like"),
            _allow_rule("allow_export", "graph.export"),
        ],
    )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setattr(
        "okg.substrate.exchange.outbox.sign_envelope",
        _forbidden_sign_envelope,
    )

    with pytest.raises(PorosOutboxError, match="Nomos denied secret.use"):
        _export(clean_runtime, generation_id=generation_id)

    assert _outbox_count(clean_runtime) == 0
    assert _decision_rows(clean_runtime) == [
        ("graph.read.export_like", "allow", "poros_export_read"),
        ("graph.read.export_like", "allow", "poros_export_node_source_read"),
        # Fine-grained access control also decides each exported row as a
        # graph object and as each contributing source record.
        ("graph.read.export_like", "allow", "poros_export_node_graph_object_read"),
        ("graph.read.export_like", "allow", "poros_export_node_source_record_read"),
        ("graph.export", "allow", "poros_graph_export"),
        ("secret.use", "deny", "poros_signing_key_use"),
    ]


def test_poros_cli_outbox_export_nomos_secret_denial_blocks_key_file_read(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    generation_id = _publish_fixture_graph(clean_runtime)
    _activate_policy(clean_runtime, rules=[])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    missing_key_file = tmp_path / "missing-signing-key.json"

    from okg.substrate.cli.poros import _cmd_outbox_export

    exit_code = _cmd_outbox_export(
        _Args(
            dsn=clean_runtime,
            sender=SUBJECT,
            recipient="okg:mit-demo",
            sender_generation_id=generation_id,
            graph_id="poros:graph:nomos",
            root_node_id=["node:poros-nomos"],
            signing_keys=str(missing_key_file),
            classification="public",
            graph_profile="context",
            message_id=None,
            ttl_seconds=900,
            object_store_root=None,
            detach=False,
            detached_chunk_size_bytes=64 * 1024,
            envelope_out=None,
        ),
        as_json=True,
    )

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "Nomos denied secret.use" in output
    assert str(missing_key_file) not in output
    assert _decision_rows(clean_runtime) == [
        ("secret.use", "deny", "poros_signing_key_use"),
    ]


def test_poros_outbox_nomos_redaction_is_carried_into_envelope(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = _publish_fixture_graph(clean_runtime)
    _activate_policy(
        clean_runtime,
        rules=[
            _allow_rule("allow_export_read", "graph.read.export_like"),
            {
                "rule_id": "redact_restricted_export",
                "effect": "redact",
                "actions": ["graph.export"],
                "subjects": [SUBJECT],
                "priority": 100,
                "reason_code": "nomos.test.redact_export",
                "obligations": [{"type": "redaction"}, {"type": "audit"}],
            },
            _allow_rule("allow_secret_use", "secret.use"),
        ],
    )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    result = _export(
        clean_runtime,
        generation_id=generation_id,
        classification="restricted",
    )

    body = result.envelope["body"]
    assert body["nodes"][0]["attrs"]["title"] == "visible"
    assert body["nodes"][0]["attrs"]["nomos_redacted"] is True
    assert "text" not in body["nodes"][0]["attrs"]
    assert "private_body" not in body["nodes"][0]["attrs"]
    assert "secret body" not in str(body)
    assert _decision_rows(clean_runtime) == [
        ("graph.read.export_like", "allow", "poros_export_read"),
        ("graph.read.export_like", "allow", "poros_export_node_source_read"),
        # Fine-grained access control also decides each exported row as a
        # graph object and as each contributing source record.
        ("graph.read.export_like", "allow", "poros_export_node_graph_object_read"),
        ("graph.read.export_like", "allow", "poros_export_node_source_record_read"),
        ("graph.export", "redact", "poros_graph_export"),
        ("secret.use", "allow", "poros_signing_key_use"),
    ]


def test_poros_delivery_nomos_denial_blocks_http_post(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = _publish_fixture_graph(clean_runtime)
    _activate_policy(
        clean_runtime,
        rules=[
            _allow_rule("allow_export_read", "graph.read.export_like"),
            _allow_rule(
                "allow_export_create",
                "graph.export",
                match={"use": "poros.export"},
            ),
            _allow_rule("allow_secret_use", "secret.use"),
        ],
    )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setattr(
        "requests.post",
        lambda *_args, **_kwargs: (
            pytest.fail("Poros delivery must not POST after Nomos denial")
        ),
    )
    export = _export(clean_runtime, generation_id=generation_id)

    results = deliver_next_outbox_messages(
        dsn=clean_runtime,
        receiver_inbox_url="https://example.com/poros/v1/inbox",
        receiver_public_key=generate_ed25519_keypair("receiver").public_key,
        message_id=export.message_id,
    )

    assert len(results) == 1
    assert results[0].status == "failed"
    assert results[0].reason_code == "nomos_denied"
    assert results[0].outbox_status == "terminal_failed"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, terminal_failure_reason, next_retry_at
              FROM okg.poros_outbox_messages
             WHERE message_id = %s
            """,
            (export.message_id,),
        )
        outbox_status, terminal_reason, next_retry_at = cur.fetchone()
    assert outbox_status == "terminal_failed"
    assert terminal_reason == "nomos_denied"
    assert next_retry_at is None
    assert _decision_rows(clean_runtime) == [
        ("graph.read.export_like", "allow", "poros_export_read"),
        ("graph.read.export_like", "allow", "poros_export_node_source_read"),
        # Fine-grained access control also decides each exported row as a
        # graph object and as each contributing source record.
        ("graph.read.export_like", "allow", "poros_export_node_graph_object_read"),
        ("graph.read.export_like", "allow", "poros_export_node_source_record_read"),
        ("graph.export", "allow", "poros_graph_export"),
        ("secret.use", "allow", "poros_signing_key_use"),
        ("graph.export", "deny", "poros_delivery"),
    ]


def test_poros_proof_nomos_denial_blocks_proof_assembly(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = _publish_fixture_graph(clean_runtime)
    _activate_policy(clean_runtime, rules=[])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    _seed_inbox_proof_fixture(clean_runtime, receiver_generation_id=generation_id)

    from okg.substrate.cli import poros as poros_cli

    monkeypatch.setattr(
        poros_cli,
        "_load_proof_nodes",
        lambda *_args, **_kwargs: (
            pytest.fail("proof nodes must not load after Nomos denial")
        ),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(ValueError, match="Nomos denied graph.read.export_like"):
            poros_cli._load_poros_proof(
                conn,
                message_id="proof-denied-message",
                graph_id=None,
                body_sha256=None,
                mode="audit",
            )

    assert _decision_rows(clean_runtime) == [
        ("graph.read.export_like", "deny", "poros_proof"),
    ]


def _publish_fixture_graph(dsn: str) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    node_id="node:poros-nomos",
                    subtype="page",
                    attrs={
                        "classification": "restricted",
                        "title": "visible",
                        "text": "secret body",
                        "private_body": "hidden body",
                    },
                    source_record_id={"id": "node:poros-nomos"},
                    source_revision={"v": 1},
                )
            ],
            source="poros_fixture",
        )
    result = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert result["status"] == "published"
    return str(result["generation_id"])


def _activate_policy(dsn: str, *, rules: list[dict[str, Any]]) -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:poros-outbox",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": rules or [_unmatched_allow_rule()],
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name="default",
            bundle=bundle,
            scope="deployment:default",
            activated_by="test:operator",
        )
        conn.commit()


def _allow_rule(
    rule_id: str,
    action: str,
    *,
    match: dict[str, Any] | None = None,
    subjects: list[str] | None = None,
) -> dict[str, Any]:
    rule = {
        "rule_id": rule_id,
        "effect": "allow",
        "actions": [action],
        "subjects": subjects or [SUBJECT],
        "priority": 100,
        "reason_code": f"nomos.test.{rule_id}",
        "obligations": [{"type": "audit"}],
    }
    if match is not None:
        rule["match"] = match
    return rule


def _unmatched_allow_rule() -> dict[str, Any]:
    return {
        "rule_id": "allow_other_subject",
        "effect": "allow",
        "actions": ["graph.read.export_like"],
        "subjects": ["okg:someone-else"],
        "priority": 1,
        "reason_code": "nomos.test.unmatched",
    }


def _export(
    dsn: str,
    *,
    generation_id: str,
    classification: str = "public",
):
    signing_key = generate_ed25519_keypair("lab-a-sign-1")
    return export_published_subgraph_to_outbox(
        dsn=dsn,
        sender_authority_id=SUBJECT,
        recipient_authority_id="okg:mit-demo",
        sender_generation_id=generation_id,
        graph_id="poros:graph:nomos",
        root_node_ids=["node:poros-nomos"],
        signing_private_key=signing_key.private_key,
        signing_key_id=signing_key.kid,
        classification=classification,
        graph_profile="context",
        signing_key_reference="secret://poros/lab-a/signing",
    )


def _decision_rows(dsn: str) -> list[tuple[str, str, str]]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT action, effect, target->>'kind'
              FROM okg.nomos_decisions
             WHERE deployment_name = 'default'
               AND action <> 'policy.activate'
               AND target->>'kind' <> 'policy_activation'
             ORDER BY created_at, decision_row_id
            """
        )
        return [(str(a), str(e), str(k)) for a, e, k in cur.fetchall()]


def _outbox_count(dsn: str) -> int:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.poros_outbox_messages")
        (count,) = cur.fetchone()
        return int(count)


def _seed_inbox_proof_fixture(
    dsn: str,
    *,
    receiver_generation_id: str,
) -> None:
    envelope = {
        "message_id": "proof-denied-message",
        "body_sha256": "proof-denied-body",
        "body": {
            "graph_id": "poros:graph:proof-denied",
            "classification": "restricted",
            "origin_generation_id": receiver_generation_id,
        },
    }
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.poros_inbox_messages (
                receiver_authority_id, sender_authority_id, message_id,
                status, reason_code, body_sha256, signing_key_id,
                envelope_sha256, receipt_sha256, envelope, trusted,
                signature_valid, recipient_valid, body_hash_valid, fresh
            )
            VALUES (
                'okg:mit-demo', %s, 'proof-denied-message',
                'accepted_for_admission', 'accepted', 'proof-denied-body',
                'lab-a-sign-1', 'proof-denied-envelope',
                'proof-denied-receipt', %s, true, true, true, true, true
            )
            """,
            (SUBJECT, Jsonb(envelope)),
        )
        cur.execute(
            """
            INSERT INTO okg.poros_incorporation_events (
                receiver_authority_id, sender_authority_id, message_id,
                body_sha256, event_type, status, reason_code,
                receiver_generation_id, payload
            )
            VALUES (
                'okg:mit-demo', %s, 'proof-denied-message',
                'proof-denied-body', 'preflight_accepted', 'allow',
                'accepted', %s, '{}'::jsonb
            )
            """,
            (SUBJECT, receiver_generation_id),
        )
        conn.commit()


def _forbidden_sign_envelope(*_args: object, **_kwargs: object) -> dict[str, Any]:
    raise AssertionError("Poros signing must not run after Nomos denial")


class _Args:
    def __init__(self, **kwargs: Any) -> None:
        self.__dict__.update(kwargs)
