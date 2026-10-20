"""End-to-end: at-rest pseudonymization + per-principal read masking together.

On the isolated test DB, with the REAL components: the at-rest transform
pseudonymizes a direct identifier and keeps a content field; the transformed
fact is published into a real Chronos generation and read back (proving the
stored generation holds a pseudonym, not the raw identifier); then the live
Nomos read filter (now the single classification-driven ``mask_attrs`` path)
applies a real policy so a privileged principal reveals the content while an
unprivileged one has it redacted -- with the raw identifier absent from both
projections and the same stable pseudonym in each.
"""
from __future__ import annotations

import json

import psycopg
import pytest

from okg.substrate.alias.pseudonymize import PseudonymService
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest.pii_sanitize import sanitize_fact_pii
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import inspect_node, open_session
from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph
from okg.substrate.nomos.read_filter import filter_node_rows_for_session
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

# DB-backed (clean_runtime): route to a Postgres lane, not fast/smoke.
pytestmark = pytest.mark.postgres

_VAULT = PseudonymService(key=b"umbrella-e2e-key-material-32bytes", epoch="e1")
_RAW_EMAIL = "alice@example.com"


def _at_rest_classifier(key: str):
    # Ingest posture: pseudonymize the identifier, keep the content field and the
    # governance classification (body is dropped at READ, not at rest).
    return {"email": "direct_identifier", "body": "non_pii"}.get(key)


def _activate_policy(dsn: str) -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:anonymization-e2e",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_privileged",
                "effect": "allow",
                "actions": ["graph.read.mcp"],
                "subjects": ["subject:privileged"],
                "match": {"deployment": "default", "scope": "deployment:default", "source_id": "pii_fixture"},
                "priority": 100,
            },
            {
                "rule_id": "redact_unprivileged",
                "effect": "redact",
                "actions": ["graph.read.mcp"],
                "subjects": ["subject:unprivileged"],
                "match": {"deployment": "default", "scope": "deployment:default", "source_id": "pii_fixture"},
                "priority": 100,
                "reason_code": "nomos.test.redact_unprivileged",
                "obligations": [{"type": "redaction"}, {"type": "audit"}],
            },
        ],
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


def _row(attrs: dict) -> dict:
    return {
        "node_id": "node:person-a",
        "subtype": "page",
        "attrs": dict(attrs),
        "source": "pii_fixture",
        "source_record_id": {"id": "a"},
        "source_revision": {"updated_at": "2026-05-01T00:00:00Z"},
    }


def test_anonymization_end_to_end_two_principal(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # 1. At-rest transform, then publish a REAL generation from the fixture fact.
    raw = NodeFact(
        "node:person-a",
        "page",
        {"classification": "internal", "email": _RAW_EMAIL, "body": "secret body notes"},
        {"id": "a"},
        {"updated_at": "2026-05-01T00:00:00Z"},
    )
    stored = sanitize_fact_pii(
        raw, classifier=_at_rest_classifier, store_raw=False, pseudonym_service=_VAULT
    )
    assert not stored.held
    mem_attrs = stored.fact.attrs  # the transformed attrs, before storage
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ChronosGraphStore.from_connection(conn).apply_facts(
            [stored.fact], source="pii_fixture"
        )
    assert Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()["status"] == "published"

    # 2. Read the stored node back (no enforcement) -> the published generation
    #    physically holds a pseudonym, not the raw identifier.
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        node = inspect_node(conn, session, "node:person-a")
    assert node is not None
    assert _RAW_EMAIL not in json.dumps(node)  # raw absent in the published gen
    pseudonym = mem_attrs["email"]
    # The published node carries the same pseudonym (the store fences the value
    # in its read-back UNTRUSTED_DATA marker).
    assert "_e1_" in pseudonym and pseudonym in node["attrs"]["email"]

    # 3. Two principals read the transformed row through the real Nomos filter.
    _activate_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        privileged = filter_node_rows_for_session(
            conn, session, [_row(mem_attrs)], subject="subject:privileged"
        )
        unprivileged = filter_node_rows_for_session(
            conn, session, [_row(mem_attrs)], subject="subject:unprivileged"
        )

    assert len(privileged) == 1 and len(unprivileged) == 1
    priv_attrs = privileged[0]["attrs"]
    unpriv_attrs = unprivileged[0]["attrs"]

    # Read-side redaction: privileged keeps the content field; unprivileged has it
    # DROPPED. This body difference over one identical input row can only come
    # from the read filter's redact -> mask_attrs(drop free_text) path.
    assert "secret body notes" in str(priv_attrs.get("body"))
    assert "body" not in unpriv_attrs

    # The raw identifier is in NEITHER projection; both carry the same stable
    # pseudonym (joins survive the masked read).
    assert _RAW_EMAIL not in json.dumps(privileged)
    assert _RAW_EMAIL not in json.dumps(unprivileged)
    assert priv_attrs["email"] == unpriv_attrs["email"] == pseudonym
