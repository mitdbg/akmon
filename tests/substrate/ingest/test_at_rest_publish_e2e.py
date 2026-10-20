"""End-to-end: at-rest transform -> real publish -> stored pseudonyms (test DB).

Publishes not-store-raw facts through the at-rest transform into a real Chronos
generation on the isolated test DB, then reads them back and proves the store
holds pseudonyms (raw absent), the same identifier is join-consistent across two
nodes, and non_pii attributes survive. The live auto-wiring into the source
runner is staged; this proves the transform integrates with a real published
generation.
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
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

# DB-backed (clean_runtime): route to a Postgres lane, not fast/smoke.
pytestmark = pytest.mark.postgres

_VAULT = PseudonymService(key=b"at-rest-e2e-test-key-material-32b", epoch="e1")


def _classifier(key: str):
    return {"email": "direct_identifier", "text": "non_pii"}.get(key)


def test_at_rest_transform_publishes_pseudonyms_not_raw(clean_runtime: str) -> None:
    raw_facts = [
        NodeFact(
            "node:person-a",
            "page",
            {"classification": "internal", "email": "alice@example.com", "text": "note A"},
            {"id": "a"},
            {"updated_at": "2026-05-01T00:00:00Z"},
        ),
        NodeFact(
            "node:person-b",
            "page",
            {"classification": "internal", "email": "alice@example.com", "text": "note B"},
            {"id": "b"},
            {"updated_at": "2026-05-01T00:00:00Z"},
        ),
    ]

    transformed = []
    for fact in raw_facts:
        out = sanitize_fact_pii(
            fact, classifier=_classifier, store_raw=False, pseudonym_service=_VAULT
        )
        assert not out.held
        transformed.append(out.fact)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ChronosGraphStore.from_connection(conn).apply_facts(
            transformed, source="pii_fixture"
        )
    result = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert result["status"] == "published"

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        node_a = inspect_node(conn, session, "node:person-a")
        node_b = inspect_node(conn, session, "node:person-b")

    assert node_a is not None and node_b is not None
    # The raw identifier was never stored anywhere on the node.
    assert "alice@example.com" not in json.dumps(node_a)
    # The same identifier yields the same pseudonym across both nodes (joins survive).
    assert node_a["attrs"]["email"] == node_b["attrs"]["email"]
    assert "_e1_" in node_a["attrs"]["email"]  # keyed pseudonym, epoch-tagged
    # A non_pii attribute keeps its content (the store fences source text with an
    # UNTRUSTED_DATA marker; the value survives inside it, unlike the masked PII).
    assert "note A" in node_a["attrs"]["text"]
    assert "note B" in node_b["attrs"]["text"]
    # The classification governance attr the policy keys on is NOT masked (its
    # content survives; it would be "[nomos:masked]" without the governance-key
    # exemption). The store fences it in the same read-back marker as text.
    assert "internal" in node_a["attrs"]["classification"]
    assert "[nomos:masked]" not in node_a["attrs"]["classification"]
