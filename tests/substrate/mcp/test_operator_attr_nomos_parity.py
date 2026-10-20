"""scale-operator-attr-reads / req.parity (Layer 2) — MUST-FIX #2.

The Layer-2 SQL fast path (`_effective_view_fast_rows`) MUST feed the Nomos
read-filter the SAME final effective attrs the Layer-1 slow path does. Accepted
overlays can contain both policy inputs and classified data, so every MCP path
must apply them before the final policy decision and mask. Once redacted, the
normal downstream overlay pass must not restore raw values.

The bugs this pins: slow and fast paths previously evaluated different attrs,
and post-filter overlays could restore the classified value Nomos had masked.
"""

from __future__ import annotations

import dataclasses
import json
import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.attributes import (  # noqa: E402
    materialize_effective_attributes_for_generation,
    record_attribute_observation,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.mcp import open_session  # noqa: E402
from okg.substrate.mcp.server import build_server  # noqa: E402
from okg.substrate.nomos import (  # noqa: E402
    activate_policy_bundle,
    compile_policy_graph,
)
from okg.substrate.nomos.read_masking import MASK_TOKEN  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import (  # noqa: E402
    SubtypeSpec,
    seed_catalog,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres

_DEPLOYMENT = "default"


def _redact_on_restricted_policy() -> dict:
    """Allow the mcp reader in the clear EXCEPT when classification=restricted,
    where the ``text`` payload is redacted. Keyed on the MCP read action."""
    return {
        "policy_bundle_id": "nomos:test:mcp-classification",
        "policy_version": "v1",
        "default_effect": "allow",
        "rules": [
            {
                "rule_id": "redact_restricted_mcp_read",
                "effect": "redact",
                "priority": 200,
                "subjects": ["subject:mcp-reader"],
                "actions": ["graph.read.mcp"],
                "match": {"classification": "restricted"},
                "obligations": [{"type": "redaction"}],
            },
            {
                "rule_id": "allow_mcp_read",
                "effect": "allow",
                "priority": 100,
                "subjects": ["subject:mcp-reader"],
                "actions": ["graph.read.mcp"],
            },
        ],
    }


def _seed_with_classification_overlay(clean_runtime: str) -> dict[str, str]:
    """Publish one node, then overlay its policy input and direct identifier."""
    suffix = uuid.uuid4().hex[:8]
    nid = f"doc:{suffix}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=[
                SubtypeSpec(
                    "document",
                    metadata={
                        "pii_classes": {
                            "classification": "non_pii",
                            "owner_email": "direct_identifier",
                            "text": "free_text",
                        },
                    },
                ),
            ],
        )
        activate_policy_bundle(
            conn,
            deployment_name=_DEPLOYMENT,
            bundle=compile_policy_graph(_redact_on_restricted_policy()),
            scope=f"deployment:{_DEPLOYMENT}",
            activated_by="test:operator",
            bootstrap=True,
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    nid,
                    "document",
                    {
                        "classification": "internal",
                        # At-rest anonymization persists this public marker. It
                        # must not be mistaken for a read-time terminal state.
                        "nomos_redacted": True,
                        "owner_email": "raw@example.com",
                        "text": "secret-body",
                    },
                    {"id": nid},
                    {"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
    published = Publisher(clean_runtime, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT)).cycle()
    assert published["status"] == "published", published
    gen = str(published["generation_id"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Overlay flips the policy input and also supplies raw classified data.
        record_attribute_observation(
            conn,
            target_kind="node",
            target_id=nid,
            generation_id=gen,
            attr_key="classification",
            value="restricted",
            producer="reviewer",
        )
        record_attribute_observation(
            conn,
            target_kind="node",
            target_id=nid,
            generation_id=gen,
            attr_key="owner_email",
            value="alice@example.com",
            producer="reviewer",
        )
        materialize_effective_attributes_for_generation(conn, generation_id=gen)
    return {"generation_id": gen, "node_id": nid}


def test_fast_and_slow_paths_agree_when_overlay_flips_classification(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Effective overlays drive the policy and cannot restore masked values."""
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-reader")
    fixture = _seed_with_classification_overlay(clean_runtime)
    gen, nid = fixture["generation_id"], fixture["node_id"]
    mcp = build_server(dsn=clean_runtime)
    pred = {"op": "eq", "field": "subtype", "value": "document"}
    try:
        attr_rows = mcp._okg_operator_attr_rows  # type: ignore[attr-defined]
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = dataclasses.replace(
                open_session(conn, deployment_name=_DEPLOYMENT),
                generation_id=gen,
                nomos_subject="subject:mcp-reader",
            )
            # Fast path (snapshot present).
            fast, _ = attr_rows(
                conn, session, kind="nodes", limit=10_000, scan_predicate=pred
            )
            # Force Layer-1 fallback.
            conn.execute(
                "DELETE FROM okg.attribute_effective_view WHERE generation_id = %s",
                (gen,),
            )
            slow, _ = attr_rows(
                conn, session, kind="nodes", limit=10_000, scan_predicate=pred
            )

        def _by_id(rows):
            return {r["node_id"]: dict(r.get("attrs") or {}) for r in rows}

        fast_by, slow_by = _by_id(fast), _by_id(slow)
        # Same id set and attrs for the SQL snapshot and Layer-1 fallback.
        assert set(fast_by) == set(slow_by) == {nid}, (fast_by, slow_by)
        assert fast_by[nid] == slow_by[nid], (fast_by[nid], slow_by[nid])
        # The effective classification triggers redaction. The accepted raw
        # overlay is masked and remains masked after downstream overlay helpers.
        assert fast_by[nid]["classification"] == "restricted"
        assert fast_by[nid]["owner_email"] == MASK_TOKEN
        assert "text" not in fast_by[nid]
        assert fast_by[nid]["nomos_redacted"] is True
        serialized = json.dumps({"fast": fast_by, "slow": slow_by})
        assert "alice@example.com" not in serialized
        assert "raw@example.com" not in serialized
        assert "secret-body" not in serialized
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
