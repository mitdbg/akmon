"""req.external-backed-nodes.federation-export.

A node's external HANDLE is stripped on Poros export so a peer never chases a
dangling pointer; stable metadata still travels.
"""
from __future__ import annotations

import json

import psycopg
import pytest

from okg.substrate.exchange.outbox import (
    _strip_external_handle,
    export_published_subgraph_to_outbox,
)
from okg.substrate.exchange.signing import generate_ed25519_keypair
from okg.substrate.external_backed.handle import HANDLE_ATTR
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

_NODE = "node:cluster:gpu07"
_HANDLE = {
    "metrics": {
        "current": "latest_from_series",
        "series": {"store": "timescaledb", "metric": "SECRET_HANDLE_METRIC"},
        "access": {"secret_refs": ["env:PROBE_TOKEN"]},
    }
}


# ---- unit ----

def test_strip_removes_handle_keeps_metadata() -> None:
    node = {"node_id": _NODE, "subtype": "compute_node",
            "attrs": {"label": "gpu07", "static": "KEEP", HANDLE_ATTR: _HANDLE}}
    out = _strip_external_handle(node)
    assert HANDLE_ATTR not in out["attrs"]
    assert out["attrs"]["static"] == "KEEP"
    assert out["attrs"]["label"] == "gpu07"


def test_strip_noop_without_handle() -> None:
    node = {"node_id": "n", "subtype": "x", "attrs": {"a": 1}}
    assert _strip_external_handle(node) == node


# ---- end-to-end export ----

@pytest.mark.postgres
def test_exported_envelope_omits_handle(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime) as conn:
        seed_catalog_version(conn)
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(
                _NODE, "compute_node",
                {"text": "gpu07", "static": "KEEP_THIS_PUBLIC", HANDLE_ATTR: _HANDLE},
                {"id": _NODE}, {"v": 1},
            )],
            source="default", strict_endpoints=False,
        )
    publish = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert publish["status"] == "published"

    sender = generate_ed25519_keypair("ebn-sender")
    export = export_published_subgraph_to_outbox(
        dsn=clean_runtime,
        sender_authority_id="okg:lab-a",
        recipient_authority_id="okg:peer",
        sender_generation_id=publish["generation_id"],
        graph_id="ebn:graph:1",
        root_node_ids=[_NODE],
        signing_private_key=sender.private_key,
        signing_key_id=sender.kid,
        message_id="ebn-export-1",
    )
    wire = json.dumps(export.envelope, default=str)
    # Stable metadata travels; the handle (and its private locator) does not.
    assert "KEEP_THIS_PUBLIC" in wire
    assert HANDLE_ATTR not in wire
    assert "SECRET_HANDLE_METRIC" not in wire
    assert "PROBE_TOKEN" not in wire
