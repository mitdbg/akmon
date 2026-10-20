"""task.external-backed-nodes.proof — the whole nucleus loop end to end.

On the `default` deployment: a node carries a handle (a series field + a
privileged probe field); samples are seeded; a generation publishes;
inspect-with-live returns a labeled overlay (series fresh; privileged probe
denied with the adapter not invoked); plain inspect does not fetch; the read
mutates no graph rows; and a Poros export omits the handle.
"""
from __future__ import annotations

import json

import psycopg
import pytest

from okg.substrate.exchange.outbox import export_published_subgraph_to_outbox
from okg.substrate.exchange.signing import generate_ed25519_keypair
from okg.substrate.external_backed.handle import HANDLE_ATTR
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp.session import open_session
from okg.substrate.mcp.tools import inspect_node
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

_NODE = "node:cluster:gpu07"
_HANDLE = {
    "metrics": {
        "current": "latest_from_series",
        "series": {"store": "timescaledb", "metric": "gpu_util"},
        "ttl_seconds": 300,
    },
    "scheduler": {
        "current": "probe",
        "adapter": {"kind": "python"},
        "ttl_seconds": 60,
        "cache_policy": "never-cache",
        "access": {"permission": "external.probe.invoke", "secret_refs": ["env:TOK"]},
    },
}


def _conn(dsn):
    return psycopg.connect(dsn, autocommit=True)


def test_external_backed_nucleus_end_to_end(clean_runtime: str) -> None:
    # 1. a node carries a handle; 2. samples seeded.
    with _conn(clean_runtime) as conn:
        seed_catalog_version(conn)
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(
                _NODE, "compute_node",
                {"text": "gpu07", "static": "PUBLIC", HANDLE_ATTR: _HANDLE},
                {"id": _NODE}, {"v": 1},
            )],
            source="default", strict_endpoints=False,
        )
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.node_series (deployment_name, node_id, metric, value) "
                "VALUES ('default',%s,'gpu_util',87.0)",
                (_NODE,),
            )

    # 3. publish.
    publish = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert publish["status"] == "published"

    probe_calls = {"n": 0}

    def _runner(conn, spec, node_id):
        probe_calls["n"] += 1
        return ("ok", {"queued": 3}, None)

    with _conn(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")

        def counts():
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM okg.graph_nodes")
                a = cur.fetchone()[0]
                cur.execute("SELECT COALESCE(max(fact_id),0) FROM okg.node_facts")
                b = cur.fetchone()[0]
            return a, b

        before = counts()

        # 4-5. inspect WITH live: series fresh; privileged probe DENIED
        #      (no policy bundle -> default deny), adapter not invoked.
        live = inspect_node(
            conn, session, _NODE, hydrate=True,
            # A named caller, so the no-bundle default deny is what refuses
            # it, not the missing-principal refusal (#2361).
            principal="subject:probe-caller",
            probe_runners={"python": _runner},
        )
        fields = live["external_live"]["fields"]
        assert fields["metrics"]["state"] == "fresh"
        assert fields["metrics"]["value"] == 87.0
        assert fields["scheduler"]["state"] == "denied"
        assert fields["scheduler"]["error"]["reason"] == "nomos.default_deny"
        assert probe_calls["n"] == 0, "denied probe must not run the adapter"
        assert live["pinned_generation_id"] == session.generation_id

        # 6. plain inspect does not fetch.
        plain = inspect_node(conn, session, _NODE)
        assert "external_live" not in plain

        # 7. no graph rows changed during the reads.
        assert counts() == before

    # 8. Poros export omits the handle; public metadata travels.
    sender = generate_ed25519_keypair("ebn-proof")
    export = export_published_subgraph_to_outbox(
        dsn=clean_runtime,
        sender_authority_id="okg:lab-a",
        recipient_authority_id="okg:peer",
        sender_generation_id=publish["generation_id"],
        graph_id="ebn:proof:graph",
        root_node_ids=[_NODE],
        signing_private_key=sender.private_key,
        signing_key_id=sender.kid,
        message_id="ebn-proof-export",
    )
    wire = json.dumps(export.envelope, default=str)
    assert "PUBLIC" in wire
    assert HANDLE_ATTR not in wire
    assert "external.probe.invoke" not in wire
