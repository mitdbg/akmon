"""End-to-end Chronos cutover tests.

Both tests drive the same arc -- seed a catalog, write a ticket--reported_by-->person
graph, publish a chronos_native generation (checkpoint the branch head), then read the
graph back through the CONVERTED operator surface (describe_graph/inspect, expand,
traverse) -- and both assert Chronos is the canonical store: graph_nodes/graph_edges are
populated (read through a checkout, the way the interval backend reconstructs current
state) while the legacy okg.nodes_live/edges_live are EMPTY after a chronos_native publish.

They differ only in the WRITE leg:
  - test_cutover_e2e_ingest_publish_read     -> the canonical ChronosGraphStore.apply_facts
  - test_cutover_e2e_sourcerunner_full_pipeline -> the full SourceRunner ingest pipeline
    (source adapter -> runner -> apply_facts), no shortcuts.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.ingest import SourceRunner  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.library.sources.jira import JiraSource  # noqa: E402
from okg.substrate.mcp import (  # noqa: E402
    expand_neighbors,
    expand_path,
    inspect_graph,
    open_session,
    parse_path_hops,
)
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


def _read_back_over_chronos(clean_runtime: str, ticket: str, person: str) -> None:
    """Read the graph back through the CONVERTED operator surface (over Chronos)
    and assert the ticket--reported_by-->person shape is visible."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Chronos current state is read through a CHECKOUT (the interval backend
        # reconstructs it).
        graph = ChronosGraphStore.from_connection(conn, ensure_schema=False).checkout()
        graph_nodes = int(graph.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"])
        graph_edges = int(graph.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"])
        with conn.cursor() as cur:
            # The legacy bitemporal tables are DROPPED by the cutover migration --
            # Chronos graph_nodes/graph_edges is the sole graph store.
            cur.execute(
                "SELECT to_regclass('okg.nodes_live'), to_regclass('okg.edges_live')"
            )
            nodes_live_reg, edges_live_reg = cur.fetchone()
        assert graph_nodes >= 2 and graph_edges >= 1     # Chronos populated by ingest
        assert nodes_live_reg is None and edges_live_reg is None  # legacy tables dropped

        s = open_session(conn, deployment_name="default")

        desc = inspect_graph(conn, s)                       # inspect / graph_stats
        assert desc["node_count"] >= 2
        assert desc["edge_count"] >= 1
        assert desc["subtype_count"] >= 2

        nb = expand_neighbors(conn, s, ticket)                 # expand
        assert [n["dst"] for n in nb["neighbors"]] == [person]
        assert nb["neighbors"][0]["edge_type"] == "reported_by"

        tp = expand_path(                                    # typed-path traverse
            conn, s, ticket, hops=parse_path_hops(["reported_by:out"]),
            target_subtype="person",
        )
        assert any(r.get("node_id") == person for r in tp["results"])

pytestmark = pytest.mark.postgres


def test_cutover_e2e_ingest_publish_read(clean_runtime: str) -> None:
    sfx = uuid.uuid4().hex[:8]
    ticket, person = f"t:{sfx}", f"p:{sfx}"

    # 1. Catalog: ticket --reported_by--> person.
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        seed_catalog(
            cur.connection,
            subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )

    # 2. INGEST: write the graph through the canonical Chronos write path
    #    (ChronosGraphStore.apply_facts -> okg.graph_nodes/graph_edges) -- the same
    #    call the SourceRunner makes internally.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(ticket, "ticket", {"text": "a bug report"}, {"id": ticket}, {"v": 1}),
                NodeFact(person, "person", {"name": "alice"}, {"id": person}, {"v": 1}),
                EdgeFact(ticket, person, "reported_by",
                         source_record_id={"id": "e0"}, source_revision={"v": 1}),
            ],
            source="default",
            strict_endpoints=True,
        )

    # 3. PUBLISH (chronos_native): checkpoint the branch head as a generation.
    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    # 4-5. Chronos is canonical; read back through the converted operator surface.
    _read_back_over_chronos(clean_runtime, ticket, person)


def test_cutover_e2e_sourcerunner_full_pipeline(clean_runtime: str, tmp_path: Path) -> None:
    """The full pipeline, no shortcuts: a source adapter feeds the real SourceRunner,
    which writes okg.graph_nodes/graph_edges via ChronosGraphStore; a chronos_native
    publish checkpoints the branch head; the graph reads back through the converted
    operators over Chronos with the legacy tables left empty."""
    sfx = uuid.uuid4().hex[:8]
    ticket, person = f"t:{sfx}", f"p:{sfx}"

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        seed_catalog(
            cur.connection,
            subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )

    fx = tmp_path / "jira.json"
    fx.write_text(json.dumps({
        "nodes": [
            {"node_id": ticket, "subtype": "ticket", "attrs": {"text": "a bug report"}},
            {"node_id": person, "subtype": "person", "attrs": {"name": "alice"}},
        ],
        "edges": [{"src": ticket, "dst": person, "edge_type": "reported_by"}],
    }))
    ingest = SourceRunner(clean_runtime, source_name="default").run(
        JiraSource(fixture_path=str(fx)), mode="cursor",
    )
    assert ingest.nodes_appended == 2
    assert ingest.edges_appended == 1

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    _read_back_over_chronos(clean_runtime, ticket, person)
