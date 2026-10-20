"""Published-generation proof that `actor_ref` links a record to its actor.

`actor_ref` is advertised by 8 record templates and could never emit an edge:
no record prefix resolved an actor id, and no narrowing declared the route. This
proves both layers are fixed against a real database, and that the catalog route
is what admits the edge.

The catalog seeded here is DERIVED from the real composed substrate library, so
if the library stops declaring the route the seeded catalog loses it and the row
assertion fails. `okg.edges_live` is retired; the live graph is Chronos
(`okg.graph_edges`), read through `ChronosGraphStore`.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
import yaml

psycopg = pytest.importorskip("psycopg")

from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402
from okg.substrate.consistency import TransformConsistencyGate  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.mcp.record import lower_record_payload  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

#: agent_observation is the record subtype; agent is the actor subtype, owned by
#: the agent_sessions module, so the route is a cross-module narrowing. `study`
#: is here for a non-obvious reason: the narrowing gate is SKIPPED outright when
#: the active catalog version has zero narrowing rows
#: (`_narrowing_gate_active`, src/okg/substrate/consistency/transform_gate.py:190,
#: mirroring writer-path NarrowingChecker activation). Restricting the seeded
#: catalog to just (agent_observation, agent) leaves exactly one triple, so
#: removing it for the negative arm empties the catalog and the arm passes
#: VACUOUSLY -- observed: the publish succeeded instead of blocking. Keeping a
#: third subtype guarantees the arm-1 catalog still declares a real narrowing.
_SUBTYPES = ("agent_observation", "agent", "study")

_ACTOR_ROUTE = ("agent_observation", "references", "agent")


#: The modules this proof actually needs: `agent_memory` owns agent_observation
#: and study, `agent_sessions` owns agent, and the rest are their composition
#: dependencies. Deliberately NOT the all-38-module set: composing every module
#: cost this test ~23s on the 2-core CI runner (measured — it pushed the
#: substrate-db lane from ~189s to 212-217s against its enforced 200s budget)
#: while proving nothing extra, since the route under test comes from these
#: modules. The library declaration is still the source of the triple, so the
#: test remains sensitive to the narrowing being removed.
_MODULES = (
    "git_graph",
    "openspec",
    "agent_sessions",
    "compute_env",
    "ci",
    "code_graph",
    "quality_evidence",
    "agent_memory",
)


def _library_catalog(tmp_path: Path):
    deployment = tmp_path / "actor-route"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump({"name": "actor-route", "modules": list(_MODULES)}),
        encoding="utf-8",
    )
    (deployment / "schemas").mkdir()
    return compose_catalog(deployment)


def _facts_from_lowered(lowered: dict) -> list:
    payload = lowered["admission_payload"]
    facts: list = []
    for node in payload.get("nodes") or []:
        facts.append(NodeFact(
            node["node_id"],
            node["subtype"],
            dict(node["attrs"]),
            {"id": node["node_id"]},
            {"v": 1},
        ))
    for edge in payload.get("edges") or []:
        facts.append(EdgeFact(
            edge["src"],
            edge["dst"],
            edge["edge_type"],
            attrs=dict(edge.get("attrs") or {}),
            source_record_id={"id": f"{edge['src']}->{edge['dst']}"},
            source_revision={"v": 1},
        ))
    return facts


def test_actor_edge_lands_in_published_generation(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    composed = _library_catalog(tmp_path)
    library_triples = composed.narrowing_triples()

    # The library's own declaration, not a triple this test invented.
    assert _ACTOR_ROUTE in library_triples

    seeded = sorted(
        triple for triple in library_triples
        if triple[0] in _SUBTYPES and triple[2] in _SUBTYPES
    )
    without_route = [t for t in seeded if t != _ACTOR_ROUTE]
    assert len(without_route) == len(seeded) - 1
    # The negative arm is only meaningful while the catalog it seeds still
    # declares SOMETHING: a catalog with zero narrowings switches the gate off
    # and the arm would pass without proving anything. See _SUBTYPES above.
    assert without_route, (
        "arm 1 would be vacuous: removing the actor route emptied the seeded "
        "catalog, which disables the narrowing gate entirely"
    )

    sfx = uuid.uuid4().hex[:8]
    actor_id = f"agent:claude-opus-5:claude-code-{sfx}"

    lowered = lower_record_payload(
        kind="observation",
        payload={
            "observation_id": f"actor-proof-{sfx}",
            "summary": "The closure table was regenerated.",
            "actor_ref": actor_id,
        },
        authority={"producer_id": "agent:test"},
        active_subtypes=composed.subtypes,
        narrowing_triples=library_triples,
    )
    assert lowered["reference_diagnostics"] == []
    assert lowered["emitted_edge_count"] == 1
    record_id = lowered["record_id"]

    # The actor node is ingest-owned; the record may only reference it, so the
    # test stands in for the agent_sessions source and creates it directly.
    actor_node = NodeFact(
        actor_id, "agent", {"agent_id": actor_id}, {"id": actor_id}, {"v": 1},
    )

    # Arm 1 -- without the route in the applied catalog the publish BLOCKS, so
    # the narrowing is what admits the edge, not the lowerer.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=list(_SUBTYPES), narrowings=without_route)
        ChronosGraphStore.from_connection(conn).apply_facts(
            [actor_node, *_facts_from_lowered(lowered)],
            source="default",
            strict_endpoints=True,
        )

    blocked = Publisher(
        clean_runtime, "default", consistency_gates=[TransformConsistencyGate()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert blocked["status"] == "blocked", blocked
    details = blocked["consistency_gates"][0]["details"]
    # Pin the REASON: a block for any other cause would satisfy a bare status
    # check and prove nothing.
    assert details["narrowing_violation_edge_ids"], details
    assert details["dangling_edge_ids"] == [], details

    # Arm 2 -- a later applied catalog version wins, so this swaps the active
    # catalog under the same graph.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=list(_SUBTYPES), narrowings=seeded)

    out = Publisher(
        clean_runtime, "default", consistency_gates=[TransformConsistencyGate()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert out["status"] == "published", out

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        published = store.checkout_generation(out["generation_id"]).query(
            "SELECT src, dst, edge_type FROM okg.graph_edges "
            "WHERE src = :src AND dst = :dst",
            {"src": record_id, "dst": actor_id},
        )

    assert [
        (row["src"], row["dst"], row["edge_type"]) for row in published
    ] == [(record_id, actor_id, "references")]
