"""#947 published-generation proof: a study links to its project for real.

`record(kind="study", payload={... "project_ref": "proj:x"})` created the study
node and dropped the project link, because no catalog narrowing declared
`study --references--> project`. Unit-level coverage cannot prove the fix: the
pre-existing study lowering tests pass no catalog constraints at all, so they
skip the route check entirely, and a catalog seeded from a literal triple list
in the test would pass even if the shipped ontology still omitted the rule.

This test closes both holes. The catalog it seeds is DERIVED from the real
composed substrate library (`agent_memory` + `bookkeeping`), and the assertion
is made against the published generation's Chronos graph, not against the
lowering return value. If the library stops declaring the route, this test fails
at the `library_triples` precheck below -- before either Postgres arm runs, so
the published-row assertion itself is not what catches a reverted declaration.
`test_record_operator.py::test_study_record_emits_project_and_domain_edges` is
the compose-derived lowering check; this file's job is the published row.

Note `okg.edges_live` is retired; the live graph is Chronos
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

#: The record subtypes this proof exercises. `study` is the #947 source;
#: `project` is owned by the `bookkeeping` module, so the route is a
#: cross-module narrowing.
_SUBTYPES = ("study", "project", "domain_entity")

_MODULES = (
    "git_graph",
    "openspec",
    "agent_sessions",
    "compute_env",
    "ci",
    "code_graph",
    "quality_evidence",
    "agent_memory",
    "bookkeeping",
)


def _composed_library_catalog(tmp_path: Path):
    deployment = tmp_path / "record-study-project"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": "record-study-project",
            "description": "#947 study/project narrowing proof",
            "modules": list(_MODULES),
        }),
        encoding="utf-8",
    )
    (deployment / "schemas").mkdir()
    return compose_catalog(deployment)


def _facts_from_lowered(lowered: dict) -> list:
    """Turn a lowered record admission payload into source facts."""
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
    # A record with no emitted edges omits the key entirely.
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


def test_study_project_edge_lands_in_published_generation(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    composed = _composed_library_catalog(tmp_path)
    library_triples = composed.narrowing_triples()

    # The library declaration itself, not a triple the test invented.
    assert ("study", "references", "project") in library_triples
    assert ("study", "references", "domain_entity") in library_triples

    seeded_narrowings = sorted(
        triple for triple in library_triples
        if triple[0] in _SUBTYPES and triple[2] in _SUBTYPES
    )
    assert ("study", "references", "project") in seeded_narrowings

    without_route = [
        triple for triple in seeded_narrowings
        if triple != ("study", "references", "project")
    ]
    assert len(without_route) == len(seeded_narrowings) - 1
    # Arm 1 below is only meaningful while the catalog it seeds still declares
    # SOMETHING. The narrowing gate is skipped outright when the active catalog
    # version has zero narrowing rows (`_narrowing_gate_active`,
    # src/okg/substrate/consistency/transform_gate.py:190, mirroring
    # writer-path NarrowingChecker activation), so a seeded catalog that this
    # filter empties would let the publish SUCCEED and the arm would prove
    # nothing. Today `study --references--> domain_entity` keeps it non-empty;
    # this guard fails loudly if that stops being true.
    assert without_route, (
        "arm 1 would be vacuous: removing the study->project route emptied the "
        "seeded catalog, which disables the narrowing gate entirely"
    )

    sfx = uuid.uuid4().hex[:8]
    project_id = f"proj:cooper-{sfx}"
    study_id = f"study:closure-{sfx}"

    project = lower_record_payload(
        kind="project",
        payload={"project_id": project_id, "title": "Cooper"},
        authority={"producer_id": "agent:test"},
        active_subtypes=composed.subtypes,
        narrowing_triples=library_triples,
    )
    study = lower_record_payload(
        kind="study",
        payload={
            "study_id": study_id,
            "title": "ABCD closure",
            "project_ref": project_id,
        },
        authority={"producer_id": "agent:test"},
        active_subtypes=composed.subtypes,
        narrowing_triples=library_triples,
    )

    # Nothing was suppressed on the way in -- the pre-#947 failure mode.
    assert study["reference_diagnostics"] == []
    assert study["suppressed_edge_count"] == 0
    assert study["emitted_edge_count"] == 1
    assert study["record_id"] == study_id

    # Arm 1 -- the narrowing is load-bearing at the publish gate. Seed a catalog
    # that declares every OTHER study route, apply the same facts, and the
    # generation BLOCKS. (In production the lowerer suppresses the edge before
    # the gate ever sees it -- that arm is
    # test_record_operator.py::test_study_record_project_edge_is_suppressed_without_the_narrowing.
    # This one proves the catalog declaration, not the lowerer, is what admits
    # the edge, so the fix cannot be satisfied by a lowering-side loophole.)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=list(_SUBTYPES),
            narrowings=without_route,
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            _facts_from_lowered(project) + _facts_from_lowered(study),
            source="default",
            strict_endpoints=True,
        )

    publisher = Publisher(
        clean_runtime, "default",
        consistency_gates=[TransformConsistencyGate()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    )
    blocked = publisher.cycle()
    assert blocked["status"] == "blocked", blocked
    # Pin the REASON, not just the status: a generation blocked for any other
    # cause would otherwise satisfy this arm and prove nothing about #947.
    # The gate sums four violation classes into one "blocked" verdict, so pin
    # all of them: a block caused by any other class would otherwise satisfy
    # this arm and prove nothing about the narrowing.
    details = blocked["consistency_gates"][0]["details"]
    assert details["narrowing_violation_edge_ids"], details
    assert details["dangling_edge_ids"] == [], details
    assert details["node_subtype_violation_node_ids"] == [], details
    assert details["cardinality_violation_edge_ids"] == [], details

    # Arm 2 -- with the route the library declares, the same graph publishes.
    # A later applied catalog version wins (MAX(catalog_version_id) where
    # status='applied'), so this swaps the active catalog under the same graph.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=list(_SUBTYPES),
            narrowings=seeded_narrowings,
        )

    out = Publisher(
        clean_runtime, "default",
        consistency_gates=[TransformConsistencyGate()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert out["status"] == "published", out

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        published = store.checkout_generation(out["generation_id"]).query(
            "SELECT src, dst, edge_type FROM okg.graph_edges "
            "WHERE src = :src AND dst = :dst",
            {"src": study_id, "dst": project_id},
        )

    assert [
        (row["src"], row["dst"], row["edge_type"]) for row in published
    ] == [(study_id, project_id, "references")]
