"""migrate-reads-through-transform-ir (task.mr.promote-transform-gate) -- the
transform-IR consistency programs ARE the publisher gate.

TransformConsistencyGate runs no_dangling_edges + narrowing_conformance as
TransformPrograms. Post chronos-native cutover the gate lowers the programs over
the Chronos surfaces (okg.graph_nodes / okg.graph_edges) and executes them through
a checkpoint-pinned Chronos checkout. A generation with a dangling edge
or a narrowing violation is BLOCKED (result=fail) with the offending edge ids; a clean
generation passes.

The legacy live projection tables were dropped by the cutover, so these tests
seed the Chronos graph via ChronosGraphStore.apply_facts (the canonical write
path) on a per-test branch, checkpoint the generation, and evaluate through the
Chronos gate.

The Publisher-integration leg of this requirement (a real cycle wired with the gate) is
covered by test_route_publish over the chronos publish path; the gate's chronos
block/pass behavior is also exercised by test_chronos_ir_seam.py::test_chronos_gate_*.
"""

from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.consistency import TransformConsistencyGate  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore, _edge_id  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres


def _seed_catalog(cur, cv: int = 1) -> None:
    """A page->references->page narrowing under catalog version `cv`."""
    cur.execute(
        "INSERT INTO okg.catalog_versions (catalog_version_id, status, applied_at) "
        "VALUES (%s,'applied',now()) ON CONFLICT DO NOTHING",
        (cv,),
    )
    cur.execute(
        "INSERT INTO okg.subtypes (name, catalog_version_id) VALUES ('page',%s) "
        "ON CONFLICT DO NOTHING",
        (cv,),
    )
    cur.execute(
        "INSERT INTO okg.subtype_ancestors (catalog_version_id, subtype_name, ancestor_name, depth) "
        "VALUES (%s,'page','page',0) ON CONFLICT DO NOTHING",
        (cv,),
    )
    cur.execute(
        "INSERT INTO okg.narrowings (parent_subtype, edge_type, child_subtype, catalog_version_id) "
        "VALUES ('page','references','page',%s) ON CONFLICT DO NOTHING",
        (cv,),
    )


def test_transform_gate_blocks_dangling_and_narrowing(clean_runtime: str) -> None:
    sfx = uuid.uuid4().hex[:8]
    src, dst, missing = f"n:src-{sfx}", f"n:dst-{sfx}", f"n:missing-{sfx}"
    cv = 1
    # Expected offending edge ids, computed the way ChronosGraphStore derives them.
    dangling_id = _edge_id(src, "references", missing)  # dangling dst (endpoint absent)
    narrowing_id = _edge_id(src, "cites", dst)  # (page,cites,page) not admitted

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            _seed_catalog(cur, cv)
        # Isolate on a per-test branch so the gate sees only this test's edges.
        branch = f"gate-{sfx}"
        store = ChronosGraphStore.from_connection(conn)
        store.ensure_branch(branch, from_branch="main")
        store.apply_facts(
            [
                NodeFact(src, "page", {"text": "src"}, {"id": src}, {"v": 1}),
                NodeFact(dst, "page", {"text": "dst"}, {"id": dst}, {"v": 1}),
                EdgeFact(
                    src,
                    dst,
                    "references",  # clean
                    source_record_id={"id": "e-clean"},
                    source_revision={"v": 1},
                ),
                EdgeFact(
                    src,
                    missing,
                    "references",  # dangling dst
                    source_record_id={"id": "e-dangle"},
                    source_revision={"v": 1},
                ),
                EdgeFact(
                    src,
                    dst,
                    "cites",  # narrowing violation
                    source_record_id={"id": "e-narrow"},
                    source_revision={"v": 1},
                ),
            ],
            branch=branch,
            strict_endpoints=False,  # admit the dangling edge so the gate can flag it
        )
        gen = store.checkpoint_generation(
            deployment_name="mr-test",
            branch=branch,
            catalog_version_id=cv,
            metadata={"correlation_id": f"mr:{sfx}"},
        )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ev = TransformConsistencyGate().evaluate(
            conn,
            target_generation_id=gen,
            catalog_version_id=cv,
            deployment_name="mr-test",
        )

    # The seeded catalog carries no at_most_one_target annotation, so the
    # cardinality family is inactive and the runtime basis discloses the
    # skip instead of naming a check that did not run.
    assert ev.details["runtime"] == (
        "chronos_transform_ir+skipped_families:cardinality"
    )
    assert ev.result == "evaluated"
    assert ev.blocks_publish is True
    assert ev.details["dangling_edge_ids"] == [
        dangling_id
    ]  # only the dangling-dst edge
    assert ev.details["narrowing_violation_edge_ids"] == [
        narrowing_id
    ]  # only the non-admitted triple
    assert ev.open_observation_count == 2


def test_transform_gate_passes_on_clean_generation(clean_runtime: str) -> None:
    sfx = uuid.uuid4().hex[:8]
    src, dst = f"n:src-{sfx}", f"n:dst-{sfx}"
    cv = 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            _seed_catalog(cur, cv)
        branch = f"gate-{sfx}"
        store = ChronosGraphStore.from_connection(conn)
        store.ensure_branch(branch, from_branch="main")
        store.apply_facts(
            [
                NodeFact(src, "page", {"text": "src"}, {"id": src}, {"v": 1}),
                NodeFact(dst, "page", {"text": "dst"}, {"id": dst}, {"v": 1}),
                EdgeFact(
                    src,
                    dst,
                    "references",  # admitted + endpoints present
                    source_record_id={"id": "e-clean"},
                    source_revision={"v": 1},
                ),
            ],
            branch=branch,
            strict_endpoints=True,
        )
        gen = store.checkpoint_generation(
            deployment_name="mr-test",
            branch=branch,
            catalog_version_id=cv,
            metadata={"correlation_id": f"mr:{sfx}"},
        )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ev = TransformConsistencyGate().evaluate(
            conn,
            target_generation_id=gen,
            catalog_version_id=cv,
            deployment_name="mr-test",
        )

    assert ev.result == "evaluated"
    assert ev.blocks_publish is False
    assert ev.details["dangling_edge_ids"] == []
    assert ev.details["narrowing_violation_edge_ids"] == []


def test_publisher_publishes_clean_with_transform_gate(clean_runtime: str) -> None:
    sfx = uuid.uuid4().hex[:8]
    src, dst = f"p:{sfx}:a", f"p:{sfx}:b"
    deployment_name = f"mr-pub-{sfx}"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["page"],
            narrowings=[("page", "references", "page")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.ensure_branch("default", from_branch="main")
        store.apply_facts(
            [
                NodeFact(src, "page", {"text": src}, {"id": src}, {"v": 1}),
                NodeFact(dst, "page", {"text": dst}, {"id": dst}, {"v": 1}),
                EdgeFact(
                    src,
                    dst,
                    "references",
                    source_record_id={"id": "e-clean"},
                    source_revision={"v": 1},
                ),
            ],
            branch="default",
            source="fixture",
            strict_endpoints=True,
        )

    out = Publisher(
        clean_runtime,
        deployment_name,
        consistency_gates=[TransformConsistencyGate()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment_name),
    ).cycle()

    assert out["status"] == "published", out
    summaries = {s["gate_name"]: s for s in out["consistency_gates"]}
    assert "transform_consistency_gate" in summaries
    assert summaries["transform_consistency_gate"]["result"] == "evaluated"
    assert summaries["transform_consistency_gate"]["blocks_publish"] is False


def test_publisher_blocks_on_narrowing_violation_via_transform_gate(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    src, dst = f"p:{sfx}:a", f"p:{sfx}:b"
    deployment_name = f"mr-pub-{sfx}"
    edge_id = _edge_id(src, "cites", dst)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["page"],
            narrowings=[("page", "references", "page")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.ensure_branch("default", from_branch="main")
        store.apply_facts(
            [
                NodeFact(src, "page", {"text": src}, {"id": src}, {"v": 1}),
                NodeFact(dst, "page", {"text": dst}, {"id": dst}, {"v": 1}),
                EdgeFact(
                    src,
                    dst,
                    "cites",
                    source_record_id={"id": "e-narrow"},
                    source_revision={"v": 1},
                ),
            ],
            branch="default",
            source="fixture",
            strict_endpoints=True,
        )

    out = Publisher(
        clean_runtime,
        deployment_name,
        consistency_gates=[TransformConsistencyGate()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment_name),
    ).cycle()

    assert out["status"] == "blocked", out
    assert out["phase_b_blocked"] is True
    summaries = {s["gate_name"]: s for s in out["consistency_gates"]}
    assert "transform_consistency_gate" in summaries
    tg = summaries["transform_consistency_gate"]
    assert tg["result"] == "evaluated"
    assert tg["blocks_publish"] is True
    assert edge_id in tg["details"]["narrowing_violation_edge_ids"]
