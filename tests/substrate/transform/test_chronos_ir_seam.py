"""chronos-native-graph-cutover -- the IR is the read seam over Chronos.

Proves the transform-IR expand program lowers over the Chronos-bound surfaces
(okg.graph_edges) and executes through a pinned Chronos checkout session, returning
the correct neighbors -- i.e. the IR mediates Chronos reads (not just the legacy
okg.v_* views). This is the foundation for routing the AKMON operator surface + the
publisher consistency gate over Chronos via the IR.
"""

from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.consistency import TransformConsistencyGate  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore, _edge_id  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.transform.operators import (  # noqa: E402
    execute_list_neighbors_chronos,
    lower_neighbors_chronos,
    lower_program_over_chronos,
    neighbors_program,
    to_chronos_sql,
)

pytestmark = pytest.mark.postgres


def test_to_chronos_sql_translates_named_params() -> None:
    sql = (
        "SELECT * FROM okg.graph_edges WHERE src = %(focal)s AND edge_type = %(etype)s"
    )
    out = to_chronos_sql(sql)
    assert ":focal" in out and ":etype" in out
    assert "%(" not in out


def test_lowered_chronos_neighbors_targets_graph_edges() -> None:
    sql, _ = lower_neighbors_chronos(direction="both", edge_type="references", limit=10)
    assert "okg.graph_edges" in sql  # Chronos surface, not okg.v_edges
    assert "okg.v_edges" not in sql
    assert ":focal" in sql  # :param bind style for the session


def _seed(conn, sfx: str) -> str:
    src, d1, d2 = f"n:src-{sfx}", f"n:d1-{sfx}", f"n:d2-{sfx}"
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO okg.subtypes (name, catalog_version_id) VALUES ('page', %s)",
            (cv,),
        )
        cur.execute(
            "INSERT INTO okg.subtype_ancestors "
            "(subtype_name, ancestor_name, depth, catalog_version_id) "
            "VALUES ('page','page',0,%s)",
            (cv,),
        )
        cur.execute(
            "INSERT INTO okg.narrowings (parent_subtype, edge_type, child_subtype, catalog_version_id) "
            "VALUES ('page','references','page',%s)",
            (cv,),
        )
    store = ChronosGraphStore.from_connection(conn)
    facts = [
        NodeFact(src, "page", {"text": "src"}, {"id": src}, {"v": 1}),
        NodeFact(d1, "page", {"text": "d1"}, {"id": d1}, {"v": 1}),
        NodeFact(d2, "page", {"text": "d2"}, {"id": d2}, {"v": 1}),
        EdgeFact(
            src,
            d1,
            "references",
            source_record_id={"id": "e0"},
            source_revision={"v": 1},
        ),
        EdgeFact(
            src,
            d2,
            "references",
            source_record_id={"id": "e1"},
            source_revision={"v": 1},
        ),
    ]
    store.apply_facts(facts)
    return store.checkpoint_generation(
        deployment_name="test",
        branch="default",
        catalog_version_id=cv,
        metadata={"correlation_id": f"irseam-{sfx}"},
    )


def test_ir_expand_reads_chronos_neighbors(clean_runtime: str) -> None:
    # Unique focal id: the Chronos default branch accumulates across tests (graph
    # tables aren't reset per-test), so isolate on a per-test node namespace.
    sfx = uuid.uuid4().hex[:8]
    src = f"n:src-{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        gen = _seed(conn, sfx)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        session = store.checkout_generation(gen, branch="default")
        rows = execute_list_neighbors_chronos(
            session,
            node_id=src,
            edge_type=None,
            direction="out",
            limit=100,
        )

    dsts = sorted(r["dst"] for r in rows)
    assert dsts == [f"n:d1-{sfx}", f"n:d2-{sfx}"]  # IR-over-Chronos read works
    assert all(r["edge_type"] == "references" for r in rows)
    assert all(r["src"] == src for r in rows)


def test_lower_program_over_chronos_seam(clean_runtime: str) -> None:
    """The generalized 'survivors go through the IR over Chronos' seam: lower a
    TransformProgram over chronos surfaces + execute via the checkout session."""
    sfx = uuid.uuid4().hex[:8]
    src = f"n:src-{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        gen = _seed(conn, sfx)

    prog = neighbors_program(direction="out", edge_type=None, limit=100)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = lower_program_over_chronos(
            prog, conn, generation_id=gen, extra_params={"focal": src}
        )

    assert sorted(r["dst"] for r in rows) == [f"n:d1-{sfx}", f"n:d2-{sfx}"]


# --------------------------------------------------------------------------- #
# Consistency gate re-pointed at Chronos.
# --------------------------------------------------------------------------- #


def _seed_gate_case(conn, sfx: str, *, include_violation: bool) -> tuple[str, int, int]:
    """Seed a Chronos generation: an admitted page--references-->page edge, and
    optionally a non-admitted page--cites-->page edge (a narrowing violation).
    Returns (generation_id, catalog_version_id, cites_edge_id)."""
    a, b = f"n:a-{sfx}", f"n:b-{sfx}"
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO okg.subtypes (name, catalog_version_id) VALUES ('page', %s)",
            (cv,),
        )
        cur.execute(
            "INSERT INTO okg.subtype_ancestors (subtype_name, ancestor_name, depth, catalog_version_id) "
            "VALUES ('page','page',0,%s)",
            (cv,),
        )
        cur.execute(
            "INSERT INTO okg.narrowings (parent_subtype, edge_type, child_subtype, catalog_version_id) "
            "VALUES ('page','references','page',%s)",
            (cv,),
        )
    # Isolate on a per-test branch: the gate reads ALL edges in the generation's
    # checkpoint, so the shared `default` branch would leak other tests' edges into
    # this generation. A fresh branch off `main` captures only this test's edges.
    branch = f"gate-{sfx}"
    store = ChronosGraphStore.from_connection(conn)
    store.ensure_branch(branch, from_branch="main")
    facts = [
        NodeFact(a, "page", {"text": "a"}, {"id": a}, {"v": 1}),
        NodeFact(b, "page", {"text": "b"}, {"id": b}, {"v": 1}),
        EdgeFact(
            a, b, "references", source_record_id={"id": "ref"}, source_revision={"v": 1}
        ),
    ]
    if include_violation:
        facts.append(
            EdgeFact(
                a, b, "cites", source_record_id={"id": "cit"}, source_revision={"v": 1}
            )
        )
    store.apply_facts(facts, branch=branch, strict_endpoints=True)
    gen = store.checkpoint_generation(
        deployment_name="test",
        branch=branch,
        catalog_version_id=cv,
        metadata={"correlation_id": f"gate-{sfx}"},
    )
    return gen, cv, _edge_id(a, "cites", b)


def test_chronos_gate_blocks_narrowing_violation(clean_runtime: str) -> None:
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        gen, cv, cites_id = _seed_gate_case(conn, sfx, include_violation=True)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ev = TransformConsistencyGate().evaluate(
            conn,
            target_generation_id=gen,
            catalog_version_id=cv,
            deployment_name="test",
        )

    # The seeded catalog carries no at_most_one_target annotation, so the
    # cardinality family is inactive and the runtime basis discloses the
    # skip instead of naming a check that did not run.
    assert ev.details["runtime"] == (
        "chronos_transform_ir+skipped_families:cardinality"
    )
    assert ev.result == "evaluated"
    assert ev.blocks_publish is True
    assert cites_id in ev.details["narrowing_violation_edge_ids"]


def test_chronos_gate_passes_clean(clean_runtime: str) -> None:
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        gen, cv, _ = _seed_gate_case(conn, sfx, include_violation=False)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ev = TransformConsistencyGate().evaluate(
            conn,
            target_generation_id=gen,
            catalog_version_id=cv,
            deployment_name="test",
        )

    assert ev.result == "evaluated"
    assert ev.blocks_publish is False
    assert ev.details["narrowing_violation_edge_ids"] == []
