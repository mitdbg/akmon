"""Publish parity for the batch-projection deletion, measured as DELTAS.

`req.retire-dead-projection-pool.publish-unchanged` requires that removing the
unreachable batch-projection subsystem changed nothing observable. It also
requires the measurement be a DELTA on a freshly seeded fixture rather than an
absolute count on a live deployment: `_run_chronos_extractor` re-selects every
text-bearing node each cycle and dedupes emission against `okg.edge_facts`, so
absolute counts on a deployment with history are path-dependent and two
consecutive cycles need not agree.

What is asserted here:

* a cycle over a seeded fixture reaches `published`, not `blocked`;
* the node delta equals exactly what was seeded, per source;
* a second identical cycle adds nothing — the idempotence the deleted
  `ON CONFLICT DO NOTHING` batch path used to provide, now provided by the
  chronos-native path;
* the removed subsystem is genuinely gone from the runtime object, so a future
  refactor cannot quietly resurrect a second projection path.

Run against a dedicated Postgres; the shared dev database holds published
okg-workspace generations and the fixture guard correctly refuses to truncate
it.
"""
from __future__ import annotations


import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres


def _seed_catalog(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        return cur.fetchone()[0]


def _apply_chronos_nodes(dsn: str, *, source: str, prefix: str, n: int) -> None:
    facts = [
        NodeFact(
            node_id=f"{prefix}:{i}",
            subtype="concept",
            attrs={"text": f"text:{prefix}:{i}"},
            source_record_id={"id": f"{prefix}:{i}"},
            source_revision={"v": 1},
        )
        for i in range(n)
    ]
    store = ChronosGraphStore.connect(dsn)
    try:
        store.apply_facts(facts, source=source)
    finally:
        store.close()


def _counts_by_source(dsn: str, generation_id, sources: tuple[str, ...]) -> dict:
    store = ChronosGraphStore.connect(dsn)
    try:
        checkout = store.checkout_generation(generation_id)
        rows = checkout.query(
            "SELECT source, count(*) AS n FROM okg.graph_nodes "
            " WHERE source = ANY(:sources) GROUP BY source",
            {"sources": list(sources)},
        )
    finally:
        store.close()
    return {row["source"]: int(row["n"]) for row in rows}


def test_seeded_cycle_publishes_the_exact_seeded_delta(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)
    _apply_chronos_nodes(clean_runtime, source="par_A", prefix="n:pa", n=6)
    _apply_chronos_nodes(clean_runtime, source="par_B", prefix="n:pb", n=4)

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    assert out["status"] == "published", out
    assert _counts_by_source(
        clean_runtime, out["generation_id"], ("par_A", "par_B")
    ) == {"par_A": 6, "par_B": 4}


def test_a_second_identical_cycle_adds_nothing(clean_runtime: str) -> None:
    """Idempotence. The deleted batch path had `ON CONFLICT DO NOTHING`; if the
    chronos-native path lacked the equivalent, a re-publish would double the
    rows and the deletion WOULD have changed behavior."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)
    _apply_chronos_nodes(clean_runtime, source="par_C", prefix="n:pc", n=5)

    first = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert first["status"] == "published", first
    after_first = _counts_by_source(
        clean_runtime, first["generation_id"], ("par_C",)
    )

    second = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert second["status"] in {"published", "noop"}, second
    gen = second.get("generation_id") or first["generation_id"]
    after_second = _counts_by_source(clean_runtime, gen, ("par_C",))

    assert after_first == {"par_C": 5}
    assert after_second == after_first, (
        "a second identical cycle changed the node counts; the deletion did "
        "not preserve publish behavior"
    )


def test_the_removed_subsystem_is_absent_from_the_runtime_object(
    clean_runtime: str,
) -> None:
    """Guard against quiet resurrection: a second projection path is exactly
    what this PACT deleted, and it survived a prior migration by being
    unreachable rather than absent."""
    pub = Publisher(clean_runtime, "default")

    for gone in (
        "n_phase_a_workers", "max_node_ids_per_batch",
    ):
        assert not hasattr(pub, gone), f"Publisher grew {gone} back"

    projector = pub._projector
    for gone in (
        "run_pool", "project_batch", "_project_batch_inner",
        "partition", "partition_by_source",
        # schema-canonical-read-path: the live-table projection half. Each of
        # these read or wrote okg.nodes_live / okg.edges_live (or the
        # okg.projectable_* filters over them), all of which are deleted, so a
        # resurrection would be code that cannot run against any deployment.
        "project_retractions", "project_new_edges",
        "project_cascade_retractions", "retire_permanent_strandings",
    ):
        assert not hasattr(projector, gone), f"Projector grew {gone} back"

    # The LIVE surface the publisher actually uses must still be there.
    for live in ("_extractor", "_router", "_resolver_factory"):
        assert hasattr(projector, live), f"live surface {live} disappeared"
